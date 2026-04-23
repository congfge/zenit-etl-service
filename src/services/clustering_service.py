"""
ClusteringService
=================
Replica la lógica del notebook Clustering/zenit_clustering.ipynb dentro del servicio Python.

Pipeline:
  1. Leer creditos_historico_etl (últimas 3 fechas de cierre)
  2. Leer sectores_geometria como GeoDataFrame
  3. Asignar cada crédito a su sector (2 rutas)
  4. Aplicar DBSCAN por sector (solo registros con coordenadas)
  5. Calcular métricas por cluster para cada uno de los 3 meses
  6. Consolidar métricas: tendencias, volatilidad, retención
  7. Generar geometrías circulares (100 m de radio) por centroide
  8. Clasificar clusters (MORA / CASTIGOS / COLOCACION / SANO)
  9. TRUNCATE + carga en clusters_analisis
  10. Sincronizar con layer_feature via fn_sync_clusters_to_layer()
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

# numpy and pandas are always available (used by the ETL pipeline too)
import numpy as np
import pandas as pd

# geopandas, shapely, sklearn are imported lazily inside methods
# so the module loads even if these packages aren't installed yet.

from connectors.postgres_connector import PostgresConnector
from core.config import settings
from services.ai_description_service import AIDescriptionService
from utils.logger import logger


# ---------------------------------------------------------------------------
# Parámetros DBSCAN (replicados del notebook)
# ---------------------------------------------------------------------------
DBSCAN_EPS_KM = 0.10          # 100 metros
EARTH_RADIUS_KM = 6371.0
DBSCAN_MIN_SAMPLES = 8

# Radio del círculo buffer en grados (≈ 100 m en latitud de Guatemala)
# 100 m / (111 km/grado) ≈ 0.0009°
CLUSTER_BUFFER_DEGREES = 0.0009

# Umbrales de clasificación (réplica del notebook)
MORA_ALTA_THRESHOLD = 0.15    # tasa_mora_m3 > 15% → ALTO
MORA_MEDIA_THRESHOLD = 0.05   # tasa_mora_m3 > 5% → MEDIO

# Categorías de KPI (valores normalizados en DataCleaner)
KPI_MORA_VALUES = {"MORA1_30D", "Mora31_60D", "Mora61_90D"}
KPI_CASTIGADO = "Castigado"
KPI_COLOCACION = "Colocación"
KPI_SANA = "Sana"


@dataclass
class ClusteringMetrics:
    clusters_generados: int = 0
    sectores_procesados: int = 0
    features_sincronizadas: int = 0
    duration_seconds: float = 0.0
    fecha_proceso: str = field(default_factory=lambda: date.today().isoformat())


class ClusteringService:
    def __init__(self, postgres: PostgresConnector):
        self.postgres = postgres
        self.ai_service: AIDescriptionService | None = None
        if settings.openai_api_key:
            try:
                self.ai_service = AIDescriptionService(settings.openai_api_key)
            except ImportError as e:
                logger.warning(f"[ClusteringService] openai no instalado ({e}) — descripciones IA deshabilitadas")
        else:
            logger.warning("[ClusteringService] OPENAI_API_KEY no configurada — descripciones IA deshabilitadas")

    # ------------------------------------------------------------------
    # Punto de entrada principal
    # ------------------------------------------------------------------

    async def run_clustering(self, job_id: str, closing_dates: list[str], status_callback=None) -> ClusteringMetrics:
        import time
        start = time.time()
        metrics = ClusteringMetrics()

        logger.info(f"[CLUSTERING {job_id}] Iniciando — fechas: {closing_dates}")

        # 1. Cargar datos de créditos
        df_raw = self.postgres.get_creditos_by_dates(closing_dates)
        if df_raw.empty:
            logger.warning(f"[CLUSTERING {job_id}] Sin registros en creditos_historico_etl para las fechas dadas")
            return metrics

        # 2. Cargar geometrías de sectores
        sectores_gdf = self.postgres.get_sector_geometries()
        if sectores_gdf.empty:
            logger.warning(f"[CLUSTERING {job_id}] Sin sectores en sectores_geometria — ejecuta POST /sectores/refresh primero")
            return metrics

        # 3. Asignar créditos a sectores
        df = self._assign_to_sectors(df_raw, sectores_gdf)

        # 4. DBSCAN por sector
        df = self._apply_dbscan(df)

        # 5. Métricas por mes
        metricas_mes = self._calculate_monthly_metrics(df, closing_dates)

        # 6. Consolidar 3 meses
        df_consolidado = self._consolidate_temporal_metrics(metricas_mes)

        # 7. Geometrías circulares
        df_consolidado = self._generate_circular_geometries(df_consolidado)

        # 8. Categorizar
        df_consolidado = self._categorize_clusters(df_consolidado)

        # 8.5 — Columnas IA vacías (requeridas por _prepare_for_db)
        df_consolidado["insights"] = ""
        df_consolidado["acciones_recomendadas"] = ""

        # 9. Preparar y cargar en DB (mapa disponible de inmediato)
        df_final = self._prepare_for_db(df_consolidado)
        self.postgres.load_clusters(df_final)
        metrics.clusters_generados = len(df_final)

        # 10. Sincronizar con layer_feature
        synced = self.postgres.sync_clusters_to_layer(settings.cluster_layer_name)
        metrics.features_sincronizadas = synced

        # 11. Generar descripciones IA y actualizar DB por batches
        total_clusters = len(df_final)
        if self.ai_service is not None:
            if status_callback:
                status_callback(
                    "generating_ai",
                    f"Generando descripciones IA: 0/{total_clusters} clusters…",
                )

            def _ai_progress(done: int, total: int) -> None:
                if status_callback:
                    status_callback(
                        "generating_ai",
                        f"Generando descripciones IA: {done}/{total} clusters…",
                    )

            self.ai_service.enriquecer_con_db_update(
                df_final,
                batch_update_fn=self.postgres.update_clusters_ai_descriptions,
                batch_size=10,
                progress_callback=_ai_progress,
            )

        metrics.duration_seconds = round(time.time() - start, 1)
        logger.info(
            f"[CLUSTERING {job_id}] Completado — "
            f"{metrics.clusters_generados} clusters | {synced} features | {metrics.duration_seconds}s"
        )
        return metrics

    # ------------------------------------------------------------------
    # Paso 3: Asignación de créditos a sectores
    # ------------------------------------------------------------------

    def _assign_to_sectors(self, df: pd.DataFrame, sectores_gdf) -> pd.DataFrame:
        """
        Ruta 1 (tiene_coordenadas=True):  spatial join punto-en-polígono.
        Ruta 2 (tiene_coordenadas=False): match COD_PROMOTOR == gforms.
        Clientes sin asignación conservan gforms=NaN y se excluyen del clustering.
        """
        import geopandas as gpd

        df = df.copy()
        df["gforms_asignado"]       = np.nan
        df["promotor_asignado"]     = None
        df["sucursal_asignada"]     = None
        df["region_asignada"]       = None
        df["distrito_asignado"]     = None
        df["cod_sucursal_asignado"] = None
        df["cod_region_asignado"]   = None
        df["cod_distrito_asignado"] = None
        df["nom_sector_asignado"]   = None

        # Excluir gforms=0 — es un marcador de "ZONA ROJA" que cubre todo Guatemala.
        # Si se incluye, el sjoin asigna gforms=0 a todos los créditos del país
        # y los clusters resultantes son descartados por el filtro gforms > 0.
        sectores_validos = sectores_gdf[sectores_gdf["gforms"] > 0].copy()

        # Columna de lookup sectores: gforms → propiedades
        sectores_lookup = sectores_validos.set_index("gforms")[
            ["promotor", "sucursal", "region", "distrito",
             "cod_sucursal", "cod_region", "cod_distrito", "nom_sector"]
        ].to_dict("index")

        # ── Ruta 1: spatial join ──────────────────────────────────────────
        mask_coords = df["tiene_coordenadas"] == True
        if mask_coords.any():
            gdf_clientes = gpd.GeoDataFrame(
                df[mask_coords].copy(),
                geometry=gpd.points_from_xy(
                    df.loc[mask_coords, "longitud"],
                    df.loc[mask_coords, "latitud"],
                ),
                crs="EPSG:4326",
            )
            # Renombrar columnas del GDF de sectores para evitar colisión con
            # columnas homónimas del DataFrame de créditos (sucursal, promotor, etc.)
            sectores_right = sectores_validos[
                ["gforms", "promotor", "sucursal", "region", "distrito",
                 "cod_sucursal", "cod_region", "cod_distrito", "nom_sector", "geometry"]
            ].rename(columns={
                "gforms":       "_sec_gforms",
                "promotor":     "_sec_promotor",
                "sucursal":     "_sec_sucursal",
                "region":       "_sec_region",
                "distrito":     "_sec_distrito",
                "cod_sucursal": "_sec_cod_sucursal",
                "cod_region":   "_sec_cod_region",
                "cod_distrito": "_sec_cod_distrito",
                "nom_sector":   "_sec_nom_sector",
            })

            joined = gpd.sjoin(
                gdf_clientes,
                sectores_right,
                how="left",
                predicate="within",
            )
            # En caso de múltiples matches, tomar el primero
            joined = joined[~joined.index.duplicated(keep="first")]
            df.loc[mask_coords, "gforms_asignado"]       = joined["_sec_gforms"].values
            df.loc[mask_coords, "promotor_asignado"]     = joined["_sec_promotor"].values
            df.loc[mask_coords, "sucursal_asignada"]     = joined["_sec_sucursal"].values
            df.loc[mask_coords, "region_asignada"]       = joined["_sec_region"].values
            df.loc[mask_coords, "distrito_asignado"]     = joined["_sec_distrito"].values
            df.loc[mask_coords, "cod_sucursal_asignado"] = joined["_sec_cod_sucursal"].values
            df.loc[mask_coords, "cod_region_asignado"]   = joined["_sec_cod_region"].values
            df.loc[mask_coords, "cod_distrito_asignado"] = joined["_sec_cod_distrito"].values
            df.loc[mask_coords, "nom_sector_asignado"]   = joined["_sec_nom_sector"].values

        # ── Ruta 2: match por COD_PROMOTOR ───────────────────────────────
        mask_no_coords = ~mask_coords
        if mask_no_coords.any() and "COD_PROMOTOR" in df.columns:
            cod_col = df.loc[mask_no_coords, "COD_PROMOTOR"]
            df.loc[mask_no_coords, "gforms_asignado"] = cod_col.map(
                lambda c: c if c in sectores_lookup else np.nan
            )
            for idx in df[mask_no_coords].index:
                gf = df.at[idx, "gforms_asignado"]
                if not pd.isna(gf) and gf in sectores_lookup:
                    info = sectores_lookup[gf]
                    df.at[idx, "promotor_asignado"]     = info["promotor"]
                    df.at[idx, "sucursal_asignada"]     = info["sucursal"]
                    df.at[idx, "region_asignada"]       = info["region"]
                    df.at[idx, "distrito_asignado"]     = info["distrito"]
                    df.at[idx, "cod_sucursal_asignado"] = info.get("cod_sucursal")
                    df.at[idx, "cod_region_asignado"]   = info.get("cod_region")
                    df.at[idx, "cod_distrito_asignado"] = info.get("cod_distrito")
                    df.at[idx, "nom_sector_asignado"]   = info.get("nom_sector")

        asignados = df["gforms_asignado"].notna().sum()
        logger.info(f"[ClusteringService] {asignados:,}/{len(df):,} créditos asignados a sector")
        return df

    # ------------------------------------------------------------------
    # Paso 4: DBSCAN por sector
    # ------------------------------------------------------------------

    def _apply_dbscan(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica DBSCAN por cada sector (gforms_asignado) sobre los créditos
        con coordenadas válidas. Los registros sin coordenadas o sin sector
        asignado reciben cluster_id=-1.

        Para replicar el comportamiento del notebook (que corre sobre un snapshot
        de un solo mes), se deduplica por (dpi, gforms_asignado) antes de DBSCAN.
        Esto evita que los 3 meses de datos tripliquen la densidad de puntos y
        generen ~3x más clusters de los esperados (~7,000 como en el notebook).
        Los cluster_ids se propagan de vuelta a todos los meses vía clave (dpi, gforms).
        """
        from sklearn.cluster import DBSCAN

        df = df.copy()
        df["cluster_id"] = -1

        mask_valid = (df["tiene_coordenadas"] == True) & df["gforms_asignado"].notna()
        df_valid = df[mask_valid].copy()

        # Un cliente por sector (el registro más reciente): evita triple conteo mensual
        df_dedup = (
            df_valid.sort_values("fecha")
            .drop_duplicates(subset=["dpi", "gforms_asignado"], keep="last")
            .copy()
        )
        df_dedup["_cluster_label"] = -1

        eps_rad = DBSCAN_EPS_KM / EARTH_RADIUS_KM
        total_clusters = 0

        for _gforms, grupo in df_dedup.groupby("gforms_asignado"):
            if len(grupo) < DBSCAN_MIN_SAMPLES:
                continue

            coords = np.radians(grupo[["latitud", "longitud"]].values)
            labels = DBSCAN(
                eps=eps_rad,
                min_samples=DBSCAN_MIN_SAMPLES,
                metric="haversine",
                n_jobs=-1,
            ).fit_predict(coords)

            df_dedup.loc[grupo.index, "_cluster_label"] = labels
            total_clusters += len(set(labels)) - (1 if -1 in labels else 0)

        # Propagar cluster_id a todos los meses vía (dpi, gforms_asignado)
        cluster_keys = df_dedup[["dpi", "gforms_asignado", "_cluster_label"]]
        merged = df_valid[["dpi", "gforms_asignado"]].merge(
            cluster_keys, on=["dpi", "gforms_asignado"], how="left"
        )
        df.loc[mask_valid, "cluster_id"] = merged["_cluster_label"].fillna(-1).astype(int).values

        logger.info(f"[ClusteringService] {total_clusters} clusters DBSCAN generados")
        return df

    # ------------------------------------------------------------------
    # Paso 5: Métricas por mes
    # ------------------------------------------------------------------

    def _calculate_monthly_metrics(
        self,
        df: pd.DataFrame,
        closing_dates: list[str],
    ) -> dict[str, pd.DataFrame]:
        """
        Calcula métricas agregadas por (gforms_asignado, cluster_id) para cada mes.
        Solo procesa clusters con cluster_id >= 0.
        """
        # Normalizar la columna fecha una sola vez (evita datetime64 → "2026-01-31 00:00:00")
        fecha_str_series = pd.to_datetime(df["fecha"]).dt.strftime("%Y-%m-%d")

        metricas = {}
        for i, fecha in enumerate(closing_dates, start=1):
            mes_key = f"m{i}"
            from datetime import datetime as _dt
            try:
                fecha_iso = _dt.strptime(fecha, "%d/%m/%Y").strftime("%Y-%m-%d")
            except ValueError:
                fecha_iso = fecha  # ya en ISO
            df_mes = df[
                (fecha_str_series == fecha_iso) & (df["cluster_id"] >= 0)
            ].copy()

            if df_mes.empty:
                metricas[mes_key] = pd.DataFrame()
                continue

            grp = df_mes.groupby(["gforms_asignado", "cluster_id"], as_index=False)
            agg = grp.agg(
                count=("dpi", "count"),
                mora_total=("mora", "sum"),
                capital_total=("capital_concedido", "sum"),
                centroid_lat=("latitud", "mean"),
                centroid_lon=("longitud", "mean"),
                promotor=("promotor_asignado", "first"),
                sucursal=("sucursal_asignada", "first"),
                region=("region_asignada", "first"),
                distrito=("distrito_asignado", "first"),
                cod_sucursal=("cod_sucursal_asignado", "first"),
                cod_region=("cod_region_asignado", "first"),
                cod_distrito=("cod_distrito_asignado", "first"),
                nom_sector=("nom_sector_asignado", "first"),
            )

            # KPI distribution
            for kpi_val, col_name in [
                (KPI_SANA, "sana"),
                (KPI_COLOCACION, "colocacion"),
                ("MORA1_30D", "mora1_30"),
                ("Mora31_60D", "mora31_60"),
                ("Mora61_90D", "mora61_90"),
                (KPI_CASTIGADO, "castigado"),
            ]:
                kpi_counts = (
                    df_mes[df_mes["KPI"] == kpi_val]
                    .groupby(["gforms_asignado", "cluster_id"])["dpi"]
                    .count()
                    .reset_index(name=col_name)
                )
                agg = agg.merge(kpi_counts, on=["gforms_asignado", "cluster_id"], how="left")
                agg[col_name] = agg[col_name].fillna(0).astype(int)

            # Tasa de mora
            agg["tasa_mora"] = np.where(
                agg["capital_total"] > 0,
                agg["mora_total"] / agg["capital_total"],
                0.0,
            )

            metricas[mes_key] = agg
            logger.info(f"[ClusteringService] Mes {mes_key} ({fecha}): {len(agg)} clusters con métricas")

        return metricas

    # ------------------------------------------------------------------
    # Paso 6: Consolidación temporal
    # ------------------------------------------------------------------

    def _consolidate_temporal_metrics(
        self,
        metricas: dict[str, pd.DataFrame],
    ) -> pd.DataFrame:
        """
        Une las métricas de los 3 meses con sufijos _m1/_m2/_m3.
        Calcula tendencias, volatilidad, retención y churn.
        """
        # Columnas de metadatos del sector — se sufijan igual que las métricas para que
        # el merge outer no produzca colisiones (_x/_y) y el backfill funcione correctamente.
        META_COLS = (
            "promotor", "sucursal", "region", "distrito",
            "cod_sucursal", "cod_region", "cod_distrito", "nom_sector",
            "centroid_lat", "centroid_lon",
        )

        dfs = []
        for mes_key, df_mes in metricas.items():
            if df_mes.empty:
                continue
            # Sufijar TODAS las columnas excepto las claves del merge
            df_renamed = df_mes.rename(
                columns={
                    c: f"{c}_{mes_key}"
                    for c in df_mes.columns
                    if c not in ("gforms_asignado", "cluster_id")
                }
            )
            dfs.append(df_renamed)

        if not dfs:
            return pd.DataFrame()

        # Merge progresivo sobre gforms_asignado + cluster_id (outer para conservar todos los clusters)
        base = dfs[0]
        for other in dfs[1:]:
            base = base.merge(other, on=["gforms_asignado", "cluster_id"], how="outer")

        df = base.copy()

        # Consolidar columnas de metadatos: tomar primer valor no-nulo entre meses (m1→m2→m3)
        for col in META_COLS:
            candidates = [f"{col}_{mk}" for mk in ("m1", "m2", "m3") if f"{col}_{mk}" in df.columns]
            if candidates:
                df[col] = df[candidates].bfill(axis=1).iloc[:, 0]
                df = df.drop(columns=candidates, errors="ignore")

        # Tendencia de mora (pendiente sobre 3 puntos)
        mora_cols = ["tasa_mora_m1", "tasa_mora_m2", "tasa_mora_m3"]
        available = [c for c in mora_cols if c in df.columns]
        if len(available) >= 2:
            def _slope(row: pd.Series) -> float:
                vals = [row.get(c, np.nan) for c in available]
                valid = [(i, v) for i, v in enumerate(vals) if not pd.isna(v)]
                if len(valid) < 2:
                    return 0.0
                xs = np.array([v[0] for v in valid], dtype=float)
                ys = np.array([v[1] for v in valid], dtype=float)
                return float(np.polyfit(xs, ys, 1)[0])

            df["tendencia_mora"] = df.apply(_slope, axis=1)
        else:
            df["tendencia_mora"] = 0.0

        # Capital trend
        cap_cols = ["capital_total_m1", "capital_total_m2", "capital_total_m3"]
        available_cap = [c for c in cap_cols if c in df.columns]
        if len(available_cap) >= 2:
            def _slope_cap(row: pd.Series) -> float:
                vals = [row.get(c, np.nan) for c in available_cap]
                valid = [(i, v) for i, v in enumerate(vals) if not pd.isna(v)]
                if len(valid) < 2:
                    return 0.0
                xs = np.array([v[0] for v in valid], dtype=float)
                ys = np.array([v[1] for v in valid], dtype=float)
                return float(np.polyfit(xs, ys, 1)[0])

            df["tendencia_capital"] = df.apply(_slope_cap, axis=1)
        else:
            df["tendencia_capital"] = 0.0

        # Volatilidad
        if len(available) >= 2:
            mora_matrix = df[available].values.astype(float)
            diffs = np.abs(np.diff(mora_matrix, axis=1))
            df["cambio_maximo_mora"] = np.nanmax(diffs, axis=1)
            df["tasa_cambio_mora_mensual"] = np.nanmean(diffs, axis=1)
            df["flag_volatil"] = df["cambio_maximo_mora"] > 0.10
        else:
            df["cambio_maximo_mora"] = 0.0
            df["tasa_cambio_mora_mensual"] = 0.0
            df["flag_volatil"] = False

        # Retención y churn (clientes en m3 que estuvieron en m1 o m2)
        # Aproximación con ratio de conteos (datos exactos de DPIs no disponibles aquí)
        count_cols = [c for c in ("count_m1", "count_m2", "count_m3") if c in df.columns]
        if "count_m3" in df.columns and len(count_cols) >= 2:
            prev_cols = [c for c in count_cols if c != "count_m3"]
            avg_prev = df[prev_cols].mean(axis=1).replace(0, np.nan)
            df["tasa_retencion"] = (df["count_m3"] / avg_prev).clip(upper=1.0).fillna(0.0)
            df["tasa_churn"] = 1.0 - df["tasa_retencion"]
        else:
            df["tasa_retencion"] = 0.0
            df["tasa_churn"] = 0.0

        return df

    # ------------------------------------------------------------------
    # Paso 7: Geometrías circulares
    # ------------------------------------------------------------------

    def _generate_circular_geometries(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Genera un polígono circular (buffer) alrededor del centroide de cada cluster.
        El buffer se calcula en grados aproximados (≈ 100 m en Guatemala).
        """
        from shapely.geometry import Point

        df = df.copy()

        def _make_circle(row: pd.Series):
            lat = row.get("centroid_lat")
            lon = row.get("centroid_lon")
            if pd.isna(lat) or pd.isna(lon):
                return None
            return Point(lon, lat).buffer(CLUSTER_BUFFER_DEGREES)

        df["geometry_obj"] = df.apply(_make_circle, axis=1)
        # WKT para almacenar como texto en PostgreSQL (load_clusters convertirá a GEOMETRY)
        df["geometria"] = df["geometry_obj"].apply(
            lambda g: g.wkt if g is not None else None
        )
        df = df.drop(columns=["geometry_obj"])
        return df

    # ------------------------------------------------------------------
    # Paso 8: Clasificación de clusters
    # ------------------------------------------------------------------

    def _categorize_clusters(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clasifica clusters replicando la lógica del notebook zenit_clustering.ipynb.
        Prioridad: MORA > CASTIGOS > COLOCACION > SANO.

        MORA:      ≥1 moroso  Y  (tasa_mora > 10%  O  mora_total > Q10,000)
        CASTIGOS:  ≥3 castigados  Y  (>3 castigados  O  tasa_castigo > 10%)
        COLOCACION:≥5 colocacion  Y  (>5 colocacion  O  tasa_colocacion > 15%)
        SANO:      resto
        """
        df = df.copy()

        def _categoria(row: pd.Series) -> str:
            mora_clientes = (
                (row.get("mora1_30_m3") or 0)
                + (row.get("mora31_60_m3") or 0)
                + (row.get("mora61_90_m3") or 0)
            )
            tasa_mora  = row.get("tasa_mora_m3")  or 0
            mora_total = row.get("mora_total_m3") or 0

            if mora_clientes >= 1 and (tasa_mora > 0.10 or mora_total > 10_000):
                return "MORA"

            count      = max(row.get("count_m3") or 1, 1)
            castigados = row.get("castigado_m3") or 0
            if castigados >= 3 and (castigados > 3 or castigados / count > 0.10):
                return "CASTIGOS"

            colocacion = row.get("colocacion_m3") or 0
            if colocacion >= 5 and (colocacion > 5 or colocacion / count > 0.15):
                return "COLOCACION"

            return "SANO"

        def _nivel(row: pd.Series) -> str:
            categoria  = row.get("categoria") or "SANO"
            tasa_mora  = row.get("tasa_mora_m3") or 0
            count      = max(row.get("count_m3") or 1, 1)
            castigados = row.get("castigado_m3") or 0
            colocacion = row.get("colocacion_m3") or 0

            if categoria == "MORA":
                if tasa_mora > 0.30: return "CRITICO"
                if tasa_mora > 0.20: return "ALTO"
                return "MEDIO"

            if categoria == "CASTIGOS":
                if castigados > 20: return "CRITICO"
                if castigados > 10: return "ALTO"
                return "MEDIO"

            if categoria == "COLOCACION":
                tasa_col = colocacion / count
                if tasa_col > 0.15: return "OPORTUNIDAD ALTA"
                if tasa_col > 0.08: return "OPORTUNIDAD"
                return "CRECIMIENTO"

            return "BAJO"

        df["categoria"] = df.apply(_categoria, axis=1)
        df["nivel_impacto"] = df.apply(_nivel, axis=1)
        return df

    # ------------------------------------------------------------------
    # Paso 8.5: Descripciones IA
    # ------------------------------------------------------------------

    def _generate_ai_descriptions(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enriquece cada cluster con insights y acciones_recomendadas via GPT-4.
        Si no hay api_key configurada, agrega columnas vacías sin error.
        (Mantenido como referencia — run_clustering usa enriquecer_con_db_update)
        """
        if self.ai_service is None:
            df = df.copy()
            df["insights"] = ""
            df["acciones_recomendadas"] = ""
            return df
        return self.ai_service.enriquecer_dataframe(df)

    async def resume_ai_descriptions(self, job_id: str, status_callback=None) -> int:
        """
        Genera descripciones IA solo para clusters donde insights es NULL o vacío.
        Útil para retomar tras un crash durante la fase generating_ai.
        Retorna el número de clusters procesados.
        """
        df = self.postgres.get_clusters_without_ai_descriptions()
        if df.empty:
            logger.info(f"[ClusteringService] resume_ai {job_id}: no hay clusters pendientes")
            return 0

        total = len(df)

        if self.ai_service is None:
            logger.warning("[ClusteringService] resume_ai: OPENAI_API_KEY no configurada — nada que hacer")
            return 0

        if status_callback:
            status_callback("generating_ai", f"Retomando descripciones IA: 0/{total} clusters…")

        def _ai_progress(done: int, t: int) -> None:
            if status_callback:
                status_callback("generating_ai", f"Retomando descripciones IA: {done}/{t} clusters…")

        self.ai_service.enriquecer_con_db_update(
            df,
            batch_update_fn=self.postgres.update_clusters_ai_descriptions,
            batch_size=10,
            progress_callback=_ai_progress,
        )
        return total

    # ------------------------------------------------------------------
    # Paso 9: Preparar DataFrame para inserción en DB
    # ------------------------------------------------------------------

    def _prepare_for_db(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Renombra columnas y selecciona solo las columnas de clusters_analisis.
        """
        if df.empty:
            raise ValueError(
                "_prepare_for_db recibió un DataFrame vacío — "
                "_consolidate_temporal_metrics no produjo datos. "
                "Verifica que las closing_dates coincidan con fechas almacenadas en creditos_historico_etl."
            )
        df = df.copy()
        df = df.rename(columns={"gforms_asignado": "gforms"})

        # Diagnóstico: loguear cuántas filas tienen gforms nulo/cero antes de filtrar
        df["gforms"] = pd.to_numeric(df["gforms"], errors="coerce")
        _total_pre = len(df)
        _nan_count  = int(df["gforms"].isna().sum())
        _zero_count = int((df["gforms"] == 0).sum())
        if _nan_count or _zero_count:
            logger.warning(
                f"[ClusteringService] _prepare_for_db: {_total_pre} filas totales — "
                f"{_nan_count} con gforms=NaN, {_zero_count} con gforms=0 → se descartan"
            )
        df = df[df["gforms"].notna() & (df["gforms"] > 0)].copy()
        logger.info(f"[ClusteringService] _prepare_for_db: {_total_pre} → {len(df)} clusters con gforms válido")

        if df.empty:
            raise ValueError("_prepare_for_db: todos los registros tienen gforms nulo/cero tras el filtro.")

        # cluster_key único: "{gforms}_{cluster_id}"
        df["cluster_key"] = (
            df["gforms"].astype(int).astype(str)
            + "_"
            + df["cluster_id"].astype(int).astype(str)
        )

        df["fecha_proceso"] = date.today().isoformat()
        df["radio_metros"] = CLUSTER_BUFFER_DEGREES * 111_000  # metros aproximados

        # Conteo de clientes por categoría en el mes más reciente (m3)
        # Consolida los KPI individuales en totales por tipo para facilitar análisis
        def _safe_col(col: str) -> pd.Series:
            return df[col].fillna(0) if col in df.columns else pd.Series(0, index=df.index)

        df["clientes_mora"]       = (_safe_col("mora1_30_m3") + _safe_col("mora31_60_m3") + _safe_col("mora61_90_m3")).astype(int)
        df["clientes_sanos"]      = _safe_col("sana_m3").astype(int)
        df["clientes_colocacion"] = _safe_col("colocacion_m3").astype(int)
        df["clientes_castigo"]    = _safe_col("castigado_m3").astype(int)

        # Columnas esperadas por la tabla (en orden)
        expected_cols = [
            "cluster_key", "gforms", "cluster_id",
            "promotor", "sucursal", "region", "distrito",
            "cod_sucursal", "cod_region", "cod_distrito", "nom_sector",
            "centroid_lat", "centroid_lon", "radio_metros", "geometria",
            "categoria", "nivel_impacto",
            "clientes_mora", "clientes_sanos", "clientes_colocacion", "clientes_castigo",
            "count_m1", "mora_total_m1", "capital_total_m1", "tasa_mora_m1",
            "count_m2", "mora_total_m2", "capital_total_m2", "tasa_mora_m2",
            "count_m3", "mora_total_m3", "capital_total_m3", "tasa_mora_m3",
            "sana_m3", "colocacion_m3", "mora1_30_m3", "mora31_60_m3", "mora61_90_m3", "castigado_m3",
            "tendencia_mora", "tendencia_capital", "tasa_cambio_mora_mensual",
            "flag_volatil", "cambio_maximo_mora",
            "tasa_retencion", "tasa_churn",
            "insights", "acciones_recomendadas",
            "fecha_proceso",
        ]

        for col in expected_cols:
            if col not in df.columns:
                df[col] = None

        df = df[expected_cols]

        # Redondear todos los floats a máximo 2 decimales antes de insertar
        float_cols = df.select_dtypes(include=["float64", "float32"]).columns
        df[float_cols] = df[float_cols].round(2)

        return df
