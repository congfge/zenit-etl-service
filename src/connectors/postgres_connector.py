from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
import pandas as pd
from core.config import settings
from utils.logger import logger

class PostgresConnector:
    def __init__(self):
        connection_url = URL.create(
            "postgresql+psycopg2",
            username=settings.postgres_user,
            password=settings.postgres_password,
            host=settings.postgres_host,
            database=settings.postgres_database,
            port=int(settings.postgres_port),
        )

        self.engine = create_engine(
            connection_url,
            pool_pre_ping=True,
            pool_size=5,
            pool_recycle=3600,
        )
        logger.info("[PostgresConnector] DB successfully connected")

    def truncate_table(self, table_name: str) -> None:
        """Trunca la tabla sin borrar su schema ni índices."""
        with self.engine.begin() as conn:
            conn.execute(text(f'TRUNCATE TABLE "{table_name}"'))
        logger.info(f"[PostgresConnector] Tabla {table_name} truncada")

    def copy_append(self, df: pd.DataFrame, table_name: str) -> int:
        """
        Inserta el DataFrame en la tabla usando COPY FROM STDIN (append, sin truncar).
        Llamar a truncate_table() antes del primer copy_append() del ciclo ETL.
        """
        import io

        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=True, na_rep="")
        buffer.seek(0)

        # Especificar columnas explícitamente para que PostgreSQL mapee por nombre,
        # no por posición — evita errores cuando el orden de columnas del DataFrame
        # difiere del orden de creación de la tabla (p.ej. columnas añadidas con ALTER TABLE).
        col_list = ", ".join(f'"{c}"' for c in df.columns)

        raw_conn = self.engine.raw_connection()
        try:
            with raw_conn.cursor() as cur:
                cur.copy_expert(
                    f'COPY "{table_name}" ({col_list}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL \'\')',
                    buffer,
                )
            raw_conn.commit()
        finally:
            raw_conn.close()

        return len(df)

    def load_data(self, df: pd.DataFrame, table_name: str = "creditos_historico_etl"):
        """Trunca + COPY en una sola llamada. Wrapper de truncate_table + copy_append."""
        self.truncate_table(table_name)
        loaded = self.copy_append(df, table_name)
        logger.info(f"[PostgresConnector] {loaded} registros cargados en {table_name}")

    def load_promotores(self, df: pd.DataFrame, table_name: str = "catalogo_promotores"):
        """Trunca la tabla y carga el catálogo de promotores."""
        # Normalizar nombres de columna a minúscula para evitar conflictos de case con PostgreSQL
        df = df.copy()
        df.columns = df.columns.str.lower()

        with self.engine.begin() as conn:
            conn.execute(text(f'TRUNCATE TABLE "{table_name}"'))
            logger.info(f"[PostgresConnector] Tabla {table_name} truncada")

        # Insertar en lotes de 200 filas sin method="multi" para evitar el límite de parámetros
        chunk_size = 200
        for start in range(0, len(df), chunk_size):
            chunk = df.iloc[start:start + chunk_size]
            chunk.to_sql(table_name, self.engine, if_exists="append", index=False)

        logger.info(f"[PostgresConnector] {len(df)} promotores cargados en {table_name}")

    def load_sucursales(self, df: pd.DataFrame, table_name: str = "sucursales") -> int:
        """
        Carga el catálogo de sucursales desde DB2.
        Usa if_exists='replace' para crear la tabla automáticamente si no existe
        y recrearla en cada refresh con el schema actualizado de DB2.
        """
        df = df.copy()
        df.columns = df.columns.str.lower()
        df.to_sql(table_name, self.engine, if_exists="replace", index=False)
        logger.info(f"[PostgresConnector] {len(df)} sucursales cargadas en {table_name}")
        return len(df)

    def sync_sucursales_to_layer(self) -> int:
        """
        Llama fn_sync_sucursales_to_layer() y retorna el número de features sincronizadas.
        La lógica de propiedades, filtros y creación de capa vive en la función SQL
        (database/10_fn_sync_sucursales_to_layer.sql) — editable desde cualquier cliente SQL.
        """
        with self.engine.begin() as conn:
            result = conn.execute(text("SELECT fn_sync_sucursales_to_layer()"))
            count = result.scalar()
        logger.info(f"[PostgresConnector] {count} sucursales sincronizadas en capa 'Sucursales'")
        return count

    def load_sectores_raw(self, df: pd.DataFrame, table_name: str = "sectores_puntos_raw") -> None:
        """TRUNCATE + carga masiva de puntos crudos de sectores (vértices del polígono)."""
        df = df.copy()
        df.columns = df.columns.str.lower()

        # Filtrar solo las columnas que existen en la tabla destino.
        # DB2 puede retornar columnas extra (ej. 'sector') que no están en el schema de PostgreSQL.
        with self.engine.connect() as _conn:
            existing_cols = set(
                pd.read_sql(
                    text(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"),
                    _conn,
                )["column_name"].tolist()
            )
        extra = set(df.columns) - existing_cols
        if extra:
            logger.warning(f"[PostgresConnector] Columnas ignoradas (no existen en {table_name}): {sorted(extra)}")
        df = df[[c for c in df.columns if c in existing_cols]]

        with self.engine.begin() as conn:
            conn.execute(text(f'TRUNCATE TABLE "{table_name}"'))
            logger.info(f"[PostgresConnector] Tabla {table_name} truncada")

        chunk_size = 500
        for start in range(0, len(df), chunk_size):
            df.iloc[start:start + chunk_size].to_sql(
                table_name, self.engine, if_exists="append", index=False
            )

        logger.info(f"[PostgresConnector] {len(df)} puntos cargados en {table_name}")

    def build_sector_polygons(self) -> int:
        """Ejecuta fn_build_sector_polygons() y retorna el número de polígonos generados."""
        with self.engine.connect() as conn:
            conn.execute(text("SET statement_timeout = '300s'"))
            result = conn.execute(text("SELECT fn_build_sector_polygons()"))
            conn.commit()
            count = result.scalar()
        logger.info(f"[PostgresConnector] {count} polígonos de sectores generados")
        return count

    def sync_sectores_to_layer_feature(self, layer_name: str = "Sectores-Promotor") -> int:
        """
        Sincroniza sectores_geometria → layer_feature para la capa indicada.

        Busca la capa por nombre en la tabla `layer`, elimina sus features
        e inserta los polígonos de sectores_geometria con propiedades estandarizadas.
        Actualiza total_features en la tabla `layer`.

        Retorna el número de features insertadas.
        Lanza ValueError si la capa no existe.
        """
        with self.engine.begin() as conn:
            conn.execute(text("SET statement_timeout = '300s'"))
            row = conn.execute(
                text('SELECT id FROM "layer" WHERE name = :name LIMIT 1'),
                {"name": layer_name},
            ).fetchone()

            if not row:
                raise ValueError(f"Capa '{layer_name}' no encontrada en la tabla layer")

            layer_id = row[0]

            conn.execute(
                text('DELETE FROM "layer_feature" WHERE layer_id = :layer_id'),
                {"layer_id": layer_id},
            )

            conn.execute(
                text("""
                    INSERT INTO layer_feature (layer_id, feature_index, geometry, properties)
                    SELECT
                        :layer_id,
                        (ROW_NUMBER() OVER (ORDER BY sg.gforms))::integer - 1,
                        sg.geometria,
                        jsonb_build_object(
                            'GFORMS',        sg.gforms,
                            'PROMOTOR',      sg.promotor,
                            'NOMINA',        sg.nomina,
                            'SUCURSAL',      sg.sucursal,
                            'No_SUC',        sg.cod_sucursal,
                            'REGION',        sg.region,
                            'No_REGIÓN',     sg.cod_region,
                            'DISTRITO',      sg.distrito,
                            'NO_DISTRIT',    sg.cod_distrito,
                            'NOM_SECTOR',    sg.nom_sector,
                            'TIPO_SECTO',    sg.id_tiposec,
                            'TIPO_PROMOTOR', cp.tipo_promotor,
                            'COD_TIPO_PROM', cp.cod_tipo_promomtor,
                            'ESTADO',        cp.estado,
                            'SECTOR_DESC',   cp.sector_descripcion,
                            'DEPTO',         cp.depto,
                            'MUNICIPIO',     cp.municipio
                        )
                    FROM sectores_geometria sg
                    LEFT JOIN catalogo_promotores cp ON cp.cod_promotor = sg.gforms
                    WHERE sg.geometria IS NOT NULL
                """),
                {"layer_id": layer_id},
            )

            count = conn.execute(
                text('SELECT COUNT(*) FROM "layer_feature" WHERE layer_id = :layer_id'),
                {"layer_id": layer_id},
            ).scalar()

            conn.execute(
                text('UPDATE "layer" SET total_features = :count, updated_at = now() WHERE id = :layer_id'),
                {"count": count, "layer_id": layer_id},
            )

        logger.info(f"[PostgresConnector] {count} features sincronizadas en capa '{layer_name}' (id={layer_id})")
        return count

    def get_creditos_by_dates(self, closing_dates: list[str]) -> pd.DataFrame:
        """
        Lee creditos_historico_etl filtrando por las fechas de cierre indicadas.
        closing_dates puede venir en formato DD/MM/YYYY o YYYY-MM-DD;
        se normalizan a YYYY-MM-DD que es como se almacenan en la tabla.
        """
        from datetime import datetime

        iso_dates = []
        for d in closing_dates:
            try:
                iso_dates.append(datetime.strptime(d, "%d/%m/%Y").strftime("%Y-%m-%d"))
            except ValueError:
                iso_dates.append(d)  # ya está en otro formato, se usa tal cual

        params = {f"d{i}": d for i, d in enumerate(iso_dates)}
        placeholders = ", ".join([f":d{i}" for i in range(len(iso_dates))])
        query = text(f"SELECT * FROM creditos_historico_etl WHERE fecha::date IN ({placeholders})")
        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params=params)
        logger.info(f"[PostgresConnector] {len(df)} registros leídos de creditos_historico_etl")
        return df

    def get_sector_geometries(self):
        """
        Lee sectores_geometria y retorna un GeoDataFrame con la columna geometry.
        Requiere que sectores_geometria esté poblada (POST /sectores/refresh).
        """
        import geopandas as gpd
        from shapely import wkb

        query = text("""
            SELECT gforms, promotor, nomina, sucursal, cod_sucursal,
                   region, cod_region, distrito, cod_distrito, nom_sector,
                   id_tiposec, ST_AsEWKB(geometria) AS geom_wkb
            FROM sectores_geometria
            WHERE geometria IS NOT NULL
        """)
        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)

        df["geometry"] = df["geom_wkb"].apply(lambda b: wkb.loads(bytes(b), hex=False) if b else None)
        df = df.drop(columns=["geom_wkb"])
        gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
        logger.info(f"[PostgresConnector] {len(gdf)} sectores cargados desde sectores_geometria")
        return gdf

    def load_clusters(self, df: pd.DataFrame) -> int:
        """
        TRUNCATE clusters_analisis + bulk insert desde el DataFrame.
        La columna `geometria` debe contener geometrías WKT (texto).

        Estrategia de inserción de geometría:
          1. Insertar todas las columnas excepto `geometria` via to_sql
          2. Cargar los WKT en una tabla temporal
          3. UPDATE batch desde la temp table usando ST_GeomFromText

        Todo ocurre en una sola transacción: si cualquier paso falla,
        se hace rollback y clusters_analisis queda intacta.
        """
        df = df.copy()

        # Separar WKT del DataFrame principal
        wkt_df = df[["cluster_key", "geometria"]].dropna(subset=["geometria"])
        df_no_geom = df.drop(columns=["geometria"])

        # Detectar columnas válidas fuera de la transacción principal
        with self.engine.connect() as _conn:
            existing_cols = set(
                pd.read_sql(
                    text("SELECT column_name FROM information_schema.columns WHERE table_name = 'clusters_analisis'"),
                    _conn,
                )["column_name"].tolist()
            ) - {"id", "geometria", "created_at", "updated_at"}
        df_no_geom = df_no_geom[[c for c in df_no_geom.columns if c in existing_cols]]

        chunk_size = 500

        # Todo en una sola transacción: TRUNCATE + INSERTs + geom UPDATE
        # Si cualquier paso falla se hace rollback automático y la tabla queda intacta.
        with self.engine.begin() as conn:
            conn.execute(text('TRUNCATE TABLE clusters_analisis'))
            logger.info("[PostgresConnector] Tabla clusters_analisis truncada")

            for start in range(0, len(df_no_geom), chunk_size):
                df_no_geom.iloc[start:start + chunk_size].to_sql(
                    "clusters_analisis",
                    conn,
                    if_exists="append",
                    index=False,
                    method="multi",
                )

            # Actualizar geometría via tabla temporal (evita incompatibilidad pandas ↔ PostGIS)
            if not wkt_df.empty:
                conn.execute(text(
                    "CREATE TEMP TABLE IF NOT EXISTS _clusters_geom_tmp "
                    "(cluster_key TEXT, wkt TEXT) ON COMMIT DROP"
                ))
                conn.execute(text("TRUNCATE _clusters_geom_tmp"))
                conn.execute(
                    text("INSERT INTO _clusters_geom_tmp (cluster_key, wkt) VALUES (:key, :wkt)"),
                    [{"key": r["cluster_key"], "wkt": r["geometria"]} for _, r in wkt_df.iterrows()],
                )
                conn.execute(text("""
                    UPDATE clusters_analisis ca
                    SET geometria = ST_SetSRID(ST_GeomFromText(t.wkt), 4326)
                    FROM _clusters_geom_tmp t
                    WHERE ca.cluster_key = t.cluster_key
                """))

        logger.info(f"[PostgresConnector] {len(df)} clusters cargados en clusters_analisis")
        return len(df)

    def update_clusters_ai_descriptions(self, batch: list[dict]) -> None:
        """
        Actualiza insights y acciones_recomendadas para un batch de clusters.
        batch: list de dicts con keys: cluster_key, insights, acciones_recomendadas
        """
        if not batch:
            return
        with self.engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE clusters_analisis
                    SET insights = :insights,
                        acciones_recomendadas = :acciones_recomendadas,
                        updated_at = NOW()
                    WHERE cluster_key = :cluster_key
                """),
                batch,
            )

    def get_clusters_without_ai_descriptions(self) -> "pd.DataFrame":
        """Retorna todas las filas de clusters_analisis donde insights es NULL o vacío."""
        import pandas as pd
        with self.engine.connect() as conn:
            df = pd.read_sql(
                text("SELECT * FROM clusters_analisis WHERE insights IS NULL OR insights = ''"),
                conn,
            )
        logger.info(f"[PostgresConnector] {len(df)} clusters sin descripción IA")
        return df

    def sync_clusters_to_layer(self, layer_name: str) -> int:
        """
        Llama fn_sync_clusters_to_layer(layer_name) y retorna el número de features sincronizadas.
        Lanza ValueError si la capa no existe o está inactiva.
        """
        with self.engine.begin() as conn:
            result = conn.execute(
                text("SELECT fn_sync_clusters_to_layer(:layer_name)"),
                {"layer_name": layer_name},
            )
            count = result.scalar()
        logger.info(f"[PostgresConnector] {count} clusters sincronizados en capa '{layer_name}'")
        return count

    def sync_clientes_to_layers(self) -> dict:
        """
        Llama fn_sync_clientes_to_layers() y sincroniza creditos_historico_etl
        con las capas ClientesSanos, ClientesCastigo, ClientesMora, ExClientes
        y ClientesColocacion (vacía).

        Retorna un dict {layer_name: features_synced}.
        """
        with self.engine.begin() as conn:
            conn.execute(text("SET LOCAL statement_timeout = '600s'"))
            result = conn.execute(text("SELECT * FROM fn_sync_clientes_to_layers()"))
            rows = result.fetchall()
        summary = {row[0]: int(row[1]) for row in rows}
        logger.info(f"[PostgresConnector] sync_clientes_to_layers: {summary}")
        return summary

    def close(self):
        self.engine.dispose()
