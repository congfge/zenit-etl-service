"""
AIDescriptionService
====================
Genera descripciones de IA para cada cluster usando GPT-4.
Porta la lógica de `generar_descripcion_ia` / `agregar_analisis_ia`
del notebook Clustering/zenit_clustering.ipynb al servicio.
"""

from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable

import pandas as pd

from utils.logger import logger


def _tendencia_label(slope: float, flag_volatil: bool) -> str:
    """Convierte la pendiente numérica de mora a etiqueta legible."""
    if flag_volatil:
        return "VOLATIL"
    if slope > 0.001:
        return "CRECIENTE"
    if slope < -0.001:
        return "DECRECIENTE"
    return "ESTABLE"


def _safe(row: dict, col: str, default=0):
    v = row.get(col, default)
    return default if (v is None or (isinstance(v, float) and v != v)) else v


class AIDescriptionService:
    def __init__(self, api_key: str):
        from openai import OpenAI
        self._client = OpenAI(api_key=api_key)

    # ------------------------------------------------------------------
    # Generación para un único cluster
    # ------------------------------------------------------------------

    def generar_descripcion(self, row: dict) -> dict:
        """
        Genera insights y acciones para un cluster.
        Retorna {"insights": str, "acciones_recomendadas": str}.
        En caso de error retorna strings vacíos (nunca bloquea el pipeline).
        """
        categoria = _safe(row, "categoria", "SANO")

        objetivo_estrategico = self._objetivo_por_categoria(categoria)
        contexto_mora = self._contexto_mora(row, categoria)
        prompt = self._build_prompt(row, categoria, objetivo_estrategico, contexto_mora)

        try:
            response = self._client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "Eres asesor estratégico de microfinanzas en Guatemala. "
                            "Respondes únicamente en formato JSON."
                        ),
                    },
                    {"role": "user", "content": prompt},
                ],
                max_tokens=250,
                temperature=0.7,
            )

            texto = response.choices[0].message.content.strip()
            try:
                resultado = json.loads(texto)
                return {
                    "insights": resultado.get("insights", ""),
                    "acciones_recomendadas": resultado.get("acciones_recomendadas", ""),
                }
            except json.JSONDecodeError:
                return {"insights": texto, "acciones_recomendadas": ""}

        except Exception as exc:
            logger.warning(f"[AIDescriptionService] Error al generar descripción: {exc}")
            return {"insights": "", "acciones_recomendadas": ""}

    # ------------------------------------------------------------------
    # Enriquecimiento de DataFrame completo
    # ------------------------------------------------------------------

    def enriquecer_dataframe(
        self,
        df: pd.DataFrame,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> pd.DataFrame:
        """
        Aplica generar_descripcion a cada fila y agrega las columnas
        'insights' y 'acciones_recomendadas' al DataFrame.

        Args:
            progress_callback: función opcional (done, total) → None.
                               Se invoca cada 5 clusters para publicar progreso.
        """
        df = df.copy()
        df["insights"] = ""
        df["acciones_recomendadas"] = ""

        total = len(df)
        logger.info(f"[AIDescriptionService] Generando descripciones para {total} clusters…")

        errores = 0
        for i, (idx, row) in enumerate(df.iterrows(), start=1):
            resultado = self.generar_descripcion(row.to_dict())
            df.at[idx, "insights"] = resultado["insights"]
            df.at[idx, "acciones_recomendadas"] = resultado["acciones_recomendadas"]

            if not resultado["insights"]:
                errores += 1

            if i % 10 == 0:
                logger.info(f"[AIDescriptionService] {i}/{total} clusters procesados")
                if progress_callback:
                    progress_callback(i, total)

        # Publicar progreso final
        if progress_callback:
            progress_callback(total, total)

        completados = total - errores
        logger.info(
            f"[AIDescriptionService] Completado — {completados}/{total} clusters con descripción IA"
        )
        if errores > 0:
            tasa_error = errores / total
            log_fn = logger.error if tasa_error > 0.5 else logger.warning
            log_fn(
                f"[AIDescriptionService] {errores}/{total} clusters sin descripción "
                f"({tasa_error:.0%}) — revisa conectividad con OpenAI API"
            )
        return df

    def enriquecer_con_db_update(
        self,
        df: pd.DataFrame,
        batch_update_fn: Callable[[list[dict]], None],
        batch_size: int = 10,
        max_workers: int = 20,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> None:
        """
        Genera descripciones en paralelo (max_workers llamadas simultáneas a OpenAI)
        y las escribe a la DB en batches de batch_size.

        Con max_workers=20 y gpt-4o-mini el throughput es ~20x mayor que secuencial.
        """
        total = len(df)
        logger.info(
            f"[AIDescriptionService] Generando descripciones para {total} clusters "
            f"(workers={max_workers}, batch_size={batch_size})…"
        )

        rows = [(row["cluster_key"], row.to_dict()) for _, row in df.iterrows()]

        # Estado compartido entre threads (acceso protegido con lock)
        lock = threading.Lock()
        pending_batch: list[dict] = []
        completed_count = 0
        errores = 0

        def _process(cluster_key: str, row_dict: dict) -> dict:
            resultado = self.generar_descripcion(row_dict)
            return {
                "cluster_key": cluster_key,
                "insights": resultado["insights"],
                "acciones_recomendadas": resultado["acciones_recomendadas"],
            }

        def _flush_if_ready(force: bool = False) -> None:
            nonlocal pending_batch
            if len(pending_batch) >= batch_size or (force and pending_batch):
                to_flush = pending_batch[:batch_size] if not force else pending_batch[:]
                pending_batch = pending_batch[batch_size:] if not force else []
                batch_update_fn(to_flush)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_process, cluster_key, row_dict): cluster_key
                for cluster_key, row_dict in rows
            }

            for future in as_completed(futures):
                try:
                    result = future.result()
                except Exception as exc:
                    cluster_key = futures[future]
                    logger.warning(f"[AIDescriptionService] Error en cluster {cluster_key}: {exc}")
                    result = {"cluster_key": cluster_key, "insights": "", "acciones_recomendadas": ""}

                with lock:
                    pending_batch.append(result)
                    if not result["insights"]:
                        errores += 1
                    completed_count += 1
                    _flush_if_ready()
                    done = completed_count

                if done % 50 == 0:
                    logger.info(f"[AIDescriptionService] {done}/{total} clusters procesados")
                    if progress_callback:
                        progress_callback(done, total)

        # Flush del batch final
        with lock:
            _flush_if_ready(force=True)

        if progress_callback:
            progress_callback(total, total)

        completados = total - errores
        logger.info(
            f"[AIDescriptionService] Completado — {completados}/{total} clusters con descripción IA"
        )
        if errores > 0:
            tasa_error = errores / total
            log_fn = logger.error if tasa_error > 0.5 else logger.warning
            log_fn(
                f"[AIDescriptionService] {errores}/{total} sin descripción ({tasa_error:.0%}) "
                f"— revisa conectividad con OpenAI API"
            )

    # ------------------------------------------------------------------
    # Helpers internos
    # ------------------------------------------------------------------

    def _objetivo_por_categoria(self, categoria: str) -> str:
        if categoria == "MORA":
            return """
ENFOQUE ESTRATÉGICO - REDUCCIÓN DE MOROSIDAD:

Diagnosticar severidad del problema de morosidad basado en evolución de 3 meses.
Identificar si la situación está mejorando, empeorando o es volátil.

Niveles de mora por días de atraso:
- 1-30 días (MORA1_30D): Cartera contaminada, no alarmante, recuperable
- 31-60 días (MORA31_60D): Cartera contaminada de prioridad regular
- 61-90 días (MORA61_90D): Recuperación URGENTE - evitar que pase de 90 días

Acciones según tendencia:
- Si CRECIENTE: visitas presenciales urgentes, planes de pago inmediatos
- Si DECRECIENTE: reforzar estrategias actuales, mantener seguimiento
- Si VOLATIL: investigar causas de inestabilidad, segmentar clientes problemáticos
- Si alta tasa de abandono: recuperar clientes antes de pérdida total

Objetivo: recuperar al menos 1 cuota para bajar nivel de contaminación.
CRÍTICO: evitar que créditos pasen de 90 días al cierre de mes (desaparece de sucursal y afecta KPIs).
"""
        if categoria == "CASTIGOS":
            return """
ENFOQUE ESTRATÉGICO - MITIGACIÓN DE CASTIGOS:

Evaluar gravedad de castigos acumulados en últimos 3 meses.
Determinar si hay tendencia creciente de pérdida de clientes.

Acciones:
- Si CRECIENTE: seguimiento preventivo intensivo
- Si alta rotación: investigar causas de deserción masiva
- Si baja retención: fortalecer relación con clientes activos antes de perderlos
- Identificar patrones de riesgo temprano para prevenir futuros castigos
"""
        if categoria == "COLOCACION":
            return """
ENFOQUE ESTRATÉGICO - APROVECHAMIENTO DE CRECIMIENTO:

Identificar potencial de crecimiento basado en colocaciones de últimos 3 meses.
Evaluar calidad del crecimiento (retención + baja mora).

Acciones:
- Si CRECIENTE: intensificar captación de nuevos clientes
- Si alta retención: aprovechar base sólida para referencias y expansión
- Si baja mora: promover productos adicionales a clientes nuevos confiables
- Identificar estrategias de cross-selling en zona de alto crecimiento
"""
        # SANO y cualquier otro
        return """
ENFOQUE ESTRATÉGICO - MANTENIMIENTO Y PROTECCIÓN:

Mantener salud de cartera y prevenir contaminación futura.
Aprovechar estabilidad para crecimiento controlado.

Acciones:
- Seguimiento preventivo para detectar señales tempranas de riesgo
- Oportunidades de productos adicionales para clientes confiables
- Fortalecer relación para retención a largo plazo
- Proteger y crecer base sólida de ingresos
"""

    def _contexto_mora(self, row: dict, categoria: str) -> str:
        if categoria != "MORA":
            return ""
        mora_1_30  = int(_safe(row, "mora1_30_m3",  0))
        mora_31_60 = int(_safe(row, "mora31_60_m3", 0))
        mora_61_90 = int(_safe(row, "mora61_90_m3", 0))
        return f"""
DESGLOSE DE MORA POR DÍAS DE ATRASO:
- 1-30 días (contaminada recuperable): {mora_1_30} clientes
- 31-60 días (prioridad regular): {mora_31_60} clientes
- 61-90 días (recuperación URGENTE): {mora_61_90} clientes
"""

    def _build_prompt(
        self,
        row: dict,
        categoria: str,
        objetivo_estrategico: str,
        contexto_mora: str,
    ) -> str:
        promotor        = _safe(row, "promotor", "N/A")
        nivel_impacto   = _safe(row, "nivel_impacto", "N/A")
        count_m3        = int(_safe(row, "count_m3", 0))
        capital_m3      = _safe(row, "capital_total_m3", 0)
        tasa_mora_m3    = _safe(row, "tasa_mora_m3", 0) * 100
        mora_total_m3   = _safe(row, "mora_total_m3", 0)

        clientes_morosos  = (
            int(_safe(row, "mora1_30_m3",  0))
            + int(_safe(row, "mora31_60_m3", 0))
            + int(_safe(row, "mora61_90_m3", 0))
        )
        clientes_castigo  = int(_safe(row, "castigado_m3",  0))
        clientes_coloc    = int(_safe(row, "colocacion_m3", 0))
        clientes_sanos    = int(_safe(row, "sana_m3",       0))

        tasa_mora_m1 = _safe(row, "tasa_mora_m1", 0) * 100
        tasa_mora_m2 = _safe(row, "tasa_mora_m2", 0) * 100
        capital_m1   = _safe(row, "capital_total_m1", 0)
        capital_m2   = _safe(row, "capital_total_m2", 0)

        slope_mora    = _safe(row, "tendencia_mora",    0.0)
        flag_volatil  = bool(_safe(row, "flag_volatil", False))
        tendencia_lbl = _tendencia_label(slope_mora, flag_volatil)

        tasa_cambio  = _safe(row, "tasa_cambio_mora_mensual", 0) * 100
        cambio_max   = _safe(row, "cambio_maximo_mora",       0) * 100
        retencion    = _safe(row, "tasa_retencion", 0) * 100
        churn        = _safe(row, "tasa_churn",     0) * 100

        castigos_total   = (
            int(_safe(row, "castigado_m1", 0))
            + int(_safe(row, "castigado_m2", 0))
            + int(_safe(row, "castigado_m3", 0))
        )
        colocaciones_total = (
            int(_safe(row, "colocacion_m1", 0))
            + int(_safe(row, "colocacion_m2", 0))
            + int(_safe(row, "colocacion_m3", 0))
        )

        slope_capital = _safe(row, "tendencia_capital", 0.0)
        tendencia_cap = (
            "CRECIENTE" if slope_capital > 0 else
            "DECRECIENTE" if slope_capital < 0 else
            "ESTABLE"
        )

        return f"""
Eres asesor estratégico de microfinanzas. Analiza el sector del promotor {promotor}.

SITUACIÓN ACTUAL (MES3):
- Tipo: {categoria}
- Nivel Impacto: {nivel_impacto}
- Clientes: {count_m3}
- Capital: Q{capital_m3:,.0f}
- Mora: {tasa_mora_m3:.1f}%
- Morosos: {clientes_morosos}
- Castigados: {clientes_castigo}
- Colocación: {clientes_coloc}
- Sanos: {clientes_sanos}
{contexto_mora}
EVOLUCIÓN 3 MESES:
- Mora: {tasa_mora_m1:.1f}% → {tasa_mora_m2:.1f}% → {tasa_mora_m3:.1f}%
- Tendencia mora: {tendencia_lbl} ({tasa_cambio:+.1f}% mensual)
- Capital: Q{capital_m1:,.0f} → Q{capital_m2:,.0f} → Q{capital_m3:,.0f}
- Tendencia capital: {tendencia_cap}
- Castigos 3m: {castigos_total}
- Colocaciones 3m: {colocaciones_total}
- Volatilidad: {'ALTA' if flag_volatil else 'BAJA'} (máx cambio: {cambio_max:.1f}%)
- Retención: {retencion:.1f}%
- Churn: {churn:.1f}%

{objetivo_estrategico}

FORMATO DE RESPUESTA (MUY IMPORTANTE):
Responde en formato JSON con exactamente esta estructura:
{{
  "insights": "Análisis de situación con evolución temporal (máximo 2 oraciones)",
  "acciones_recomendadas": "Recomendación específica y accionable (máximo 2 oraciones)"
}}

Habla directo al promotor en segunda persona. No uses términos técnicos como "cluster", "volatilidad", "churn".
"""
