import asyncio
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError

import pandas as pd

from celery_config.celery_app import celery_app
from services.etl_service import EtlService
from services.redis_job_tracker import RedisJobTracker
from services.clustering_service import ClusteringService
from connectors.sqlserver_connector import SQLServerConnector
from connectors.postgres_connector import PostgresConnector
from transformers.data_cleaner import DataCleaner
from core.config import settings
from core.enums import JobStatus
from utils.logger import logger
from utils.date_utils import get_last_3_closing_dates


# Cache global para reutilizar servicios (lazy initialization)
_etl_service = None
_redis_tracker = None
_clustering_service = None


def _get_etl_service() -> EtlService:
    global _etl_service
    if _etl_service is None:
        sqlserver = SQLServerConnector()
        postgres = PostgresConnector()
        _etl_service = EtlService(
            sqlserver=sqlserver,
            postgres=postgres,
            cleaner=DataCleaner(),
            chunk_size=settings.chunk_size
        )
    return _etl_service


def _get_redis_tracker() -> RedisJobTracker:
    global _redis_tracker
    if _redis_tracker is None:
        _redis_tracker = RedisJobTracker()
    return _redis_tracker


def _get_clustering_service() -> ClusteringService:
    global _clustering_service
    if _clustering_service is None:
        _clustering_service = ClusteringService(postgres=PostgresConnector())
    return _clustering_service


@celery_app.task(
    bind=True,
    name="refresh_data",
    max_retries=3,
    default_retry_delay=60,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_jitter=True,
)
def refresh_data_task(self, job_id: str):
    """
    Celery task que ejecuta el refresh completo desde SQL Server.
    Extrae las últimas 3 fechas de cierre y carga todos los registros en PostgreSQL.
    """
    closing_dates = get_last_3_closing_dates()
    logger.info(
        f"[CELERY TASK {job_id}] Iniciando refresh (intento {self.request.retries + 1}) "
        f"| fechas: {closing_dates}"
    )

    redis_tracker = _get_redis_tracker()
    etl_service = _get_etl_service()

    try:
        redis_tracker.update_status(
            job_id,
            JobStatus.EXTRACTING,
            message=f"Extrayendo fechas: {', '.join(closing_dates)} (intento {self.request.retries + 1})"
        )

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            metrics = loop.run_until_complete(
                etl_service.run_full_refresh(job_id=job_id, closing_dates=closing_dates)
            )
        finally:
            loop.close()

        redis_tracker.update_status(
            job_id,
            JobStatus.COMPLETED,
            message=f"Procesados {metrics.records_loaded:,} registros — iniciando clustering automático",
            total_records=metrics.records_extracted,
            processed_records=metrics.records_loaded,
            records_dropped=metrics.records_dropped,
            duration_seconds=metrics.duration_seconds
        )

        logger.info(f"[CELERY TASK {job_id}] Completado: {metrics.records_loaded:,} registros")

        # Encolar clustering automáticamente al terminar el ETL
        clustering_job_id = str(uuid.uuid4())
        redis_tracker.create_job(clustering_job_id)
        redis_tracker.update_status(clustering_job_id, JobStatus.QUEUED, message=f"Encolado por ETL job {job_id}")
        run_clustering_task.delay(clustering_job_id, closing_dates)
        logger.info(f"[CELERY TASK {job_id}] Clustering encolado automáticamente → job {clustering_job_id}")

        return {
            "job_id": job_id,
            "status": "COMPLETED",
            "records_processed": metrics.records_loaded,
            "duration_seconds": metrics.duration_seconds,
            "clustering_job_id": clustering_job_id,
        }

    except Exception as exc:
        logger.error(f"[CELERY TASK {job_id}] Error: {str(exc)}", exc_info=True)

        if self.request.retries >= self.max_retries:
            redis_tracker.update_status(
                job_id,
                JobStatus.FAILED,
                message=f"Falló después de {self.max_retries + 1} intentos",
                error=str(exc)
            )
        else:
            redis_tracker.update_status(
                job_id,
                JobStatus.RETRYING,
                message=f"Reintentando... (intento {self.request.retries + 2}/{self.max_retries + 1})",
                error=str(exc)
            )

        raise


@celery_app.task(
    bind=True,
    name="run_clustering",
    max_retries=2,
)
def run_clustering_task(self, job_id: str, closing_dates: list[str]):
    """
    Celery task que ejecuta el pipeline de clustering DBSCAN sobre
    creditos_historico_etl y sincroniza los clusters con layer_feature.
    """
    logger.info(
        f"[CELERY CLUSTERING {job_id}] Iniciando (intento {self.request.retries + 1}) "
        f"| fechas: {closing_dates}"
    )

    # redis_tracker puede quedar None si el crash es en _get_redis_tracker() mismo
    redis_tracker = None
    try:
        redis_tracker = _get_redis_tracker()
        clustering_service = _get_clustering_service()

        redis_tracker.update_status(
            job_id,
            JobStatus.CLUSTERING,
            message=f"Ejecutando clustering DBSCAN (intento {self.request.retries + 1})"
        )

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            def _status_cb(status: str, message: str) -> None:
                redis_tracker.update_status(job_id, status, message=message)

            metrics = loop.run_until_complete(
                clustering_service.run_clustering(
                    job_id=job_id,
                    closing_dates=closing_dates,
                    status_callback=_status_cb,
                )
            )
        finally:
            loop.close()

        redis_tracker.update_status(
            job_id,
            JobStatus.COMPLETED,
            message=(
                f"{metrics.clusters_generados} clusters generados — "
                f"{metrics.features_sincronizadas} features en capa"
            ),
            duration_seconds=metrics.duration_seconds,
        )

        logger.info(
            f"[CELERY CLUSTERING {job_id}] Completado: "
            f"{metrics.clusters_generados} clusters, {metrics.features_sincronizadas} features"
        )

        return {
            "job_id": job_id,
            "status": "COMPLETED",
            "clusters_generados": metrics.clusters_generados,
            "features_sincronizadas": metrics.features_sincronizadas,
            "duration_seconds": metrics.duration_seconds,
        }

    except Exception as exc:
        logger.error(f"[CELERY CLUSTERING {job_id}] Error: {str(exc)}", exc_info=True)

        if self.request.retries < self.max_retries:
            # Retry manual con backoff explícito: 60s → 120s
            countdown = 60 * (2 ** self.request.retries)
            if redis_tracker:
                redis_tracker.update_status(
                    job_id,
                    JobStatus.RETRYING,
                    message=(
                        f"Reintentando clustering en {countdown}s… "
                        f"(intento {self.request.retries + 2}/{self.max_retries + 1})"
                    ),
                    error=str(exc),
                )
            raise self.retry(exc=exc, countdown=countdown)
        else:
            if redis_tracker:
                redis_tracker.update_status(
                    job_id,
                    JobStatus.FAILED,
                    message=f"Clustering falló después de {self.max_retries + 1} intentos",
                    error=str(exc),
                )
            raise


@celery_app.task(
    bind=True,
    name="resume_ai",
    max_retries=1,
)
def resume_ai_task(self, job_id: str):
    """
    Retoma la generación de descripciones IA para clusters con insights vacío o NULL.
    Útil si la app crasheó durante la fase generating_ai.
    """
    logger.info(f"[CELERY RESUME_AI {job_id}] Iniciando")
    redis_tracker = None
    try:
        redis_tracker = _get_redis_tracker()
        clustering_service = _get_clustering_service()

        redis_tracker.update_status(
            job_id,
            JobStatus.GENERATING_AI,
            message="Retomando generación de descripciones IA…",
        )

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            def _cb(status: str, message: str) -> None:
                redis_tracker.update_status(job_id, status, message=message)

            total = loop.run_until_complete(
                clustering_service.resume_ai_descriptions(job_id=job_id, status_callback=_cb)
            )
        finally:
            loop.close()

        redis_tracker.update_status(
            job_id,
            JobStatus.COMPLETED,
            message=f"{total} clusters actualizados con descripciones IA",
        )
        logger.info(f"[CELERY RESUME_AI {job_id}] Completado: {total} clusters")
        return {"job_id": job_id, "status": "COMPLETED", "clusters_actualizados": total}

    except Exception as exc:
        logger.error(f"[CELERY RESUME_AI {job_id}] Error: {str(exc)}", exc_info=True)
        if redis_tracker:
            redis_tracker.update_status(
                job_id,
                JobStatus.FAILED,
                message=f"Resume IA falló: {str(exc)}",
                error=str(exc),
            )
        raise


# ─────────────────────────────────────────────────────────────
# Catalog refresh tasks (sectores, promotores, sucursales, clientes)
# ─────────────────────────────────────────────────────────────

def _db2_load_sectores() -> pd.DataFrame:
    """Carga sectores desde DB2. Lanza excepción si falla (el caller aplica fallback a CSV)."""
    from connectors.db2_connector import DB2Connector
    db2 = DB2Connector()
    df = db2.extract_sectores()
    # conn.close() puede colgar en JT400 — daemon thread para no bloquear
    threading.Thread(target=db2.close, daemon=True, name="db2-sect-close-task").start()
    return df


def _db2_load_sucursales() -> pd.DataFrame:
    """Carga sucursales desde DB2 lbgeocli."""
    from connectors.db2_connector import DB2Connector
    db2 = DB2Connector(database=settings.db2_suc_database)
    df = db2.extract_sucursales()
    threading.Thread(target=db2.close, daemon=True, name="db2-suc-close-task").start()
    return df


@celery_app.task(bind=True, name="refresh_sectores", max_retries=2)
def refresh_sectores_task(self, job_id: str):
    """
    Extrae vértices de sectores desde DB2 (fallback CSV), genera polígonos
    y sincroniza la capa Sectores-Promotor en layer_feature.
    """
    logger.info(f"[CELERY SECTORES {job_id}] Iniciando (intento {self.request.retries + 1})")
    redis_tracker = None
    postgres = None
    try:
        redis_tracker = _get_redis_tracker()
        redis_tracker.update_status(job_id, JobStatus.EXTRACTING,
                                    message="Extrayendo sectores desde DB2...")

        data_source = "db2"
        df: pd.DataFrame | None = None
        executor = ThreadPoolExecutor(max_workers=1)
        try:
            future = executor.submit(_db2_load_sectores)
            try:
                df = future.result(timeout=settings.db2_timeout_sec)
                logger.info(f"[CELERY SECTORES {job_id}] {len(df):,} filas desde DB2")
            except (FuturesTimeoutError, Exception) as e:
                logger.warning(f"[CELERY SECTORES {job_id}] DB2 falló ({e!r}), usando CSV fallback")
                path = settings.sectores_csv_path
                if not path or not os.path.exists(path):
                    raise FileNotFoundError(f"CSV no disponible en: {path}")
                df = pd.read_csv(path, sep=';', low_memory=False, encoding='latin-1')
                df.columns = df.columns.str.upper()
                data_source = "csv"
        finally:
            executor.shutdown(wait=False)

        redis_tracker.update_status(job_id, JobStatus.LOADING,
                                    message=f"Generando polígonos desde {len(df):,} puntos ({data_source})...")
        postgres = PostgresConnector()
        postgres.load_sectores_raw(df)
        count = postgres.build_sector_polygons()
        synced = postgres.sync_sectores_to_layer_feature()

        suffix = f" (datos desde CSV — DB2 no respondió en {settings.db2_timeout_sec}s)" if data_source == "csv" else ""
        msg = f"{count} polígonos generados — {synced} features sincronizadas en Sectores-Promotor{suffix}"
        redis_tracker.update_status(job_id, JobStatus.COMPLETED, message=msg)
        logger.info(f"[CELERY SECTORES {job_id}] {msg}")
        return {"job_id": job_id, "status": "COMPLETED", "polygons": count, "features_synced": synced}

    except Exception as exc:
        logger.error(f"[CELERY SECTORES {job_id}] Error: {exc}", exc_info=True)
        if redis_tracker:
            if self.request.retries < self.max_retries:
                countdown = 60 * (2 ** self.request.retries)
                redis_tracker.update_status(job_id, JobStatus.RETRYING,
                                            message=f"Reintentando en {countdown}s…", error=str(exc))
                raise self.retry(exc=exc, countdown=countdown)
            redis_tracker.update_status(job_id, JobStatus.FAILED,
                                        message=f"Falló después de {self.max_retries + 1} intentos",
                                        error=str(exc))
        raise
    finally:
        if postgres:
            postgres.close()


@celery_app.task(bind=True, name="refresh_promotores", max_retries=2)
def refresh_promotores_task(self, job_id: str):
    """Extrae el catálogo de promotores desde SQL Server y lo carga en catalogo_promotores."""
    logger.info(f"[CELERY PROMOTORES {job_id}] Iniciando (intento {self.request.retries + 1})")
    redis_tracker = None
    sqlserver = None
    postgres = None
    try:
        redis_tracker = _get_redis_tracker()
        redis_tracker.update_status(job_id, JobStatus.EXTRACTING,
                                    message="Extrayendo catálogo de promotores desde SQL Server...")
        sqlserver = SQLServerConnector()
        df = sqlserver.extract_promotores()

        redis_tracker.update_status(job_id, JobStatus.LOADING,
                                    message=f"Cargando {len(df):,} promotores en PostgreSQL...")
        postgres = PostgresConnector()
        postgres.load_promotores(df)

        msg = f"{len(df):,} promotores actualizados"
        redis_tracker.update_status(job_id, JobStatus.COMPLETED, message=msg)
        logger.info(f"[CELERY PROMOTORES {job_id}] {msg}")
        return {"job_id": job_id, "status": "COMPLETED", "records": len(df)}

    except Exception as exc:
        logger.error(f"[CELERY PROMOTORES {job_id}] Error: {exc}", exc_info=True)
        if redis_tracker:
            if self.request.retries < self.max_retries:
                countdown = 60 * (2 ** self.request.retries)
                redis_tracker.update_status(job_id, JobStatus.RETRYING,
                                            message=f"Reintentando en {countdown}s…", error=str(exc))
                raise self.retry(exc=exc, countdown=countdown)
            redis_tracker.update_status(job_id, JobStatus.FAILED,
                                        message=f"Falló después de {self.max_retries + 1} intentos",
                                        error=str(exc))
        raise
    finally:
        if sqlserver:
            sqlserver.close()
        if postgres:
            postgres.close()


@celery_app.task(bind=True, name="refresh_sucursales", max_retries=2)
def refresh_sucursales_task(self, job_id: str):
    """
    Extrae agencias (tipo=1) desde DB2 lbgeocli, las carga en la tabla `sucursales`
    y sincroniza la capa de puntos 'Sucursales' en layer_feature.
    """
    logger.info(f"[CELERY SUCURSALES {job_id}] Iniciando (intento {self.request.retries + 1})")
    redis_tracker = None
    postgres = None
    try:
        redis_tracker = _get_redis_tracker()
        redis_tracker.update_status(job_id, JobStatus.EXTRACTING,
                                    message="Extrayendo sucursales desde DB2 (lbgeocli)...")
        df = _db2_load_sucursales()

        redis_tracker.update_status(job_id, JobStatus.LOADING,
                                    message=f"Cargando {len(df):,} sucursales en PostgreSQL...")
        postgres = PostgresConnector()
        count = postgres.load_sucursales(df)

        redis_tracker.update_status(job_id, JobStatus.LOADING,
                                    message="Sincronizando capa de puntos Sucursales en layer_feature...")
        synced = postgres.sync_sucursales_to_layer()

        msg = f"{count:,} sucursales cargadas — {synced:,} features sincronizadas en capa Sucursales"
        redis_tracker.update_status(job_id, JobStatus.COMPLETED, message=msg)
        logger.info(f"[CELERY SUCURSALES {job_id}] {msg}")
        return {"job_id": job_id, "status": "COMPLETED", "records": count, "features_synced": synced}

    except Exception as exc:
        logger.error(f"[CELERY SUCURSALES {job_id}] Error: {exc}", exc_info=True)
        if redis_tracker:
            if self.request.retries < self.max_retries:
                countdown = 60 * (2 ** self.request.retries)
                redis_tracker.update_status(job_id, JobStatus.RETRYING,
                                            message=f"Reintentando en {countdown}s…", error=str(exc))
                raise self.retry(exc=exc, countdown=countdown)
            redis_tracker.update_status(job_id, JobStatus.FAILED,
                                        message=f"Falló después de {self.max_retries + 1} intentos",
                                        error=str(exc))
        raise
    finally:
        if postgres:
            postgres.close()


@celery_app.task(bind=True, name="refresh_clientes", max_retries=2)
def refresh_clientes_task(self, job_id: str):
    """Sincroniza capas de clientes desde creditos_historico_etl via fn_sync_clientes_to_layers()."""
    logger.info(f"[CELERY CLIENTES {job_id}] Iniciando (intento {self.request.retries + 1})")
    redis_tracker = None
    postgres = None
    try:
        redis_tracker = _get_redis_tracker()
        redis_tracker.update_status(job_id, JobStatus.LOADING,
                                    message="Sincronizando capas de clientes...")
        postgres = PostgresConnector()
        summary = postgres.sync_clientes_to_layers()

        total = sum(summary.values())
        detail = ", ".join(f"{k}: {v:,}" for k, v in summary.items())
        msg = f"{total:,} features sincronizadas — {detail}"
        redis_tracker.update_status(job_id, JobStatus.COMPLETED, message=msg)
        logger.info(f"[CELERY CLIENTES {job_id}] {msg}")
        return {"job_id": job_id, "status": "COMPLETED", "total_features": total, "summary": summary}

    except Exception as exc:
        logger.error(f"[CELERY CLIENTES {job_id}] Error: {exc}", exc_info=True)
        if redis_tracker:
            if self.request.retries < self.max_retries:
                countdown = 60 * (2 ** self.request.retries)
                redis_tracker.update_status(job_id, JobStatus.RETRYING,
                                            message=f"Reintentando en {countdown}s…", error=str(exc))
                raise self.retry(exc=exc, countdown=countdown)
            redis_tracker.update_status(job_id, JobStatus.FAILED,
                                        message=f"Falló después de {self.max_retries + 1} intentos",
                                        error=str(exc))
        raise
    finally:
        if postgres:
            postgres.close()
