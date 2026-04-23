import uuid

from fastapi import APIRouter, Depends, status

from api.dependencies import verify_internal_token, get_job_tracker
from api.models.response_model import ApiResponse
from api.models.job_response_model import JobResponse
from celery_config.tasks import run_clustering_task, resume_ai_task
from connectors.postgres_connector import PostgresConnector
from core.config import settings
from core.enums import JobStatus
from services.redis_job_tracker import RedisJobTracker
from utils.date_utils import get_last_3_closing_dates
from utils.logger import logger

clustering_router = APIRouter(prefix="/clustering")


@clustering_router.post(
    "/run",
    response_model=ApiResponse,
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(verify_internal_token)],
)
def run_clustering(tracker: RedisJobTracker = Depends(get_job_tracker)):
    """
    Dispara el pipeline de clustering DBSCAN sobre creditos_historico_etl.

    Flujo (ejecutado en background por Celery):
      1. Lee creditos_historico_etl para las últimas 3 fechas de cierre
      2. Lee sectores_geometria para asignación espacial
      3. Asigna créditos a sectores (spatial join + fallback por COD_PROMOTOR)
      4. Aplica DBSCAN por sector (eps=100 m, min_samples=8) solo a registros con coordenadas
      5. Calcula métricas por cluster para los 3 meses
      6. Consolida tendencias, volatilidad y retención
      7. Genera geometrías circulares (100 m de radio) por centroide
      8. Carga resultados en clusters_analisis (TRUNCATE + insert)
      9. Sincroniza clusters_analisis → layer_feature (capa Clusters-Creditos)

    Prerrequisitos:
      - creditos_historico_etl poblada (POST /refresh)
      - sectores_geometria poblada (POST /sectores/refresh)
      - Capa 'Clusters-Creditos' creada en la tabla layer
    """
    closing_dates = get_last_3_closing_dates()
    job_id = str(uuid.uuid4())

    tracker.create_job(job_id)
    tracker.update_status(job_id, JobStatus.QUEUED, message="Clustering encolado")

    run_clustering_task.delay(job_id, closing_dates)

    logger.info(f"[clustering] Job {job_id} encolado | fechas: {closing_dates}")

    return ApiResponse(
        success=True,
        data=JobResponse(
            job_id=job_id,
            status=JobStatus.QUEUED,
            message=f"Clustering encolado para fechas: {', '.join(closing_dates)}",
        ).model_dump(),
        message="Clustering iniciado en background",
    )


@clustering_router.post(
    "/resume-ai",
    response_model=ApiResponse,
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(verify_internal_token)],
)
def resume_clustering_ai(tracker: RedisJobTracker = Depends(get_job_tracker)):
    """
    Retoma la generación de descripciones IA para clusters con insights vacío o NULL.
    Útil si la app crasheó durante la fase generating_ai de un clustering previo.
    """
    job_id = str(uuid.uuid4())
    tracker.create_job(job_id)
    tracker.update_status(job_id, JobStatus.QUEUED, message="Resume IA encolado")
    resume_ai_task.delay(job_id)
    logger.info(f"[clustering] Resume AI job {job_id} encolado")
    return ApiResponse(
        success=True,
        data=JobResponse(
            job_id=job_id,
            status=JobStatus.QUEUED,
            message="Retomando descripciones IA para clusters pendientes",
        ).model_dump(),
        message="Resume IA iniciado en background",
    )


@clustering_router.post(
    "/sync-layer",
    response_model=ApiResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(verify_internal_token)],
)
def sync_clusters_layer():
    """
    Sincroniza clusters_analisis → layer_feature para la capa Clusters-Creditos.
    Operación síncrona (llama fn_sync_clusters_to_layer directamente).
    Útil para refrescar el mapa sin re-ejecutar el pipeline completo.
    """
    postgres = PostgresConnector()
    synced = postgres.sync_clusters_to_layer(settings.cluster_layer_name)
    logger.info(f"[clustering] sync-layer: {synced} features sincronizadas")
    return ApiResponse(
        success=True,
        data={"features_sincronizadas": synced, "layer": settings.cluster_layer_name},
        message=f"{synced} features sincronizadas en capa '{settings.cluster_layer_name}'",
    )
