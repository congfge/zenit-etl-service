import uuid

from fastapi import APIRouter, Depends, status

from api.dependencies import verify_internal_token, get_job_tracker
from api.models.response_model import ApiResponse
from api.models.job_response_model import JobResponse
from celery_config.tasks import refresh_sectores_task
from core.enums import JobStatus
from services.redis_job_tracker import RedisJobTracker
from utils.logger import logger

sectores_router = APIRouter(prefix='/sectores')


@sectores_router.post(
    '/refresh',
    response_model=ApiResponse,
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(verify_internal_token)],
)
def refresh_sectores(tracker: RedisJobTracker = Depends(get_job_tracker)):
    """
    Encola el proceso de regeneración de sectores en segundo plano (Celery).

    Flujo (ejecutado en background):
      1. Extrae vértices desde DB2 lbgeoubi.sectores (timeout configurable, fallback a CSV)
      2. TRUNCATE + carga masiva → sectores_puntos_raw
      3. fn_build_sector_polygons() → sectores_geometria (un polígono por GFORMS)
      4. sync_sectores_to_layer_feature() → layer_feature (capa Sectores-Promotor)

    Monitorear progreso via:
      GET  /api/v1/jobs/{job_id}
      WS   /api/v1/ws/progress/{job_id}
    """
    job_id = str(uuid.uuid4())
    tracker.create_job(job_id)
    tracker.update_status(job_id, JobStatus.QUEUED, message="Regeneración de sectores encolada")
    refresh_sectores_task.delay(job_id)
    logger.info(f"[sectores] Job {job_id} encolado")

    return ApiResponse(
        success=True,
        data=JobResponse(
            job_id=job_id,
            status=JobStatus.QUEUED,
            message="Regeneración de sectores iniciada en background",
        ).model_dump(),
        message="Sectores encolado",
    )
