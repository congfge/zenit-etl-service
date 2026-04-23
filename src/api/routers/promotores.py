import uuid

from fastapi import APIRouter, Depends, status

from api.dependencies import verify_internal_token, get_job_tracker
from api.models.response_model import ApiResponse
from api.models.job_response_model import JobResponse
from celery_config.tasks import refresh_promotores_task
from core.enums import JobStatus
from services.redis_job_tracker import RedisJobTracker
from utils.logger import logger

promotores_router = APIRouter(prefix='/promotores')


@promotores_router.post(
    '/refresh',
    response_model=ApiResponse,
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(verify_internal_token)],
)
def refresh_promotores(tracker: RedisJobTracker = Depends(get_job_tracker)):
    """
    Encola la recarga del catálogo de promotores desde SQL Server (Celery).

    Flujo (ejecutado en background):
      1. Extrae Zenit_Catalogo_promotores desde SQL Server (unpivot wide → tabular)
      2. TRUNCATE + insert en lotes → catalogo_promotores

    Monitorear progreso via:
      GET  /api/v1/jobs/{job_id}
      WS   /api/v1/ws/progress/{job_id}
    """
    job_id = str(uuid.uuid4())
    tracker.create_job(job_id)
    tracker.update_status(job_id, JobStatus.QUEUED, message="Actualización de promotores encolada")
    refresh_promotores_task.delay(job_id)
    logger.info(f"[promotores] Job {job_id} encolado")

    return ApiResponse(
        success=True,
        data=JobResponse(
            job_id=job_id,
            status=JobStatus.QUEUED,
            message="Recarga de catálogo de promotores iniciada en background",
        ).model_dump(),
        message="Promotores encolado",
    )
