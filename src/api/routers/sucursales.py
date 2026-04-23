import uuid

from fastapi import APIRouter, Depends, status

from api.dependencies import verify_internal_token, get_job_tracker
from api.models.response_model import ApiResponse
from api.models.job_response_model import JobResponse
from celery_config.tasks import refresh_sucursales_task
from core.enums import JobStatus
from services.redis_job_tracker import RedisJobTracker
from utils.logger import logger

sucursales_router = APIRouter(prefix='/sucursales')


@sucursales_router.post(
    '/refresh',
    response_model=ApiResponse,
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(verify_internal_token)],
)
def refresh_sucursales(tracker: RedisJobTracker = Depends(get_job_tracker)):
    """
    Encola la recarga del catálogo de sucursales desde DB2 (lbgeocli.agencias, tipo=1).

    Flujo (ejecutado en background):
      1. Extrae agencias con tipo=1 desde DB2 lbgeocli
      2. DROP + recreate + bulk insert → tabla sucursales en PostgreSQL

    La tabla se crea automáticamente si no existe en el primer run.

    Monitorear progreso via:
      GET  /api/v1/jobs/{job_id}
      WS   /api/v1/ws/progress/{job_id}
    """
    job_id = str(uuid.uuid4())
    tracker.create_job(job_id)
    tracker.update_status(job_id, JobStatus.QUEUED, message="Actualización de sucursales encolada")
    refresh_sucursales_task.delay(job_id)
    logger.info(f"[sucursales] Job {job_id} encolado")

    return ApiResponse(
        success=True,
        data=JobResponse(
            job_id=job_id,
            status=JobStatus.QUEUED,
            message="Recarga de catálogo de sucursales iniciada en background",
        ).model_dump(),
        message="Sucursales encolado",
    )
