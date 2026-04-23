import uuid

from fastapi import APIRouter, Depends, status

from api.dependencies import verify_internal_token, get_job_tracker
from api.models.response_model import ApiResponse
from api.models.job_response_model import JobResponse
from celery_config.tasks import refresh_clientes_task
from core.enums import JobStatus
from services.redis_job_tracker import RedisJobTracker
from utils.logger import logger

clientes_router = APIRouter(prefix='/clientes')


@clientes_router.post(
    '/refresh',
    response_model=ApiResponse,
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(verify_internal_token)],
)
def refresh_clientes(tracker: RedisJobTracker = Depends(get_job_tracker)):
    """
    Encola la sincronización de capas de clientes desde creditos_historico_etl (Celery).

    Flujo (ejecutado en background):
      Llama fn_sync_clientes_to_layers() que sincroniza:
        'Sana'       → ClientesSanos
        'Castigado'  → ClientesCastigo
        'MORA%'      → ClientesMora
        'EXCLIENTE'  → ExClientes
        (vacía)      → ClientesColocacion

    Solo registros con coordenadas válidas (latitud/longitud != 0 y != NULL).

    Monitorear progreso via:
      GET  /api/v1/jobs/{job_id}
      WS   /api/v1/ws/progress/{job_id}
    """
    job_id = str(uuid.uuid4())
    tracker.create_job(job_id)
    tracker.update_status(job_id, JobStatus.QUEUED, message="Sincronización de capas de clientes encolada")
    refresh_clientes_task.delay(job_id)
    logger.info(f"[clientes] Job {job_id} encolado")

    return ApiResponse(
        success=True,
        data=JobResponse(
            job_id=job_id,
            status=JobStatus.QUEUED,
            message="Sincronización de capas de clientes iniciada en background",
        ).model_dump(),
        message="Clientes encolado",
    )
