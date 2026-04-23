from uuid import uuid4
from fastapi import APIRouter, Depends, status
from ..models import response_model, job_response_model
from ..dependencies import verify_internal_token
from celery_config.tasks import refresh_data_task
from services.redis_job_tracker import RedisJobTracker
from core.enums import JobStatus


refresh_router = APIRouter(prefix='/refresh')

@refresh_router.post('/', response_model=response_model.ApiResponse[job_response_model.JobResponse], status_code=status.HTTP_200_OK, dependencies=[Depends(verify_internal_token)])
def trigger_refresh():

    job_id = str(uuid4())
    redis_tracker = RedisJobTracker()
    redis_tracker.create_job(job_id)

    refresh_data_task.apply_async(
        args=[job_id],
        task_id=job_id,
    )

    return response_model.ApiResponse(
        success=True,
        data=job_response_model.JobResponse(
            job_id=job_id,
            status=JobStatus.QUEUED.value,
            progress_percentage=0.0,
            message="Job encolado — extrayendo últimas 3 fechas de cierre desde SQL Server"
        ).model_dump()
    )
