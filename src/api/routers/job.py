from fastapi import APIRouter, Depends, status
from ..models import response_model, job_model
from ..handlers.job_handler import list_recent_jobs, get_job
from ..schemas import JobListQueryParams

job_router = APIRouter(prefix='/jobs')

@job_router.get('/', response_model=response_model.ApiResponse[list], status_code=status.HTTP_200_OK)
def get_recent_jobs(params: JobListQueryParams = Depends()):

    jobs = list_recent_jobs(params.limit)
    
    return response_model.ApiResponse(
        success=True,
        data=jobs
    )
    
@job_router.get('/{job_id}', response_model=response_model.ApiResponse[dict], status_code=status.HTTP_200_OK)
def get_job_by_id(job_id: str):

    job = get_job(job_id)
    
    return response_model.ApiResponse(
        success=True,
        data=job
    )