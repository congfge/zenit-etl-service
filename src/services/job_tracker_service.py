from typing import Dict
from uuid import UUID, uuid4
from api.models.job_model import Job
from utils.exceptions import ResourceNotFoundException

class JobTracker:
    
    def __init__(self):
        self._jobs: Dict[str, Job] = {}

    def create_job(self):
        job = Job()
        self._jobs[str(job.id)] = job
        return job
    
    def get_job(self, job_id: str):
        job = self._jobs[job_id]
        if not job:
            raise ResourceNotFoundException(message="Job not found")
        return job
    
    def list_jobs(self):
        return list(self._jobs.values())
    
    def delete_job(self, job_id: str):
        if job_id in self._jobs:
            del self._jobs[job_id]