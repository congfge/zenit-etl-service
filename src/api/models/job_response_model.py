
from pydantic import BaseModel
from core.enums import JobStatus

class JobResponse(BaseModel):
    job_id: str
    status: JobStatus
    progress_percentage: float = 0.0
    total_records: int = 0
    processed_records: int = 0
    errors: int = 0
    message: str | None = None