from datetime import datetime
 
from dataclasses import dataclass, field
from uuid import UUID, uuid4
from core.enums import JobStatus

@dataclass
class Job:
    id: UUID = field(default_factory=uuid4)
    status: JobStatus = JobStatus.PENDING
    total_records: int = 0
    errors: int = 0
    started_at: datetime | None = None
    completed_at: datetime | None = None
    processed_records: int = 0

