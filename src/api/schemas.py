from pydantic import BaseModel, Field
from typing import Optional


class JobListQueryParams(BaseModel):
    limit: int = Field(default=10, ge=1, le=100, description="Número máximo de jobs a retornar")


class HealthCheckResult(BaseModel):
    status: str
    latency_ms: Optional[int] = None
    host: Optional[str] = None
    error: Optional[str] = None
