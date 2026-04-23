from enum import Enum

class JobStatus(str, Enum):
    PENDING = "pending"
    QUEUED = "queued"
    EXTRACTING = "extracting"
    TRANSFORMING = "transforming"
    LOADING = "loading"
    RETRYING = "retrying"
    CLUSTERING = "clustering"
    GENERATING_AI = "generating_ai"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    
class ProcessingStage(str, Enum):
    EXTRACT = "extract"
    TRANSFORM = "transform"
    LOAD = "load"