from dataclasses import dataclass

@dataclass
class ProcessingMetrics:
    records_extracted: int = 0
    records_dropped: int = 0
    records_cleaned: int = 0
    records_loaded: int = 0
    records_seconds: int = 0
    duration_seconds: float = 0.0