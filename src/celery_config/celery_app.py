from celery import Celery
from dotenv import load_dotenv
from pathlib import Path
import os

load_dotenv()

_LOGS_DIR = Path(__file__).resolve().parents[2] / "logs"
_LOGS_DIR.mkdir(exist_ok=True)

celery_broker_url: str = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0")
celery_result_backend: str = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")
celery_task_time_limit: int = int(os.environ.get("CELERY_TASK_TIME_LIMIT", "600"))
celery_task_soft_time_limit: int = int(os.environ.get("CELERY_TASK_SOFT_TIME_LIMIT", "540"))

celery_app = Celery(
    "zenit_worker",
    broker=celery_broker_url,
    backend=celery_result_backend,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="America/Guatemala",
    enable_utc=True,
    task_track_started=True,
    task_time_limit=celery_task_time_limit,
    task_soft_time_limit=celery_task_soft_time_limit,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=1,
    worker_logfile=str(_LOGS_DIR / "celery_worker.log"),
    worker_loglevel="INFO",
)

from celery_config import tasks