import json
from typing import Optional
from datetime import datetime
from redis import Redis
from core.config import settings
from core.enums import JobStatus
from utils.logger import logger


class RedisJobTracker:
    """
    JobTracker con dual-write: Redis para pub/sub en tiempo real +
    PostgreSQL como fuente de verdad para historial persistente.
    """

    def __init__(self):
        self.redis = Redis.from_url(settings.redis_url, decode_responses=True)
        self.ttl = 86400 * 7  # 7 días de retención en Redis

        try:
            from services.postgres_job_tracker import PostgresJobTracker
            self._pg = PostgresJobTracker()
        except Exception as e:
            logger.warning(f"[RedisJobTracker] PostgresJobTracker no disponible: {e}. Usando solo Redis.")
            self._pg = None

    def _sync_to_pg(self, job_data: dict) -> None:
        """Escribe el estado del job a PostgreSQL de forma no bloqueante."""
        if self._pg:
            try:
                self._pg.upsert_job(job_data)
            except Exception as e:
                logger.warning(f"[RedisJobTracker] Sync a PG falló (no crítico): {e}")

    def create_job(self, job_id: str) -> dict:
        """Inicializa un job en Redis"""
        job_data = {
            "id": job_id,
            "status": JobStatus.QUEUED.value,
            "progress": 0.0,
            "message": "Job en cola",
            "total_records": 0,
            "processed_records": 0,
            "errors": 0,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
        }

        self.redis.setex(
            f"job:{job_id}",
            self.ttl,
            json.dumps(job_data)
        )

        self._sync_to_pg(job_data)
        logger.info(f"[REDIS] Job {job_id} created")
        return job_data

    def update_progress(self, job_id: str, progress: float, message: str):
        """Actualiza progress del job"""
        job_data = self.get_job(job_id)
        if not job_data:
            logger.warning(f"[REDIS] Job {job_id} not found for progress update")
            return

        job_data["progress"] = progress
        job_data["message"] = message
        job_data["updated_at"] = datetime.utcnow().isoformat()

        self.redis.setex(f"job:{job_id}", self.ttl, json.dumps(job_data))
        self.redis.publish(f"progress:{job_id}", json.dumps(job_data))
        self._sync_to_pg(job_data)

    def update_status(
        self,
        job_id: str,
        status: JobStatus,
        message: Optional[str] = None,
        **kwargs
    ):
        """Actualiza status del job"""
        job_data = self.get_job(job_id)
        if not job_data:
            logger.warning(f"[REDIS] Job {job_id} not found for status update")
            return

        job_data["status"] = status.value if isinstance(status, JobStatus) else status
        if message:
            job_data["message"] = message
        job_data["updated_at"] = datetime.utcnow().isoformat()
        job_data.update(kwargs)

        self.redis.setex(f"job:{job_id}", self.ttl, json.dumps(job_data))
        self.redis.publish(f"progress:{job_id}", json.dumps(job_data))
        self._sync_to_pg(job_data)

        logger.info(f"[REDIS] Job {job_id} status updated to {status}")

    def get_job(self, job_id: str) -> Optional[dict]:
        """Obtiene estado actual del job. Redis primero (rápido), PG como fallback."""
        data = self.redis.get(f"job:{job_id}")
        if data:
            return json.loads(data)
        return self._pg.get_job(job_id) if self._pg else None

    def delete_job(self, job_id: str):
        """Elimina un job de Redis"""
        self.redis.delete(f"job:{job_id}")
        logger.info(f"[REDIS] Job {job_id} deleted")

    def list_jobs(self, limit: int = 10) -> list[dict]:
        """Lista los últimos N jobs. Lee de PostgreSQL para orden correcto y persistencia."""
        if self._pg:
            return self._pg.list_jobs(limit)

        # Fallback a Redis si PG no está disponible (carga todos, ordena, luego slice)
        job_keys = self.redis.keys("job:*")
        jobs = []
        for key in job_keys:
            job_data = self.redis.get(key)
            if job_data:
                jobs.append(json.loads(job_data))
        jobs.sort(key=lambda x: x.get('created_at', ''), reverse=True)
        return jobs[:limit]