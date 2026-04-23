from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from core.config import settings
from utils.logger import logger

TERMINAL_STATUSES = {"completed", "failed", "cancelled"}


class PostgresJobTracker:
    """
    Persistencia de jobs ETL en PostgreSQL.
    Fuente de verdad para el historial — Redis sigue siendo el canal pub/sub.
    Requiere que la tabla etl_job_executions exista (database/08_etl_job_executions.sql).
    """

    def __init__(self):
        connection_url = URL.create(
            "postgresql+psycopg2",
            username=settings.postgres_user,
            password=settings.postgres_password,
            host=settings.postgres_host,
            database=settings.postgres_database,
            port=int(settings.postgres_port),
        )
        self.engine = create_engine(
            connection_url,
            pool_pre_ping=True,
            pool_size=2,
            max_overflow=2,
        )
        logger.info("[PostgresJobTracker] Conectado")

    @staticmethod
    def _parse_ts(value) -> datetime | None:
        """Convierte string ISO (de Redis) a datetime. Acepta None y datetimes directamente."""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(str(value))
        except (ValueError, TypeError):
            return None

    def upsert_job(self, job_data: dict) -> None:
        """INSERT … ON CONFLICT DO UPDATE — válido tanto para crear como para actualizar."""
        status = job_data.get("status", "queued")
        updated_at = self._parse_ts(job_data.get("updated_at")) or datetime.utcnow()

        params = {
            "id":                job_data["id"],
            "status":            status,
            "message":           job_data.get("message"),
            "total_records":     job_data.get("total_records", 0) or 0,
            "processed_records": job_data.get("processed_records", 0) or 0,
            "errors":            job_data.get("errors", 0) or 0,
            "created_at":        self._parse_ts(job_data.get("created_at")) or datetime.utcnow(),
            "updated_at":        updated_at,
            "completed_at":      updated_at if status in TERMINAL_STATUSES else None,
        }

        with self.engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO etl_job_executions
                    (id, status, message, total_records, processed_records, errors,
                     created_at, updated_at, completed_at)
                VALUES
                    (:id, :status, :message, :total_records, :processed_records, :errors,
                     :created_at, :updated_at, :completed_at)
                ON CONFLICT (id) DO UPDATE SET
                    status            = EXCLUDED.status,
                    message           = EXCLUDED.message,
                    total_records     = EXCLUDED.total_records,
                    processed_records = EXCLUDED.processed_records,
                    errors            = EXCLUDED.errors,
                    updated_at        = EXCLUDED.updated_at,
                    completed_at      = EXCLUDED.completed_at
            """), params)

    def list_jobs(self, limit: int = 10) -> list[dict]:
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT id, status, message, total_records, processed_records,
                       errors, created_at, updated_at, completed_at
                FROM etl_job_executions
                ORDER BY created_at DESC
                LIMIT :limit
            """), {"limit": limit}).mappings().all()

        result = []
        for r in rows:
            row = dict(r)
            # Serializar timestamps a ISO string para compatibilidad con la respuesta JSON actual
            for field in ("created_at", "updated_at", "completed_at"):
                if row.get(field) and isinstance(row[field], datetime):
                    row[field] = row[field].isoformat()
            result.append(row)
        return result

    def get_job(self, job_id: str) -> dict | None:
        with self.engine.connect() as conn:
            row = conn.execute(text("""
                SELECT id, status, message, total_records, processed_records,
                       errors, created_at, updated_at, completed_at
                FROM etl_job_executions
                WHERE id = :id
            """), {"id": job_id}).mappings().fetchone()

        if not row:
            return None
        result = dict(row)
        for field in ("created_at", "updated_at", "completed_at"):
            if result.get(field) and isinstance(result[field], datetime):
                result[field] = result[field].isoformat()
        return result
