from fastapi import Header, HTTPException, status
from services.etl_service import EtlService
from services.redis_job_tracker import RedisJobTracker
from connectors.sqlserver_connector import SQLServerConnector
from connectors.postgres_connector import PostgresConnector
from core.config import settings
from transformers.data_cleaner import DataCleaner


async def verify_internal_token(x_internal_token: str = Header(...)):
    if not settings.internal_api_secret or x_internal_token != settings.internal_api_secret:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")


def get_job_tracker() -> RedisJobTracker:
    return RedisJobTracker()


def get_etl_service() -> EtlService:
    sqlserver = SQLServerConnector()
    postgres = PostgresConnector()
    return EtlService(
        sqlserver=sqlserver,
        postgres=postgres,
        cleaner=DataCleaner(),
        chunk_size=settings.chunk_size
    )
