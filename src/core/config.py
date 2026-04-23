import os
from dotenv import load_dotenv
from pydantic_settings import BaseSettings

load_dotenv()

class Settings(BaseSettings):
    app_name: str = "ZenitAIServiceAPI"
    debug: bool = False
    items_per_user: int = 50
    port: int = int(os.getenv("PORT"))
    openai_api_key: str = os.getenv("OPENAI_API_KEY")
    api_version: str = os.getenv("API_VERSION")
    
    # ? SQLServer Config
    sqlserver_user: str = os.getenv("SQLSERVER_USER")
    sqlserver_password: str = os.getenv("SQLSERVER_PASSWORD")
    sqlserver_host: str = os.getenv("SQLSERVER_HOST")
    sqlserver_port: str = os.getenv("SQLSERVER_PORT")
    sqlserver_database: str = os.getenv("SQLSERVER_DATABASE")
    sqlserver_driver: str = os.environ.get("SQLSERVER_DRIVER")
    sqlserver_pool_size: str = os.environ.get("SQLSERVER_POOL_SIZE")
    sqlserver_pool_max_overflow: str = os.environ.get("SQLSERVER_POOL_MAX_OVERFLOW")
    sqlserver_pool_timeout: str = os.environ.get("SQLSERVER_POOL_TIMEOUT")
    sqlserver_pool_recycle: str = os.environ.get("SQLSERVER_POOL_RECYCLE")
    
    # ? Postgres Config
    postgres_host: str = os.environ.get("POSTGRES_HOST")
    postgres_port: str = os.environ.get("POSTGRES_PORT")
    postgres_user: str = os.environ.get("POSTGRES_USER")
    postgres_password: str = os.environ.get("POSTGRES_PASSWORD")
    postgres_database: str = os.environ.get("POSTGRES_DATABASE")
    postgres_etl_table: str = os.environ.get("POSTGRES_ETL_TABLE")
    
    
    # ? ETL Config
    chunk_size: int = int(os.getenv("CHUNK_SIZE"))
    
    redis_host: str = os.environ.get("REDIS_HOST", "localhost")
    redis_port: int = int(os.environ.get("REDIS_PORT", "6379"))
    redis_db: int = int(os.environ.get("REDIS_DB", "0"))
    redis_url: str = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

    # ? DB2 for IBM i (AS400) Config (read-only — fuente de sectores cuando se habilite)
    db2_host: str = os.environ.get("DB2_HOST", "")
    db2_port: int = int(os.environ.get("DB2_PORT", "446"))
    db2_user: str = os.environ.get("DB2_USER", "")
    db2_password: str = os.environ.get("DB2_PASSWORD", "")
    db2_database: str = os.environ.get("DB2_DATABASE", "")
    jt400_jar_path: str = os.environ.get("JT400_JAR_PATH", "/opt/jt400.jar")
    db2_timeout_sec: int = int(os.environ.get("DB2_TIMEOUT_SEC", "30"))
    db2_suc_database: str = os.environ.get("DB2_SUC_DATABASE", "lbgeocli")

    # ? Sectores CSV (fuente temporal hasta habilitar DB2)
    sectores_csv_path: str = os.environ.get("SECTORES_CSV_PATH", "")

    # ? Security
    internal_api_secret: str = os.environ.get("INTERNAL_API_SECRET", "")
    cors_allowed_origins: str = os.environ.get("CORS_ALLOWED_ORIGINS", "http://localhost:3200")

    # ? Clustering
    cluster_layer_name: str = os.environ.get("CLUSTER_LAYER_NAME", "Clusters-Creditos")
    system_user_id: int = int(os.environ.get("SYSTEM_USER_ID", "1"))

    # Celery Config
    celery_broker_url: str = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0")
    celery_result_backend: str = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")
    celery_task_time_limit: int = int(os.environ.get("CELERY_TASK_TIME_LIMIT", "600"))
    celery_task_soft_time_limit: int = int(os.environ.get("CELERY_TASK_SOFT_TIME_LIMIT", "540"))
    
    
    class Config:
        env_file = ".env"
    
settings = Settings()

