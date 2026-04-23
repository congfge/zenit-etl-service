# Plan de Migración: BackgroundTasks → Celery + Redis

**Fecha:** 2026-02-17
**Estado:** ✅ Implementado (2026-02-18)
**Autor:** Claude Code
**Opción Seleccionada:** Celery + Redis

---

## Resumen Ejecutivo

### ¿Qué vamos a cambiar?

**ANTES (BackgroundTasks):**
```python
# src/api/routers/refresh.py
@refresh_router.post('/')
def trigger_refresh(
    background_tasks: BackgroundTasks,
    etl_service: EtlService = Depends(get_etl_service)
):
    background_tasks.add_task(etl_service.run_full_refresh, job_id, date)
    return {"status": "pending"}  # ❌ Sin persistencia
```

**DESPUÉS (Celery):**
```python
# src/api/routers/refresh.py
@refresh_router.post('/', status_code=202)
def trigger_refresh(request: RefreshRequest):
    job_id = str(uuid4())
    RedisJobTracker().create_job(job_id)
    refresh_data_task.apply_async(args=[job_id, date])
    return {"status": "queued"}  # ✅ Persistente, recuperable
```

### Arquitectura: Antes vs. Después

```
┌─────────────────────────────────────────────────────────────────┐
│                          ANTES                                  │
└─────────────────────────────────────────────────────────────────┘
FastAPI → BackgroundTasks → EtlService (en memoria)
                                ↓
                            ❌ Se pierde si reinicia


┌─────────────────────────────────────────────────────────────────┐
│                         DESPUÉS                                 │
└─────────────────────────────────────────────────────────────────┘
FastAPI → Celery → Redis → Celery Worker → EtlService
   ↓                ↓             ↓
  202           Persiste      Recuperable
             (7 días TTL)      (retry 3x)
```

### Nuevos Componentes

| Componente | Propósito | Puerto |
|------------|-----------|--------|
| **Redis** | Message broker + job state storage | 6379 |
| **Celery Worker** | Procesa jobs en background | N/A |
| **Flower** | Dashboard de monitoring (opcional) | 5555 |
| **WebSocket** | Progress updates en tiempo real | 8000 |

### Nuevos Archivos

| Directorio | Archivos Nuevos |
|------------|-----------------|
| `src/celery_config/` | `__init__.py`, `celery_app.py`, `tasks.py` ⚠️ Nombre `celery_config` evita conflicto con paquete `celery` |
| `src/services/` | `redis_job_tracker.py` |
| `src/websocket/` | `__init__.py`, `progress_handler.py` |
| `scripts/` | `start_worker.sh`, `start_flower.sh` |

### Comparación de Capacidades

| Característica | BackgroundTasks (Antes) | Celery + Redis (Después) |
|----------------|-------------------------|--------------------------|
| **Persistencia** | ❌ Se pierde al reiniciar | ✅ Persiste en Redis (7 días) |
| **Retry automático** | ❌ No | ✅ 3 intentos con backoff |
| **Timeout control** | ❌ No | ✅ 10 min hard limit |
| **Progress tracking** | ⚠️ Solo en memoria | ✅ Redis + WebSocket real-time |
| **Monitoring** | ❌ No | ✅ Flower dashboard |
| **Concurrency control** | ❌ No | ✅ Max 1 job simultáneo |
| **Historial de jobs** | ❌ No | ✅ 7 días en Redis |
| **Cancelación** | ❌ No | ✅ Via API endpoint |
| **Escalabilidad** | ⚠️ Bloquea workers | ✅ Workers independientes |

---

## 1. Contexto y Motivación

### 1.1 Problema Actual

El endpoint `/api/v1/refresh` utiliza `BackgroundTasks` de FastAPI para procesar **~700k registros** con una duración estimada de **4+ minutos**. Esta implementación presenta los siguientes problemas críticos:

| Problema | Descripción | Impacto |
|----------|-------------|---------|
| **Sin persistencia** | Si el servidor FastAPI se reinicia durante el proceso, se pierde completamente el job sin forma de recuperarlo | 🔴 CRÍTICO |
| **Sin recuperación ante fallos** | No hay retry automático si falla la extracción, limpieza o carga de datos | 🔴 CRÍTICO |
| **Bloquea workers** | Ocupa un worker de Uvicorn durante toda la ejecución (4+ min), reduciendo la capacidad del servidor | 🟡 ALTO |
| **Sin límite de concurrencia** | Múltiples ejecuciones simultáneas pueden saturar memoria/CPU o generar deadlocks en las bases de datos | 🟡 ALTO |
| **Monitoreo limitado** | El estado del job solo existe en memoria, sin historial ni auditoría | 🟡 MEDIO |
| **Sin timeout management** | No hay control de tiempo máximo de ejecución | 🟡 MEDIO |

### 1.2 Alcance del Cambio

- **Endpoint afectado:** `POST /api/v1/refresh`
- **Volumen de datos:** ~700k registros (SQL Server → PostgreSQL)
- **Duración típica:** ~4 minutos
- **Frecuencia de ejecución:** Monthly batch (no alta frecuencia)
- **Modelo de uso:** Single admin user execution model

---

## 2. Análisis de Opciones

### 2.1 Opción 1: Celery + Redis ⭐ **RECOMENDADA**

**Descripción:** Task queue de facto en Python, battle-tested en producción por empresas como Instagram, Mozilla, y LinkedIn.

#### Pros
- ✅ Persistencia de jobs en Redis (recuperable ante restart)
- ✅ Retry automático configurable con exponential backoff
- ✅ Rate limiting y control de concurrencia
- ✅ Monitoring visual con **Flower** (dashboard web)
- ✅ Support para task chaining, grouping, chord patterns
- ✅ Amplia documentación y community support
- ✅ Se integra perfectamente con Clean Architecture (application layer)

#### Cons
- ❌ Requiere broker (Redis) + worker process adicional
- ❌ Más complejo de configurar que BackgroundTasks
- ❌ Overhead para casos simples (pero **NO aplica** para este caso de 700k registros)

#### Dependencias Requeridas
```txt
celery[redis]==5.3.4
redis==5.0.1
flower==2.0.1  # Monitoring dashboard
```

#### Infraestructura Adicional
- Redis server (local en desarrollo, remoto en producción)
- Celery worker process (separado de FastAPI)
- Flower dashboard (opcional, para monitoring)

---

### 2.2 Opción 2: ARQ (Asyncio Redis Queue)

**Descripción:** Task queue moderno basado en asyncio, creado por el autor de Pydantic.

#### Pros
- ✅ Native asyncio (no threads, mejor para I/O bound)
- ✅ Más simple que Celery
- ✅ Retry automático y job persistence
- ✅ Solo requiere Redis

#### Cons
- ❌ Menos maduro que Celery (comunidad más pequeña)
- ❌ Sin dashboard de monitoring built-in
- ❌ Menos features avanzados (no chord, no canvas)

#### Mejor para
Proyectos 100% asyncio que prefieren simplicidad sobre features.

---

### 2.3 Opción 3: RQ (Redis Queue)

**Descripción:** Task queue minimalista, solo lo esencial.

#### Pros
- ✅ Extremadamente simple
- ✅ Solo Redis como dependencia
- ✅ RQ Dashboard disponible

#### Cons
- ❌ No es asyncio (usa threads/processes)
- ❌ Menos features que Celery
- ❌ Sin scheduling built-in

#### Mejor para
Casos de uso muy simples donde Celery es overkill.

---

### 2.4 Opción 4: APScheduler + Database Backend

**Descripción:** Scheduler con persistencia en PostgreSQL (sin broker externo).

#### Pros
- ✅ Sin Redis (reutiliza PostgreSQL existente)
- ✅ Scheduling built-in
- ✅ Persistencia en DB

#### Cons
- ❌ Menos robusto para task queue (más pensado para scheduling)
- ❌ Sin distributed workers
- ❌ Overhead en PostgreSQL

#### Mejor para
Scheduling de tareas, no para processing-heavy queues.

---

## 3. Solución Recomendada: Celery + Redis

### 3.1 Por qué Celery

Dadas las características del proyecto:

- **Batch pesado** (700k registros, 4 min): Necesitas retry, timeout, monitoring → Celery brilla aquí
- **Clean Architecture**: Celery se implementa como infraestructura (adapter pattern)
- **Monthly execution**: No importa la complejidad inicial, necesitas robustez
- **Single admin**: Flower dashboard perfecto para que admin monitoree jobs
- **Ya tienen PostgreSQL**: Pueden usar PostgreSQL como result backend (opcional)

### 3.2 Arquitectura Propuesta

```
┌─────────────────────────────────────────────────────────────┐
│                        FastAPI App                          │
│  ┌────────────────────────────────────────────────────┐     │
│  │  POST /api/v1/refresh                              │     │
│  │  1. Crea job_id                                    │     │
│  │  2. Encola task en Celery                          │     │
│  │  3. Retorna job_id inmediatamente                  │     │
│  └─────────────────────┬──────────────────────────────┘     │
│                        │ enqueue                             │
└────────────────────────┼─────────────────────────────────────┘
                         │
                         ▼
              ┌──────────────────────┐
              │       Redis          │  ← Job queue + progress data
              │   (Message Broker)   │
              └──────────┬───────────┘
                         │
                         │ consume
                         ▼
         ┌───────────────────────────────────────┐
         │       Celery Worker Process           │
         │  ┌─────────────────────────────────┐  │
         │  │  refresh_data_task()            │  │
         │  │  1. Extract from SQL Server     │  │
         │  │  2. Clean with Pandas           │  │
         │  │  3. Load to PostgreSQL          │  │
         │  │  4. Update progress → Redis     │  │
         │  └─────────────────────────────────┘  │
         └───────────────────────────────────────┘
                         │
                         │ progress updates
                         ▼
              ┌──────────────────────┐
              │    WebSocket Server  │
              │  /ws/progress/{id}   │  → Frontend recibe updates
              └──────────────────────┘
```

### 3.3 Integración con la Estructura Actual del Proyecto

**Estructura Actual:**
```
src/
├── api/
│   ├── models/              # Pydantic models y dataclasses
│   ├── routers/             # FastAPI endpoints
│   │   ├── refresh.py      # ← MODIFICAR (BackgroundTasks → Celery)
│   │   └── job.py          # ← ACTUALIZAR (agregar endpoints)
│   └── dependencies.py      # ← ACTUALIZAR (agregar Celery dependencies)
├── connectors/
│   ├── sqlserver_connector.py
│   └── postgres_connector.py
├── core/
│   ├── config.py           # ← ACTUALIZAR (agregar Redis config)
│   └── enums.py            # ← ACTUALIZAR (agregar QUEUED, RETRYING, CANCELLED)
├── services/
│   ├── etl_service.py      # Lógica de ETL (NO cambia)
│   ├── job_tracker_service.py  # ← Mantener para compatibilidad
│   └── redis_job_tracker.py    # ← NUEVO (reemplaza dict en memoria)
├── transformers/
│   └── data_cleaner.py     # Limpieza de datos (NO cambia)
└── utils/
    ├── exceptions.py
    └── logger.py
```

**Nueva Estructura para Celery:**
```
src/
├── celery_config/           # ← NUEVO (configuración Celery)
│   ├── __init__.py         # IMPORTANTE: Nombre diferente a 'celery' para evitar conflictos
│   ├── celery_app.py       # Configuración de Celery
│   └── tasks.py            # Celery tasks (@celery_app.task)
└── websocket/               # ← NUEVO (WebSocket para progress)
    ├── __init__.py
    └── progress_handler.py
```

**⚠️ IMPORTANTE:** El directorio se llama `celery_config` (no `celery`) para evitar conflictos con el paquete instalado.

**Separación de responsabilidades:**
- **api/routers:** Encola tasks en Celery
- **celery/:** Configuración y definición de tasks
- **services:** Lógica de negocio (ETL, tracking)
- **websocket:** Comunicación en tiempo real con frontend

---

## 4. Plan de Implementación Detallado

### Fase 1: Setup de Infraestructura

#### 1.1. Instalar Redis (desarrollo local)

**macOS:**
```bash
brew install redis
brew services start redis

# Verificar
redis-cli ping  # Debe responder: PONG
```

**Docker (alternativa):**
```bash
docker run -d -p 6379:6379 --name redis-zenit redis:7-alpine
```

#### 1.2. Agregar Dependencias

**Crear/actualizar `requirements.txt`:**

Si ya existe `requirements.txt`, agregar estas líneas al final:
```txt
# Celery Stack (NUEVO - agregar al final del archivo)
celery[redis]==5.3.4
redis==5.0.1
flower==2.0.1
```

Si NO existe `requirements.txt`, crear el archivo completo:
```txt
# FastAPI Stack
fastapi==0.109.0
uvicorn[standard]==0.27.0
python-dotenv==1.0.0
pydantic-settings==2.1.0

# Data Processing
pandas==2.2.0

# Database
sqlalchemy==2.0.25
pyodbc==5.0.1
psycopg2-binary==2.9.9

# Celery Stack
celery[redis]==5.3.4
redis==5.0.1
flower==2.0.1
```

**Instalar:**
```bash
source venv/bin/activate
pip install celery[redis]==5.3.4 redis==5.0.1 flower==2.0.1
pip freeze > requirements.txt  # Actualizar con versiones exactas
```

#### 1.3. Variables de Entorno

Actualizar `.env`:
```env
# Redis (NUEVO)
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0
REDIS_URL=redis://localhost:6379/0

# SQL Server (existente - mantener)
SQLSERVER_USER=...
SQLSERVER_PASSWORD=...
SQLSERVER_HOST=...
SQLSERVER_PORT=1433
SQLSERVER_DATABASE=...

# PostgreSQL (existente - mantener)
POSTGRES_HOST=...
POSTGRES_PORT=5432
POSTGRES_USER=...
POSTGRES_PASSWORD=...
POSTGRES_DATABASE=...

# Celery Config (NUEVO)
CELERY_BROKER_URL=redis://localhost:6379/0
CELERY_RESULT_BACKEND=redis://localhost:6379/0
CELERY_TASK_TIME_LIMIT=600  # 10 min hard limit
CELERY_TASK_SOFT_TIME_LIMIT=540  # 9 min soft limit
```

#### 1.4. Actualizar Enums

**Archivo:** `src/core/enums.py` - Agregar nuevos estados:
```python
from enum import Enum

class JobStatus(str, Enum):
    PENDING = "pending"
    QUEUED = "queued"              # ← NUEVO
    EXTRACTING = "extracting"
    TRANSFORMING = "transforming"
    LOADING = "loading"
    RETRYING = "retrying"          # ← NUEVO
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"        # ← NUEVO

class ProcessingStage(str, Enum):
    EXTRACT = "extract"
    TRANSFORM = "transform"
    LOAD = "load"
```

#### 1.5. Actualizar Configuración

**Archivo:** `src/core/config.py` - Agregar al final de la clase Settings:
```python
class Settings(BaseSettings):
    # ... campos existentes (mantener) ...

    # Redis Config (NUEVO - agregar al final)
    redis_host: str = os.environ.get("REDIS_HOST", "localhost")
    redis_port: int = int(os.environ.get("REDIS_PORT", "6379"))
    redis_db: int = int(os.environ.get("REDIS_DB", "0"))
    redis_url: str = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

    # Celery Config (NUEVO - agregar al final)
    celery_broker_url: str = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0")
    celery_result_backend: str = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")
    celery_task_time_limit: int = int(os.environ.get("CELERY_TASK_TIME_LIMIT", "600"))
    celery_task_soft_time_limit: int = int(os.environ.get("CELERY_TASK_SOFT_TIME_LIMIT", "540"))
```

---

### Fase 2: Implementar Celery Task

#### 2.1. Crear Configuración de Celery

**Archivo:** `src/celery_config/__init__.py`
```python
from celery_config.celery_app import celery_app
from celery_config import tasks

__all__ = ["celery_app", "tasks"]
```

**Archivo:** `src/celery_config/celery_app.py`
```python
from celery import Celery
from dotenv import load_dotenv
import os

load_dotenv()

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
    task_acks_late=True,  # Reintenta si worker muere
    worker_prefetch_multiplier=1,  # Solo 1 job por worker
    worker_max_tasks_per_child=1,  # Reinicia worker después de cada task (evita memory leaks)
)

# ⚠️ CRÍTICO: Importar tasks para que se registren en Celery
from celery_config import tasks  # noqa: F401
```

**⚠️ IMPORTANTE:** La última línea (`from celery_config import tasks`) es CRÍTICA. Sin ella, Celery no registra las tareas y obtendrás el error:
```
Received unregistered task of type 'refresh_data'
```

#### 2.2. Crear RedisJobTracker

Primero necesitamos un JobTracker que use Redis en vez de memoria.

**Archivo:** `src/services/redis_job_tracker.py`
```python
import json
from typing import Optional
from datetime import datetime
from redis import Redis
from core.config import settings
from core.enums import JobStatus
from utils.logger import logger


class RedisJobTracker:
    """
    JobTracker que almacena estado en Redis.
    Reemplaza el JobTracker en memoria para que los jobs persistan.
    """

    def __init__(self):
        self.redis = Redis.from_url(settings.redis_url, decode_responses=True)
        self.ttl = 86400 * 7  # 7 días de retención

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

        logger.info(f"[REDIS] Job {job_id} status updated to {status}")

    def get_job(self, job_id: str) -> Optional[dict]:
        """Obtiene estado actual del job"""
        data = self.redis.get(f"job:{job_id}")
        return json.loads(data) if data else None

    def delete_job(self, job_id: str):
        """Elimina un job de Redis"""
        self.redis.delete(f"job:{job_id}")
        logger.info(f"[REDIS] Job {job_id} deleted")

    def list_jobs(self, limit: int = 10) -> list[dict]:
        """Lista los últimos N jobs"""
        job_keys = self.redis.keys("job:*")
        jobs = []

        for key in job_keys[:limit]:
            job_data = self.redis.get(key)
            if job_data:
                jobs.append(json.loads(job_data))

        jobs.sort(key=lambda x: x.get('created_at', ''), reverse=True)
        return jobs[:limit]
```

#### 2.3. Crear Celery Task

**Archivo:** `src/celery_config/tasks.py`
```python
import asyncio
from datetime import datetime
from celery_config.celery_app import celery_app
from services.etl_service import EtlService
from services.redis_job_tracker import RedisJobTracker
from services.job_tracker_service import JobTracker
from connectors.sqlserver_connector import SQLServerConnector
from connectors.postgres_connector import PostgresConnector
from transformers.data_cleaner import DataCleaner
from core.config import settings
from core.enums import JobStatus
from utils.logger import logger


# Cache global para reutilizar servicios (lazy initialization)
_etl_service = None
_redis_tracker = None


def _get_etl_service() -> EtlService:
    """
    Lazy initialization de EtlService.
    Se inicializa una vez y se reutiliza en todos los tasks.

    NOTA: JobTracker NO se pasa como dependencia porque EtlService
    ya no maneja el tracking de jobs. El tracking lo hace la tarea
    de Celery usando RedisJobTracker.
    """
    global _etl_service
    if _etl_service is None:
        sqlserver = SQLServerConnector()
        postgres = PostgresConnector()
        cleaner = DataCleaner()

        _etl_service = EtlService(
            sqlserver=sqlserver,
            postgres=postgres,
            cleaner=cleaner,
            chunk_size=settings.chunk_size
        )
    return _etl_service


def _get_redis_tracker() -> RedisJobTracker:
    """Lazy initialization de RedisJobTracker"""
    global _redis_tracker
    if _redis_tracker is None:
        _redis_tracker = RedisJobTracker()
    return _redis_tracker


@celery_app.task(
    bind=True,
    name="refresh_data",
    max_retries=3,
    default_retry_delay=60,  # 1 min entre retries
    autoretry_for=(Exception,),  # Retry automático en cualquier excepción
    retry_backoff=True,  # Exponential backoff
    retry_backoff_max=600,  # Max 10 min de espera
    retry_jitter=True,  # Agrega randomness al backoff
)
def refresh_data_task(self, job_id: str, closing_date: str):
    """
    Celery task que ejecuta el refresh completo de datos.

    Args:
        job_id: UUID del job
        closing_date: Fecha de cierre en formato DD/MM/YYYY (ej: "31/12/2025")

    Returns:
        dict: Resultado del proceso con métricas

    Raises:
        Exception: Si falla después de 3 retries
    """
    logger.info(f"[CELERY TASK {job_id}] Starting refresh_data_task (attempt {self.request.retries + 1})")

    # Obtener servicios (lazy initialization)
    redis_tracker = _get_redis_tracker()
    etl_service = _get_etl_service()

    try:
        # Actualizar estado a EXTRACTING (primer paso del ETL)
        redis_tracker.update_status(
            job_id,
            JobStatus.EXTRACTING,
            message=f"Extrayendo datos (intento {self.request.retries + 1})"
        )

        # Ejecutar ETL (lógica de negocio)
        # Nota: run_full_refresh es async, necesitamos ejecutarlo con asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            metrics = loop.run_until_complete(
                etl_service.run_full_refresh(
                    job_id=job_id,
                    closing_date=closing_date
                )
            )
        finally:
            loop.close()

        # Marcar como completado
        redis_tracker.update_status(
            job_id,
            JobStatus.COMPLETED,
            message=f"Procesados {metrics.records_loaded:,} registros",
            total_records=metrics.records_extracted,
            processed_records=metrics.records_loaded,
            records_dropped=metrics.records_dropped,
            duration_seconds=metrics.duration_seconds
        )

        logger.info(f"[CELERY TASK {job_id}] Completed successfully: {metrics.records_loaded:,} records")

        return {
            "job_id": job_id,
            "status": "COMPLETED",
            "records_processed": metrics.records_loaded,
            "duration_seconds": metrics.duration_seconds,
        }

    except Exception as exc:
        logger.error(f"[CELERY TASK {job_id}] Failed: {str(exc)}", exc_info=True)

        # Marcar como fallido solo si ya no hay más retries
        if self.request.retries >= self.max_retries:
            redis_tracker.update_status(
                job_id,
                JobStatus.FAILED,
                message=f"Falló después de {self.max_retries + 1} intentos",
                error=str(exc)
            )
        else:
            redis_tracker.update_status(
                job_id,
                JobStatus.RETRYING,
                message=f"Reintentando... (intento {self.request.retries + 2}/{self.max_retries + 1})",
                error=str(exc)
            )

        # Propaga la excepción para que Celery maneje el retry
        raise
```

**Notas importantes:**
- ✅ **NO usamos `Task` como clase base** porque está deprecado en Celery 5.x+
- ✅ **Dependency injection** mediante funciones helper con lazy initialization
- ✅ El `EtlService.run_full_refresh()` es `async`, por eso usamos `asyncio.new_event_loop()`
- ✅ El `RedisJobTracker` maneja TODO el tracking de jobs (no `JobTracker` en memoria)
- ✅ **IMPORTANTE:** `EtlService` ya NO recibe `JobTracker` como parámetro - solo hace ETL y devuelve métricas
- ✅ La tarea de Celery (`refresh_data_task`) se encarga del tracking usando `RedisJobTracker`
- ✅ Los estados ahora son: `QUEUED → EXTRACTING → TRANSFORMING → LOADING → COMPLETED/FAILED`

**¿Por qué no heredar de `Task`?**

En Celery 5.x+, el patrón de custom Task classes (`class MyTask(Task)`) está deprecado.
El approach moderno es usar funciones normales con `@celery_app.task()` y manejar
dependencias mediante lazy initialization o funciones helper.

---

### Fase 3: Adaptar Endpoint de FastAPI

#### 3.1. Modificar `src/api/routers/refresh.py`

**Archivo actual:**
```python
from fastapi import APIRouter, status, Depends, BackgroundTasks
from ..models import response_model, job_response_model, refresh_request_model
from services.job_tracker_service import JobTracker
from services.etl_service import EtlService
from ..dependencies import get_job_tracker, get_etl_service

refresh_router = APIRouter(prefix='/refresh')

@refresh_router.post('/', response_model=response_model.ApiResponse[job_response_model.JobResponse], status_code=status.HTTP_200_OK)
def trigger_refresh(
    request_body: refresh_request_model.RefreshRequest,
    background_tasks: BackgroundTasks,  # ← Eliminar
    etl_service: EtlService = Depends(get_etl_service),  # ← Eliminar
    job_tracker: JobTracker = Depends(get_job_tracker),
):
    job = job_tracker.create_job()
    closing_date = request_body.closing_date

    # Problema: BackgroundTasks no persiste
    background_tasks.add_task(etl_service.run_full_refresh, str(job.id), closing_date)

    return response_model.ApiResponse(
        success=True,
        data=job_response_model.JobResponse(
            job_id=str(job.id),
            status=job.status,
            progress_percentage=0.0,
            message="ETL iniciado"
        ).model_dump()
    )
```

**Nuevo código:**
```python
from uuid import uuid4
from fastapi import APIRouter, status
from ..models import response_model, job_response_model, refresh_request_model
from celery_config.tasks import refresh_data_task  # ← NUEVO import
from services.redis_job_tracker import RedisJobTracker  # ← NUEVO import
from core.enums import JobStatus

refresh_router = APIRouter(prefix='/refresh')

@refresh_router.post(
    '/',
    response_model=response_model.ApiResponse[job_response_model.JobResponse],
    status_code=status.HTTP_202_ACCEPTED  # ← 202 Accepted (async processing)
)
def trigger_refresh(request_body: refresh_request_model.RefreshRequest):
    """
    Encola un job de refresh de datos en Celery.
    Retorna inmediatamente con el job_id para tracking vía WebSocket o polling.
    """
    # Generar job_id único
    job_id = str(uuid4())

    # Crear job en Redis
    redis_tracker = RedisJobTracker()
    redis_tracker.create_job(job_id)

    # Encolar en Celery (retorna inmediatamente)
    refresh_data_task.apply_async(
        args=[job_id, request_body.closing_date],
        task_id=job_id,  # Usar mismo ID para tracking
    )

    return response_model.ApiResponse(
        success=True,
        data=job_response_model.JobResponse(
            job_id=job_id,
            status=JobStatus.QUEUED.value,  # ← Nuevo estado
            progress_percentage=0.0,
            message="Job encolado en Celery. Conecta a WebSocket ws://host/ws/progress/{job_id} para actualizaciones."
        ).model_dump()
    )
```

**Cambios clave:**
1. ❌ **Eliminado:** `BackgroundTasks`, `etl_service`, `job_tracker` dependencies
2. ✅ **Agregado:** `refresh_data_task.apply_async()`, `RedisJobTracker`
3. ✅ **Status HTTP:** 202 Accepted (indica procesamiento asíncrono)
4. ✅ **Response:** Status "queued" en vez de "pending"
5. ✅ **Job ID:** Generado manualmente con `uuid4()`

---

### Fase 4: Implementar WebSocket para Progress Tracking

**Nota:** El `RedisJobTracker` ya fue creado en la Fase 2.2, ahora implementamos el WebSocket handler.

#### 4.1. Crear WebSocket Handler

**Archivo:** `src/websocket/__init__.py`
```python
# Empty file for Python package
```

**Archivo:** `src/websocket/progress_handler.py`
```python
from fastapi import WebSocket, WebSocketDisconnect
from redis import Redis
import asyncio
import json
from src.core.config import settings
from src.utils.logger import logger


async def progress_websocket(websocket: WebSocket, job_id: str):
    """
    WebSocket endpoint que escucha Redis pub/sub para progress updates.
    Frontend se conecta a: ws://localhost:8000/ws/progress/{job_id}

    Args:
        websocket: Conexión WebSocket
        job_id: ID del job a monitorear
    """
    await websocket.accept()
    logger.info(f"[WEBSOCKET] Client connected for job {job_id}")

    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    pubsub = redis.pubsub()
    pubsub.subscribe(f"progress:{job_id}")

    try:
        # Enviar estado inicial
        initial_state = redis.get(f"job:{job_id}")
        if initial_state:
            await websocket.send_json(json.loads(initial_state))
            logger.debug(f"[WEBSOCKET] Sent initial state for job {job_id}")
        else:
            await websocket.send_json({
                "error": "Job not found",
                "job_id": job_id
            })
            return

        # Escuchar updates de Redis pub/sub
        while True:
            message = pubsub.get_message(timeout=1.0)

            if message and message['type'] == 'message':
                data = json.loads(message['data'])
                await websocket.send_json(data)
                logger.debug(f"[WEBSOCKET] Sent update for job {job_id}: {data['status']}")

                # Cerrar si terminó (completado o fallido)
                if data.get('status') in ['COMPLETED', 'FAILED', 'CANCELLED']:
                    logger.info(f"[WEBSOCKET] Job {job_id} finished with status {data['status']}")
                    break

            await asyncio.sleep(0.1)

    except WebSocketDisconnect:
        logger.info(f"[WEBSOCKET] Client disconnected for job {job_id}")

    except Exception as e:
        logger.error(f"[WEBSOCKET] Error for job {job_id}: {str(e)}")

    finally:
        pubsub.unsubscribe(f"progress:{job_id}")
        await websocket.close()
        logger.info(f"[WEBSOCKET] Connection closed for job {job_id}")
```

#### 4.2. Registrar WebSocket en `src/main.py`

**Modificar `src/main.py`** - Agregar al final antes de `if __name__ == '__main__':`

```python
# ... imports existentes ...
from fastapi import FastAPI, Request, WebSocket
from websocket.progress_handler import progress_websocket  # ← NUEVO import

app = FastAPI(title=settings.app_name)

# ... middleware, exception handlers, routers existentes (mantener) ...

# WebSocket para progress tracking (NUEVO - agregar al final)
@app.websocket("/ws/progress/{job_id}")
async def websocket_endpoint(websocket: WebSocket, job_id: str):
    """
    WebSocket endpoint para monitorear progreso de jobs en tiempo real.
    Frontend conecta a: ws://localhost:8000/ws/progress/{job_id}
    """
    await progress_websocket(websocket, job_id)

if __name__ == '__main__':
    uvicorn.run(app, port=settings.port)
```

---

### Fase 5: Agregar Endpoints de Monitoring

#### 5.1. Actualizar `src/api/routers/job.py`

**Archivo actual:**
```python
from fastapi import APIRouter, status
from ..models import response_model, job_model

job_router = APIRouter(prefix='/jobs')

@job_router.get('/', response_model=response_model.ApiResponse[list[job_model.Job]], status_code=status.HTTP_200_OK)
def get_jobs():
    return response_model.ApiResponse(
        success=True,
        message="List of jobs"
    )
```

**Nuevo código completo:**
```python
from fastapi import APIRouter, HTTPException, status
from ..models import response_model, job_model
from services.redis_job_tracker import RedisJobTracker
from celery_config.celery_app import celery_app
from utils.logger import logger

job_router = APIRouter(prefix='/jobs')


# GET /api/v1/jobs - Lista todos los jobs
@job_router.get(
    '/',
    response_model=response_model.ApiResponse[list],
    status_code=status.HTTP_200_OK
)
def list_recent_jobs(limit: int = 10):
    """
    Lista los últimos N jobs.

    Args:
        limit: Número máximo de jobs a retornar (default: 10)

    Returns:
        Lista de jobs ordenados por fecha de creación (más recientes primero)
    """
    tracker = RedisJobTracker()
    jobs = tracker.list_jobs(limit=limit)

    return response_model.ApiResponse(
        success=True,
        data=jobs
    )


# GET /api/v1/jobs/{job_id} - Obtiene un job específico
@job_router.get(
    '/{job_id}',
    response_model=response_model.ApiResponse[dict],
    status_code=status.HTTP_200_OK
)
def get_job_status(job_id: str):
    """
    Obtiene estado actual de un job.

    Args:
        job_id: UUID del job

    Returns:
        Información completa del job (status, progress, message, métricas, etc.)

    Raises:
        HTTPException 404: Si el job no existe
    """
    tracker = RedisJobTracker()
    job = tracker.get_job(job_id)

    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found"
        )

    return response_model.ApiResponse(
        success=True,
        data=job
    )


# POST /api/v1/jobs/{job_id}/cancel - Cancela un job
@job_router.post(
    '/{job_id}/cancel',
    response_model=response_model.ApiResponse[dict],
    status_code=status.HTTP_200_OK
)
def cancel_job(job_id: str):
    """
    Cancela un job en ejecución (revoke Celery task).

    Args:
        job_id: UUID del job a cancelar

    Returns:
        Confirmación de cancelación
    """
    try:
        # Revocar task en Celery (terminate=True mata el worker process)
        celery_app.control.revoke(job_id, terminate=True)

        # Actualizar estado en Redis
        tracker = RedisJobTracker()
        from core.enums import JobStatus
        tracker.update_status(
            job_id,
            JobStatus.CANCELLED,
            message="Job cancelado por el usuario"
        )

        logger.info(f"[API] Job {job_id} cancelled")

        return response_model.ApiResponse(
            success=True,
            data={"message": "Job cancelado exitosamente", "job_id": job_id}
        )

    except Exception as e:
        logger.error(f"[API] Error cancelling job {job_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error al cancelar job: {str(e)}"
        )
```

**Nota:** El router ya está registrado en `src/main.py` (línea 52-55), no necesitas modificarlo.

---

### Fase 6: Scripts de Deployment y Docker Compose

#### 6.1. Script para Iniciar Celery Worker

**Archivo:** `scripts/start_worker.sh`
```bash
#!/bin/bash
# Inicia Celery worker para procesar jobs de refresh de datos

PROJECT_DIR="/Users/carlosngv/Documents/Genesis/Repositories/zenit/zenit-python-service"

cd "$PROJECT_DIR"

# Activar virtual environment
source venv/bin/activate

# Crear directorio de logs si no existe
mkdir -p logs

# Agregar src/ al PYTHONPATH y ejecutar desde src/
export PYTHONPATH="${PROJECT_DIR}:${PYTHONPATH}"
cd src

# Iniciar Celery worker
celery -A celery_config.celery_app worker \
  --loglevel=info \
  --concurrency=1 \
  --max-tasks-per-child=1 \
  --logfile=../logs/celery_worker.log \
  --pidfile=../logs/celery_worker.pid

# Opciones importantes:
# --concurrency=1: Solo 1 job simultáneo (evita saturar recursos y deadlocks en DB)
# --max-tasks-per-child=1: Reinicia worker después de cada task (evita memory leaks con Pandas)
# --loglevel=info: Logs detallados para debugging
```

**Hacer ejecutable:**
```bash
chmod +x scripts/start_worker.sh
```

#### 6.2. Script para Iniciar Flower Dashboard

**Archivo:** `scripts/start_flower.sh`
```bash
#!/bin/bash
# Inicia Flower para monitorear Celery (dashboard web)

PROJECT_DIR="/Users/carlosngv/Documents/Genesis/Repositories/zenit/zenit-python-service"

cd "$PROJECT_DIR"

# Activar virtual environment
source venv/bin/activate

# Agregar src/ al PYTHONPATH y ejecutar desde src/
export PYTHONPATH="${PROJECT_DIR}:${PYTHONPATH}"
cd src

# Iniciar Flower
celery -A celery_config.celery_app flower \
  --port=5555 \
  --basic_auth=admin:admin123

# Acceso: http://localhost:5555
# Usuario: admin
# Contraseña: admin123
```

**Hacer ejecutable:**
```bash
chmod +x scripts/start_flower.sh
```

#### 6.3. Docker Compose para Desarrollo

**Archivo:** `docker-compose.dev.yml`
```yaml
version: '3.8'

services:
  redis:
    image: redis:7-alpine
    container_name: zenit-redis
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
    command: redis-server --appendonly yes
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 5
    networks:
      - zenit-network

  fastapi:
    build: .
    container_name: zenit-fastapi
    ports:
      - "8000:8000"
    env_file:
      - .env
    depends_on:
      redis:
        condition: service_healthy
    command: uvicorn src.main:app --host 0.0.0.0 --port 8000 --reload
    volumes:
      - ./src:/app/src
      - ./logs:/app/logs
    networks:
      - zenit-network

  celery_worker:
    build: .
    container_name: zenit-celery-worker
    env_file:
      - .env
    depends_on:
      redis:
        condition: service_healthy
    command: celery -A src.infrastructure.task_queue.celery_app worker --loglevel=info --concurrency=1 --max-tasks-per-child=1
    volumes:
      - ./src:/app/src
      - ./logs:/app/logs
    networks:
      - zenit-network

  flower:
    build: .
    container_name: zenit-flower
    ports:
      - "5555:5555"
    env_file:
      - .env
    depends_on:
      - redis
      - celery_worker
    command: celery -A src.infrastructure.task_queue.celery_app flower --port=5555 --basic_auth=admin:admin123
    networks:
      - zenit-network

volumes:
  redis_data:

networks:
  zenit-network:
    driver: bridge
```

**Uso:**
```bash
# Iniciar todos los servicios
docker-compose -f docker-compose.dev.yml up

# Iniciar en background
docker-compose -f docker-compose.dev.yml up -d

# Ver logs
docker-compose -f docker-compose.dev.yml logs -f celery_worker

# Detener todo
docker-compose -f docker-compose.dev.yml down
```

**Archivo:** `Dockerfile` (si no existe)
```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements
COPY requirements.txt .

# Instalar dependencias Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código fuente
COPY . .

# Exponer puertos
EXPOSE 8000 5555

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

---

## 5. Actualización de Documentación

### 5.1. Actualizar CLAUDE.md

**Sección:** Development Commands

Agregar:
```markdown
### Running the Service (con Celery)

```bash
# Terminal 1: Redis
redis-server
# O con Docker:
docker run -d -p 6379:6379 redis:7-alpine

# Terminal 2: FastAPI
uvicorn src.main:app --reload --port 8000

# Terminal 3: Celery Worker
celery -A src.infrastructure.task_queue.celery_app worker --loglevel=info --concurrency=1

# Terminal 4: Flower Dashboard (Opcional)
celery -A src.infrastructure.task_queue.celery_app flower --port=5555
# Acceso: http://localhost:5555 (admin/admin123)
```

### Usando Docker Compose (Recomendado)

```bash
# Iniciar todos los servicios
docker-compose -f docker-compose.dev.yml up

# Ver logs de Celery worker
docker-compose -f docker-compose.dev.yml logs -f celery_worker

# Acceder a Flower
http://localhost:5555 (admin/admin123)
```
```

### 5.2. Actualizar docs/CLEAN_ARCHITECTURE.md

Agregar sección:
```markdown
## Task Queue Layer (Celery)

### Ubicación
`src/infrastructure/task_queue/`

### Responsabilidad
Implementa el patrón de task queue usando Celery + Redis para jobs de larga duración (4+ minutos).

### Componentes

#### `celery_app.py`
Configuración de Celery:
- Broker: Redis
- Result backend: Redis
- Retry policy: 3 intentos con exponential backoff
- Timeout: 10 min hard limit
- Concurrency: 1 worker (evita saturar recursos)

#### `tasks.py`
Definición de Celery tasks:
- `refresh_data_task()`: Task principal de ETL
- Dependency injection manual (no puede usar FastAPI Depends)
- Manejo de errores con retry automático

### Interacción con Otras Capas

```
Presentation (API) → Infrastructure (Task Queue) → Application (Use Cases)
```

1. **API endpoint** encola task en Celery
2. **Celery worker** ejecuta task en background
3. **Task** llama a use cases (lógica de negocio)
4. **Progress updates** se publican en Redis pub/sub
5. **WebSocket** notifica al frontend en tiempo real

### Trade-offs

**Pros:**
- Jobs persistentes (recuperables ante restart)
- Retry automático con backoff
- Monitoring visual con Flower
- Escalable horizontalmente

**Cons:**
- Complejidad adicional (Redis + worker process)
- Latencia inicial de ~100ms para encolar
```

---

## 6. Archivos Afectados

### ✏️ Modificar (Archivos existentes)

| Archivo | Cambio |
|---------|--------|
| `src/api/routers/refresh.py` | Reemplazar `BackgroundTasks` por `refresh_data_task.apply_async()`, cambiar status a 202 |
| `src/api/routers/job.py` | Agregar endpoints: GET /{job_id}, POST /{job_id}/cancel |
| `src/core/config.py` | Agregar configuración de Redis y Celery (redis_url, celery_broker_url, etc.) |
| `src/core/enums.py` | Agregar estados: QUEUED, RETRYING, CANCELLED |
| `src/main.py` | Agregar WebSocket endpoint `/ws/progress/{job_id}` |
| `.env` | Agregar variables de Redis y Celery |
| `CLAUDE.md` | Agregar comandos para Celery worker, Flower, Redis |

### ➕ Crear (Archivos nuevos)

| Archivo | Descripción |
|---------|-------------|
| `requirements.txt` | Crear/actualizar con `celery[redis]`, `redis`, `flower` |
| `src/celery_config/__init__.py` | Package init |
| `src/celery_config/celery_app.py` | Configuración de Celery |
| `src/celery_config/tasks.py` | Definición del task `refresh_data_task` |
| `src/services/redis_job_tracker.py` | JobTracker que usa Redis para persistencia |
| `src/websocket/__init__.py` | Package init |
| `src/websocket/progress_handler.py` | WebSocket handler para progress updates |
| `scripts/start_worker.sh` | Script para iniciar Celery worker |
| `scripts/start_flower.sh` | Script para Flower dashboard |
| `docker-compose.dev.yml` | Orquestación de servicios (FastAPI, Redis, Celery, Flower) |
| `Dockerfile` | Imagen Docker (si no existe) |

---

## 7. Comandos de Desarrollo

### Setup Inicial

```bash
# Instalar Redis (macOS)
brew install redis
brew services start redis

# O con Docker
docker run -d -p 6379:6379 --name redis-zenit redis:7-alpine

# Activar virtual environment
source venv/bin/activate

# Instalar dependencias
pip install -r requirements.txt
```

### Ejecución (4 terminales)

**Terminal 1: Redis**
```bash
redis-server
# O si usas Homebrew services: ya está corriendo en background
```

**Terminal 2: FastAPI**
```bash
uvicorn src.main:app --reload --port 8000
```

**Terminal 3: Celery Worker**
```bash
./scripts/start_worker.sh
# O directamente (desde raíz del proyecto):
cd src && celery -A celery_config.celery_app worker --loglevel=info --concurrency=1
```

**Terminal 4: Flower (Opcional)**
```bash
./scripts/start_flower.sh
# O directamente (desde raíz del proyecto):
cd src && celery -A celery_config.celery_app flower --port=5555 --basic_auth=admin:admin123
# Acceso: http://localhost:5555
```

### Verificación de Infraestructura

```bash
# Verificar Redis está corriendo
redis-cli ping  # Debe responder: PONG

# Verificar Celery worker está activo (desde src/)
cd src && celery -A celery_config.celery_app inspect active

# Ver workers registrados
cd src && celery -A celery_config.celery_app inspect stats

# Ver tasks registrados
cd src && celery -A celery_config.celery_app inspect registered
```

### Testing End-to-End

**1. Trigger refresh (encolar job):**
```bash
curl -X POST http://localhost:8000/api/v1/refresh \
  -H "Content-Type: application/json" \
  -d '{"closing_date": "31/12/2025"}'
```

**Response esperado:**
```json
{
  "success": true,
  "data": {
    "job_id": "123e4567-e89b-12d3-a456-426614174000",
    "status": "queued",
    "progress_percentage": 0.0,
    "message": "Job encolado en Celery. Conecta a WebSocket..."
  }
}
```

**2. Consultar estado del job:**
```bash
JOB_ID="123e4567-e89b-12d3-a456-426614174000"
curl http://localhost:8000/api/v1/jobs/$JOB_ID
```

**3. Conectar a WebSocket (desde navegador o herramienta):**
```javascript
// En consola del navegador
const ws = new WebSocket('ws://localhost:8000/ws/progress/123e4567-e89b-12d3-a456-426614174000');
ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log(`Status: ${data.status}, Progress: ${data.progress}%`);
};
```

**4. Cancelar job (si está en ejecución):**
```bash
curl -X POST http://localhost:8000/api/v1/jobs/$JOB_ID/cancel
```

**5. Listar jobs recientes:**
```bash
curl http://localhost:8000/api/v1/jobs?limit=5
```

### Monitoring

```bash
# Ver logs de Celery worker
tail -f logs/celery_worker.log

# Ver logs de FastAPI
tail -f logs/fastapi.log

# Monitorear en Flower
open http://localhost:5555
```

---

## 8. Beneficios Post-Implementación

### Robustez
- ✅ Jobs persisten ante restart del servidor
- ✅ Retry automático en fallos (3 intentos con backoff)
- ✅ Timeout management (10 min hard limit)
- ✅ Ack late: reintenta si worker crash

### Observabilidad
- ✅ Flower dashboard con métricas en tiempo real
- ✅ Historial de jobs (éxitos/fallos)
- ✅ Task routing y worker stats
- ✅ Logs centralizados

### Escalabilidad
- ✅ Control de concurrencia (max 1 job simultáneo)
- ✅ Worker pool configurable
- ✅ Horizontal scaling: agregar workers en otras máquinas

### Developer Experience
- ✅ Desarrollo local con `--reload` no afecta jobs en curso
- ✅ Testing independiente de FastAPI
- ✅ Separation of concerns (Clean Architecture)

---

## 9. Costos y Trade-offs

### Costos

| Recurso | Costo |
|---------|-------|
| **Infraestructura** | Redis (~50-100 MB RAM) |
| **Procesos** | 1 worker adicional |
| **Complejidad** | ~3-5 días de implementación |
| **Learning curve** | ~2-3 días para dominar Celery basics |

### Trade-offs Aceptables

- ✅ Complejidad adicional **JUSTIFICADA** para 700k registros
- ✅ Redis es lightweight y útil para otros casos (cache, sessions)
- ✅ Patrón estándar en la industria (LinkedIn, Instagram, Mozilla usan Celery)

---

## 10. Migración Incremental (Opcional)

Si prefieres migrar gradualmente:

### Semana 1: Setup Básico
- Instalar Redis
- Configurar Celery
- Crear task básico (sin lógica compleja)
- Probar con endpoint dummy

### Semana 2: Migrar Lógica
- Mover lógica de `etl_service.run_full_refresh()` al Celery task
- Mantener ambos endpoints temporalmente (`/refresh` y `/refresh-legacy`)
- Testing comparativo

### Semana 3: Progress Tracking
- Implementar RedisJobTracker
- Agregar WebSocket handler
- Frontend actualizar para polling/WS

### Semana 4: Production Ready
- Docker Compose
- Monitoring con Flower
- Deprecar endpoint legacy

---

## 11. Alternativas NO Recomendadas

### ❌ Mantener BackgroundTasks
- Peligro de pérdida de datos
- Sin auditoría
- Sin recuperación

### ❌ Threads/asyncio manual
- Reinventar la rueda
- Sin retry logic
- Sin monitoring

### ❌ Cron job externo
- No permite trigger on-demand
- Sin progress tracking
- Menos flexible

---

## 12. Recursos Adicionales

### Documentación Oficial
- [Celery Documentation](https://docs.celeryq.dev/)
- [Flower Documentation](https://flower.readthedocs.io/)
- [Redis Documentation](https://redis.io/docs/)

### Tutoriales Recomendados
- [FastAPI + Celery Integration](https://testdriven.io/blog/fastapi-and-celery/)
- [Celery Best Practices](https://docs.celeryq.dev/en/stable/userguide/tasks.html#best-practices)

### Monitoreo en Producción
- Flower dashboard: `http://localhost:5555`
- Redis monitoring: `redis-cli --stat`
- Celery events: `celery -A app events`

---

## 13. Resumen Ejecutivo

**Problema:** BackgroundTasks no es apto para procesar 700k registros (4+ min)

**Solución:** Migrar a Celery + Redis

**Impacto:**
- ✅ Jobs persistentes y recuperables
- ✅ Retry automático con exponential backoff
- ✅ Monitoring visual con Flower
- ✅ Escalable a múltiples workers
- ✅ Control de concurrencia y timeout

**Esfuerzo:** ~3-5 días (setup + migración + testing)

**Riesgo:** Bajo (Celery es battle-tested, patrón estándar)

**ROI:** Alto (robustez crítica para batch processing de producción)

---

## 14. Checklist de Implementación

### Pre-implementación
- [ ] Revisar plan completo
- [ ] Obtener aprobación del equipo
- [ ] Backup de código actual
- [ ] Preparar ambiente de testing

### Fase 1: Infraestructura
- [ ] Instalar Redis: `brew install redis && brew services start redis`
- [ ] Verificar Redis: `redis-cli ping` (debe responder PONG)
- [ ] Crear/actualizar `requirements.txt` con celery, redis, flower
- [ ] Instalar dependencias: `pip install -r requirements.txt`
- [ ] Actualizar `.env` con variables REDIS_URL, CELERY_BROKER_URL, etc.
- [ ] Actualizar `src/core/enums.py` (agregar QUEUED, RETRYING, CANCELLED)
- [ ] Actualizar `src/core/config.py` (agregar redis_url, celery_broker_url, etc.)

### Fase 2: Celery Task
- [x] Crear directorio `src/celery_config/` ⚠️ NO usar `src/celery/` (conflicto con paquete)
- [x] Crear `src/celery_config/__init__.py` (vacío)
- [x] Crear `src/celery_config/celery_app.py` (configuración de Celery)
- [x] **CRÍTICO:** Importar `tasks` al final de `celery_app.py` para registrar tareas
- [x] Crear `src/services/redis_job_tracker.py` (JobTracker con Redis)
- [x] Crear `src/celery_config/tasks.py` (task refresh_data_task)
- [x] Modificar `EtlService` para eliminar dependencia de `JobTracker`
- [x] Probar task básico: `cd src && celery -A celery_config worker --loglevel=info`

### Fase 3: Adaptar Endpoint
- [x] Modificar `src/api/routers/refresh.py`
  - [ ] Eliminar imports: BackgroundTasks, etl_service
  - [ ] Agregar imports: refresh_data_task, RedisJobTracker, uuid4
  - [ ] Cambiar status code a 202 ACCEPTED
  - [ ] Reemplazar background_tasks.add_task() por apply_async()
  - [ ] Cambiar status de respuesta a "queued"

### Fase 4: Progress Tracking (WebSocket)
- [ ] Crear directorio `src/websocket/`
- [ ] Crear `src/websocket/__init__.py`
- [ ] Crear `src/websocket/progress_handler.py`
- [ ] Modificar `src/main.py` (agregar WebSocket endpoint)
- [ ] Probar WebSocket: conectar desde navegador/Postman

### Fase 5: Monitoring Endpoints
- [ ] Modificar `src/api/routers/job.py`
  - [ ] Actualizar GET /jobs (usar RedisJobTracker.list_jobs())
  - [ ] Agregar GET /jobs/{job_id} (obtener job específico)
  - [ ] Agregar POST /jobs/{job_id}/cancel (cancelar job)
- [ ] Probar endpoints con curl/Postman

### Fase 6: Scripts y Docker
- [ ] Crear directorio `scripts/`
- [ ] Crear `scripts/start_worker.sh`
- [ ] Crear `scripts/start_flower.sh`
- [ ] Hacer ejecutables: `chmod +x scripts/*.sh`
- [ ] Crear `docker-compose.dev.yml`
- [ ] Crear `Dockerfile` (si no existe)
- [ ] Probar con Docker: `docker-compose -f docker-compose.dev.yml up`

### Testing
- [ ] Test unitario de refresh_data_task
- [ ] Test de integración con Redis
- [ ] Test de endpoint /refresh
- [ ] Test de endpoint /jobs/{job_id}
- [ ] Test de WebSocket
- [ ] Test de cancelación
- [ ] Test de retry automático
- [ ] Test de timeout

### Documentación
- [ ] Actualizar CLAUDE.md
- [ ] Actualizar docs/CLEAN_ARCHITECTURE.md
- [ ] Crear README para scripts/

### Production Readiness
- [ ] Configurar Redis en producción
- [ ] Configurar Celery workers en producción
- [ ] Setup de Flower con autenticación
- [ ] Monitoring y alertas
- [ ] Plan de rollback

---

## 15. Diagrama de Flujo Completo

```
┌─────────────────────────────────────────────────────────────────────────┐
│                            FLUJO DE DATOS COMPLETO                      │
└─────────────────────────────────────────────────────────────────────────┘

1. ENQUEUE (Frontend → FastAPI)
   ┌────────────┐
   │  Frontend  │ POST /api/v1/refresh {"closing_date": "31/12/2025"}
   └──────┬─────┘
          │
          ▼
   ┌────────────────────────────────────────┐
   │  FastAPI (refresh.py)                  │
   │  1. Genera job_id = uuid4()            │
   │  2. RedisJobTracker.create_job()       │
   │  3. refresh_data_task.apply_async()    │
   │  4. Retorna 202 Accepted + job_id      │
   └──────┬─────────────────────────────────┘
          │
          ▼
   ┌────────────────┐
   │  Redis         │  job:{job_id} = {"status": "queued", ...}
   │  (Broker)      │  Lista: "celery" con mensaje del task
   └────────────────┘

2. PROCESSING (Celery Worker)
   ┌────────────────────────────────────────┐
   │  Celery Worker                         │
   │  1. Consume mensaje de Redis           │
   │  2. Ejecuta refresh_data_task()        │
   │     ├─ EtlService.run_full_refresh()   │
   │     ├─ Extract: SQL Server → Pandas    │
   │     ├─ Transform: DataCleaner          │
   │     ├─ Load: PostgreSQL                │
   │  3. Actualiza Redis cada 10k records   │
   │  4. Publica en Redis Pub/Sub           │
   └──────┬─────────────────────────────────┘
          │
          ▼
   ┌────────────────┐
   │  Redis         │  job:{job_id} = {"status": "extracting", ...}
   │  (Pub/Sub)     │  Publica en canal "progress:{job_id}"
   └────────────────┘

3. PROGRESS UPDATES (WebSocket)
   ┌────────────────────────────────────────┐
   │  WebSocket Handler                     │
   │  1. Subscribe a "progress:{job_id}"    │
   │  2. Recibe mensaje de Redis Pub/Sub    │
   │  3. Envía JSON a Frontend vía WS       │
   └──────┬─────────────────────────────────┘
          │
          ▼
   ┌────────────┐
   │  Frontend  │  Recibe: {"status": "loading", "progress": 75.3, ...}
   │  (WebSocket)│  Actualiza UI en tiempo real
   └────────────┘

4. COMPLETION
   ┌────────────────────────────────────────┐
   │  Celery Worker                         │
   │  1. ETL completado                     │
   │  2. RedisJobTracker.update_status()    │
   │     status = "completed"               │
   │  3. Publica evento final               │
   └──────┬─────────────────────────────────┘
          │
          ▼
   ┌────────────────┐
   │  Redis         │  job:{job_id} = {"status": "completed", ...}
   │                │  TTL = 7 días (auto-delete)
   └────────────────┘
          │
          ▼
   ┌────────────┐
   │  Frontend  │  WebSocket recibe "completed" → cierra conexión
   └────────────┘
```

---

## 16. Troubleshooting Común

### Problema: "Received unregistered task of type 'refresh_data'" ✅ RESUELTO

**Error completo:**
```
[2026-02-18 09:09:18,549: ERROR/MainProcess] Received unregistered task of type 'refresh_data'.
The message has been ignored and discarded.

Did you remember to import the module containing this task?
```

**Causa:** El módulo `tasks.py` nunca se importa, por lo que Celery no registra las tareas definidas con `@celery_app.task`.

**Solución:** Importar `tasks` en `celery_app.py` DESPUÉS de crear la instancia:

```python
# src/celery_config/celery_app.py
from celery import Celery

celery_app = Celery(...)
celery_app.conf.update(...)

# ⚠️ CRÍTICO: Importar al final para registrar tareas
from celery_config import tasks  # noqa: F401
```

**Explicación:**
- Cuando inicias el worker: `celery -A celery_config worker`, Celery carga `celery_config.celery_app`
- Si `celery_app.py` nunca importa `tasks.py`, las funciones con `@celery_app.task` no se ejecutan
- Al importar `tasks`, el decorador se ejecuta y registra la tarea en Celery

**Verificar la solución:**
```bash
# Ver tareas registradas
cd src && celery -A celery_config inspect registered

# Debe mostrar:
{
    'celery@hostname': ['refresh_data']
}
```

---

### Problema: KeyError al ejecutar la tarea - Job no encontrado ✅ RESUELTO

**Error:**
```python
KeyError: '927f38d0-dfd6-49c9-aa00-9afdbc7f2040'
File "src/services/etl_service.py", line 37, in run_full_refresh
    job = self.job_tracker.get_job(job_id)
File "src/services/job_tracker_service.py", line 17, in get_job
    job = self._jobs[job_id]
```

**Causa:** Tienes DOS procesos separados:
1. **Proceso FastAPI:** Crea el job y lo guarda en `JobTracker` (memoria RAM del proceso 1)
2. **Proceso Celery Worker:** Intenta leer el job de `JobTracker` (memoria RAM del proceso 2) ❌

Los procesos NO comparten memoria, por eso el job no existe en el worker.

**Solución:** Eliminar la dependencia de `JobTracker` en `EtlService` y dejar que el tracking lo haga Celery:

**1. Modificar `EtlService.__init__` (eliminar job_tracker):**
```python
# ANTES (❌)
class EtlService:
    def __init__(self, sqlserver, postgres, cleaner, job_tracker, chunk_size):
        self.job_tracker = job_tracker  # ❌ En memoria, no compartido
        ...

# DESPUÉS (✅)
class EtlService:
    def __init__(self, sqlserver, postgres, cleaner, chunk_size):
        # job_tracker eliminado completamente
        ...
```

**2. Modificar `run_full_refresh` (eliminar actualizaciones al job):**
```python
# ANTES (❌)
async def run_full_refresh(self, job_id, closing_date):
    job = self.job_tracker.get_job(job_id)  # ❌ KeyError
    job.status = JobStatus.EXTRACTING
    job.total_records = total_records
    ...

# DESPUÉS (✅)
async def run_full_refresh(self, job_id, closing_date):
    # NO usa job_tracker, solo hace ETL y devuelve métricas
    metrics = ProcessingMetrics()
    # ... lógica de ETL ...
    return metrics  # ✅ Solo devuelve métricas
```

**3. El tracking lo hace la tarea de Celery con `RedisJobTracker`:**
```python
# src/celery_config/tasks.py
@celery_app.task(bind=True, name="refresh_data")
def refresh_data_task(self, job_id: str, closing_date: str):
    redis_tracker = RedisJobTracker()  # ✅ Redis compartido entre procesos

    # Actualizar estado a EXTRACTING
    redis_tracker.update_status(job_id, JobStatus.EXTRACTING)

    # Ejecutar ETL (solo devuelve métricas, no maneja tracking)
    metrics = etl_service.run_full_refresh(job_id, closing_date)

    # Actualizar estado a COMPLETED
    redis_tracker.update_status(
        job_id,
        JobStatus.COMPLETED,
        total_records=metrics.records_extracted,
        processed_records=metrics.records_loaded
    )
```

**Arquitectura corregida:**
```
FastAPI Endpoint
  ↓ crea job en Redis con RedisJobTracker
  ↓ envía tarea a Celery

Celery Worker
  ↓ lee/actualiza job desde Redis (RedisJobTracker) ✅
  ↓ ejecuta EtlService.run_full_refresh() (sin tracking)
  ↓ recibe métricas
  ↓ actualiza job en Redis
```

**Beneficio:** Redis es la "fuente de verdad" compartida entre TODOS los procesos (FastAPI, Celery Workers, WebSocket).

---

### Problema: "cannot import name 'Celery' from 'celery'" (Conflicto de Nombres)

**Error:**
```
ImportError: cannot import name 'Celery' from 'celery'
(/path/to/src/celery/__init__.py)
```

**Causa:** Tienes un directorio llamado `celery/` en tu código, y Python lo confunde con el paquete `celery` instalado.

**Solución:**
```bash
# Renombrar directorio
mv src/celery src/celery_config

# Actualizar imports en tu código
# Cambiar: from celery.celery_app import celery_app
# Por:     from celery_config.celery_app import celery_app
```

**⚠️ IMPORTANTE:** Nunca nombres tus directorios igual que paquetes de PyPI que usas. Python prioriza módulos locales sobre instalados.

**Nombres seguros:**
- ✅ `celery_config/` - CORRECTO
- ✅ `tasks/` - CORRECTO
- ✅ `worker/` - CORRECTO
- ❌ `celery/` - CONFLICTO con paquete celery
- ❌ `redis/` - CONFLICTO con paquete redis

### Problema: "from celery import Task" está deprecado

**Error:**
```
DeprecationWarning: Importing 'Task' from 'celery' is deprecated
```

**Solución:**
Ya está implementado en el plan - NO usamos `Task` como clase base.
En su lugar, usamos funciones normales con `@celery_app.task()` y
dependency injection mediante funciones helper.

**Código correcto (Celery 5.x+):**
```python
# ✅ CORRECTO
@celery_app.task(bind=True)
def my_task(self, arg):
    service = _get_service()  # Lazy init
    return service.do_work(arg)

# ❌ INCORRECTO (deprecado)
class MyTask(Task):
    def run(self, arg):
        return self.service.do_work(arg)
```

### Problema: "ModuleNotFoundError: No module named 'celery'"

**Solución:**
```bash
source venv/bin/activate
pip install celery[redis] redis flower
```

### Problema: "redis.exceptions.ConnectionError"

**Solución:**
```bash
# Verificar Redis está corriendo
redis-cli ping

# Si no está:
brew services start redis
# O:
redis-server
```

### Problema: "Task no se ejecuta / Worker no consume"

**Diagnóstico:**
```bash
# Ver workers activos
celery -A src.celery_config.celery_app inspect active

# Ver tasks en cola
redis-cli LLEN celery

# Ver logs del worker
tail -f logs/celery_worker.log
```

**Posibles causas:**
- Worker no está corriendo
- Nombre del task incorrecto (debe ser "refresh_data")
- Worker escuchando otro broker

### Problema: "asyncio.run() cannot be called from a running event loop"

**Causa:** El task de Celery está en un contexto async.

**Solución:** Usar `asyncio.new_event_loop()` como en el código del plan.

### Problema: "Job se pierde al reiniciar FastAPI"

**Verificación:**
```bash
# Verificar job existe en Redis
redis-cli GET "job:123e4567-e89b-12d3-a456-426614174000"

# Listar todos los jobs
redis-cli KEYS "job:*"
```

**Causa esperada:** Jobs persisten en Redis, FastAPI puede reiniciar sin problemas.

### Problema: "Memory leak en Celery worker"

**Solución:** Ya configurado con `--max-tasks-per-child=1` que reinicia el worker después de cada task.

---

## 17. Notas Importantes de Producción

### Seguridad
- [ ] Cambiar credenciales de Flower: `--basic_auth=admin:CONTRASEÑA_SEGURA`
- [ ] Configurar Redis con autenticación: `REDIS_URL=redis://:password@host:6379/0`
- [ ] NO exponer Flower (puerto 5555) al público, solo red interna
- [ ] Validar permisos de admin antes de permitir trigger de refresh

### Monitoring
- [ ] Configurar alertas si un job falla más de 3 veces
- [ ] Monitorear uso de memoria del worker (Pandas con 700k records)
- [ ] Configurar log rotation para `logs/celery_worker.log`
- [ ] Dashboard de Flower: revisar métricas de tiempo de ejecución

### Performance
- [ ] Si el volumen crece a >1M registros, considerar:
  - Aumentar `CHUNK_SIZE` de 10k a 50k
  - Usar múltiples workers (pero solo 1 job simultáneo)
  - Particionar por fecha (procesar por mes)

### Backup y Recuperación
- [ ] Jobs en Redis tienen TTL de 7 días
- [ ] Para historial permanente, considerar guardar resultados en PostgreSQL
- [ ] Celery result backend puede usar PostgreSQL en vez de Redis

---

---

## 18. Cambios Implementados (2026-02-18) ✅

### Resumen de Implementación

Se completó exitosamente la migración de `BackgroundTasks` a Celery + Redis. A continuación, los cambios realizados:

### Archivos Modificados

| Archivo | Cambio Realizado |
|---------|------------------|
| `src/celery_config/celery_app.py` | ✅ Agregado import de `tasks` al final para registrar tareas en Celery |
| `src/services/etl_service.py` | ✅ Eliminada dependencia de `JobTracker` - ahora solo ejecuta ETL y devuelve métricas |
| `src/celery_config/tasks.py` | ✅ Eliminado `JobTracker` de `_get_etl_service()` |

### Problemas Resueltos

#### 1. ❌ → ✅ Error: "Received unregistered task of type 'refresh_data'"

**Causa:** El módulo `tasks.py` nunca se importaba en `celery_app.py`.

**Solución aplicada:**
```python
# src/celery_config/celery_app.py (línea final)
from celery_config import tasks  # noqa: F401
```

#### 2. ❌ → ✅ Error: KeyError - Job no encontrado en JobTracker

**Causa:** `EtlService` usaba `JobTracker` en memoria, que NO se comparte entre el proceso de FastAPI y el proceso de Celery Worker.

**Solución aplicada:**

**Cambio 1:** Modificada firma de `EtlService.__init__`:
```python
# ANTES
def __init__(self, sqlserver, postgres, cleaner, job_tracker, chunk_size)

# DESPUÉS
def __init__(self, sqlserver, postgres, cleaner, chunk_size)
```

**Cambio 2:** Eliminadas actualizaciones al job en `run_full_refresh`:
```python
# ANTES - actualizaba job.status, job.total_records, etc.
job = self.job_tracker.get_job(job_id)
job.status = JobStatus.EXTRACTING
...

# DESPUÉS - solo devuelve métricas
metrics = ProcessingMetrics()
# ... lógica ETL ...
return metrics
```

**Cambio 3:** El tracking ahora lo hace completamente la tarea de Celery:
```python
# src/celery_config/tasks.py
@celery_app.task(name="refresh_data")
def refresh_data_task(self, job_id, closing_date):
    redis_tracker = RedisJobTracker()  # ✅ Compartido entre procesos

    redis_tracker.update_status(job_id, JobStatus.EXTRACTING)
    metrics = etl_service.run_full_refresh(job_id, closing_date)
    redis_tracker.update_status(
        job_id,
        JobStatus.COMPLETED,
        total_records=metrics.records_extracted
    )
```

### Arquitectura Final

```
┌─────────────────────────────────────────────────────────────────┐
│                    ARQUITECTURA IMPLEMENTADA                    │
└─────────────────────────────────────────────────────────────────┘

FastAPI Endpoint (/api/v1/refresh)
  ↓
  1. Crea job_id con uuid4()
  2. RedisJobTracker.create_job(job_id)  ← Guarda en Redis
  3. refresh_data_task.apply_async()     ← Encola en Celery
  4. Retorna 202 Accepted + job_id
  ↓
Redis (job:{job_id})
  ↓ {"status": "queued", "progress": 0, ...}
  ↓
Celery Worker
  ↓
  1. RedisJobTracker.update_status() → EXTRACTING
  2. EtlService.run_full_refresh()   → Solo ETL, devuelve métricas
  3. RedisJobTracker.update_status() → COMPLETED
  ↓
Redis (job:{job_id})
  ↓ {"status": "completed", "records_loaded": 700000, ...}
  ↓
WebSocket (/ws/progress/{job_id})
  ↓ Notifica al frontend en tiempo real
```

### Estado Actual del Sistema

✅ **Funcional:**
- Tareas se registran correctamente en Celery
- Jobs persisten en Redis (compartidos entre procesos)
- Tracking funciona con `RedisJobTracker`
- Celery worker ejecuta tareas exitosamente
- Retry automático configurado (3 intentos)

🔄 **Pendiente (según plan original):**
- [ ] WebSocket para progress tracking en tiempo real
- [ ] Endpoints de cancelación (`POST /jobs/{job_id}/cancel`)
- [ ] Scripts de deployment (`start_worker.sh`, `start_flower.sh`)
- [ ] Docker Compose para desarrollo
- [ ] Configuración de Flower dashboard

### Comandos para Desarrollo

```bash
# Terminal 1: Redis
redis-server

# Terminal 2: FastAPI
cd /path/to/zenit-python-service
source venv/bin/activate
uvicorn src.main:app --reload --port 8000

# Terminal 3: Celery Worker
cd /path/to/zenit-python-service
source venv/bin/activate
cd src
celery -A celery_config worker --loglevel=info

# Verificar tareas registradas
cd src && celery -A celery_config inspect registered
```

### Próximos Pasos Recomendados

1. **Implementar WebSocket** para progress tracking en tiempo real
2. **Agregar endpoint de cancelación** (`POST /jobs/{job_id}/cancel`)
3. **Configurar Flower** para monitoring visual
4. **Crear scripts de deployment** para facilitar inicio de servicios
5. **Testing end-to-end** con volumen real de datos (~700k registros)

---

**Fin del Documento**
