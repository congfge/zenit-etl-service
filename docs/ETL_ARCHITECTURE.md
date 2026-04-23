# Arquitectura ETL - Guía Práctica

## 📚 Índice

1. [¿Por qué NO Clean Architecture?](#por-qué-no-clean-architecture)
2. [Arquitectura Propuesta: Layered ETL](#arquitectura-propuesta-layered-etl)
3. [Estructura de Directorios](#estructura-de-directorios)
4. [Flujo de Datos](#flujo-de-datos)
5. [Patrones y Convenciones](#patrones-y-convenciones)
6. [Ejemplo Práctico](#ejemplo-práctico)
7. [Testing Strategy](#testing-strategy)

---

## ¿Por qué NO Clean Architecture?

### Tu Proyecto ES:
- ✅ **ETL simple**: SQL Server → Transformación → PostgreSQL
- ✅ **Ejecución mensual**: No es tiempo real
- ✅ **Usuario único**: Solo admin
- ✅ **Alcance definido**: 300k registros, proceso predecible
- ✅ **Tiempo de vida**: Mantenible pero no legacy de 10 años

### Clean Architecture es EXCESIVA para:
- ❌ Proyectos donde la lógica de negocio ES la transformación de datos
- ❌ ETLs que no van a cambiar de framework cada año
- ❌ Sistemas con 1-2 desarrolladores
- ❌ Procesos batch sin lógica de dominio compleja

### Lo que SÍ necesitas:
- ✅ **Separación clara de responsabilidades**
- ✅ **Código testeable**
- ✅ **Fácil mantenimiento**
- ✅ **Menos archivos, menos abstracciones**

---

## Arquitectura Propuesta: Layered ETL

Un enfoque **pragmático de 3 capas** sin over-engineering:

```
┌─────────────────────────────────────────┐
│           API LAYER                     │  ← FastAPI endpoints, WebSocket
│  - Recibe requests                      │
│  - Valida input                         │
│  - Llama al servicio ETL                │
└──────────────┬──────────────────────────┘
               │
┌──────────────▼──────────────────────────┐
│         SERVICE LAYER                   │  ← Orquestación del ETL
│  - ETLService coordina el flujo         │
│  - JobTracker maneja estado             │
│  - Llama a connectors y transformers    │
└─────┬────────────────────────┬──────────┘
      │                        │
┌─────▼────────┐      ┌────────▼──────────┐
│ CONNECTORS   │      │  TRANSFORMERS     │
│ - SQLServer  │      │  - DataCleaner    │
│ - Postgres   │      │  - Validators     │
└──────────────┘      └───────────────────┘
```

### Principios:

1. **Pragmatismo sobre pureza**: Si una abstracción no te ahorra trabajo, no la uses
2. **Código directo**: Un archivo `etl_service.py` puede tener 200 líneas si es claro
3. **Testing enfocado**: Testea lo que puede romperse (transformers), no cada línea
4. **Configuración centralizada**: Un solo `config.py` con Pydantic Settings

---

## Estructura de Directorios

```
zenit-python-service/
├── src/
│   ├── core/                   # 🎯 Configuración y modelos base
│   │   ├── __init__.py
│   │   ├── config.py          # Settings con pydantic-settings
│   │   ├── models.py          # Pydantic models + dataclasses
│   │   └── enums.py           # JobStatus, ProcessingStage, etc.
│   │
│   ├── connectors/             # 🔌 Conexiones a bases de datos
│   │   ├── __init__.py
│   │   ├── sqlserver.py       # Clase SQLServerConnector
│   │   └── postgres.py        # Clase PostgresConnector
│   │
│   ├── transformers/           # 🧹 Lógica de limpieza y validación
│   │   ├── __init__.py
│   │   ├── data_cleaner.py    # Limpieza con pandas
│   │   └── validators.py      # Validaciones de negocio
│   │
│   ├── services/               # 🚀 Orquestación
│   │   ├── __init__.py
│   │   ├── etl_service.py     # Flujo completo Extract-Transform-Load
│   │   └── job_tracker.py     # Estado y progreso de jobs
│   │
│   ├── api/                    # 🌐 FastAPI
│   │   ├── __init__.py
│   │   ├── routes/
│   │   │   ├── __init__.py
│   │   │   ├── health.py      # GET /health
│   │   │   ├── refresh.py     # POST /refresh
│   │   │   └── jobs.py        # GET /jobs/{id}
│   │   ├── schemas.py         # Pydantic request/response models
│   │   ├── dependencies.py    # FastAPI dependencies
│   │   └── websocket.py       # WebSocket /ws/progress/{job_id}
│   │
│   ├── utils/                  # 🛠️ Utilidades
│   │   ├── __init__.py
│   │   ├── logger.py          # Configuración de logging
│   │   └── exceptions.py      # Custom exceptions
│   │
│   └── main.py                 # 🎬 Entry point de FastAPI
│
├── tests/
│   ├── __init__.py
│   ├── conftest.py            # Fixtures de pytest
│   ├── test_transformers/     # Tests de limpieza
│   │   └── test_data_cleaner.py
│   ├── test_services/         # Tests de orquestación
│   │   └── test_etl_service.py
│   └── test_api/              # Tests de endpoints
│       └── test_refresh.py
│
├── docs/
│   ├── ETL_ARCHITECTURE.md    # Este archivo
│   └── API.md                 # Documentación de endpoints
│
├── scripts/
│   └── run_etl.py             # Script CLI para ejecutar ETL manualmente
│
├── .env.example
├── .gitignore
├── requirements.txt
├── pyproject.toml             # Opcional: poetry/ruff config
└── README.md
```

---

## Directorio por Directorio

### 📁 **`core/`** - Configuración y Modelos Base

**Propósito:** Definiciones compartidas por toda la app.

#### **`core/config.py`**
```python
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # SQL Server
    sqlserver_host: str
    sqlserver_port: int = 1433
    sqlserver_user: str
    sqlserver_password: str
    sqlserver_database: str

    # PostgreSQL
    postgres_host: str
    postgres_port: int = 5432
    postgres_user: str
    postgres_password: str
    postgres_database: str

    # Processing
    chunk_size: int = 10000
    max_workers: int = 4

    # API
    api_port: int = 8000
    admin_api_key: str

    class Config:
        env_file = ".env"

settings = Settings()
```

#### **`core/models.py`**
```python
from dataclasses import dataclass, field
from datetime import datetime
from uuid import UUID, uuid4
from .enums import JobStatus

@dataclass
class Job:
    """Representa un job de procesamiento ETL"""
    id: UUID = field(default_factory=uuid4)
    status: JobStatus = JobStatus.PENDING
    total_records: int = 0
    processed_records: int = 0
    errors: int = 0
    started_at: datetime | None = None
    completed_at: datetime | None = None

    @property
    def progress_percentage(self) -> float:
        if self.total_records == 0:
            return 0.0
        return (self.processed_records / self.total_records) * 100

@dataclass
class ProcessingMetrics:
    """Métricas del proceso de limpieza"""
    records_extracted: int = 0
    records_dropped: int = 0
    records_cleaned: int = 0
    records_loaded: int = 0
    duration_seconds: float = 0.0
```

#### **`core/enums.py`**
```python
from enum import Enum

class JobStatus(str, Enum):
    PENDING = "pending"
    EXTRACTING = "extracting"
    TRANSFORMING = "transforming"
    LOADING = "loading"
    COMPLETED = "completed"
    FAILED = "failed"

class ProcessingStage(str, Enum):
    EXTRACT = "extract"
    TRANSFORM = "transform"
    LOAD = "load"
```

---

### 📁 **`connectors/`** - Conexiones a Bases de Datos

**Propósito:** Encapsular la lógica de conexión y queries básicos.

#### **`connectors/sqlserver.py`**
```python
import pandas as pd
from sqlalchemy import create_engine, text
from typing import Iterator
from ..core.config import settings

class SQLServerConnector:
    """Maneja la conexión y extracción de datos desde SQL Server"""

    def __init__(self):
        connection_string = (
            f"mssql+pyodbc://{settings.sqlserver_user}:{settings.sqlserver_password}"
            f"@{settings.sqlserver_host}:{settings.sqlserver_port}/{settings.sqlserver_database}"
            f"?driver=ODBC+Driver+17+for+SQL+Server"
        )
        self.engine = create_engine(connection_string)

    def extract_data(self, chunk_size: int = 10000) -> Iterator[pd.DataFrame]:
        """
        Extrae datos en chunks para evitar problemas de memoria.

        Yields:
            DataFrame con datos crudos de SQL Server
        """
        query = """
        SELECT
            DPI, latitud, longitud, capital_concedido, mora,
            Sana, Colocacion, MORA1_30D, MORA31_60D, MORA61_90D,
            MORA91D_MAS, Castigados
        FROM monthly_data
        WHERE fecha_proceso = (SELECT MAX(fecha_proceso) FROM monthly_data)
        """

        for chunk in pd.read_sql(query, self.engine, chunksize=chunk_size):
            yield chunk

    def get_total_records(self) -> int:
        """Cuenta el total de registros a procesar"""
        query = "SELECT COUNT(*) FROM monthly_data WHERE fecha_proceso = (SELECT MAX(fecha_proceso) FROM monthly_data)"
        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            return result.scalar()

    def close(self):
        self.engine.dispose()
```

#### **`connectors/postgres.py`**
```python
import pandas as pd
from sqlalchemy import create_engine
from ..core.config import settings

class PostgresConnector:
    """Maneja la conexión y carga de datos a PostgreSQL"""

    def __init__(self):
        connection_string = (
            f"postgresql://{settings.postgres_user}:{settings.postgres_password}"
            f"@{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_database}"
        )
        self.engine = create_engine(connection_string)

    def load_data(self, df: pd.DataFrame, table_name: str = "cleaned_data"):
        """
        Carga el DataFrame limpio a PostgreSQL.

        Args:
            df: DataFrame con datos limpios
            table_name: Nombre de la tabla destino
        """
        df.to_sql(
            table_name,
            self.engine,
            if_exists="replace",  # O 'append' dependiendo de tu caso
            index=False,
            method="multi"  # Batch insert para mejor performance
        )

    def close(self):
        self.engine.dispose()
```

---

### 📁 **`transformers/`** - Lógica de Limpieza

**Propósito:** Toda la lógica de transformación y validación de datos.

#### **`transformers/data_cleaner.py`**
```python
import pandas as pd
import numpy as np
from ..utils.logger import logger

class DataCleaner:
    """
    Limpia y transforma datos según reglas de negocio.

    Basado en zenitclustering_limpiezadata.py
    """

    # Límites geográficos de Guatemala
    LAT_MIN, LAT_MAX = 13.7, 17.8
    LON_MIN, LON_MAX = -92.3, -88.2

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Pipeline completo de limpieza.

        Args:
            df: DataFrame crudo desde SQL Server

        Returns:
            DataFrame limpio y validado
        """
        logger.info(f"Iniciando limpieza de {len(df)} registros")

        df = self._drop_nulls(df)
        df = self._validate_coordinates(df)
        df = self._clean_financial_fields(df)
        df = self._calculate_derived_fields(df)
        df = self._deduplicate_by_dpi(df)

        logger.info(f"Limpieza completada: {len(df)} registros válidos")
        return df

    def _drop_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        """Elimina registros sin KPIs o coordenadas"""
        initial_count = len(df)

        df = df.dropna(subset=['latitud', 'longitud', 'capital_concedido'])

        # Drop si todos los KPIs son null
        kpi_columns = ['Sana', 'Colocacion', 'MORA1_30D', 'MORA31_60D',
                       'MORA61_90D', 'MORA91D_MAS', 'Castigados']
        df = df.dropna(subset=kpi_columns, how='all')

        dropped = initial_count - len(df)
        logger.info(f"Eliminados {dropped} registros con nulls")
        return df

    def _validate_coordinates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Valida que las coordenadas estén dentro de Guatemala"""
        initial_count = len(df)

        df = df[
            (df['latitud'] >= self.LAT_MIN) & (df['latitud'] <= self.LAT_MAX) &
            (df['longitud'] >= self.LON_MIN) & (df['longitud'] <= self.LON_MAX)
        ]

        dropped = initial_count - len(df)
        logger.info(f"Eliminados {dropped} registros con coordenadas inválidas")
        return df

    def _clean_financial_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpia campos financieros (mora, capital)"""
        # Convertir negativos a NaN
        df.loc[df['mora'] < 0, 'mora'] = np.nan
        df.loc[df['capital_concedido'] < 0, 'capital_concedido'] = np.nan

        # Llenar NaN con 0 (depende de tu lógica de negocio)
        df['mora'] = df['mora'].fillna(0)
        df['capital_concedido'] = df['capital_concedido'].fillna(0)

        return df

    def _calculate_derived_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calcula campos derivados (tasa_mora, es_moroso, etc.)"""
        # Tasa de mora
        df['tasa_mora'] = np.where(
            df['capital_concedido'] > 0,
            df['mora'] / df['capital_concedido'],
            0
        )

        # Es moroso (si tiene mora > 0)
        df['es_moroso'] = (df['mora'] > 0).astype(int)

        # Es castigado
        df['es_castigado'] = (df['Castigados'] > 0).astype(int)

        return df

    def _deduplicate_by_dpi(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Consolida múltiples créditos por DPI:
        - Suma capital_concedido
        - Usa primeras coordenadas encontradas
        """
        initial_count = len(df)

        df = df.groupby('DPI', as_index=False).agg({
            'latitud': 'first',
            'longitud': 'first',
            'capital_concedido': 'sum',
            'mora': 'sum',
            'Sana': 'sum',
            'Colocacion': 'sum',
            'MORA1_30D': 'sum',
            'MORA31_60D': 'sum',
            'MORA61_90D': 'sum',
            'MORA91D_MAS': 'sum',
            'Castigados': 'sum',
            'tasa_mora': 'mean',  # Promedio de tasa
            'es_moroso': 'max',   # Si alguno es moroso, es moroso
            'es_castigado': 'max'
        })

        deduplicated = initial_count - len(df)
        logger.info(f"Consolidados {deduplicated} DPIs duplicados")
        return df
```

#### **`transformers/validators.py`**
```python
import pandas as pd
from typing import List, Dict

class DataValidator:
    """Validaciones de calidad de datos"""

    def validate_schema(self, df: pd.DataFrame, required_columns: List[str]) -> bool:
        """Valida que el DataFrame tenga las columnas requeridas"""
        missing = set(required_columns) - set(df.columns)
        if missing:
            raise ValueError(f"Columnas faltantes: {missing}")
        return True

    def get_quality_report(self, df: pd.DataFrame) -> Dict[str, any]:
        """Genera reporte de calidad de datos"""
        return {
            "total_records": len(df),
            "null_counts": df.isnull().sum().to_dict(),
            "duplicate_dpis": df['DPI'].duplicated().sum(),
            "coordinate_outliers": self._count_coordinate_outliers(df),
            "negative_values": self._count_negative_values(df)
        }

    def _count_coordinate_outliers(self, df: pd.DataFrame) -> int:
        """Cuenta coordenadas fuera de rango"""
        from .data_cleaner import DataCleaner
        outliers = df[
            (df['latitud'] < DataCleaner.LAT_MIN) | (df['latitud'] > DataCleaner.LAT_MAX) |
            (df['longitud'] < DataCleaner.LON_MIN) | (df['longitud'] > DataCleaner.LON_MAX)
        ]
        return len(outliers)

    def _count_negative_values(self, df: pd.DataFrame) -> Dict[str, int]:
        """Cuenta valores negativos en campos financieros"""
        return {
            "mora": (df['mora'] < 0).sum(),
            "capital_concedido": (df['capital_concedido'] < 0).sum()
        }
```

---

### 📁 **`services/`** - Orquestación del ETL

**Propósito:** Coordinar el flujo Extract → Transform → Load.

#### **`services/etl_service.py`**
```python
import asyncio
from datetime import datetime
from typing import Callable
import pandas as pd

from ..core.models import Job, ProcessingMetrics
from ..core.enums import JobStatus
from ..connectors.sqlserver import SQLServerConnector
from ..connectors.postgres import PostgresConnector
from ..transformers.data_cleaner import DataCleaner
from ..utils.logger import logger
from .job_tracker import JobTracker

class ETLService:
    """
    Servicio principal que orquesta el flujo completo del ETL.

    Maneja:
    - Extracción desde SQL Server
    - Transformación con DataCleaner
    - Carga a PostgreSQL
    - Tracking de progreso vía WebSocket
    """

    def __init__(
        self,
        sqlserver: SQLServerConnector,
        postgres: PostgresConnector,
        cleaner: DataCleaner,
        job_tracker: JobTracker,
        chunk_size: int = 10000
    ):
        self.sqlserver = sqlserver
        self.postgres = postgres
        self.cleaner = cleaner
        self.job_tracker = job_tracker
        self.chunk_size = chunk_size

    async def run_full_refresh(
        self,
        job_id: str,
        progress_callback: Callable[[int, int], None] | None = None
    ) -> ProcessingMetrics:
        """
        Ejecuta el proceso completo de ETL.

        Args:
            job_id: UUID del job para tracking
            progress_callback: Función opcional para reportar progreso

        Returns:
            ProcessingMetrics con estadísticas del proceso
        """
        job = self.job_tracker.get_job(job_id)
        job.status = JobStatus.EXTRACTING
        job.started_at = datetime.utcnow()

        metrics = ProcessingMetrics()
        start_time = datetime.utcnow()

        try:
            # EXTRACT
            logger.info(f"[Job {job_id}] Iniciando extracción desde SQL Server")
            total_records = self.sqlserver.get_total_records()
            job.total_records = total_records

            all_cleaned_chunks = []

            for chunk_idx, raw_chunk in enumerate(self.sqlserver.extract_data(self.chunk_size)):
                # TRANSFORM
                job.status = JobStatus.TRANSFORMING
                cleaned_chunk = self.cleaner.clean(raw_chunk)
                all_cleaned_chunks.append(cleaned_chunk)

                # Update progress
                job.processed_records = min((chunk_idx + 1) * self.chunk_size, total_records)
                metrics.records_extracted += len(raw_chunk)
                metrics.records_cleaned += len(cleaned_chunk)
                metrics.records_dropped += (len(raw_chunk) - len(cleaned_chunk))

                # Callback para WebSocket
                if progress_callback:
                    await progress_callback(job.processed_records, total_records)

                logger.info(f"[Job {job_id}] Procesado chunk {chunk_idx + 1}: {len(cleaned_chunk)} registros limpios")

            # LOAD
            job.status = JobStatus.LOADING
            logger.info(f"[Job {job_id}] Cargando datos a PostgreSQL")

            final_df = pd.concat(all_cleaned_chunks, ignore_index=True)
            self.postgres.load_data(final_df, table_name="cleaned_monthly_data")
            metrics.records_loaded = len(final_df)

            # Success
            job.status = JobStatus.COMPLETED
            job.completed_at = datetime.utcnow()
            metrics.duration_seconds = (job.completed_at - job.started_at).total_seconds()

            logger.info(f"[Job {job_id}] ETL completado exitosamente: {metrics.records_loaded} registros cargados")

        except Exception as e:
            job.status = JobStatus.FAILED
            job.errors += 1
            logger.error(f"[Job {job_id}] ETL falló: {str(e)}")
            raise

        finally:
            self.sqlserver.close()
            self.postgres.close()

        return metrics
```

#### **`services/job_tracker.py`**
```python
from typing import Dict
from uuid import UUID, uuid4
from ..core.models import Job
from ..utils.exceptions import JobNotFoundError

class JobTracker:
    """
    Maneja el estado de jobs en memoria.

    Para producción, considera usar Redis o una DB.
    """

    def __init__(self):
        self._jobs: Dict[str, Job] = {}

    def create_job(self) -> Job:
        """Crea un nuevo job"""
        job = Job()
        self._jobs[str(job.id)] = job
        return job

    def get_job(self, job_id: str) -> Job:
        """Obtiene un job por ID"""
        job = self._jobs.get(job_id)
        if not job:
            raise JobNotFoundError(f"Job {job_id} no encontrado")
        return job

    def list_jobs(self) -> list[Job]:
        """Lista todos los jobs"""
        return list(self._jobs.values())

    def delete_job(self, job_id: str):
        """Elimina un job del tracker"""
        if job_id in self._jobs:
            del self._jobs[job_id]
```

---

### 📁 **`api/`** - FastAPI Layer

**Propósito:** Endpoints HTTP y WebSocket.

#### **`api/routes/refresh.py`**
```python
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from ...services.etl_service import ETLService
from ...services.job_tracker import JobTracker
from ..schemas import JobResponse
from ..dependencies import get_etl_service, get_job_tracker

router = APIRouter(prefix="/api/v1", tags=["ETL"])

@router.post("/refresh", response_model=JobResponse, status_code=202)
async def trigger_refresh(
    background_tasks: BackgroundTasks,
    etl_service: ETLService = Depends(get_etl_service),
    job_tracker: JobTracker = Depends(get_job_tracker)
):
    """
    Inicia el proceso de ETL en background.

    Returns:
        202 Accepted con job_id para tracking de progreso
    """
    job = job_tracker.create_job()

    # Ejecutar ETL en background
    background_tasks.add_task(etl_service.run_full_refresh, str(job.id))

    return JobResponse(
        job_id=str(job.id),
        status=job.status,
        progress_percentage=0.0,
        message="ETL iniciado"
    )
```

#### **`api/routes/jobs.py`**
```python
from fastapi import APIRouter, Depends, HTTPException
from ...services.job_tracker import JobTracker
from ..schemas import JobResponse
from ..dependencies import get_job_tracker

router = APIRouter(prefix="/api/v1/jobs", tags=["Jobs"])

@router.get("/{job_id}", response_model=JobResponse)
async def get_job_status(
    job_id: str,
    job_tracker: JobTracker = Depends(get_job_tracker)
):
    """Obtiene el estado de un job por ID"""
    try:
        job = job_tracker.get_job(job_id)
        return JobResponse(
            job_id=str(job.id),
            status=job.status,
            progress_percentage=job.progress_percentage,
            total_records=job.total_records,
            processed_records=job.processed_records,
            errors=job.errors
        )
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))
```

#### **`api/websocket.py`**
```python
from fastapi import WebSocket, WebSocketDisconnect
from typing import Dict
import asyncio
import json

class ConnectionManager:
    """Maneja conexiones WebSocket para progreso en tiempo real"""

    def __init__(self):
        self.active_connections: Dict[str, list[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, job_id: str):
        await websocket.accept()
        if job_id not in self.active_connections:
            self.active_connections[job_id] = []
        self.active_connections[job_id].append(websocket)

    def disconnect(self, websocket: WebSocket, job_id: str):
        if job_id in self.active_connections:
            self.active_connections[job_id].remove(websocket)

    async def send_progress(self, job_id: str, data: dict):
        """Envía actualización de progreso a todos los clientes conectados"""
        if job_id in self.active_connections:
            for connection in self.active_connections[job_id]:
                await connection.send_json(data)

manager = ConnectionManager()
```

#### **`api/schemas.py`**
```python
from pydantic import BaseModel
from ..core.enums import JobStatus

class JobResponse(BaseModel):
    """Schema de respuesta para endpoints de jobs"""
    job_id: str
    status: JobStatus
    progress_percentage: float = 0.0
    total_records: int = 0
    processed_records: int = 0
    errors: int = 0
    message: str | None = None
```

#### **`api/dependencies.py`**
```python
from functools import lru_cache
from ..services.etl_service import ETLService
from ..services.job_tracker import JobTracker
from ..connectors.sqlserver import SQLServerConnector
from ..connectors.postgres import PostgresConnector
from ..transformers.data_cleaner import DataCleaner
from ..core.config import settings

@lru_cache()
def get_job_tracker() -> JobTracker:
    """Singleton de JobTracker"""
    return JobTracker()

def get_etl_service() -> ETLService:
    """Crea una nueva instancia de ETLService con sus dependencias"""
    sqlserver = SQLServerConnector()
    postgres = PostgresConnector()
    cleaner = DataCleaner()
    job_tracker = get_job_tracker()

    return ETLService(
        sqlserver=sqlserver,
        postgres=postgres,
        cleaner=cleaner,
        job_tracker=job_tracker,
        chunk_size=settings.chunk_size
    )
```

---

### 📁 **`utils/`** - Utilidades

#### **`utils/logger.py`**
```python
import logging
import sys

def setup_logger():
    """Configura el logger de la aplicación"""
    logger = logging.getLogger("zenit-etl")
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    return logger

logger = setup_logger()
```

#### **`utils/exceptions.py`**
```python
class ZenitETLException(Exception):
    """Base exception para el proyecto"""
    pass

class JobNotFoundError(ZenitETLException):
    """Job no encontrado en el tracker"""
    pass

class DataValidationError(ZenitETLException):
    """Error en validación de datos"""
    pass

class ConnectionError(ZenitETLException):
    """Error de conexión a base de datos"""
    pass
```

---

### 📁 **`main.py`** - Entry Point

```python
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .api.routes import health, refresh, jobs
from .api.websocket import manager
from .core.config import settings
from .utils.logger import logger

app = FastAPI(
    title="ZENIT ETL Service",
    description="Servicio de limpieza y transformación de datos",
    version="1.0.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:9007"],  # Frontend de ZENIT
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routes
app.include_router(health.router)
app.include_router(refresh.router)
app.include_router(jobs.router)

# WebSocket
@app.websocket("/ws/progress/{job_id}")
async def websocket_endpoint(websocket, job_id: str):
    await manager.connect(websocket, job_id)
    try:
        while True:
            await websocket.receive_text()  # Keep connection alive
    except Exception:
        manager.disconnect(websocket, job_id)

@app.on_event("startup")
async def startup_event():
    logger.info("🚀 ZENIT ETL Service iniciado")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("🛑 ZENIT ETL Service detenido")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=settings.api_port)
```

---

## Flujo de Datos

### Ejemplo: POST /api/v1/refresh

```
1. Cliente HTTP → POST /refresh
   ↓
2. API Layer (refresh.py)
   └─ Crea Job en JobTracker
   └─ Lanza ETLService en BackgroundTask
   └─ Devuelve 202 Accepted con job_id
   ↓
3. ETLService.run_full_refresh()
   │
   ├─ EXTRACT
   │  └─ SQLServerConnector.extract_data() → Chunks de DataFrame
   │
   ├─ TRANSFORM (por cada chunk)
   │  └─ DataCleaner.clean() → DataFrame limpio
   │     ├─ _drop_nulls()
   │     ├─ _validate_coordinates()
   │     ├─ _clean_financial_fields()
   │     ├─ _calculate_derived_fields()
   │     └─ _deduplicate_by_dpi()
   │
   └─ LOAD
      └─ PostgresConnector.load_data() → INSERT a PostgreSQL
   ↓
4. Job actualiza estado → COMPLETED
   ↓
5. Cliente consulta GET /jobs/{job_id} para ver resultado
   (o escucha por WebSocket para progreso en tiempo real)
```

---

## Patrones y Convenciones

### 1. **Dependency Injection con FastAPI**
Todas las dependencias se inyectan vía `Depends()`, no hay singletons globales excepto `JobTracker`.

### 2. **Procesamiento por Chunks**
Siempre usa `chunksize` al leer de SQL Server para evitar memory overflow.

```python
for chunk in pd.read_sql(query, engine, chunksize=10000):
    process(chunk)
```

### 3. **Logging Estructurado**
Incluye `job_id` en todos los logs:
```python
logger.info(f"[Job {job_id}] Mensaje descriptivo")
```

### 4. **Error Handling**
- Use custom exceptions (`ZenitETLException` hierarchy)
- Siempre cierra conexiones en `finally` blocks
- Propaga errores a FastAPI para respuestas HTTP correctas

### 5. **Configuración**
Todo configurable vía `.env`, nunca hardcodees valores:
```python
# ❌ MAL
chunk_size = 10000

# ✅ BIEN
chunk_size = settings.chunk_size
```

---

## Testing Strategy

### Tests que SÍ debes escribir:

#### **Transformers (crítico)**
```python
# tests/test_transformers/test_data_cleaner.py
def test_clean_removes_invalid_coordinates():
    df = pd.DataFrame({
        'latitud': [14.5, 50.0, 16.0],  # 50.0 fuera de Guatemala
        'longitud': [-90.0, -90.0, -90.0],
        'capital_concedido': [1000, 2000, 3000]
    })

    cleaner = DataCleaner()
    result = cleaner._validate_coordinates(df)

    assert len(result) == 2  # Solo 2 registros válidos
```

#### **Services (orquestación)**
```python
# tests/test_services/test_etl_service.py
@pytest.mark.asyncio
async def test_run_full_refresh_updates_job_status(mocker):
    # Mock de connectors
    mock_sqlserver = mocker.Mock()
    mock_postgres = mocker.Mock()

    etl = ETLService(mock_sqlserver, mock_postgres, DataCleaner(), JobTracker())

    # Assert que el estado cambia correctamente
```

### Tests que NO necesitas:

- ❌ Tests de cada línea de pandas (confía en la librería)
- ❌ Tests de FastAPI routing (FastAPI ya está testeado)
- ❌ Tests de conexión real a DB (usa mocks)

---

## Comparación con Clean Architecture

| Aspecto | Clean Architecture | Layered ETL (Propuesto) |
|---------|-------------------|-------------------------|
| **Capas** | 4 (Domain, Application, Infrastructure, Presentation) | 3 (API, Services, Connectors/Transformers) |
| **Archivos** | ~30+ archivos | ~15 archivos |
| **Abstracciones** | Interfaces para todo | Solo donde aporta valor |
| **Testing** | 100% testeable sin deps | 80% testeable (suficiente) |
| **Complejidad** | Alta | Media |
| **Tiempo setup** | 2-3 días | 4-6 horas |
| **Ideal para** | Legacy de 10+ años | ETLs batch de 1-3 años |

---

## Ventajas de Este Enfoque

### ✅ **Para tu proyecto:**
1. **Pragmático**: Menos archivos, menos abstracciones innecesarias
2. **Mantenible**: Responsabilidades claras pero sin over-engineering
3. **Testeable**: Puedes testear `DataCleaner` sin DB
4. **Escalable**: Si crece, puedes refactorizar a Clean Architecture
5. **Rápido de implementar**: Setup en horas, no días

### ✅ **Vs. Clean Architecture:**
- 50% menos archivos
- No necesitas interfaces para todo
- Código más directo y legible
- Aún puedes cambiar SQL Server → otra DB sin reescribir todo

---

## Cuando Refactorizar a Clean Architecture

Considera migrar a Clean Architecture SI:
- ✅ El proyecto crece a 10+ casos de uso
- ✅ Múltiples equipos trabajando simultáneamente
- ✅ Necesitas reutilizar la lógica de limpieza en CLI, Airflow, Celery, etc.
- ✅ Requisitos de negocio cambian cada mes

Mientras tanto, **este enfoque es suficiente y recomendado**.

---

## Ejemplo Completo: Agregar Validación Nueva

**Requisito:** Agregar validación de que `capital_concedido` no exceda $100,000

### 1. Agregar lógica al transformer
```python
# src/transformers/data_cleaner.py
def _validate_capital(self, df: pd.DataFrame) -> pd.DataFrame:
    MAX_CAPITAL = 100_000
    initial_count = len(df)
    df = df[df['capital_concedido'] <= MAX_CAPITAL]
    dropped = initial_count - len(df)
    logger.warning(f"Eliminados {dropped} registros con capital > ${MAX_CAPITAL}")
    return df
```

### 2. Agregar al pipeline
```python
def clean(self, df: pd.DataFrame) -> pd.DataFrame:
    df = self._drop_nulls(df)
    df = self._validate_coordinates(df)
    df = self._validate_capital(df)  # ← Nueva validación
    # ...
```

### 3. Test
```python
# tests/test_transformers/test_data_cleaner.py
def test_validate_capital_removes_outliers():
    df = pd.DataFrame({
        'capital_concedido': [50_000, 150_000, 80_000],
        'latitud': [14.5] * 3,
        'longitud': [-90.0] * 3
    })

    cleaner = DataCleaner()
    result = cleaner._validate_capital(df)

    assert len(result) == 2  # 150k fue eliminado
```

**¡Eso es todo!** No necesitas tocar domain, application, infrastructure, repositories, interfaces, etc.

---

## Conclusión

Para tu caso (ETL de 300k registros, ejecución mensual, admin único):

- ❌ **Clean Architecture es excesiva**
- ✅ **Layered ETL es apropiada**
- ✅ **Mantiene buenas prácticas sin complejidad innecesaria**
- ✅ **Implementable en 1-2 días vs. 1 semana**

**Próximos pasos:**
1. Implementar `core/` (config, models, enums)
2. Implementar `transformers/data_cleaner.py` (la lógica más crítica)
3. Implementar `connectors/` (SQLServer, Postgres)
4. Implementar `services/etl_service.py` (orquestación)
5. Implementar `api/` (endpoints + WebSocket)
6. Tests de `transformers/`
7. Integrar con frontend de ZENIT

**Tiempo estimado:** 2-3 días de desarrollo full-time.

---

## Referencias

- 📘 [Three-layer Architecture Pattern](https://www.oreilly.com/library/view/software-architecture-patterns/9781491971437/ch01.html)
- 📗 [FastAPI Best Practices](https://github.com/zhanymkanov/fastapi-best-practices)
- 📙 [Pandas ETL Patterns](https://pandas.pydata.org/docs/user_guide/cookbook.html)
