# zenit-python-service

Servicio FastAPI de ETL para la plataforma ZENIT (Fundación Génesis Empresarial). Extrae registros financieros desde SQL Server, los limpia y transforma, y carga el resultado en PostgreSQL para análisis geoespacial y clustering. El procesamiento en segundo plano lo maneja Celery con Redis como broker.

---

## Requisitos

- Python 3.11+
- Redis (local o remoto)
- PostgreSQL con PostGIS y las tablas destino ya creadas (ver sección **Base de datos**)
- SQL Server con ODBC Driver 18 for SQL Server
- Archivo `sectores.csv` accesible en la ruta configurada

---

## Configuración

### 1. Crear y activar el entorno virtual

```bash
python3 -m venv venv
source venv/bin/activate  # macOS / Linux
```

### 2. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 3. Configurar variables de entorno

```bash
cp .example.env .env
```

Editar `.env` con los valores reales:

```env
PORT=3666
API_VERSION=v1

# SQL Server — fuente principal de datos ETL y catálogo de promotores
SQLSERVER_HOST=host_sqlserver
SQLSERVER_PORT=1433
SQLSERVER_USER=usuario
SQLSERVER_PASSWORD=contrasena
SQLSERVER_DATABASE=DBI_ECODIGITAL
SQLSERVER_DRIVER=ODBC Driver 18 for SQL Server
SQLSERVER_POOL_SIZE=5
SQLSERVER_POOL_MAX_OVERFLOW=10
SQLSERVER_POOL_TIMEOUT=30
SQLSERVER_POOL_RECYCLE=3600

# PostgreSQL — base de datos destino
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=usuario
POSTGRES_PASSWORD=contrasena
POSTGRES_DATABASE=zenit_db
POSTGRES_ETL_TABLE=creditos_historico_etl

# Procesamiento
CHUNK_SIZE=10000

# Sectores CSV (temporal — migración a DB2 pendiente)
SECTORES_CSV_PATH=/ruta/absoluta/al/archivo/sectores.csv

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0
REDIS_URL=redis://localhost:6379/0

# Celery
CELERY_BROKER_URL=redis://localhost:6379/0
CELERY_RESULT_BACKEND=redis://localhost:6379/0
CELERY_TASK_TIME_LIMIT=600
CELERY_TASK_SOFT_TIME_LIMIT=540
```

---

## Base de datos

Ejecutar los scripts SQL en orden antes de levantar el servicio. Todos viven en `../database/` (raíz del monorepo).

### Habilitar PostGIS

```sql
CREATE EXTENSION IF NOT EXISTS postgis;
```

### Tabla principal ETL

```sql
ALTER TABLE "public"."creditos_historico_etl"
ADD COLUMN IF NOT EXISTS "tiene_coordenadas" boolean;
```

> Si la tabla no existe aún, crearla con la definición completa que incluye esta columna.

### Tablas de sectores (puntos y polígonos)

```bash
psql -d zenit_db -f ../database/01_sectores_puntos_raw.sql
psql -d zenit_db -f ../database/02_sectores_geometria.sql
psql -d zenit_db -f ../database/03_fn_build_sector_polygons.sql
```

---

## Levantar el servicio

Todos los comandos se ejecutan desde la raíz del proyecto (`zenit-python-service/`). El prefijo `PYTHONPATH=src` es obligatorio porque todas las importaciones internas son relativas a `src/`.

### Terminal 1 — Redis

```bash
brew services start redis   # macOS con Homebrew
# o
redis-server
```

### Terminal 2 — Worker Celery

```bash
 PYTHONPATH=src celery -A celery_config.celery_app worker --loglevel=info --concurrency=1 --max-tasks-per-child=1
```

### Terminal 3 — Servidor FastAPI

```bash
PYTHONPATH=src uvicorn src.main:app --reload --port 3666
```

La documentación interactiva queda disponible en `http://localhost:3666/docs`.

---

## Endpoints

| Método | Ruta | Descripción |
|--------|------|-------------|
| `GET` | `/api/v1/health/check-health` | Verificación de salud del servicio |
| `POST` | `/api/v1/refresh/` | Disparar el job ETL completo (últimas 3 fechas de cierre desde SQL Server) |
| `GET` | `/api/v1/jobs/` | Listar jobs recientes (`?limit=10`) |
| `GET` | `/api/v1/jobs/{job_id}` | Consultar estado de un job por ID |
| `WS` | `/api/v1/ws/progress/{job_id}` | Progreso del job en tiempo real via WebSocket |
| `POST` | `/api/v1/promotores/refresh` | Recargar catálogo de promotores desde SQL Server |
| `POST` | `/api/v1/sectores/refresh` | Cargar puntos de sectores desde CSV y generar polígonos PostGIS |

Todas las respuestas HTTP usan el envelope `ApiResponse[T]`:

```json
{
  "success": true,
  "data": { ... },
  "message": "...",
  "error": null
}
```

---

## Flujo ETL principal (`POST /api/v1/refresh/`)

```
POST /api/v1/refresh/
    Genera job_id (UUID)
    Crea job en Redis (QUEUED)
    Encola tarea Celery
    Retorna job_id de inmediato

Worker Celery (segundo plano):
    EXTRACTING  — consulta SQL Server por las últimas 3 fechas de cierre
                  (calculadas automáticamente con get_last_3_closing_dates())
                  extracción en chunks de CHUNK_SIZE registros
    TRANSFORMING — pipeline DataCleaner por chunk
    LOADING     — DataFrame concatenado a PostgreSQL (TRUNCATE + append)
    COMPLETED   — publica métricas en Redis pub/sub

WebSocket /api/v1/ws/progress/{job_id}:
    Se suscribe al canal Redis progress:{job_id}
    Envía cambios de estado al cliente en tiempo real
    Se cierra automáticamente al llegar a COMPLETED / FAILED / CANCELLED
```

### Pipeline de limpieza de datos (en orden)

1. Parsear coordenadas string/numérico a float (`_clean_coordinate_values`)
2. Validar límites de Guatemala (lat 13.7–17.8, lon −92.3 a −88.2) → agrega columna `tiene_coordenadas` (sin eliminar registros)
3. Convertir mora/capital_concedido a numérico; negativos → NaN → 0
4. Normalizar valores de KPI (Sana, Colocación, MORA1_30D, Mora31_60D, Mora61_90D, Castigado)
5. Deduplicar por DPI (sumar mora + capital, primer valor de coordenadas, KPI más frecuente)
6. Seleccionar solo las columnas de la tabla destino

> Todos los registros se conservan en la tabla destino, incluyendo los que tienen coordenadas inválidas o nulas. El flag `tiene_coordenadas` permite al notebook de clustering separar los dos paths de análisis (spatial join vs. asignación por código de promotor).

---

## Flujo de sectores (`POST /api/v1/sectores/refresh`)

```
POST /api/v1/sectores/refresh
    Lee sectores.csv desde SECTORES_CSV_PATH
    TRUNCATE + carga masiva en sectores_puntos_raw (216k filas, ~2714 promotores)
    Llama fn_build_sector_polygons() en PostgreSQL
        → agrupa vértices por GFORMS ordenados por id_sector
        → ST_MakePolygon exacto (cierra anillo si está abierto)
        → fallback a ST_ConvexHull para geometrías inválidas
        → almacena en sectores_geometria (GEOMETRY POLYGON, SRID 4326)
    Retorna conteo de polígonos generados
```

> La fuente de datos eventualmente se migrará a DB2 (read-only). El conector `src/connectors/db2_connector.py` está disponible como skeleton con instrucciones de activación.

---

## Estructura del proyecto

```
zenit-python-service/
├── .example.env
├── requirements.txt
├── src/
│   ├── main.py                         # App FastAPI, CORS, manejador de excepciones, WebSocket
│   ├── core/
│   │   ├── config.py                   # Pydantic Settings (todas las variables de entorno)
│   │   └── enums.py                    # JobStatus, ProcessingStage
│   ├── api/
│   │   ├── dependencies.py             # Wiring con FastAPI Depends()
│   │   ├── models/                     # Modelos Pydantic y dataclasses
│   │   └── routers/
│   │       ├── health.py
│   │       ├── refresh.py              # POST /refresh — dispara ETL
│   │       ├── job.py                  # GET /jobs, GET /jobs/{id}
│   │       ├── promotores.py           # POST /promotores/refresh
│   │       └── sectores.py             # POST /sectores/refresh
│   ├── services/
│   │   ├── etl_service.py              # Orquestación ETL (SQL Server → clean → PostgreSQL)
│   │   └── redis_job_tracker.py        # Estado de jobs + pub/sub en Redis
│   ├── connectors/
│   │   ├── sqlserver_connector.py      # Extracción desde SQL Server (ETL + promotores)
│   │   ├── postgres_connector.py       # Carga masiva en PostgreSQL
│   │   └── db2_connector.py            # Skeleton — pendiente de habilitar para sectores
│   ├── transformers/
│   │   └── data_cleaner.py             # Pipeline de limpieza con Pandas
│   ├── celery_config/
│   │   ├── celery_app.py               # Configuración de Celery
│   │   └── tasks.py                    # refresh_data_task con reintento automático
│   ├── websocket/
│   │   └── progress_handler.py         # Handler WebSocket via Redis pub/sub
│   └── utils/
│       ├── logger.py
│       ├── exceptions.py
│       └── date_utils.py               # get_last_3_closing_dates()
└── ../database/                        # Scripts SQL (raíz del monorepo)
    ├── 01_sectores_puntos_raw.sql
    ├── 02_sectores_geometria.sql
    └── 03_fn_build_sector_polygons.sql
```

---

## Notas

- El worker Celery se reinicia después de cada tarea (`max_tasks_per_child=1`) para evitar fugas de memoria con DataFrames grandes.
- Los jobs se almacenan en Redis con TTL de 7 días.
- El ETL usa estrategia TRUNCATE + append — reemplaza todo el contenido de la tabla en cada ejecución.
- Las fechas de cierre (último día de cada mes) se calculan automáticamente a partir de la fecha actual; no requieren parámetro.
- Las credenciales de SQL Server deben usar una cuenta de solo lectura. Las de PostgreSQL necesitan acceso de escritura únicamente sobre las tablas ETL, promotores y sectores.
- CORS configurado con `allow_origins=["*"]` — restringir antes de pasar a producción.
