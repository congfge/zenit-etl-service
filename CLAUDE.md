# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **FastAPI-based ETL + Clustering service** for the ZENIT platform (Fundación Génesis Empresarial). The service:
1. Extracts financial records from SQL Server, cleans them, and loads results into PostgreSQL (`creditos_historico_etl`)
2. Runs DBSCAN clustering on those records, calculates 3-month metrics, and syncs clusters to `layer_feature` for map display

**Key Characteristics:**
- Monthly batch processing (not high-frequency)
- Single admin user execution model
- Processes the last 3 closing dates automatically (last day of each of the 3 prior months)
- All records are preserved — including those with invalid coordinates (flagged via `tiene_coordenadas`)
- Clustering uses `tiene_coordenadas=True` for DBSCAN; records without coordinates are assigned to sectors by `COD_PROMOTOR` match
- Celery + Redis for background task execution with retry logic
- Real-time WebSocket progress streaming via Redis pub/sub

## Architecture

Pragmatic **3-layer Layered ETL** — NOT Clean Architecture.

### Layer Structure

```
src/
├── main.py                    # FastAPI app, CORS, exception handler, WebSocket
├── core/
│   ├── config.py              # Pydantic Settings (all env vars)
│   └── enums.py               # JobStatus (includes CLUSTERING), ProcessingStage
├── api/
│   ├── dependencies.py        # FastAPI Depends() wiring
│   ├── models/                # Pydantic + dataclass models
│   │   ├── response_model.py  # Generic ApiResponse[T]
│   │   ├── job_response_model.py
│   │   ├── job_model.py       # Job dataclass
│   │   └── processing_metrics_model.py
│   ├── handlers/
│   │   └── job_handler.py     # Query helpers for job routes
│   └── routers/
│       ├── health.py          # GET /api/v1/health/check-health
│       ├── refresh.py         # POST /api/v1/refresh
│       ├── job.py             # GET /api/v1/jobs, GET /api/v1/jobs/{job_id}
│       ├── promotores.py      # POST /api/v1/promotores/refresh
│       ├── sectores.py        # POST /api/v1/sectores/refresh
│       └── clustering.py      # POST /api/v1/clustering/run
├── services/
│   ├── etl_service.py         # ETL orchestration (Extract → Transform → Load)
│   ├── clustering_service.py  # DBSCAN clustering pipeline (replicates notebook)
│   ├── redis_job_tracker.py   # Redis-based job state + pub/sub (ACTIVE)
│   └── job_tracker_service.py # Legacy in-memory tracker (unused, keep as reference)
├── connectors/
│   ├── sqlserver_connector.py # SQL Server connection + chunked extraction
│   ├── postgres_connector.py  # PostgreSQL connection + bulk load + PostGIS functions
│   └── db2_connector.py       # DB2 skeleton (NOT enabled — see file for activation steps)
├── transformers/
│   └── data_cleaner.py        # Pandas-based cleaning pipeline
├── celery_config/
│   ├── celery_app.py          # Celery app configuration (Redis broker/backend)
│   └── tasks.py               # refresh_data_task + run_clustering_task
├── websocket/
│   └── progress_handler.py    # WebSocket handler using Redis pub/sub
└── utils/
    ├── logger.py              # Structured logging ("zenit-etl" logger)
    ├── exceptions.py          # BaseAppException hierarchy
    └── date_utils.py          # get_last_3_closing_dates()
```

### Database Scripts (monorepo root `../database/`)

```
database/
├── 01_sectores_puntos_raw.sql      # Staging table for raw sector vertices
├── 02_sectores_geometria.sql       # Destination table with PostGIS polygon geometry
├── 03_fn_build_sector_polygons.sql # PostgreSQL function to build polygons from points
├── 04_clusters_analisis.sql        # Table to store DBSCAN cluster results + metrics
└── 05_fn_sync_clusters_to_layer.sql # Function to sync clusters → layer_feature
```

---

## Data Flows

### ETL Principal — `POST /api/v1/refresh`

```
POST /api/v1/refresh
    └─ Genera job_id (UUID)
    └─ Crea job en Redis (QUEUED)
    └─ Encola refresh_data_task via Celery
    └─ Retorna JobResponse inmediatamente

Celery Worker (background):
    └─ Computa closing_dates = get_last_3_closing_dates()
    └─ Status: EXTRACTING
    │   └─ Para cada closing_date:
    │       └─ SQL Server → DataFrames en chunks (chunk_size filas)
    └─ Status: TRANSFORMING (por chunk)
    │   └─ DataCleaner.clean() — 6 pasos, sin eliminar registros
    └─ Status: LOADING
    │   └─ DataFrame concatenado → PostgreSQL (TRUNCATE + append)
    └─ Status: COMPLETED
```

### Clustering — `POST /api/v1/clustering/run`

```
POST /api/v1/clustering/run
    └─ Genera job_id (UUID)
    └─ Encola run_clustering_task via Celery
    └─ Retorna JobResponse (202 Accepted)

Celery Worker (background):
    └─ Status: CLUSTERING
    │
    ├─ Lee creditos_historico_etl (últimas 3 fechas de cierre)
    ├─ Lee sectores_geometria como GeoDataFrame
    │
    ├─ Asignación de créditos a sectores (2 rutas):
    │   ├─ tiene_coordenadas=True  → spatial join (ST_Within)
    │   └─ tiene_coordenadas=False → match COD_PROMOTOR == gforms
    │
    ├─ DBSCAN por sector (solo tiene_coordenadas=True):
    │   └─ eps=100 m (haversine), min_samples=8
    │   └─ Registros sin coords: cluster_id=-1 (no clustered)
    │
    ├─ Métricas por cluster x 3 meses:
    │   └─ count, mora_total, capital_total, tasa_mora
    │   └─ Distribución KPI: Sana, Colocación, MORA1_30D, Mora31_60D, Mora61_90D, Castigado
    │
    ├─ Consolidación temporal (_m1/_m2/_m3):
    │   └─ tendencia_mora (regresión lineal sobre 3 meses)
    │   └─ flag_volatil (cambio máximo > 10%)
    │   └─ tasa_retencion, tasa_churn
    │
    ├─ Geometrías circulares (buffer 100 m ≈ 0.0009°) alrededor del centroide
    ├─ Clasificación: MORA | CASTIGOS | COLOCACION | SANO (por KPI dominante en m3)
    │
    ├─ TRUNCATE + INSERT → clusters_analisis
    └─ fn_sync_clusters_to_layer('Clusters-Creditos') → layer_feature
    └─ Status: COMPLETED
```

### Sectores-Promotor — `POST /api/v1/sectores/refresh`

```
POST /api/v1/sectores/refresh
    └─ Lee sectores desde DB2 (timeout 120s) o CSV fallback
    └─ TRUNCATE + bulk load → sectores_puntos_raw (vértices)
    └─ fn_build_sector_polygons() → sectores_geometria (un polígono por GFORMS)
    └─ sync_sectores_to_layer_feature("Sectores-Promotor") → layer_feature
    └─ Retorna conteo de polígonos + features sincronizadas
```

---

## Tablas en PostgreSQL

| Tabla | Descripción |
|---|---|
| `creditos_historico_etl` | Créditos históricos (ETL, ~900k filas, 3 meses) |
| `catalogo_promotores` | Catálogo de promotores (unpivot de SQL Server) |
| `sectores_puntos_raw` | Vértices crudos de sectores (staging) |
| `sectores_geometria` | Polígonos de sectores por promotor (GFORMS) |
| `clusters_analisis` | Clusters DBSCAN con métricas 3 meses + geometría circular |
| `layer` | Capas del mapa (NestJS backend) |
| `layer_feature` | Features GeoJSON de cada capa |

### `clusters_analisis` — columnas principales

| Columna | Tipo | Descripción |
|---|---|---|
| `cluster_key` | TEXT UNIQUE | `"{gforms}_{cluster_id}"` |
| `gforms` | INTEGER | Código del promotor |
| `cluster_id` | INTEGER | ID DBSCAN dentro del sector |
| `geometria` | GEOMETRY(POLYGON,4326) | Círculo buffer 100 m |
| `categoria` | TEXT | MORA / CASTIGOS / COLOCACION / SANO |
| `nivel_impacto` | TEXT | ALTO / MEDIO / BAJO |
| `count_m[1-3]` | INTEGER | Clientes por mes |
| `tasa_mora_m[1-3]` | FLOAT8 | mora/capital por mes |
| `tendencia_mora` | FLOAT8 | Pendiente lineal sobre 3 meses |
| `flag_volatil` | BOOLEAN | Cambio > 10% entre meses |
| `tasa_retencion` | FLOAT8 | % clientes retenidos de m1/m2 → m3 |
| `fecha_proceso` | DATE | Fecha de ejecución del clustering |

---

## API Endpoints

| Método | Ruta | Descripción |
|--------|------|-------------|
| `GET` | `/api/v1/health/check-health` | Health check |
| `POST` | `/api/v1/refresh` | Disparar ETL (últimas 3 fechas de cierre) |
| `GET` | `/api/v1/jobs` | Listar jobs recientes (`?limit=10`) |
| `GET` | `/api/v1/jobs/{job_id}` | Estado de un job |
| `WS` | `/api/v1/ws/progress/{job_id}` | Progreso en tiempo real |
| `POST` | `/api/v1/promotores/refresh` | Recargar catálogo de promotores |
| `POST` | `/api/v1/sectores/refresh` | Generar polígonos de sectores |
| `POST` | `/api/v1/clustering/run` | Ejecutar pipeline de clustering DBSCAN |

---

## Environment Files

| Archivo | PostgreSQL | Uso |
|---------|-----------|-----|
| `.env` | `localhost` (dev local) | Desarrollo local |
| `.env.prod` | `172.17.40.7` (servidor prod) | Producción |

Las variables que **difieren** entre `.env` y `.env.prod`:
- `POSTGRES_HOST` / `POSTGRES_USER` / `POSTGRES_PASSWORD`
- `JT400_JAR_PATH` (ruta local vs. ruta servidor)
- `SECTORES_CSV_PATH` (ruta local vs. ruta servidor)

SQL Server y DB2 apuntan siempre a producción (fuentes de solo lectura).

---

## Development Commands

### Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Dependencias clave: `fastapi`, `celery`, `redis`, `sqlalchemy`, `pyodbc`, `pandas`,
`geopandas`, `scikit-learn`, `shapely`, `numpy`, `psycopg2-binary`

### Ejecución

```bash
# Terminal 1 — Redis
brew services start redis

# Terminal 2 — Celery worker
PYTHONPATH=src celery -A celery_config.celery_app worker --loglevel=info

# Terminal 3 — FastAPI
PYTHONPATH=src uvicorn src.main:app --reload --port 3666
```

### Database Setup (ejecutar una vez)

```bash
# PostGIS
psql -d zenit_db -c "CREATE EXTENSION IF NOT EXISTS postgis;"

# Tablas y funciones
psql -d zenit_db -f ../database/01_sectores_puntos_raw.sql
psql -d zenit_db -f ../database/02_sectores_geometria.sql
psql -d zenit_db -f ../database/03_fn_build_sector_polygons.sql
psql -d zenit_db -f ../database/04_clusters_analisis.sql
psql -d zenit_db -f ../database/05_fn_sync_clusters_to_layer.sql

# tiene_coordenadas en ETL table (si no existe)
psql -d zenit_db -c 'ALTER TABLE creditos_historico_etl ADD COLUMN IF NOT EXISTS tiene_coordenadas boolean;'
```

### Crear la capa "Clusters-Creditos" (una vez, como admin)

Antes de ejecutar el clustering, la capa debe existir en la tabla `layer`:

```sql
INSERT INTO layer (name, label, user_id, layer_type, total_features, style, is_active, is_public, created_at, updated_at)
VALUES (
  'Clusters-Creditos',
  'Clusters de Créditos',
  1,                    -- SYSTEM_USER_ID (ajustar al ID de admin real)
  'polygon',
  0,
  '{"fillColor":"#e74c3c","fillOpacity":0.5,"strokeColor":"#c0392b","strokeWidth":1}',
  true,
  false,
  NOW(), NOW()
);
```

---

## Data Cleaning Pipeline (`transformers/data_cleaner.py`)

**Filosofía: NO se eliminan registros.** Todos los registros de SQL Server se preservan.
Los registros con coordenadas inválidas llevan `tiene_coordenadas=False` para que el
clustering los maneje por `COD_PROMOTOR` en lugar de spatial join.

Pasos (en orden):
1. `_clean_coordinate_values()` — Parsear strings/números, redondear a 5 decimales
2. `_validate_and_flag_coordinates()` — Verificar bounds Guatemala (lat 13.7–17.8, lon -92.3 a -88.2)
3. `_clean_financial_fields()` — Convertir mora/capital a numérico, negativos → 0
4. `_standarize_kpis()` — Normalizar valores KPI
5. `_deduplicate_by_dpi()` — Consolidar múltiples créditos por cliente (sumar mora + capital)
6. `_select_table_columns()` — Filtrar a TABLE_COLUMNS (incluye `tiene_coordenadas`)

---

## Clustering Pipeline (`services/clustering_service.py`)

Replica `Clustering/zenit_clustering.ipynb`. Parámetros:

| Parámetro | Valor | Descripción |
|---|---|---|
| `DBSCAN_EPS_KM` | 0.10 | Radio en km (100 m) |
| `DBSCAN_MIN_SAMPLES` | 8 | Mínimo de puntos por cluster |
| `CLUSTER_BUFFER_DEGREES` | 0.0009° | Buffer circular ≈ 100 m en Guatemala |
| `MORA_ALTA_THRESHOLD` | 0.15 | tasa_mora > 15% → nivel ALTO |
| `MORA_MEDIA_THRESHOLD` | 0.05 | tasa_mora > 5% → nivel MEDIO |

**Clasificación de clusters:**
- `categoria` = KPI con mayor conteo en m3 (MORA, CASTIGOS, COLOCACION, SANO)
- `nivel_impacto` = basado en `tasa_mora_m3`

---

## Environment Variables

| Variable | Descripción | Difiere dev/prod |
|----------|-------------|:---:|
| `PORT` | Puerto FastAPI (3666) | No |
| `SQLSERVER_*` | SQL Server (solo lectura) | No |
| `POSTGRES_HOST/USER/PASSWORD` | PostgreSQL destino | **Sí** |
| `POSTGRES_ETL_TABLE` | Tabla ETL (`creditos_historico_etl`) | No |
| `CHUNK_SIZE` | Filas por chunk de extracción (10000) | No |
| `REDIS_*` / `CELERY_*` | Redis broker | No |
| `DB2_*` / `JT400_JAR_PATH` | IBM i AS400 (solo lectura) | **Sí (path)** |
| `SECTORES_CSV_PATH` | Ruta del CSV de sectores | **Sí** |
| `CLUSTER_LAYER_NAME` | Nombre de la capa de clusters (`Clusters-Creditos`) | No |
| `SYSTEM_USER_ID` | user_id para operaciones del sistema (1) | No |
| `INTERNAL_API_SECRET` | Token `X-Internal-Token` | No |

---

## Notas de Integración con ZENIT Platform

- **Frontend** (`../zenit-frontend`, Next.js): Dispara ETL y clustering desde el panel admin;
  monitorea progreso via WebSocket.
- **Backend** (`../zenit-backend`, NestJS): Lee `layer_feature` para renderizar capas en el mapa.
  Endpoint proxy: `POST /etl/clustering/run` → Python service.
- **Clustering notebook** (`../Clustering/zenit_clustering.ipynb`): Versión de exploración/análisis
  del pipeline. La sección `agregar_analisis_ia` está **comentada** para no consumir tokens de OpenAI
  durante pruebas.

---

## Project Status (2026-04-06)

### Implementado y funcionando
- ✅ Pipeline ETL completo: SQL Server → limpieza → PostgreSQL (3 fechas, auto-calculadas)
- ✅ `tiene_coordenadas` flag — todos los registros preservados
- ✅ Celery + Redis con reintentos (hasta 3 para ETL, 2 para clustering)
- ✅ Job state en Redis (TTL 7 días) + WebSocket en tiempo real
- ✅ Todos los endpoints API (health, refresh, jobs, promotores, sectores, clustering, WS)
- ✅ Extracción en chunks (10k por defecto)
- ✅ Pipeline de limpieza (6 pasos, sin pérdida de registros)
- ✅ Pipeline de sectores: CSV/DB2 → staging → PostGIS polygons → layer_feature
- ✅ **Pipeline de clustering DBSCAN** (nuevo):
  - Asignación a sectores (spatial join + fallback COD_PROMOTOR)
  - DBSCAN por sector (100 m, min 8 clientes)
  - Métricas 3 meses + tendencias + retención
  - Geometrías circulares por centroide
  - Clasificación MORA/CASTIGOS/COLOCACION/SANO
  - `clusters_analisis` table + `fn_sync_clusters_to_layer()`
- ✅ Tabla SQL y función PostgreSQL para clusters
- ✅ Archivos `.env` con comentarios dev/prod claros

### Pendiente / Known Issues
- ❌ `requirements.txt` vacío — instalar dependencias manualmente
- ⚠️ `POST /refresh` retorna HTTP 200 en lugar de 202 Accepted
- ⚠️ Capa "Clusters-Creditos" debe crearse manualmente (ver SQL arriba)
- ⚠️ Suite de tests no escrita (tests/ vacío)
- ⚠️ Sentry SDK instalado pero no configurado
- ⚠️ DB2 connector no habilitado (requiere jaydebeapi + JPype1 + JT400 JAR)
- ⚠️ Análisis IA (`agregar_analisis_ia`) deshabilitado en notebook y no implementado en servicio

### No en scope (fase actual)
- Análisis IA por cluster (deshabilitado, pendiente fase siguiente)
- Generación de clusters (se hace en ETL + clustering endpoints separados, no en cascada automática)
- Database migrations (ETL usa TRUNCATE + replace)
