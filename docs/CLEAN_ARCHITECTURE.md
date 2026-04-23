# Clean Architecture - Guía Completa

## 📚 Índice

1. [¿Qué es Clean Architecture?](#qué-es-clean-architecture)
2. [Principios Fundamentales](#principios-fundamentales)
3. [Las Capas de Clean Architecture](#las-capas-de-clean-architecture)
4. [Regla de Dependencias](#regla-de-dependencias)
5. [Directorio por Directorio](#directorio-por-directorio)
6. [Flujo de Datos](#flujo-de-datos)
7. [Ventajas y Desventajas](#ventajas-y-desventajas)
8. [Ejemplo Práctico Completo](#ejemplo-práctico-completo)

---

## ¿Qué es Clean Architecture?

**Clean Architecture** es un patrón arquitectónico creado por **Robert C. Martin (Uncle Bob)** que propone organizar el código en capas concéntricas, donde las capas internas **no conocen ni dependen** de las capas externas.

### Objetivo Principal

> **Crear sistemas que sean independientes de frameworks, bases de datos, interfaces de usuario y cualquier detalle externo.**

Esto permite:
- ✅ **Testear** el negocio sin UI, DB o frameworks
- ✅ **Cambiar** tecnologías (FastAPI → Django, PostgreSQL → MongoDB) sin reescribir todo
- ✅ **Mantener** código con responsabilidades claras
- ✅ **Escalar** agregando features sin romper lo existente

---

## Principios Fundamentales

### 1. **Separation of Concerns** (Separación de Responsabilidades)
Cada módulo tiene **una única razón para cambiar**.

```
❌ MAL: Un archivo que maneja BD, validaciones, API y logs
✅ BIEN: Archivos separados para cada responsabilidad
```

### 2. **Dependency Inversion Principle** (DIP)
Las capas de alto nivel **no dependen** de las de bajo nivel. Ambas dependen de **abstracciones**.

```python
# ❌ MAL: Use case depende de implementación concreta
class RefreshData:
    def __init__(self):
        self.db = PostgreSQLDatabase()  # ⚠️ Acoplado a Postgres

# ✅ BIEN: Use case depende de interfaz
class RefreshData:
    def __init__(self, db: DatabaseInterface):  # ✅ Puede ser cualquier DB
        self.db = db
```

### 3. **Independence** (Independencia)
- 🔹 **Framework Independence**: No casarse con FastAPI/Django
- 🔹 **Database Independence**: La BD es un detalle intercambiable
- 🔹 **UI Independence**: La lógica de negocio no sabe si es REST, GraphQL o CLI
- 🔹 **External Agency Independence**: APIs externas, SMTP, etc. son detalles

### 4. **Testability**
Si puedes testear tu lógica de negocio **sin levantar FastAPI, sin conectar a BD, sin mocks complejos**, lo estás haciendo bien.

---

## Las Capas de Clean Architecture

Clean Architecture se organiza en **4 capas concéntricas**:

```
┌─────────────────────────────────────────┐
│   PRESENTATION (Framework & Drivers)    │  ← FastAPI, HTTP, WebSockets
├─────────────────────────────────────────┤
│   INFRASTRUCTURE (Interface Adapters)   │  ← PostgreSQL, SQLServer, APIs externas
├─────────────────────────────────────────┤
│   APPLICATION (Use Cases)               │  ← Casos de uso del negocio
├─────────────────────────────────────────┤
│   DOMAIN (Entities)                     │  ← Reglas de negocio puras
└─────────────────────────────────────────┘
      ↑                             ↑
      └─────── Dependencies ────────┘
           (Siempre hacia adentro)
```

### 🎯 **Capa 1: DOMAIN** (Núcleo)
**Lo que ES tu negocio.**

- **Entities**: Objetos de negocio con identidad (Usuario, Producto, Job)
- **Value Objects**: Objetos sin identidad (Dirección, Precio, JobStatus)
- **Repository Interfaces**: Contratos (ports) para acceder a datos
- **Domain Services**: Lógica de negocio que involucra múltiples entidades

**Características:**
- ✅ **CERO dependencias externas** (ni FastAPI, ni pandas, ni SQLAlchemy)
- ✅ Python puro, clases simples
- ✅ Representa las **reglas de negocio universales**

### 🔧 **Capa 2: APPLICATION** (Casos de Uso)
**Lo que HACE tu sistema.**

- **Use Cases**: Orquestan el flujo de datos entre capas (RefreshDataUseCase)
- **Application Services**: Coordinan tareas (JobTracker, NotificationService)
- **DTOs**: Objetos de transferencia de datos entre capas
- **Interfaces/Ports**: Contratos para servicios externos

**Características:**
- ✅ Depende SOLO de `domain/`
- ✅ Define **QUÉ hace el sistema**, no **CÓMO lo hace**
- ✅ Testeable sin infraestructura real

### 🏗️ **Capa 3: INFRASTRUCTURE** (Implementaciones)
**CÓMO se hacen las cosas.**

- **Persistence**: Implementaciones de repositorios (SQLServerRepository, PostgresRepository)
- **External Services**: APIs de terceros, SMTP, S3
- **Frameworks**: SQLAlchemy, pandas, httpx
- **Config**: Variables de entorno, settings

**Características:**
- ✅ Implementa las **interfaces definidas en domain/application**
- ✅ Aquí vive pandas, SQLAlchemy, requests, etc.
- ✅ Cambiable sin tocar domain/application

### 🌐 **Capa 4: PRESENTATION** (Entrega)
**Cómo el mundo INTERACTÚA con tu sistema.**

- **API Endpoints**: Rutas de FastAPI
- **Schemas**: Pydantic models para validación HTTP
- **WebSockets**: Comunicación en tiempo real
- **Middleware**: Auth, CORS, error handling
- **CLI**: Scripts de terminal

**Características:**
- ✅ Traduce HTTP/WebSocket ↔ Use Cases
- ✅ Validación de entrada (Pydantic)
- ✅ Formateo de salida (JSON, XML, etc.)

---

## Regla de Dependencias

### La Regla de Oro: **Las dependencias SOLO apuntan hacia adentro**

```
PRESENTATION → INFRASTRUCTURE → APPLICATION → DOMAIN
    ✅              ✅              ✅          🚫 No depende de nadie
```

### ✅ **PERMITIDO:**
```python
# presentation/api/endpoints/refresh.py
from ....application.use_cases.refresh_data import RefreshDataUseCase  # ✅

# infrastructure/persistence/sqlserver_repo.py
from ...domain.repositories.source_repository import SourceRepository  # ✅

# application/use_cases/refresh_data.py
from ...domain.entities.job import Job  # ✅
```

### ❌ **PROHIBIDO:**
```python
# domain/entities/job.py
from ...infrastructure.database import PostgresDB  # ❌ NUNCA!

# application/use_cases/refresh_data.py
from ...presentation.api.schemas import JobSchema  # ❌ NUNCA!
```

### ¿Cómo se comunican las capas externas con las internas?
**Mediante Dependency Injection (Inyección de Dependencias)**

```python
# domain/repositories/source_repository.py (Interface/Port)
class SourceRepository(ABC):
    @abstractmethod
    async def get_data(self): pass

# infrastructure/persistence/sqlserver_repo.py (Implementación/Adapter)
class SQLServerRepository(SourceRepository):
    async def get_data(self):
        return pd.read_sql(...)

# presentation/api/dependencies.py (DI Container)
def get_source_repo() -> SourceRepository:
    return SQLServerRepository(settings.connection_string)
```

---

## Directorio por Directorio

### 📁 **`domain/`** - Corazón del Sistema

```
domain/
├── entities/          # Objetos con identidad única
│   ├── job.py        # class Job: id, status, created_at
│   └── data_record.py
├── value_objects/     # Objetos sin identidad (inmutables)
│   ├── job_status.py # enum JobStatus: PENDING, PROCESSING...
│   └── coordinates.py
├── repositories/      # Interfaces (Ports)
│   ├── source_repository.py      # ABC para obtener datos
│   └── destination_repository.py # ABC para guardar datos
└── services/          # Lógica de negocio compleja
    └── validation_rules.py
```

**¿Qué va aquí?**
- ✅ Clases que modelan conceptos del negocio
- ✅ Reglas de negocio que NUNCA cambian (ej: "un email debe tener @")
- ✅ Interfaces para acceso a datos
- ❌ Nada de pandas, SQLAlchemy, FastAPI

**Ejemplo:**
```python
# domain/entities/job.py
@dataclass
class Job:
    id: UUID
    status: JobStatus
    total_records: int

    def mark_as_completed(self):
        if self.status != JobStatus.PROCESSING:
            raise InvalidStateTransition("Job must be processing")
        self.status = JobStatus.COMPLETED
```

---

### 📁 **`application/`** - Casos de Uso

```
application/
├── use_cases/              # Orquestadores de flujo
│   ├── refresh_data_use_case.py
│   ├── get_job_status_use_case.py
│   └── cancel_job_use_case.py
├── services/               # Servicios de aplicación
│   ├── job_tracker.py     # Tracking de jobs en memoria/cache
│   ├── notification_service.py  # Notificaciones (abstracción)
│   └── data_cleaner.py    # Interface para limpieza
└── dtos/                   # Data Transfer Objects
    └── job_dto.py         # class JobDTO (para comunicación entre capas)
```

**¿Qué va aquí?**
- ✅ **Use Cases**: Cada archivo = 1 historia de usuario
- ✅ **Application Services**: Coordinación, tracking, cache
- ✅ **DTOs**: Objetos para transferir datos (no entities del domain)
- ❌ No conoce HTTP, SQL, pandas

**Ejemplo:**
```python
# application/use_cases/refresh_data_use_case.py
class RefreshDataUseCase:
    def __init__(
        self,
        source: SourceRepository,      # ← Interface del domain
        dest: DestinationRepository,   # ← Interface del domain
        cleaner: DataCleanerService    # ← Interface de application
    ):
        self.source = source
        self.dest = dest
        self.cleaner = cleaner

    async def execute(self) -> Job:
        # Orquesta el flujo de negocio
        job = Job.create()
        data = await self.source.get_data()
        cleaned = self.cleaner.clean(data)
        await self.dest.save(cleaned)
        job.mark_as_completed()
        return job
```

---

### 📁 **`infrastructure/`** - Implementaciones Concretas

```
infrastructure/
├── persistence/
│   ├── sqlserver/
│   │   ├── connection.py        # SQLAlchemy engine
│   │   └── sqlserver_repository.py  # Implementa SourceRepository
│   └── postgres/
│       └── postgres_repository.py   # Implementa DestinationRepository
├── cleaning/
│   └── pandas_cleaner.py       # Implementa DataCleanerService
├── messaging/
│   └── websocket_manager.py    # WebSocket connections
└── config/
    └── settings.py             # Pydantic Settings
```

**¿Qué va aquí?**
- ✅ **Implementaciones reales** de las interfaces del domain/application
- ✅ SQLAlchemy, pandas, httpx, boto3, etc.
- ✅ Código que cambia si cambias de tecnología
- ❌ No tiene lógica de negocio (solo traduce)

**Ejemplo:**
```python
# infrastructure/persistence/sqlserver/sqlserver_repository.py
class SQLServerRepository(SourceRepository):  # ← Implementa la interface
    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)

    async def get_data(self) -> pd.DataFrame:
        # Implementación específica de SQL Server
        return pd.read_sql("SELECT * FROM table", self.engine)
```

---

### 📁 **`presentation/`** - Capa de Entrega

```
presentation/
├── api/
│   ├── dependencies.py         # Dependency Injection
│   ├── v1/
│   │   ├── router.py
│   │   └── endpoints/
│   │       ├── refresh.py     # POST /refresh
│   │       └── jobs.py        # GET /jobs/{id}
│   └── websocket/
│       └── progress.py         # WebSocket /ws/progress/{id}
├── middleware/
│   ├── auth.py
│   └── error_handler.py
└── schemas/                    # Pydantic models
    ├── job_schema.py          # JobResponse, JobRequest
    └── error_schema.py
```

**¿Qué va aquí?**
- ✅ **Endpoints de FastAPI** (o cualquier framework)
- ✅ **Pydantic schemas** para validación HTTP
- ✅ **Middleware** de autenticación, CORS, logging
- ✅ **Dependency Injection** setup
- ❌ No tiene lógica de negocio (solo valida y llama use cases)

**Ejemplo:**
```python
# presentation/api/v1/endpoints/refresh.py
@router.post("/refresh", response_model=JobResponse)
async def refresh_data(
    use_case: RefreshDataUseCase = Depends(get_refresh_use_case)
):
    # 1. Validación (Pydantic ya lo hizo)
    # 2. Llamar al use case
    job = await use_case.execute()
    # 3. Transformar domain entity → API response
    return JobResponse.from_entity(job)
```

---

### 📁 **`entities/`** vs **`value_objects/`**

#### **Entities** (Entidades)
- Tienen **identidad única** (ID)
- Dos instancias con el mismo ID son **el mismo objeto**
- **Mutables** (pueden cambiar estado)

```python
# domain/entities/job.py
class Job:
    id: UUID  # ← Identidad
    status: JobStatus

    def mark_as_completed(self):
        self.status = JobStatus.COMPLETED  # ← Muta estado

# Mismo job en diferentes estados
job1 = Job(id=UUID("..."), status=PENDING)
job2 = Job(id=UUID("..."), status=COMPLETED)
assert job1.id == job2.id  # Es el MISMO job
```

#### **Value Objects** (Objetos de Valor)
- **NO tienen identidad**
- Dos instancias con los mismos valores son **intercambiables**
- **Inmutables** (no cambian después de crearse)

```python
# domain/value_objects/job_status.py
class JobStatus(Enum):
    PENDING = "pending"
    COMPLETED = "completed"

# domain/value_objects/coordinates.py
@dataclass(frozen=True)  # ← Inmutable
class Coordinates:
    latitude: float
    longitude: float

coord1 = Coordinates(14.6349, -90.5069)
coord2 = Coordinates(14.6349, -90.5069)
assert coord1 == coord2  # Son IGUALES (no importa la identidad)
```

---

### 📁 **`services/`** vs **`use_cases/`**

#### **Use Cases** (Casos de Uso)
- Representan **una acción completa del usuario**
- Nombrados con **verbos**: `RefreshDataUseCase`, `CreateUserUseCase`
- Orquestan **múltiples services/repositories**
- **Una entrada, una salida**

```python
# application/use_cases/refresh_data_use_case.py
class RefreshDataUseCase:
    """
    Historia de usuario: "Como admin, quiero refrescar los datos mensuales"
    """
    async def execute(self) -> Job:
        # 1. Obtener datos
        # 2. Limpiar
        # 3. Guardar
        # 4. Notificar
        return job
```

#### **Services** (Servicios)
- **Domain Services**: Lógica de negocio que involucra múltiples entities
- **Application Services**: Utilidades de aplicación (cache, tracking, notificaciones)
- Nombrados con **sustantivos**: `PricingService`, `NotificationService`

```python
# domain/services/pricing_service.py (Domain Service)
class PricingService:
    """Lógica de negocio: calcular precio final"""
    def calculate_price(self, product: Product, user: User) -> Money:
        # Lógica de negocio compleja
        pass

# application/services/notification_service.py (Application Service)
class NotificationService:
    """Servicio de aplicación: enviar notificaciones"""
    async def notify(self, message: str):
        # Coordina notificación (email, push, websocket)
        pass
```

---

### 📁 **`dtos/`** (Data Transfer Objects)

**DTOs** son objetos simples para **transferir datos entre capas**.

```python
# application/dtos/job_dto.py
@dataclass
class JobDTO:
    """DTO para transferir info de Job entre capas"""
    job_id: str
    status: str
    progress: float

    @staticmethod
    def from_entity(job: Job) -> 'JobDTO':
        return JobDTO(
            job_id=str(job.id),
            status=job.status.value,
            progress=job.progress_percentage
        )
```

**¿Cuándo usarlos?**
- ✅ Para evitar exponer entities del domain directamente
- ✅ Para aplanar estructuras complejas
- ✅ Para transferir datos entre microservicios
- ❌ NO para validación HTTP (usa Pydantic schemas en presentation)

---

## Flujo de Datos

### Ejemplo: Usuario hace POST /refresh

```
1. HTTP REQUEST
   ↓
2. PRESENTATION Layer (FastAPI endpoint)
   └─ Valida con Pydantic schema
   └─ Inyecta dependencias
   └─ Llama al Use Case
   ↓
3. APPLICATION Layer (RefreshDataUseCase)
   └─ Orquesta el flujo
   └─ Llama a repositories y services
   ↓
4. INFRASTRUCTURE Layer
   └─ SQLServerRepository.get_data() → consulta SQL real
   └─ PandasCleaner.clean() → pandas transformations
   └─ PostgresRepository.save() → insert a PostgreSQL
   ↓
5. DOMAIN Layer
   └─ Job.mark_as_completed() → cambio de estado
   ↓
6. Respuesta vuelve por las capas
   ↓
7. HTTP RESPONSE (JSON)
```

**Flujo de dependencias:**
```
Presentation → Application → Domain ← Infrastructure
     ↓              ↓           ↑          ↑
     └──────────────┴───────────┴──────────┘
              (Todo apunta hacia Domain)
```

---

## Ventajas y Desventajas

### ✅ **Ventajas**

1. **Testabilidad extrema**
   - Testear use cases sin levantar FastAPI
   - Testear entities sin base de datos
   - Mocks simples (solo interfaces)

2. **Mantenibilidad**
   - Cambios localizados (cambiar DB solo toca infrastructure)
   - Código autodocumentado por estructura
   - Fácil onboarding de nuevos devs

3. **Flexibilidad tecnológica**
   - Cambiar FastAPI → Flask sin tocar domain/application
   - Cambiar PostgreSQL → MongoDB sin tocar use cases
   - Agregar GraphQL sin duplicar lógica

4. **Escalabilidad**
   - Agregar features sin romper existentes
   - Separar en microservicios fácilmente
   - Múltiples UIs (REST, GraphQL, CLI) compartiendo lógica

### ❌ **Desventajas**

1. **Complejidad inicial**
   - Más archivos y carpetas
   - Curva de aprendizaje para el equipo
   - Overhead para proyectos pequeños

2. **Over-engineering en proyectos simples**
   - Para un CRUD simple, puede ser excesivo
   - Scripts one-off no necesitan esta estructura

3. **Más código boilerplate**
   - Interfaces + implementaciones
   - DTOs + Entities + Schemas (3 modelos del "mismo" concepto)
   - Dependency injection manual

### ⚖️ **¿Cuándo usar Clean Architecture?**

#### ✅ **SÍ usarla cuando:**
- Proyectos medianos/grandes (> 3 meses desarrollo)
- Múltiples desarrolladores
- Requisitos cambiantes o inciertos
- Necesitas testear sin infraestructura real
- El proyecto vivirá años (legacy code prevention)
- **TU CASO**: 300k registros, clustering futuro, proceso mensual → ✅ SÍ

#### ❌ **NO usarla cuando:**
- Scripts one-off
- POCs rápidos (< 1 semana)
- Proyectos con 1 dev y <1000 líneas de código
- CRUD ultra simple sin lógica de negocio

---

## Ejemplo Práctico Completo

### Caso: Validar que un Job no pueda completarse si tiene errores

#### ❌ **SIN Clean Architecture:**
```python
# todo_en_un_archivo.py
@app.post("/complete-job/{job_id}")
async def complete_job(job_id: str, db: Session = Depends(get_db)):
    job = db.query(JobModel).filter_by(id=job_id).first()
    if job.error_count > 0:
        raise HTTPException(400, "Cannot complete job with errors")
    job.status = "completed"
    db.commit()
    return {"status": "ok"}

# Problema: ¿Cómo testeas esta regla de negocio sin levantar FastAPI + DB?
```

#### ✅ **CON Clean Architecture:**

```python
# domain/entities/job.py
class Job:
    def mark_as_completed(self):
        if self.has_errors():
            raise JobCannotBeCompletedWithErrors()  # ← Regla de negocio
        self.status = JobStatus.COMPLETED

# test_job.py (NO requiere FastAPI ni DB)
def test_job_with_errors_cannot_be_completed():
    job = Job(id=uuid4(), status=JobStatus.PROCESSING, error_count=5)

    with pytest.raises(JobCannotBeCompletedWithErrors):
        job.mark_as_completed()  # ✅ Test puro, sin mocks
```

```python
# application/use_cases/complete_job_use_case.py
class CompleteJobUseCase:
    def __init__(self, repo: JobRepository):
        self.repo = repo

    async def execute(self, job_id: UUID) -> Job:
        job = await self.repo.get_by_id(job_id)
        job.mark_as_completed()  # ← Llama a lógica del domain
        await self.repo.save(job)
        return job
```

```python
# presentation/api/v1/endpoints/jobs.py
@router.post("/{job_id}/complete")
async def complete_job(
    job_id: UUID,
    use_case: CompleteJobUseCase = Depends(get_complete_job_use_case)
):
    try:
        job = await use_case.execute(job_id)
        return JobResponse.from_entity(job)
    except JobCannotBeCompletedWithErrors as e:
        raise HTTPException(400, str(e))
```

**Beneficio:** La regla "no completar con errores" vive en `domain/`, testeada sin FastAPI, reutilizable en CLI/GraphQL/workers.

---

## Resumen Visual

```
┌─────────────────────────────────────────────────────────────┐
│                     PRESENTATION                            │
│  FastAPI endpoints, Pydantic schemas, WebSockets, CLI       │
│  ▸ Valida entrada                                           │
│  ▸ Llama use cases                                          │
│  ▸ Formatea salida                                          │
└──────────────────┬──────────────────────────────────────────┘
                   │ depends on
┌──────────────────▼──────────────────────────────────────────┐
│                   INFRASTRUCTURE                            │
│  SQLAlchemy, pandas, httpx, boto3, Redis                    │
│  ▸ Implementa interfaces del domain                         │
│  ▸ Detalles técnicos (SQL queries, API calls)               │
└──────────────────┬──────────────────────────────────────────┘
                   │ depends on
┌──────────────────▼──────────────────────────────────────────┐
│                    APPLICATION                              │
│  Use cases, application services, DTOs                      │
│  ▸ Orquesta flujo de negocio                                │
│  ▸ Coordina entre domain e infrastructure                   │
└──────────────────┬──────────────────────────────────────────┘
                   │ depends on
┌──────────────────▼──────────────────────────────────────────┐
│                      DOMAIN                                 │
│  Entities, value objects, repository interfaces             │
│  ▸ Reglas de negocio puras                                  │
│  ▸ CERO dependencias externas                               │
│  ▸ El corazón del sistema                                   │
└─────────────────────────────────────────────────────────────┘

REGLA: Las flechas SIEMPRE apuntan hacia abajo (hacia domain)
```

---

## Conclusión

**Clean Architecture** es una inversión a largo plazo:
- **Corto plazo**: Más código, más archivos
- **Largo plazo**: Sistema mantenible, testeable, flexible

Para tu caso (**limpieza de 300k registros + clustering futuro**):
- ✅ **Domain**: Entities (Job, ClusterResult), reglas de validación
- ✅ **Application**: RefreshDataUseCase, ClusteringUseCase
- ✅ **Infrastructure**: SQLServerRepository, PandasCleaner, ScikitLearnClusterer
- ✅ **Presentation**: FastAPI endpoints, WebSocket progress

**Siguiente paso:** Implementar capa por capa, empezando por `domain/` (sin dependencias), luego `application/`, luego `infrastructure/`, y finalmente `presentation/`.

---

## Referencias

- 📘 [The Clean Architecture (Uncle Bob)](https://blog.cleancoder.com/uncle-bob/2012/08/13/the-clean-architecture.html)
- 📗 [Hexagonal Architecture (Ports & Adapters)](https://alistair.cockburn.us/hexagonal-architecture/)
- 📙 [DDD (Domain-Driven Design) - Eric Evans](https://www.domainlanguage.com/ddd/)
- 📕 [Clean Architecture Book - Robert C. Martin](https://www.amazon.com/Clean-Architecture-Craftsmans-Software-Structure/dp/0134494164)
