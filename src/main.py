import uuid
import uvicorn

from fastapi import FastAPI, Request, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from utils.logger import logger, correlation_id_var
from core.config import settings
from api.models.response_model import ApiResponse
from websocket.progress_handler import progress_websocket
from api.routers import health, job, refresh, promotores, sectores, clustering, clientes, sucursales
# from api.dependencies import get_query_token, get_token_header
from utils.exceptions import BaseAppException


# TODO: Base.metadata.create_all(bind=engine)

app = FastAPI(
    title=settings.app_name
)

@app.middleware("http")
async def correlation_id_middleware(request: Request, call_next):
    cid = request.headers.get("X-Correlation-ID", str(uuid.uuid4()))
    token = correlation_id_var.set(cid)
    response = await call_next(request)
    response.headers["X-Correlation-ID"] = cid
    correlation_id_var.reset(token)
    return response


_allowed_origins = [o.strip() for o in settings.cors_allowed_origins.split(",")]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type", "X-Internal-Token"],
)


# ? Global exception handler
@app.exception_handler(BaseAppException)
async def app_exception_handler(request: Request, exc: BaseAppException):
    logger.error("Application error: %s", exc.message)
    return JSONResponse(
        status_code=exc.status_code,
        content=ApiResponse(
            success=False,
            error=exc.message
        ).model_dump()
    )
    
# TODO: Register routes
app.include_router(health.health_router, prefix=f"/api/{settings.api_version}", tags=['health'])
app.include_router(
job.job_router, 
prefix=f"/api/{settings.api_version}", 
# dependencies=[Depends(get_token_header)],
tags=['jobs'])
app.include_router(
refresh.refresh_router,
prefix=f"/api/{settings.api_version}",
# dependencies=[Depends(get_token_header)],
tags=['refresh'])
app.include_router(promotores.promotores_router, prefix=f"/api/{settings.api_version}", tags=['promotores'])
app.include_router(sectores.sectores_router, prefix=f"/api/{settings.api_version}", tags=['sectores'])
app.include_router(clustering.clustering_router, prefix=f"/api/{settings.api_version}", tags=['clustering'])
app.include_router(clientes.clientes_router, prefix=f"/api/{settings.api_version}", tags=['clientes'])
app.include_router(sucursales.sucursales_router, prefix=f"/api/{settings.api_version}", tags=['sucursales'])



# ? Websocket
@app.websocket("/api/"+ settings.api_version +"/ws/progress/{job_id}")
async def websocket_endpoint(websocket: WebSocket, job_id: str):
    await progress_websocket(websocket, job_id)

if __name__ == '__main__':
    uvicorn.run(app, port=settings.port)