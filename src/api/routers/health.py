import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from fastapi import APIRouter, status
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from ..models.response_model import ApiResponse
from core.config import settings
from utils.logger import logger

health_router = APIRouter(prefix='/health')


@health_router.get('/check-health', response_model=ApiResponse[None], status_code=status.HTTP_200_OK)
def check_health():
    return ApiResponse(success=True, message="Server up and running")


@health_router.get('/databases', status_code=status.HTTP_200_OK)
def check_all_databases():
    """Verifica la conectividad de todas las bases de datos en uso."""
    results = {
        "sqlserver": _check_sqlserver(),
        "postgres":  _check_postgres(),
        "db2":       _check_db2(),
    }
    all_ok = all(r["status"] == "ok" for r in results.values())
    return ApiResponse(
        success=all_ok,
        data=results,
        message="Todas las conexiones OK" if all_ok else "Una o más conexiones fallaron"
    )


@health_router.get('/databases/sqlserver', status_code=status.HTTP_200_OK)
def check_sqlserver():
    result = _check_sqlserver()
    return ApiResponse(
        success=result["status"] == "ok",
        data=result,
        message=f"SQL Server: {result['status']}"
    )


@health_router.get('/databases/postgres', status_code=status.HTTP_200_OK)
def check_postgres():
    result = _check_postgres()
    return ApiResponse(
        success=result["status"] == "ok",
        data=result,
        message=f"PostgreSQL: {result['status']}"
    )


@health_router.get('/databases/db2', status_code=status.HTTP_200_OK)
def check_db2():
    result = _check_db2()
    return ApiResponse(
        success=result["status"] == "ok",
        data=result,
        message=f"DB2: {result['status']}"
    )


@health_router.get('/databases/db2/sectores-count', status_code=status.HTTP_200_OK)
def check_db2_sectores_count():
    """Cuenta el total de filas en lbgeoubi.sectores."""
    import jaydebeapi

    def _attempt():
        jdbc_url = f"jdbc:as400://{settings.db2_host};libraries={settings.db2_database};"
        conn = jaydebeapi.connect(
            "com.ibm.as400.access.AS400JDBCDriver",
            jdbc_url,
            [settings.db2_user, settings.db2_password],
            settings.jt400_jar_path,
        )
        try:
            curs = conn.cursor()
            curs.execute("SELECT COUNT(*) FROM lbgeoubi.sectores")
            count = curs.fetchone()[0]
            curs.close()
            return {"status": "ok", "total_rows": count, "host": settings.db2_host}
        finally:
            conn.close()

    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(_attempt)
    try:
        result = future.result(timeout=30)
        return ApiResponse(success=True, data=result, message=f"Total filas: {result['total_rows']}")
    except FuturesTimeoutError:
        executor.shutdown(wait=False)
        return ApiResponse(success=False, data={"error": "Timeout >30s"}, message="DB2 count: timeout")
    except Exception as e:
        executor.shutdown(wait=False)
        return ApiResponse(success=False, data={"error": f"{type(e).__name__}: {e}"}, message="DB2 count: error")


@health_router.get('/databases/db2/sectores-sample', status_code=status.HTTP_200_OK)
def check_db2_sectores_sample():
    """Consulta los primeros 5 registros de lbgeoubi.sectores para validar acceso a la tabla."""
    import jaydebeapi

    def _attempt():
        jdbc_url = f"jdbc:as400://{settings.db2_host};libraries={settings.db2_database};"
        conn = jaydebeapi.connect(
            "com.ibm.as400.access.AS400JDBCDriver",
            jdbc_url,
            [settings.db2_user, settings.db2_password],
            settings.jt400_jar_path,
        )
        try:
            curs = conn.cursor()
            curs.execute("SELECT * FROM lbgeoubi.sectores FETCH FIRST 5 ROWS ONLY")
            columns = [desc[0] for desc in curs.description]
            rows = [dict(zip(columns, row)) for row in curs.fetchall()]
            curs.close()
            return {"status": "ok", "columns": columns, "rows": rows, "host": settings.db2_host}
        finally:
            conn.close()

    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(_attempt)
    try:
        result = future.result(timeout=30)
        return ApiResponse(success=True, data=result, message="Muestra de lbgeoubi.sectores OK")
    except FuturesTimeoutError:
        executor.shutdown(wait=False)
        return ApiResponse(success=False, data={"error": "Timeout >30s"}, message="DB2 sectores: timeout")
    except Exception as e:
        executor.shutdown(wait=False)
        logger.warning(f"[health] DB2 sectores sample failed: {type(e).__name__}: {e!r}")
        return ApiResponse(success=False, data={"error": f"{type(e).__name__}: {e}"}, message="DB2 sectores: error")


# ── Helpers internos ────────────────────────────────────────────────────────

def _check_sqlserver() -> dict:
    try:
        logger.info(
            f"[health] SQLServer → user='{settings.sqlserver_user}' "
            f"host={settings.sqlserver_host} port={settings.sqlserver_port} "
            f"db={settings.sqlserver_database}"
        )
        url = URL.create(
            "mssql+pyodbc",
            username=settings.sqlserver_user,
            password=settings.sqlserver_password,
            host=settings.sqlserver_host,
            port=int(settings.sqlserver_port),
            database=settings.sqlserver_database,
            query={"driver": settings.sqlserver_driver, "TrustServerCertificate": "yes", "Encrypt": "no"},
        )
        engine = create_engine(url, pool_pre_ping=True)
        t0 = time.monotonic()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        latency = round((time.monotonic() - t0) * 1000)
        engine.dispose()
        return {"status": "ok", "latency_ms": latency, "host": settings.sqlserver_host}
    except Exception as e:
        logger.warning(f"[health] SQL Server check failed: {e}")
        return {"status": "error", "error": str(e), "host": settings.sqlserver_host}


def _check_postgres() -> dict:
    try:
        url = URL.create(
            "postgresql+psycopg2",
            username=settings.postgres_user,
            password=settings.postgres_password,
            host=settings.postgres_host,
            port=int(settings.postgres_port),
            database=settings.postgres_database,
        )
        engine = create_engine(url, pool_pre_ping=True)
        t0 = time.monotonic()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        latency = round((time.monotonic() - t0) * 1000)
        engine.dispose()
        return {"status": "ok", "latency_ms": latency, "host": settings.postgres_host}
    except Exception as e:
        logger.warning(f"[health] PostgreSQL check failed: {e}")
        return {"status": "error", "error": str(e), "host": settings.postgres_host}


def _check_db2() -> dict:
    import jaydebeapi

    def _attempt():
        jdbc_url = f"jdbc:as400://{settings.db2_host};libraries={settings.db2_database};"
        logger.info(
            f"[health] DB2 → host={settings.db2_host} user={settings.db2_user} "
            f"port={settings.db2_port} db={settings.db2_database}"
        )
        conn = jaydebeapi.connect(
            "com.ibm.as400.access.AS400JDBCDriver",
            jdbc_url,
            [settings.db2_user, settings.db2_password],
            settings.jt400_jar_path,
        )
        t0 = time.monotonic()
        curs = conn.cursor()
        curs.execute("SELECT 1 FROM SYSIBM.SYSDUMMY1")
        latency = round((time.monotonic() - t0) * 1000)
        curs.close()
        conn.close()
        return {"status": "ok", "latency_ms": latency, "host": settings.db2_host}

    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(_attempt)
    try:
        return future.result(timeout=30)
    except FuturesTimeoutError:
        executor.shutdown(wait=False)
        return {"status": "error", "error": "Connection timeout (>30s)", "host": settings.db2_host}
    except Exception as e:
        executor.shutdown(wait=False)
        logger.warning(f"[health] DB2 check failed: {e}")
        return {"status": "error", "error": str(e), "host": settings.db2_host}
