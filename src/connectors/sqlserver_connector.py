import re
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
import pandas as pd
from core.config import settings
from utils.logger import logger

class SQLServerConnector:
    def __init__(self):
        connection_url = URL.create(
            "mssql+pyodbc",
            username=settings.sqlserver_user,
            password=settings.sqlserver_password,
            host=settings.sqlserver_host,
            database=settings.sqlserver_database,
            port=int(settings.sqlserver_port),
            query={
                "driver": settings.sqlserver_driver,
                "TrustServerCertificate": "yes",
            }
        )
        
        self.engine = create_engine(
            connection_url,
            pool_size=int(settings.sqlserver_pool_size),
            pool_recycle=int(settings.sqlserver_pool_recycle),
            max_overflow=int(settings.sqlserver_pool_max_overflow),
            pool_timeout=int(settings.sqlserver_pool_timeout),
            pool_pre_ping=True
        )
        logger.info(f"[SQLServerConnector] DB successfully connected")
        
    def extract_data_by_date(self, closing_date: str = '31/12/2025', chunk_size: int = 10000):
        query = text("SELECT * FROM dbo.Zenit_data_historico WHERE fecha = :closing_date")
        with self.engine.connect() as conn:
            for chunk in pd.read_sql(query, conn, params={"closing_date": closing_date}, chunksize=chunk_size):
                yield chunk
        
        
    def extract_promotores(self) -> pd.DataFrame:
        """
        Lee Zenit_Catalogo_promotores y despivotea el resultado.

        La vista devuelve 1 fila con todas las columnas en formato ancho:
        cod_promotor_m1, nombre_promotor_m1, ..., cod_promotor_m4229, ...
        Este metodo lo transforma en un DataFrame tabular (1 fila por promotor).
        """
        query = "SELECT * FROM DBI_ECODIGITAL.dbo.Zenit_Catalogo_promotores"
        with self.engine.connect() as conn:
            raw = pd.read_sql(query, conn)

        if raw.empty:
            return raw

        # Detectar indices unicos (los N en _mN)
        indices = set()
        for col in raw.columns:
            m = re.search(r'_m(\d+)$', col)
            if m:
                indices.add(int(m.group(1)))

        if not indices:
            # La tabla ya tiene formato normal, retornar tal cual
            logger.info(f"[SQLServerConnector] Promotores en formato tabular: {len(raw)} filas")
            return raw

        # Despivotear: una fila por indice
        source_row = raw.iloc[0]
        rows = []
        for idx in sorted(indices):
            suffix = f'_m{idx}'
            record = {
                col[: -len(suffix)]: source_row[col]
                for col in raw.columns
                if col.endswith(suffix)
            }
            if record:
                rows.append(record)

        result = pd.DataFrame(rows)
        logger.info(f"[SQLServerConnector] Promotores despivoteados: {len(result)} registros")
        return result

    def close(self):
        self.engine.dispose()

    def get_total_records(self, closing_date: str = '31/12/2025'):
        sql = text("""
            SELECT COUNT(*) FROM DBI_ECODIGITAL.dbo.Zenit_data_historico
            WHERE fecha = :closing_date
            AND latitud IS NOT NULL
            AND longitud IS NOT NULL
        """)
        with self.engine.connect() as conn:
            return conn.execute(sql, {"closing_date": closing_date}).scalar()
        