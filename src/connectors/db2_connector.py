import pandas as pd
from core.config import settings
from utils.logger import logger


class DB2Connector:
    """
    Conector para DB2 for IBM i (AS400/iSeries) — solo lectura.
    Fuente de datos para la tabla de sectores (lbgeoubi.sectores).

    Driver: com.ibm.as400.access.AS400JDBCDriver (JTOpen/JT400)
    URL JDBC: jdbc:as400://{host}:{port};libraries={database};
    Puerto por defecto: 446 (proxy socat en Docker: 10446)

    Requiere: JayDeBeApi, JPype1, JT400 JAR en JT400_JAR_PATH.
    Variables de entorno: DB2_HOST, DB2_PORT, DB2_USER, DB2_PASSWORD, DB2_DATABASE.
    """

    def __init__(self, database: str | None = None):
        import jaydebeapi  # lazy import — jaydebeapi/JPype1 opcionales hasta habilitar DB2
        db = database or settings.db2_database
        jdbc_url = f"jdbc:as400://{settings.db2_host};libraries={db};"
        self.conn = jaydebeapi.connect(
            "com.ibm.as400.access.AS400JDBCDriver",
            jdbc_url,
            [settings.db2_user, settings.db2_password],
            settings.jt400_jar_path,
        )
        logger.info(f"[DB2Connector] Conexión establecida con {settings.db2_host} library={db} (IBM i / JT400)")

    FETCH_CHUNK = 5_000

    def extract_sectores(self) -> pd.DataFrame:
        """Extrae todos los vértices de sectores desde IBM i en chunks de 5k filas."""
        query = "SELECT * FROM lbgeoubi.sectores"
        logger.info("[DB2Connector] Iniciando extracción de sectores...")
        curs = self.conn.cursor()
        try:
            curs.execute(query)
            columns = [desc[0] for desc in curs.description]
            chunks = []
            total = 0
            while True:
                rows = curs.fetchmany(self.FETCH_CHUNK)
                if not rows:
                    break
                chunks.append(pd.DataFrame(rows, columns=columns))
                total += len(rows)
                logger.info(f"[DB2Connector] {total:,} filas leídas...")
        finally:
            curs.close()
        df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame(columns=columns)
        logger.info(f"[DB2Connector] {len(df):,} filas extraídas de lbgeoubi.sectores")
        return df

    def extract_sucursales(self) -> pd.DataFrame:
        """Extrae agencias con tipo=1 desde lbgeocli en DB2 en chunks de 5k filas."""
        query = "SELECT * FROM lbgeocli.agencias WHERE tipo = 1"
        logger.info("[DB2Connector] Iniciando extracción de sucursales...")
        curs = self.conn.cursor()
        try:
            curs.execute(query)
            columns = [desc[0] for desc in curs.description]
            chunks = []
            total = 0
            while True:
                rows = curs.fetchmany(self.FETCH_CHUNK)
                if not rows:
                    break
                chunks.append(pd.DataFrame(rows, columns=columns))
                total += len(rows)
                logger.info(f"[DB2Connector] {total:,} sucursales leídas...")
        finally:
            curs.close()
        df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame(columns=columns)
        logger.info(f"[DB2Connector] {len(df):,} filas extraídas de lbgeocli.agencias")
        return df

    def close(self):
        self.conn.close()
