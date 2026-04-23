import pandas as pd
from datetime import datetime

from api.models.processing_metrics_model import ProcessingMetrics
from utils.logger import logger
from core.config import settings
from connectors.sqlserver_connector import SQLServerConnector
from connectors.postgres_connector import PostgresConnector
from transformers.data_cleaner import DataCleaner

# Número de chunks a acumular antes de cada COPY flush.
# 5 chunks × 10k filas = ~50k filas por COPY → <2s por flush.
FLUSH_EVERY = 5


class EtlService:
    def __init__(
        self,
        sqlserver: SQLServerConnector,
        postgres: PostgresConnector,
        cleaner: DataCleaner,
        chunk_size: int = 10000
    ):
        self.sqlserver = sqlserver
        self.postgres = postgres
        self.cleaner = cleaner
        self.chunk_size = chunk_size

    async def run_full_refresh(self, job_id: str, closing_dates: list[str]):
        metrics = ProcessingMetrics()
        start_time = datetime.utcnow()

        try:
            # Truncar UNA SOLA VEZ antes de empezar la extracción
            self.postgres.truncate_table(settings.postgres_etl_table)

            batch: list[pd.DataFrame] = []
            total_chunks = 0
            flush_count = 0

            for closing_date in closing_dates:
                logger.info(f"[JOB {job_id}] Extrayendo fecha de cierre: {closing_date}")
                chunk_idx = 0

                for raw_chunk in self.sqlserver.extract_data_by_date(closing_date, self.chunk_size):
                    cleaned_chunk = self.cleaner.clean(raw_chunk)
                    batch.append(cleaned_chunk)

                    metrics.records_extracted += len(raw_chunk)
                    metrics.records_cleaned += len(cleaned_chunk)
                    total_chunks += 1
                    chunk_idx += 1

                    logger.info(
                        f"[JOB {job_id}] [{closing_date}] Chunk {chunk_idx}: "
                        f"{len(raw_chunk)} extraídos → {len(cleaned_chunk)} limpios"
                    )

                    # Flush a Postgres cada FLUSH_EVERY chunks para no acumular en RAM
                    if total_chunks % FLUSH_EVERY == 0:
                        flush_count += 1
                        flushed = self.postgres.copy_append(
                            pd.concat(batch, ignore_index=True),
                            settings.postgres_etl_table,
                        )
                        metrics.records_loaded += flushed
                        batch = []
                        logger.info(
                            f"[JOB {job_id}] Flush #{flush_count}: "
                            f"{metrics.records_loaded:,} registros cargados en total"
                        )

            # Flush final con los chunks restantes
            if batch:
                flushed = self.postgres.copy_append(
                    pd.concat(batch, ignore_index=True),
                    settings.postgres_etl_table,
                )
                metrics.records_loaded += flushed

            if metrics.records_loaded == 0:
                logger.warning(f"[JOB {job_id}] No se obtuvieron registros para las fechas: {closing_dates}")
            else:
                logger.info(f"[JOB {job_id}] {metrics.records_loaded:,} registros cargados en Postgres")

            end_time = datetime.utcnow()
            metrics.duration_seconds = (end_time - start_time).total_seconds()
            logger.info(f"[JOB {job_id}] ETL finalizado en {metrics.duration_seconds:.1f}s")

        except Exception as e:
            logger.error(f"[JOB {job_id}] ETL fallido: {str(e)}")
            raise
        finally:
            self.postgres.close()
            self.sqlserver.close()

        return metrics
