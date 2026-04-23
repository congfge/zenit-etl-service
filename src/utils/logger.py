import json
import logging
import os
import sys
from contextvars import ContextVar
from logging.handlers import RotatingFileHandler
from pathlib import Path

# Raiz del proyecto: src/utils/logger.py -> ../../
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOGS_DIR = PROJECT_ROOT / "logs"
LOGS_DIR.mkdir(exist_ok=True)

# Per-request correlation ID — set by the HTTP middleware in main.py
correlation_id_var: ContextVar[str] = ContextVar("correlation_id", default="-")

_IS_PRODUCTION = os.environ.get("NODE_ENV", "development").lower() == "production"


class JsonFormatter(logging.Formatter):
    """Structured JSON formatter that includes the per-request correlation ID."""

    def format(self, record: logging.LogRecord) -> str:
        log_record = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "correlation_id": correlation_id_var.get(),
        }
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_record, ensure_ascii=False)


_TEXT_FORMATTER = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
_JSON_FORMATTER = JsonFormatter()


def _add_file_handler(logger: logging.Logger, filename: str) -> None:
    handler = RotatingFileHandler(
        LOGS_DIR / filename,
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
        encoding="utf-8",
    )
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(_JSON_FORMATTER if _IS_PRODUCTION else _TEXT_FORMATTER)
    logger.addHandler(handler)


def setup_logger() -> logging.Logger:
    logger = logging.getLogger("zenit-etl")
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    # Stdout — JSON in production, human-readable in development
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    stdout_handler.setFormatter(_JSON_FORMATTER if _IS_PRODUCTION else _TEXT_FORMATTER)
    logger.addHandler(stdout_handler)

    # Archivo general
    _add_file_handler(logger, "app.log")

    # Archivo de errores
    error_handler = RotatingFileHandler(
        LOGS_DIR / "error.log",
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(_JSON_FORMATTER if _IS_PRODUCTION else logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s\n%(exc_info)s"
    ))
    logger.addHandler(error_handler)

    logger.propagate = False
    return logger


logger = setup_logger()
