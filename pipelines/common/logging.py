"""
Utilidades de logging para Project Cloud BI Platform.

El objetivo es producir logs consistentes y fáciles de consultar en ejecución
local, Cloud Run Jobs, Dataflow y Cloud Composer.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any


class JsonFormatter(logging.Formatter):
    """Formatter simple para emitir logs en formato JSON."""

    def format(self, record: logging.LogRecord) -> str:
        log_payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        extra_fields = getattr(record, "extra_fields", None)
        if isinstance(extra_fields, dict):
            log_payload.update(extra_fields)

        if record.exc_info:
            log_payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_payload, ensure_ascii=False)


def setup_structured_logger(
    name: str,
    level: str = "INFO",
    structured: bool = True,
) -> logging.Logger:
    """
    Configura un logger reutilizable.

    Args:
        name: Nombre del logger.
        level: Nivel de logging.
        structured: Si es True, emite logs JSON. Si es False, usa formato texto.

    Returns:
        Logger configurado.
    """
    logger = logging.getLogger(name)
    logger.setLevel(_parse_log_level(level))
    logger.propagate = False

    if logger.handlers:
        logger.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)

    if structured:
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
            )
        )

    logger.addHandler(handler)

    return logger


def log_event(
    logger: logging.Logger,
    level: str,
    message: str,
    **fields: Any,
) -> None:
    """
    Emite un evento de log con campos adicionales.

    Args:
        logger: Logger configurado.
        level: Nivel del evento.
        message: Mensaje principal.
        fields: Campos adicionales del evento.
    """
    log_level = _parse_log_level(level)
    logger.log(log_level, message, extra={"extra_fields": fields})


def _parse_log_level(level: str) -> int:
    """
    Convierte un texto de nivel de log a constante de logging.
    """
    normalized = str(level).upper().strip()

    valid_levels = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }

    return valid_levels.get(normalized, logging.INFO)