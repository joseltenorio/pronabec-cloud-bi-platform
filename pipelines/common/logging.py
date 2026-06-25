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

def _clean_event_fields(fields: dict[str, Any]) -> dict[str, Any]:
    """Retira campos vacíos para evitar ruido en Cloud Logging."""
    return {
        key: value
        for key, value in fields.items()
        if value is not None and value != ""
    }


def log_pipeline_event(
    logger: logging.Logger,
    *,
    event_type: str,
    pipeline_name: str,
    pipeline_run_id: str | None = None,
    status: str | None = None,
    source_system: str | None = None,
    source_dataset: str | None = None,
    extraction_date: str | None = None,
    message: str | None = None,
    **fields: Any,
) -> None:
    """
    Emite un evento estructurado estándar para procesos batch.

    Args:
        logger: Logger configurado del componente.
        event_type: Tipo de evento operativo.
        pipeline_name: Nombre lógico del pipeline o job.
        pipeline_run_id: Identificador de corrida.
        status: Estado del evento o ejecución.
        source_system: Familia o sistema fuente.
        source_dataset: Dataset o slice procesado.
        extraction_date: Fecha lógica de extracción.
        message: Mensaje legible del evento.
        **fields: Campos adicionales del evento.
    """
    event_fields = _clean_event_fields(
        {
            "event_type": event_type,
            "pipeline_name": pipeline_name,
            "pipeline_run_id": pipeline_run_id,
            "status": status,
            "source_system": source_system,
            "source_dataset": source_dataset,
            "extraction_date": extraction_date,
            **fields,
        }
    )

    logger.info(
        message or event_type,
        extra={"extra_fields": event_fields},
    )


def log_pipeline_started(
    logger: logging.Logger,
    *,
    pipeline_name: str,
    pipeline_run_id: str | None = None,
    source_system: str | None = None,
    source_dataset: str | None = None,
    extraction_date: str | None = None,
    **fields: Any,
) -> None:
    """Emite evento estándar de inicio de pipeline."""
    log_pipeline_event(
        logger,
        event_type="pipeline_started",
        pipeline_name=pipeline_name,
        pipeline_run_id=pipeline_run_id,
        status="STARTED",
        source_system=source_system,
        source_dataset=source_dataset,
        extraction_date=extraction_date,
        message="Pipeline execution started.",
        **fields,
    )


def log_pipeline_completed(
    logger: logging.Logger,
    *,
    pipeline_name: str,
    pipeline_run_id: str | None = None,
    source_system: str | None = None,
    source_dataset: str | None = None,
    extraction_date: str | None = None,
    records_read: int | None = None,
    records_valid: int | None = None,
    records_rejected: int | None = None,
    rejection_rate: float | None = None,
    output_path: str | None = None,
    output_table: str | None = None,
    duration_seconds: float | None = None,
    **fields: Any,
) -> None:
    """Emite evento estándar de finalización exitosa."""
    log_pipeline_event(
        logger,
        event_type="pipeline_completed",
        pipeline_name=pipeline_name,
        pipeline_run_id=pipeline_run_id,
        status="SUCCEEDED",
        source_system=source_system,
        source_dataset=source_dataset,
        extraction_date=extraction_date,
        message="Pipeline execution completed.",
        records_read=records_read,
        records_valid=records_valid,
        records_rejected=records_rejected,
        rejection_rate=rejection_rate,
        output_path=output_path,
        output_table=output_table,
        duration_seconds=duration_seconds,
        **fields,
    )


def log_pipeline_failed(
    logger: logging.Logger,
    *,
    pipeline_name: str,
    pipeline_run_id: str | None = None,
    source_system: str | None = None,
    source_dataset: str | None = None,
    extraction_date: str | None = None,
    error_code: str | None = None,
    error_message: str | None = None,
    duration_seconds: float | None = None,
    **fields: Any,
) -> None:
    """Emite evento estándar de fallo de pipeline."""
    log_pipeline_event(
        logger,
        event_type="pipeline_failed",
        pipeline_name=pipeline_name,
        pipeline_run_id=pipeline_run_id,
        status="FAILED",
        source_system=source_system,
        source_dataset=source_dataset,
        extraction_date=extraction_date,
        message="Pipeline execution failed.",
        error_code=error_code,
        error_message=error_message,
        duration_seconds=duration_seconds,
        **fields,
    )


def log_pipeline_metric(
    logger: logging.Logger,
    *,
    pipeline_name: str,
    metric_name: str,
    metric_value: int | float | str,
    pipeline_run_id: str | None = None,
    source_system: str | None = None,
    source_dataset: str | None = None,
    extraction_date: str | None = None,
    **fields: Any,
) -> None:
    """Emite una métrica operativa como evento estructurado."""
    log_pipeline_event(
        logger,
        event_type="pipeline_metric",
        pipeline_name=pipeline_name,
        pipeline_run_id=pipeline_run_id,
        source_system=source_system,
        source_dataset=source_dataset,
        extraction_date=extraction_date,
        message="Pipeline metric emitted.",
        metric_name=metric_name,
        metric_value=metric_value,
        **fields,
    )