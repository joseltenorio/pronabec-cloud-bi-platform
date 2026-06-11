"""
Utilidades de auditoría para Project Cloud BI Platform.

Este módulo define estructuras y helpers para registrar eventos operativos del
pipeline. En commits posteriores estos registros podrán persistirse en BigQuery
Audit o emitirse como logs estructurados.
"""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from typing import Any


def utc_now() -> datetime:
    """Devuelve el timestamp actual en UTC."""
    return datetime.now(timezone.utc)


def generate_run_id(prefix: str = "run") -> str:
    """
    Genera un identificador único para ejecuciones.

    Args:
        prefix: Prefijo lógico del identificador.

    Returns:
        Identificador único con prefijo.
    """
    clean_prefix = prefix.strip().lower().replace(" ", "_")
    return f"{clean_prefix}_{uuid.uuid4().hex}"


def calculate_duration_seconds(
    started_at: datetime | None,
    finished_at: datetime | None,
) -> int | None:
    """
    Calcula duración en segundos entre dos timestamps.
    """
    if started_at is None or finished_at is None:
        return None

    return int((finished_at - started_at).total_seconds())


@dataclass
class AuditEvent:
    """Representa un evento de auditoría genérico."""

    event_id: str
    event_type: str
    pipeline_name: str
    status: str
    environment: str = "dev"
    run_id: str | None = None
    source_name: str | None = None
    source_dataset: str | None = None
    execution_date: date | None = None
    records_read: int | None = None
    records_written: int | None = None
    records_rejected: int | None = None
    error_message: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_seconds: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=utc_now)

    def to_dict(self) -> dict[str, Any]:
        """Convierte el evento a diccionario serializable."""
        payload = asdict(self)

        for key, value in payload.items():
            if isinstance(value, datetime):
                payload[key] = value.isoformat()
            elif isinstance(value, date):
                payload[key] = value.isoformat()

        return payload


def create_audit_event(
    event_type: str,
    pipeline_name: str,
    status: str,
    environment: str = "dev",
    run_id: str | None = None,
    source_name: str | None = None,
    source_dataset: str | None = None,
    execution_date: date | None = None,
    records_read: int | None = None,
    records_written: int | None = None,
    records_rejected: int | None = None,
    error_message: str | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
    metadata: dict[str, Any] | None = None,
) -> AuditEvent:
    """
    Crea un evento de auditoría genérico.
    """
    return AuditEvent(
        event_id=generate_run_id("event"),
        event_type=event_type,
        pipeline_name=pipeline_name,
        status=status,
        environment=environment,
        run_id=run_id,
        source_name=source_name,
        source_dataset=source_dataset,
        execution_date=execution_date,
        records_read=records_read,
        records_written=records_written,
        records_rejected=records_rejected,
        error_message=error_message,
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=calculate_duration_seconds(started_at, finished_at),
        metadata=metadata or {},
    )


def create_extraction_audit_event(
    pipeline_name: str,
    source_name: str,
    source_dataset: str,
    status: str,
    run_id: str | None = None,
    environment: str = "dev",
    execution_date: date | None = None,
    records_read: int | None = None,
    records_written: int | None = None,
    records_rejected: int | None = None,
    error_message: str | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
    metadata: dict[str, Any] | None = None,
) -> AuditEvent:
    """
    Crea un evento de auditoría específico para extracciones.
    """
    return create_audit_event(
        event_type="extraction",
        pipeline_name=pipeline_name,
        status=status,
        environment=environment,
        run_id=run_id,
        source_name=source_name,
        source_dataset=source_dataset,
        execution_date=execution_date,
        records_read=records_read,
        records_written=records_written,
        records_rejected=records_rejected,
        error_message=error_message,
        started_at=started_at,
        finished_at=finished_at,
        metadata=metadata,
    )