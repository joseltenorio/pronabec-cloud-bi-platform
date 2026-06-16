"""Dead Letter Queue (DLQ) utilities for Project Cloud BI Platform."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


def build_rejected_record(
    *,
    raw_record: dict[str, Any],
    source_system: str,
    source_dataset: str,
    extraction_date: str,
    pipeline_run_id: str,
    ingestion_timestamp: str | None = None,
    processing_stage: str,
    error_code: str,
    error_message: str,
    failed_field: str | None = None,
    failed_value: Any | None = None,
    partial_record: dict[str, Any] | None = None,
    exception_type: str | None = None,
) -> dict[str, Any]:
    """
    Build a structured rejected record dict for DLQ.
    
    Ensures raw_record is not mutated and failed_value is safe for JSON serialization.
    """
    # Defensive copy to avoid mutating the original raw_record
    copied_raw = dict(raw_record) if raw_record is not None else {}

    safe_ingestion_timestamp = ingestion_timestamp
    if not safe_ingestion_timestamp:
        safe_ingestion_timestamp = datetime.now(timezone.utc).isoformat()

    # Verify and convert failed_value defensively if it cannot be serialized as JSON
    safe_failed_value = failed_value
    if failed_value is not None:
        try:
            json.dumps(failed_value, ensure_ascii=False)
        except (TypeError, ValueError):
            safe_failed_value = str(failed_value)

    rejected = {
        "source_system": source_system,
        "source_dataset": source_dataset,
        "extraction_date": extraction_date,
        "ingestion_timestamp": safe_ingestion_timestamp,
        "pipeline_run_id": pipeline_run_id,
        "processing_stage": processing_stage,
        "error_code": error_code,
        "error_message": error_message,
        "raw_record": copied_raw,
    }

    if failed_field is not None:
        rejected["failed_field"] = failed_field
    if safe_failed_value is not None:
        rejected["failed_value"] = safe_failed_value
    if partial_record is not None:
        rejected["partial_record"] = partial_record
    if exception_type is not None:
        rejected["exception_type"] = exception_type

    return rejected


def build_dlq_path(
    *,
    output_root: str,
    source_system: str,
    source_dataset: str,
    extraction_date: str,
) -> str:
    """Build a deterministic DLQ file path/URI for a given source system and dataset."""
    root = output_root.replace("\\", "/").rstrip("/")
    return f"{root}/{source_system}/{source_dataset}/extraction_date={extraction_date}/rejected_records.jsonl"


def serialize_rejected_record(record: dict[str, Any]) -> str:
    """Serialize a rejected record dictionary into a single-line JSON string supporting non-ASCII."""
    return json.dumps(record, ensure_ascii=False)


def write_rejected_records_local(
    records: Iterable[dict[str, Any]],
    output_path: str | Path,
) -> int:
    """Write rejected records locally in JSONL format, creating directories if needed."""
    p = Path(output_path)
    records_list = list(records)
    if not records_list:
        return 0

    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        for record in records_list:
            f.write(serialize_rejected_record(record) + "\n")

    return len(records_list)


def write_rejected_records_gcs(
    records: Iterable[dict[str, Any]],
    output_path: str,
    project_id: str | None = None,
) -> int:
    """Write rejected records to GCS in JSONL format, using the GCS common helper."""
    from pipelines.common.gcs import parse_gs_uri, upload_jsonl, get_storage_client
    bucket_name, object_path = parse_gs_uri(output_path)
    records_list = list(records)
    if not records_list:
        return 0

    client = get_storage_client(project_id)
    upload_jsonl(bucket_name, object_path, records_list, client=client)
    return len(records_list)
