import json
from pathlib import Path
from typing import Iterable

from pipelines.common.dlq import (
    build_dlq_path,
    build_rejected_record,
    serialize_rejected_record,
    write_rejected_records_local,
    write_rejected_records_gcs,
)


def test_build_rejected_record_full_metadata() -> None:
    raw = {"id": "1", "name": "Test"}
    record = build_rejected_record(
        raw_record=raw,
        source_system="pronabec",
        source_dataset="convocatorias",
        extraction_date="2026-06-15",
        pipeline_run_id="test-run",
        ingestion_timestamp="2026-06-16T12:00:00Z",
        processing_stage="transform",
        error_code="TRANSFORM_ERROR",
        error_message="Invalid value",
        failed_field="status",
        failed_value="active",
        partial_record={"id": "1"},
        exception_type="ValueError",
    )

    assert record["source_system"] == "pronabec"
    assert record["source_dataset"] == "convocatorias"
    assert record["extraction_date"] == "2026-06-15"
    assert record["ingestion_timestamp"] == "2026-06-16T12:00:00Z"
    assert record["pipeline_run_id"] == "test-run"
    assert record["processing_stage"] == "transform"
    assert record["error_code"] == "TRANSFORM_ERROR"
    assert record["error_message"] == "Invalid value"
    assert record["raw_record"] == raw
    assert record["failed_field"] == "status"
    assert record["failed_value"] == "active"
    assert record["partial_record"] == {"id": "1"}
    assert record["exception_type"] == "ValueError"


def test_build_rejected_record_automatic_timestamp() -> None:
    raw = {"id": "2"}
    record = build_rejected_record(
        raw_record=raw,
        source_system="mef",
        source_dataset="presupuesto_mef",
        extraction_date="2026-06-15",
        pipeline_run_id="test-run",
        processing_stage="parse",
        error_code="PARSE_ERROR",
        error_message="Bad format",
    )
    assert record["ingestion_timestamp"] is not None
    # Verify it is in ISO format
    assert "T" in record["ingestion_timestamp"]


def test_build_rejected_record_immutability() -> None:
    raw = {"id": "3", "value": "A"}
    record = build_rejected_record(
        raw_record=raw,
        source_system="mef",
        source_dataset="presupuesto_mef",
        extraction_date="2026-06-15",
        pipeline_run_id="test-run",
        processing_stage="transform",
        error_code="UNKNOWN_ERROR",
        error_message="Fail",
    )
    # Mutate original record
    raw["value"] = "B"
    # Verify raw_record in output was not mutated
    assert record["raw_record"]["value"] == "A"


def test_build_rejected_record_non_serializable_failed_value() -> None:
    class DummyObj:
        def __str__(self):
            return "dummy_representation"

    dummy = DummyObj()
    record = build_rejected_record(
        raw_record={"id": "4"},
        source_system="pronabec_reports",
        source_dataset="report_beca18_universitarios_carrera_anual",
        extraction_date="2026-06-15",
        pipeline_run_id="test-run",
        processing_stage="transform",
        error_code="TRANSFORM_ERROR",
        error_message="Object error",
        failed_field="obj",
        failed_value=dummy,
    )
    assert record["failed_value"] == "dummy_representation"


def test_build_dlq_path_local() -> None:
    path = build_dlq_path(
        output_root="tmp/dlq",
        source_system="pronabec_reports",
        source_dataset="report_beca18_universitarios_carrera_anual",
        extraction_date="2026-06-15",
    )
    expected = "tmp/dlq/pronabec_reports/report_beca18_universitarios_carrera_anual/extraction_date=2026-06-15/rejected_records.jsonl"
    assert path == expected


def test_build_dlq_path_gcs() -> None:
    path = build_dlq_path(
        output_root="gs://my-bucket/dlq",
        source_system="mef",
        source_dataset="presupuesto_mef",
        extraction_date="2026-06-15",
    )
    expected = "gs://my-bucket/dlq/mef/presupuesto_mef/extraction_date=2026-06-15/rejected_records.jsonl"
    assert path == expected


def test_serialize_rejected_record_utf8() -> None:
    raw = {"carrera_estudio": "ARTE & DISEÑO GRAFICO EMPRESARIAL"}
    record = build_rejected_record(
        raw_record=raw,
        source_system="pronabec_reports",
        source_dataset="report_beca18_universitarios_carrera_anual",
        extraction_date="2026-06-15",
        pipeline_run_id="test-run",
        processing_stage="transform",
        error_code="TRANSFORM_ERROR",
        error_message="Diseño error",
    )
    serialized = serialize_rejected_record(record)
    assert "DISEÑO" in serialized
    # Verify it is single line
    assert "\n" not in serialized
    # Verify it loads back as JSON
    parsed = json.loads(serialized)
    assert parsed["raw_record"]["carrera_estudio"] == "ARTE & DISEÑO GRAFICO EMPRESARIAL"


def test_write_rejected_records_local(tmp_path: Path) -> None:
    records = [
        {"id": 1, "msg": "Error A"},
        {"id": 2, "msg": "Error B"},
    ]
    dest_path = tmp_path / "dlq" / "rejected.jsonl"
    count = write_rejected_records_local(records, dest_path)

    assert count == 2
    assert dest_path.exists()

    lines = dest_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["msg"] == "Error A"
    assert json.loads(lines[1])["msg"] == "Error B"


def test_write_rejected_records_local_empty(tmp_path: Path) -> None:
    dest_path = tmp_path / "dlq" / "empty.jsonl"
    count = write_rejected_records_local([], dest_path)
    assert count == 0
    assert not dest_path.exists()


def test_write_rejected_records_gcs_mock(monkeypatch) -> None:
    uploaded = {}

    def mock_upload_jsonl(bucket_name: str, object_path: str, records: Iterable[dict], client=None):
        uploaded["bucket"] = bucket_name
        uploaded["path"] = object_path
        uploaded["records"] = list(records)
        return "gs://mock/path"

    def mock_get_storage_client(project_id=None):
        return None

    monkeypatch.setattr("pipelines.common.gcs.upload_jsonl", mock_upload_jsonl)
    monkeypatch.setattr("pipelines.common.gcs.get_storage_client", mock_get_storage_client)

    records = [{"id": 100}]
    count = write_rejected_records_gcs(records, "gs://my-bucket/dlq/records.jsonl")

    assert count == 1
    assert uploaded["bucket"] == "my-bucket"
    assert uploaded["path"] == "dlq/records.jsonl"
    assert uploaded["records"] == records


def test_write_rejected_records_gcs_mock_empty(monkeypatch) -> None:
    records = []
    count = write_rejected_records_gcs(records, "gs://my-bucket/dlq/records.jsonl")
    assert count == 0
