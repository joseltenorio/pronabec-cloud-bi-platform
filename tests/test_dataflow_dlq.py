import json
from pathlib import Path
import pytest

from pipelines.dataflow_bronze_to_silver import run


def test_dataflow_dlq_pronabec_success_and_parse_error(tmp_path: Path) -> None:
    # 1. Prepare JSONL input containing 1 valid record and 1 corrupted JSON line
    input_file = tmp_path / "bronze_convocatorias.jsonl"
    valid_record = {
        "source_row_id": "1",
        "id_convocatoria": "100",
        "codigo_anual": "2026-01",
        "description_conv": "Beca",
        "modalidad": "ORDINARIA",
        "programa": "BECA18",
        "vacantes": "50",
    }
    corrupted_line = '{"source_row_id": "2", "id_convocatoria":'  # missing closing brace/value
    
    with open(input_file, "w", encoding="utf-8") as f:
        f.write(json.dumps(valid_record) + "\n")
        f.write(corrupted_line + "\n")

    dlq_root = tmp_path / "dlq"

    # Run the pipeline
    run([
        "--source-system", "pronabec",
        "--source-dataset", "convocatorias",
        "--extraction-date", "2026-06-15",
        "--input-path", str(input_file),
        "--input-format", "jsonl",
        "--runner", "DirectRunner",
        "--dlq-output-root", str(dlq_root),
        "--dry-run",
    ])

    # Verify DLQ output path
    dlq_file = dlq_root / "pronabec" / "convocatorias" / "extraction_date=2026-06-15" / "rejected_records.jsonl"
    assert dlq_file.exists()

    # Verify DLQ content
    lines = dlq_file.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    
    rejected = json.loads(lines[0])
    assert rejected["source_system"] == "pronabec"
    assert rejected["source_dataset"] == "convocatorias"
    assert rejected["processing_stage"] == "parse"
    assert rejected["error_code"] == "PARSE_ERROR"
    assert "JSON decode error" in rejected["error_message"]
    assert rejected["raw_record"] == {"raw_line": corrupted_line}


def test_dataflow_dlq_pronabec_transform_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    input_file = tmp_path / "bronze_convocatorias.jsonl"
    record = {
        "source_row_id": "1",
        "id_convocatoria": "100",
        "codigo_anual": "2026-01",
        "description_conv": "Beca",
        "modalidad": "ORDINARIA",
        "programa": "BECA18",
        "vacantes": "50",
    }
    with open(input_file, "w", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")

    def mock_transform_fail(*args, **kwargs):
        raise ValueError("Simulated transform error")

    monkeypatch.setattr("pipelines.dataflow_bronze_to_silver.transform_pronabec_record", mock_transform_fail)

    dlq_root = tmp_path / "dlq"

    run([
        "--source-system", "pronabec",
        "--source-dataset", "convocatorias",
        "--extraction-date", "2026-06-15",
        "--input-path", str(input_file),
        "--input-format", "jsonl",
        "--runner", "DirectRunner",
        "--dlq-output-root", str(dlq_root),
        "--dry-run",
    ])

    dlq_file = dlq_root / "pronabec" / "convocatorias" / "extraction_date=2026-06-15" / "rejected_records.jsonl"
    assert dlq_file.exists()

    lines = dlq_file.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1

    rejected = json.loads(lines[0])
    assert rejected["processing_stage"] == "transform"
    assert rejected["error_code"] == "TRANSFORM_ERROR"
    assert rejected["error_message"] == "Simulated transform error"
    assert rejected["raw_record"] == record


def test_dataflow_dlq_mef_transform_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    input_file = tmp_path / "bronze_mef.csv"
    csv_content = (
        "ano,codigo_entidad,entidad,pia,pim,devengado\n"
        "2026,123,PRONABEC,1000.0,1200.0,800.0\n"
    )
    input_file.write_text(csv_content, encoding="utf-8")

    def mock_transform_fail(*args, **kwargs):
        raise ValueError("Simulated MEF transform error")

    monkeypatch.setattr("pipelines.dataflow_bronze_to_silver.transform_mef_record", mock_transform_fail)

    dlq_root = tmp_path / "dlq"

    run([
        "--source-system", "mef",
        "--source-dataset", "presupuesto_mef",
        "--extraction-date", "2026-06-15",
        "--input-path", str(input_file),
        "--input-format", "csv",
        "--runner", "DirectRunner",
        "--dlq-output-root", str(dlq_root),
        "--dry-run",
    ])

    dlq_file = dlq_root / "mef" / "presupuesto_mef" / "extraction_date=2026-06-15" / "rejected_records.jsonl"
    assert dlq_file.exists()

    lines = dlq_file.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1

    rejected = json.loads(lines[0])
    assert rejected["source_system"] == "mef"
    assert rejected["source_dataset"] == "presupuesto_mef"
    assert rejected["processing_stage"] == "transform"
    assert rejected["error_code"] == "TRANSFORM_ERROR"
    assert rejected["error_message"] == "Simulated MEF transform error"


def test_dataflow_dlq_report_carrera_canonical_match(tmp_path: Path) -> None:
    input_file = tmp_path / "carrera.csv"
    csv_content = (
        "carrera_estudio,2026 (*),Total,source_document_file,source_document_title,source_publication_url,source_page,source_table,extraction_method\n"
        "ARTE & DISEO GRAFICO EMPRESARIAL,15,15,doc.pdf,Report,http://example.com,12,Table 1,camelot\n"
    )
    input_file.write_text(csv_content, encoding="utf-8")
    dlq_root = tmp_path / "dlq"

    # Run should process this successfully with flexible key-matching and tilde candidate match without errors or DLQ routing
    run([
        "--source-system", "pronabec_reports",
        "--source-dataset", "report_beca18_universitarios_carrera_anual",
        "--extraction-date", "2026-06-15",
        "--input-path", str(input_file),
        "--input-format", "csv",
        "--runner", "DirectRunner",
        "--dlq-output-root", str(dlq_root),
        "--dry-run",
    ])

    dlq_file = dlq_root / "pronabec_reports" / "report_beca18_universitarios_carrera_anual" / "extraction_date=2026-06-15" / "rejected_records.jsonl"
    
    # DLQ file should be empty of records because it was successful (or not contain records)
    if dlq_file.exists():
        lines = dlq_file.read_text(encoding="utf-8").splitlines()
        assert len(lines) == 0


def test_dataflow_dlq_report_pes_2025_success(tmp_path: Path) -> None:
    input_file = tmp_path / "grants.csv"
    csv_content = (
        "modalidad,2026 (*),Total,source_document_file,source_document_title,source_page,source_figure,extraction_method\n"
        "ORDINARIA,10,10,grants.pdf,Grants,5,Figure 1,tabula\n"
    )
    input_file.write_text(csv_content, encoding="utf-8")
    dlq_root = tmp_path / "dlq"

    # Runs a PES 2025 report successfully without error
    run([
        "--source-system", "pronabec_reports",
        "--source-dataset", "report_beca18_becas_otorgadas_modalidad_anual",
        "--extraction-date", "2026-06-15",
        "--input-path", str(input_file),
        "--input-format", "csv",
        "--runner", "DirectRunner",
        "--dlq-output-root", str(dlq_root),
        "--dry-run",
    ])

    dlq_file = dlq_root / "pronabec_reports" / "report_beca18_becas_otorgadas_modalidad_anual" / "extraction_date=2026-06-15" / "rejected_records.jsonl"
    if dlq_file.exists():
        lines = dlq_file.read_text(encoding="utf-8").splitlines()
        assert len(lines) == 0


def test_dataflow_dlq_report_schema_mismatch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    input_file = tmp_path / "grants.csv"
    csv_content = (
        "modalidad,2026 (*),Total,source_document_file,source_document_title,source_page,source_figure,extraction_method\n"
        "ORDINARIA,10,10,grants.pdf,Grants,5,Figure 1,tabula\n"
    )
    input_file.write_text(csv_content, encoding="utf-8")

    # Mock transform to return record containing columns not in schema (extra_field)
    def mock_transform_extra(*args, **kwargs):
        return [{
            "modalidad": "ORDINARIA",
            "ano_convocatoria": 2026,
            "becas_otorgadas": 10,
            "source_document_file": "grants.pdf",
            "source_document_title": "Grants",
            "source_page": 5,
            "source_figure": "Figure 1",
            "extraction_method": "tabula",
            "source_system": "pronabec_reports",
            "source_dataset": "report_beca18_becas_otorgadas_modalidad_anual",
            "extraction_date": "2026-06-15",
            "ingestion_timestamp": "2026-06-16T12:00:00Z",
            "pipeline_run_id": "test-run",
            "extra_field": "Simulated extra value to trigger schema mismatch"
        }]

    monkeypatch.setattr("pipelines.dataflow_bronze_to_silver.transform_pronabec_report_record", mock_transform_extra)

    dlq_root = tmp_path / "dlq"

    run([
        "--source-system", "pronabec_reports",
        "--source-dataset", "report_beca18_becas_otorgadas_modalidad_anual",
        "--extraction-date", "2026-06-15",
        "--input-path", str(input_file),
        "--input-format", "csv",
        "--runner", "DirectRunner",
        "--dlq-output-root", str(dlq_root),
        "--dry-run",
    ])

    dlq_file = dlq_root / "pronabec_reports" / "report_beca18_becas_otorgadas_modalidad_anual" / "extraction_date=2026-06-15" / "rejected_records.jsonl"
    assert dlq_file.exists()

    lines = dlq_file.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1

    rejected = json.loads(lines[0])
    assert rejected["processing_stage"] == "validation"
    assert rejected["error_code"] == "SCHEMA_MISMATCH"
    assert "extra_field" in rejected["error_message"]
    assert rejected["failed_field"] == "extra_field"
    assert rejected["failed_value"] == "Simulated extra value to trigger schema mismatch"


def test_dataflow_dlq_disabled(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    input_file = tmp_path / "bronze_convocatorias.jsonl"
    record = {
        "source_row_id": "1",
        "id_convocatoria": "100",
        "codigo_anual": "2026-01",
        "description_conv": "Beca",
        "modalidad": "ORDINARIA",
        "programa": "BECA18",
        "vacantes": "50",
    }
    with open(input_file, "w", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")

    def mock_transform_fail(*args, **kwargs):
        raise ValueError("Simulated transform error")

    monkeypatch.setattr("pipelines.dataflow_bronze_to_silver.transform_pronabec_record", mock_transform_fail)

    dlq_root = tmp_path / "dlq"

    # Run with --disable-dlq
    run([
        "--source-system", "pronabec",
        "--source-dataset", "convocatorias",
        "--extraction-date", "2026-06-15",
        "--input-path", str(input_file),
        "--input-format", "jsonl",
        "--runner", "DirectRunner",
        "--dlq-output-root", str(dlq_root),
        "--disable-dlq",
        "--dry-run",
    ])

    # DLQ file should NOT be created
    dlq_file = dlq_root / "pronabec" / "convocatorias" / "extraction_date=2026-06-15" / "rejected_records.jsonl"
    assert not dlq_file.exists()
