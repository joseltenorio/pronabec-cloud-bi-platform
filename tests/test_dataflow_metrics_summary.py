# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any
import pytest
import apache_beam as beam

from pipelines.dataflow_bronze_to_silver import run
from pipelines.common.bigquery import BigQueryWriteConfig


# Mock para el PTransform de escritura en BigQuery que evita llamadas reales a GCP
class FakeWriteTransform(beam.PTransform):
    def expand(self, pcoll):
        return pcoll | beam.Map(lambda x: x)


@pytest.fixture
def mock_bigquery_sink(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Fixture para mockear el sink de BigQuery y evitar llamadas a GCP."""
    captured = {}

    def fake_build_sink(config: BigQueryWriteConfig) -> beam.PTransform:
        captured["config"] = config
        return FakeWriteTransform()

    monkeypatch.setattr(
        "pipelines.dataflow_bronze_to_silver.build_bigquery_write_transform",
        fake_build_sink,
    )
    monkeypatch.setattr(
        "pipelines.dataflow_bronze_to_silver.cleanup_silver_rows_for_source_date",
        lambda **kwargs: 0,
    )
    return captured


def test_summary_success_no_rejections(tmp_path: Path) -> None:
    """1. Valida el resumen de procesamiento exitoso con cero registros rechazados."""
    # Preparar archivo de entrada con 2 registros válidos
    input_file = tmp_path / "bronze_convocatorias.jsonl"
    record1 = {
        "source_row_id": "1",
        "id_convocatoria": "100",
        "codigo_anual": "2026-01",
        "description_conv": "Beca",
        "modalidad": "ORDINARIA",
        "programa": "BECA18",
        "vacantes": "50",
    }
    record2 = {
        "source_row_id": "2",
        "id_convocatoria": "101",
        "codigo_anual": "2026-02",
        "description_conv": "Beca B",
        "modalidad": "ORDINARIA",
        "programa": "BECA18",
        "vacantes": "10",
    }

    with open(input_file, "w", encoding="utf-8") as f:
        f.write(json.dumps(record1) + "\n")
        f.write(json.dumps(record2) + "\n")

    summary_file = tmp_path / "summary.json"

    # Ejecutar pipeline en modo dry-run
    run([
        "--source-system", "pronabec",
        "--source-dataset", "convocatorias",
        "--extraction-date", "2026-06-15",
        "--input-path", str(input_file),
        "--input-format", "jsonl",
        "--runner", "DirectRunner",
        "--dry-run",
        "--summary-output-path", str(summary_file),
    ])

    # Verificar existencia y contenido del resumen
    assert summary_file.exists()
    summary = json.loads(summary_file.read_text(encoding="utf-8"))

    assert summary["records_read"] == 2
    assert summary["records_valid"] == 2
    assert summary["records_rejected"] == 0
    assert summary["rejection_rate"] == 0.0
    assert summary["status"] == "COMPLETED"
    assert summary["dry_run"] is True


def test_summary_with_rejections(tmp_path: Path) -> None:
    """2. Valida el resumen cuando existen registros rechazados en el pipeline."""
    # Preparar archivo con 1 registro válido y 1 corrupto
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
    corrupted_line = '{"source_row_id": "2", "id_convocatoria": '  # JSON truncado

    with open(input_file, "w", encoding="utf-8") as f:
        f.write(json.dumps(valid_record) + "\n")
        f.write(corrupted_line + "\n")

    summary_file = tmp_path / "summary.json"
    dlq_root = tmp_path / "dlq"

    run([
        "--source-system", "pronabec",
        "--source-dataset", "convocatorias",
        "--extraction-date", "2026-06-15",
        "--input-path", str(input_file),
        "--input-format", "jsonl",
        "--runner", "DirectRunner",
        "--dry-run",
        "--dlq-output-root", str(dlq_root),
        "--summary-output-path", str(summary_file),
    ])

    assert summary_file.exists()
    summary = json.loads(summary_file.read_text(encoding="utf-8"))

    assert summary["records_read"] == 2
    assert summary["records_valid"] == 1
    assert summary["records_rejected"] == 1
    assert summary["rejection_rate"] == 0.5
    assert summary["status"] == "COMPLETED_WITH_REJECTIONS"


def test_summary_empty_input_division_by_zero(tmp_path: Path) -> None:
    """3. Valida que records_read = 0 no cause división por cero y resulte en rejection_rate = 0."""
    input_file = tmp_path / "empty.jsonl"
    input_file.write_text("", encoding="utf-8")

    summary_file = tmp_path / "summary.json"

    run([
        "--source-system", "pronabec",
        "--source-dataset", "convocatorias",
        "--extraction-date", "2026-06-15",
        "--input-path", str(input_file),
        "--input-format", "jsonl",
        "--runner", "DirectRunner",
        "--dry-run",
        "--summary-output-path", str(summary_file),
    ])

    assert summary_file.exists()
    summary = json.loads(summary_file.read_text(encoding="utf-8"))

    assert summary["records_read"] == 0
    assert summary["records_valid"] == 0
    assert summary["records_rejected"] == 0
    assert summary["rejection_rate"] == 0.0
    assert summary["status"] == "COMPLETED"


def test_summary_fatal_error(tmp_path: Path) -> None:
    """4. Valida que un error fatal (como argumentos faltantes) registre estado FAILED y error_message."""
    summary_file = tmp_path / "summary_failed.json"

    # La omisión de --input-path causará un ValueError en la validación
    with pytest.raises(ValueError, match="El argumento crítico --input-path es requerido"):
        run([
            "--source-system", "pronabec",
            "--source-dataset", "convocatorias",
            "--extraction-date", "2026-06-15",
            # Omitimos --input-path a propósito
            "--input-format", "jsonl",
            "--runner", "DirectRunner",
            "--dry-run",
            "--summary-output-path", str(summary_file),
        ])

    assert summary_file.exists()
    summary = json.loads(summary_file.read_text(encoding="utf-8"))

    assert summary["status"] == "FAILED"
    assert "error_message" in summary
    assert "El argumento crítico --input-path es requerido" in summary["error_message"]


def test_summary_dry_run_flag(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """5. Valida que en dry-run se registre dry_run = True y se evite BigQuery."""
    input_file = tmp_path / "data.jsonl"
    record = {
        "source_row_id": "1",
        "id_convocatoria": "100",
        "codigo_anual": "2026-01",
        "description_conv": "Beca",
        "modalidad": "ORDINARIA",
        "programa": "BECA18",
        "vacantes": "50",
    }
    input_file.write_text(json.dumps(record) + "\n", encoding="utf-8")

    def fail_if_called(*args, **kwargs):
        raise AssertionError("BigQuery sink no debe construirse en modo dry-run")

    monkeypatch.setattr(
        "pipelines.dataflow_bronze_to_silver.build_bigquery_write_transform",
        fail_if_called,
    )

    summary_file = tmp_path / "summary.json"

    run([
        "--source-system", "pronabec",
        "--source-dataset", "convocatorias",
        "--extraction-date", "2026-06-15",
        "--input-path", str(input_file),
        "--input-format", "jsonl",
        "--runner", "DirectRunner",
        "--dry-run",
        "--summary-output-path", str(summary_file),
    ])

    summary = json.loads(summary_file.read_text(encoding="utf-8"))
    assert summary["dry_run"] is True


def test_summary_no_dry_run_calls_mocked_bigquery(
    tmp_path: Path,
    mock_bigquery_sink: dict[str, Any],
) -> None:
    """6. Valida que no dry-run registre dry_run = False y registre la tabla de salida."""
    input_file = tmp_path / "data.jsonl"
    record = {
        "source_row_id": "1",
        "id_convocatoria": "100",
        "codigo_anual": "2026-01",
        "description_conv": "Beca",
        "modalidad": "ORDINARIA",
        "programa": "BECA18",
        "vacantes": "50",
    }
    input_file.write_text(json.dumps(record) + "\n", encoding="utf-8")

    summary_file = tmp_path / "summary.json"

    run([
        "--source-system", "pronabec",
        "--source-dataset", "convocatorias",
        "--extraction-date", "2026-06-15",
        "--input-path", str(input_file),
        "--input-format", "jsonl",
        "--runner", "DirectRunner",
        "--output-table", "project-123:silver.pronabec_convocatorias",
        "--temp-location", "gs://test-bucket/temp",
        "--summary-output-path", str(summary_file),
    ])

    summary = json.loads(summary_file.read_text(encoding="utf-8"))
    assert summary["dry_run"] is False
    assert summary["output_table"] == "project-123:silver.pronabec_convocatorias"
    assert "config" in mock_bigquery_sink


def test_summary_dlq_enabled_and_disabled(tmp_path: Path) -> None:
    """7 & 8. Valida comportamiento del summary con DLQ habilitado y deshabilitado."""
    input_file = tmp_path / "data.jsonl"
    record = {"source_row_id": "1", "id_convocatoria": "100"}
    input_file.write_text(json.dumps(record) + "\n", encoding="utf-8")

    # Caso 7: DLQ Habilitado
    summary_file1 = tmp_path / "summary_dlq_enabled.json"
    run([
        "--source-system", "pronabec",
        "--source-dataset", "convocatorias",
        "--extraction-date", "2026-06-15",
        "--input-path", str(input_file),
        "--input-format", "jsonl",
        "--runner", "DirectRunner",
        "--dry-run",
        "--dlq-output-root", str(tmp_path / "dlq"),
        "--summary-output-path", str(summary_file1),
    ])

    summary1 = json.loads(summary_file1.read_text(encoding="utf-8"))
    assert summary1["dlq_enabled"] is True
    assert "dlq" in summary1["dlq_output_path"]

    # Caso 8: DLQ Deshabilitado
    summary_file2 = tmp_path / "summary_dlq_disabled.json"
    run([
        "--source-system", "pronabec",
        "--source-dataset", "convocatorias",
        "--extraction-date", "2026-06-15",
        "--input-path", str(input_file),
        "--input-format", "jsonl",
        "--runner", "DirectRunner",
        "--dry-run",
        "--disable-dlq",
        "--summary-output-path", str(summary_file2),
    ])

    summary2 = json.loads(summary_file2.read_text(encoding="utf-8"))
    assert summary2["dlq_enabled"] is False
    assert summary2["dlq_output_path"] is None


def test_summary_pronabec_source(tmp_path: Path) -> None:
    """9. Valida compatibilidad y resumen con el sistema origen PRONABEC."""
    input_file = tmp_path / "data.jsonl"
    record = {
        "source_row_id": "12",
        "id_convocatoria": "5",
        "codigo_anual": "2025-03",
        "description_conv": "Beca de Prueba",
        "modalidad": "ORDINARIA",
        "programa": "BECA18",
        "vacantes": "2",
    }
    input_file.write_text(json.dumps(record) + "\n", encoding="utf-8")

    summary_file = tmp_path / "summary.json"
    run([
        "--source-system", "pronabec",
        "--source-dataset", "convocatorias",
        "--extraction-date", "2026-06-15",
        "--input-path", str(input_file),
        "--input-format", "jsonl",
        "--runner", "DirectRunner",
        "--dry-run",
        "--summary-output-path", str(summary_file),
    ])

    summary = json.loads(summary_file.read_text(encoding="utf-8"))
    assert summary["source_system"] == "pronabec"
    assert summary["source_dataset"] == "convocatorias"
    assert summary["records_read"] == 1
    assert summary["records_valid"] == 1


def test_summary_mef_source(tmp_path: Path) -> None:
    """10. Valida compatibilidad y resumen con el sistema origen MEF."""
    input_file = tmp_path / "mef.csv"
    csv_content = (
        "ano,codigo_entidad,entidad,pia,pim,devengado\n"
        "2026,123,PRONABEC,1000.0,1200.0,800.0\n"
    )
    input_file.write_text(csv_content, encoding="utf-8")

    summary_file = tmp_path / "summary.json"
    run([
        "--source-system", "mef",
        "--source-dataset", "presupuesto_mef",
        "--extraction-date", "2026-06-15",
        "--input-path", str(input_file),
        "--input-format", "csv",
        "--runner", "DirectRunner",
        "--dry-run",
        "--summary-output-path", str(summary_file),
    ])

    summary = json.loads(summary_file.read_text(encoding="utf-8"))
    assert summary["source_system"] == "mef"
    assert summary["source_dataset"] == "presupuesto_mef"
    assert summary["records_read"] == 1
    assert summary["records_valid"] == 1


def test_summary_pronabec_reports_universitarios(tmp_path: Path) -> None:
    """11. Valida compatibilidad con reportes agregados universitarios PRONABEC."""
    input_file = tmp_path / "carrera.csv"
    csv_content = (
        "carrera_estudio,2026 (*),Total,source_document_file,source_document_title,source_publication_url,source_page,source_table,extraction_method\n"
        "MEDICINA,10,10,doc.pdf,Reporte,http://url,5,Tabla 1,tabula\n"
    )
    input_file.write_text(csv_content, encoding="utf-8")

    summary_file = tmp_path / "summary.json"
    run([
        "--source-system", "pronabec_reports",
        "--source-dataset", "report_beca18_universitarios_carrera_anual",
        "--extraction-date", "2026-06-15",
        "--input-path", str(input_file),
        "--input-format", "csv",
        "--runner", "DirectRunner",
        "--dry-run",
        "--summary-output-path", str(summary_file),
    ])

    summary = json.loads(summary_file.read_text(encoding="utf-8"))
    assert summary["source_system"] == "pronabec_reports"
    assert summary["source_dataset"] == "report_beca18_universitarios_carrera_anual"
    assert summary["records_read"] == 1
    assert summary["records_valid"] == 1


def test_summary_pronabec_reports_pes_2025(tmp_path: Path) -> None:
    """12. Valida compatibilidad y resumen con el subconjunto de reportes PES 2025."""
    input_file = tmp_path / "modalidad.csv"
    csv_content = (
        "modalidad,2026 (*),Total,source_document_file,source_document_title,source_page,source_figure,extraction_method\n"
        "ORDINARIA,5,5,pes.pdf,Estudios,2,Figura A,camelot\n"
    )
    input_file.write_text(csv_content, encoding="utf-8")

    summary_file = tmp_path / "summary.json"
    run([
        "--source-system", "pronabec_reports",
        "--source-dataset", "report_beca18_becas_otorgadas_modalidad_anual",
        "--extraction-date", "2026-06-15",
        "--input-path", str(input_file),
        "--input-format", "csv",
        "--runner", "DirectRunner",
        "--dry-run",
        "--summary-output-path", str(summary_file),
    ])

    summary = json.loads(summary_file.read_text(encoding="utf-8"))
    assert summary["source_system"] == "pronabec_reports"
    assert summary["source_dataset"] == "report_beca18_becas_otorgadas_modalidad_anual"
    assert summary["records_read"] == 1
    assert summary["records_valid"] == 1


def test_summary_output_validation_local(tmp_path: Path) -> None:
    """13. Valida la correcta escritura local del JSON del resumen y su integridad."""
    input_file = tmp_path / "data.jsonl"
    record = {"source_row_id": "1", "id_convocatoria": "10"}
    input_file.write_text(json.dumps(record) + "\n", encoding="utf-8")

    summary_file = tmp_path / "subdir" / "nested" / "my_summary.json"

    run([
        "--source-system", "pronabec",
        "--source-dataset", "convocatorias",
        "--extraction-date", "2026-06-15",
        "--input-path", str(input_file),
        "--input-format", "jsonl",
        "--runner", "DirectRunner",
        "--dry-run",
        "--summary-output-path", str(summary_file),
    ])

    assert summary_file.exists()
    summary = json.loads(summary_file.read_text(encoding="utf-8"))

    expected_keys = {
        "source_system", "source_dataset", "extraction_date", "pipeline_run_id",
        "input_format", "input_path", "output_table", "dry_run", "dlq_enabled",
        "dlq_output_path", "records_read", "records_valid", "records_rejected",
        "rejection_rate", "started_at", "finished_at", "duration_seconds", "status"
    }
    assert expected_keys.issubset(summary.keys())
    assert isinstance(summary["records_read"], int)
    assert isinstance(summary["rejection_rate"], float)
    assert summary["status"] in ("COMPLETED", "COMPLETED_WITH_REJECTIONS", "FAILED")
