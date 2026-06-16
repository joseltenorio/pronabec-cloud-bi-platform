"""Tests for concrete PRONABEC aggregate report transform mappings."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from pipelines.transforms.pronabec_reports import (
    REPORT_SPECS,
    DOCUMENT_METADATA_FIELDS,
    TECHNICAL_METADATA_FIELDS,
    ReportTransformSpec,
    transform_pronabec_report_record,
)


CONTEXT = {
    "extraction_date": "2026-06-15",
    "ingestion_timestamp": "2026-06-16T00:00:00+00:00",
    "pipeline_run_id": "test-run",
}
REPO_ROOT = Path(__file__).resolve().parents[1]
BRONZE_SCHEMA_DIR = REPO_ROOT / "config" / "schemas" / "bronze"
SILVER_SCHEMA_DIR = REPO_ROOT / "config" / "schemas" / "silver"


def schema_fields(path: Path) -> list[str]:
    return [field["name"] for field in json.loads(path.read_text(encoding="utf-8"))]


def silver_fields(source_dataset: str) -> list[str]:
    return schema_fields(SILVER_SCHEMA_DIR / f"pronabec_{source_dataset}_schema.json")


def sample_value(field_name: str, field_type: str) -> str:
    if field_name.startswith("ano_"):
        return "2025"
    if field_name == "source_page":
        return "7"
    if field_type == "int":
        return "1,234"
    if field_type == "percent":
        return "12,5%"
    if field_type == "numeric":
        return "16,43"
    return f" {field_name.replace('_', ' ').upper()}   TECNICO "


def build_snapshot_record(spec: ReportTransformSpec) -> dict[str, Any]:
    record = {
        field_name: sample_value(field_name, field_type)
        for field_name, field_type in spec.field_types.items()
    }
    record.update(
        {
            "source_document_file": "report.pdf",
            "source_document_title": "Reporte PRONABEC",
            "source_publication_url": "https://example.test/report",
            "source_page": "7",
            "source_figure": "Figura 1",
            "source_table": "Tabla 1",
            "extraction_method": "manual_csv",
        }
    )
    return record


def assert_exact_silver_columns(source_dataset: str, row: dict[str, Any]) -> None:
    assert list(row) == silver_fields(source_dataset)


def test_all_specs_have_bronze_and_silver_schema_pairs() -> None:
    assert REPORT_SPECS
    for source_dataset in REPORT_SPECS:
        assert (BRONZE_SCHEMA_DIR / f"{source_dataset}_schema.json").exists()
        assert (SILVER_SCHEMA_DIR / f"pronabec_{source_dataset}_schema.json").exists()


def test_each_spec_has_required_contract_fields() -> None:
    for source_dataset, spec in REPORT_SPECS.items():
        assert spec.source_dataset == source_dataset
        assert spec.target_table == f"pronabec_{source_dataset}"
        assert spec.report_type in {"annual_wide", "snapshot"}
        assert spec.dimension_columns
        assert spec.field_types
        assert spec.metadata_fields
        assert spec.output_fields


@pytest.mark.parametrize(
    "source_dataset",
    sorted(name for name, spec in REPORT_SPECS.items() if spec.report_type == "annual_wide"),
)
def test_annual_wide_specs_match_silver_schema(source_dataset: str) -> None:
    spec = REPORT_SPECS[source_dataset]
    dimension = spec.dimension_columns[0]
    record = {
        dimension: " UNIVERSIDAD   NACIONAL "
        if dimension == "universidad"
        else "INGENIERÍA   DE SISTEMAS",
        "anio_2012": "1,000",
        "anio_2026_preliminar": "10",
        "total": "1,010",
        "source_document_file": "beca18.pdf",
        "source_document_title": "Beca 18 universitarios",
        "source_publication_url": "https://example.test/beca18",
        "source_page": "8",
        "source_table": "Tabla 1",
        "extraction_method": "manual_csv",
    }

    rows = transform_pronabec_report_record(source_dataset, record, CONTEXT)

    assert len(rows) == 2
    for row in rows:
        assert_exact_silver_columns(source_dataset, row)
        assert row["source_system"] == "pronabec_reports"
        assert row["source_dataset"] == source_dataset
        assert row["source_document_file"] == "beca18.pdf"
        assert row["source_page"] == 8
    assert rows[0]["ano_convocatoria"] == 2012
    assert rows[0]["cantidad_becarios"] == 1000
    assert rows[0]["es_anio_preliminar"] is False
    assert rows[1]["ano_convocatoria"] == 2026
    assert rows[1]["cantidad_becarios"] == 10
    assert rows[1]["es_anio_preliminar"] is True


def test_universidad_and_carrera_annual_fixtures() -> None:
    universidad_rows = transform_pronabec_report_record(
        "report_beca18_universitarios_universidad_anual",
        {
            "universidad": "UNIVERSIDAD   NACIONAL",
            "2012": "1,000",
            "2026 (*)": "10",
            "Total": "1,010",
        },
        CONTEXT,
    )
    carrera_rows = transform_pronabec_report_record(
        "report_beca18_universitarios_carrera_anual",
        {
            "carrera_estudio": "INGENIERÍA   DE SISTEMAS",
            "2012": "-",
            "2026 (*)": "5",
            "Total": "5",
        },
        CONTEXT,
    )

    assert universidad_rows[0]["universidad"] == "UNIVERSIDAD NACIONAL"
    assert universidad_rows[0]["cantidad_becarios"] == 1000
    assert carrera_rows[0]["carrera_estudio"] == "INGENIERÍA DE SISTEMAS"
    assert carrera_rows[0]["cantidad_becarios"] == 0
    assert carrera_rows[1]["cantidad_becarios"] == 5


@pytest.mark.parametrize(
    "source_dataset",
    sorted(name for name, spec in REPORT_SPECS.items() if spec.report_type == "annual_wide"),
)
def test_annual_wide_specs_skip_total_general_rows(source_dataset: str) -> None:
    spec = REPORT_SPECS[source_dataset]
    dimension = spec.dimension_columns[0]

    assert transform_pronabec_report_record(
        source_dataset,
        {dimension: "Total general", "anio_2012": "10"},
        CONTEXT,
    ) == []


@pytest.mark.parametrize(
    "source_dataset",
    sorted(name for name, spec in REPORT_SPECS.items() if spec.report_type == "snapshot"),
)
def test_snapshot_specs_match_silver_schema(source_dataset: str) -> None:
    spec = REPORT_SPECS[source_dataset]
    rows = transform_pronabec_report_record(
        source_dataset,
        build_snapshot_record(spec),
        CONTEXT,
    )

    assert len(rows) == 1
    row = rows[0]
    assert_exact_silver_columns(source_dataset, row)
    assert row["source_system"] == "pronabec_reports"
    assert row["source_dataset"] == source_dataset
    assert row["extraction_date"] == "2026-06-15"
    assert row["ingestion_timestamp"] == "2026-06-16T00:00:00+00:00"
    assert row["pipeline_run_id"] == "test-run"
    if "source_page" in row:
        assert row["source_page"] == 7


def test_snapshot_2025_fixture_converts_category_and_percent() -> None:
    rows = transform_pronabec_report_record(
        "report_beca18_colegio_gestion_2025",
        {
            "ano_encuesta": "2025",
            "tipo_gestion_colegio": " Pública   Nacional ",
            "porcentaje_becarios": "12,5%",
            "source_page": "9",
        },
        CONTEXT,
    )

    assert rows == [
        {
            "ano_encuesta": 2025,
            "tipo_gestion_colegio": "Pública Nacional",
            "porcentaje_becarios": 12.5,
            "source_document_file": None,
            "source_document_title": None,
            "source_page": 9,
            "source_figure": None,
            "extraction_method": None,
            "source_system": "pronabec_reports",
            "source_dataset": "report_beca18_colegio_gestion_2025",
            "extraction_date": "2026-06-15",
            "ingestion_timestamp": "2026-06-16T00:00:00+00:00",
            "pipeline_run_id": "test-run",
        }
    ]


def test_router_supports_every_spec() -> None:
    for source_dataset, spec in REPORT_SPECS.items():
        record = (
            {
                spec.dimension_columns[0]: "VALOR",
                "anio_2012": "1",
            }
            if spec.report_type == "annual_wide"
            else build_snapshot_record(spec)
        )
        assert transform_pronabec_report_record(source_dataset, record, CONTEXT)


def test_unknown_report_dataset_raises_value_error() -> None:
    with pytest.raises(ValueError, match="Unsupported PRONABEC report dataset"):
        transform_pronabec_report_record("dataset_desconocido", {}, CONTEXT)


def test_no_business_canonicalization_for_report_categories() -> None:
    rows = transform_pronabec_report_record(
        "report_beca18_razones_eleccion_carrera_sexo_2025",
        {
            "ano_encuesta": "2025",
            "razon_eleccion_carrera": " Alta   demanda laboral ",
            "sexo": " F ",
            "porcentaje_becarios": "10",
        },
        CONTEXT,
    )

    assert rows[0]["razon_eleccion_carrera"] == "Alta demanda laboral"
    assert "razon_eleccion_carrera_canonical" not in rows[0]
    assert "match_score" not in rows[0]


def test_all_output_fields_are_from_silver_schema() -> None:
    metadata_names = set(DOCUMENT_METADATA_FIELDS) | set(TECHNICAL_METADATA_FIELDS)
    assert metadata_names
    for source_dataset, spec in REPORT_SPECS.items():
        assert set(spec.output_fields) == set(silver_fields(source_dataset))
