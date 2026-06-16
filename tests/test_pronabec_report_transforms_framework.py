"""Tests for the PRONABEC aggregate report transform framework."""

from __future__ import annotations

import pytest

from pipelines.dataflow_bronze_to_silver import transform_bronze_records
from pipelines.transforms.pronabec_reports import (
    ReportTransformSpec,
    clean_report_text,
    is_total_column,
    is_total_row,
    parse_report_int,
    parse_report_numeric,
    parse_report_percent,
    parse_year_column,
    transform_pronabec_report_record,
    transform_snapshot_report,
    unpivot_annual_report,
)


CONTEXT = {
    "extraction_date": "2026-06-15",
    "ingestion_timestamp": "2026-06-16T00:00:00+00:00",
    "pipeline_run_id": "test-run",
}


def test_clean_report_text() -> None:
    assert clean_report_text(" UNIVERSIDAD   NACIONAL ") == "UNIVERSIDAD NACIONAL"
    assert clean_report_text("Beca GeneraciÃ³n") == "Beca Generación"
    assert clean_report_text("   ") is None


def test_parse_report_int() -> None:
    assert parse_report_int("1,234") == 1234
    assert parse_report_int("66,629") == 66629
    assert parse_report_int("-") is None
    assert parse_report_int("-", zero_dash=True) == 0
    assert parse_report_int("") is None
    assert parse_report_int(None) is None


def test_parse_report_numeric() -> None:
    assert parse_report_numeric("16,43") == 16.43
    assert parse_report_numeric("16.43") == 16.43
    assert parse_report_numeric("1,234.56") == 1234.56
    assert parse_report_numeric("-") is None


def test_parse_report_percent() -> None:
    assert parse_report_percent("12.5%") == 12.5
    assert parse_report_percent("12,5%") == 12.5
    assert parse_report_percent("12.5") == 12.5
    assert parse_report_percent("-") is None


def test_parse_year_column() -> None:
    assert parse_year_column("2012") == (2012, False)
    assert parse_year_column("2025") == (2025, False)
    assert parse_year_column("2026 (*)") == (2026, True)
    assert parse_year_column("anio_2026_preliminar") == (2026, True)
    assert parse_year_column("Total") is None


def test_total_detection() -> None:
    assert is_total_column("Total") is True
    assert is_total_column("2026 (*)") is False
    assert is_total_row({"universidad": "Total general", "2012": "10"}) is True
    assert is_total_row({"carrera_estudio": "TOTAL GENERAL", "2012": "10"}) is True
    assert is_total_row({"modalidad": "BECA 18", "2012": "10"}) is False


def test_unpivot_annual_report() -> None:
    spec = ReportTransformSpec(
        source_dataset="synthetic_university",
        target_table="silver.synthetic_university",
        report_type="annual_wide",
        dimension_columns=("universidad",),
        field_types={"cantidad_becarios": "int"},
        zero_dash_fields=frozenset({"cantidad_becarios"}),
        value_field="cantidad_becarios",
        output_fields=(
            "universidad",
            "ano_convocatoria",
            "cantidad_becarios",
            "es_anio_preliminar",
            "source_document_file",
            "source_document_title",
            "source_publication_url",
            "source_page",
            "source_table",
            "extraction_method",
            "source_system",
            "source_dataset",
            "extraction_date",
            "ingestion_timestamp",
            "pipeline_run_id",
        ),
    )
    record = {
        "universidad": " UNIVERSIDAD   NACIONAL ",
        "2012": "1,000",
        "2013": "-",
        "2026 (*)": "10",
        "Total": "1,010",
        "source_document_file": "beca18.pdf",
    }

    transformed = unpivot_annual_report(record, spec, CONTEXT)

    assert len(transformed) == 3
    assert transformed[0]["universidad"] == "UNIVERSIDAD NACIONAL"
    assert transformed[0]["ano_convocatoria"] == 2012
    assert transformed[0]["cantidad_becarios"] == 1000
    assert transformed[0]["es_anio_preliminar"] is False
    assert transformed[1]["ano_convocatoria"] == 2013
    assert transformed[1]["cantidad_becarios"] == 0
    assert transformed[2]["ano_convocatoria"] == 2026
    assert transformed[2]["es_anio_preliminar"] is True
    assert all(row["source_document_file"] == "beca18.pdf" for row in transformed)


def test_unpivot_annual_report_omits_total_general_row() -> None:
    spec = ReportTransformSpec(
        source_dataset="synthetic_university",
        target_table="silver.synthetic_university",
        report_type="annual_wide",
        dimension_columns=("universidad",),
        field_types={"cantidad_becarios": "int"},
        value_field="cantidad_becarios",
    )

    assert unpivot_annual_report({"universidad": "Total general", "2012": "10"}, spec, CONTEXT) == []


def test_transform_snapshot_report() -> None:
    spec = ReportTransformSpec(
        source_dataset="synthetic_snapshot",
        target_table="silver.synthetic_snapshot",
        report_type="snapshot",
        dimension_columns=("categoria",),
        field_types={
            "ano_encuesta": "int",
            "categoria": "string",
            "cantidad": "int",
            "porcentaje": "percent",
        },
        output_fields=(
            "ano_encuesta",
            "categoria",
            "cantidad",
            "porcentaje",
            "source_page",
            "source_system",
            "source_dataset",
            "extraction_date",
            "ingestion_timestamp",
            "pipeline_run_id",
        ),
    )

    transformed = transform_snapshot_report(
        {
            "ano_encuesta": "2025",
            "categoria": " Beca   GeneraciÃ³n ",
            "cantidad": "1,234",
            "porcentaje": "12,5%",
            "source_page": "7",
        },
        spec,
        CONTEXT,
    )

    assert transformed == [
        {
            "ano_encuesta": 2025,
            "categoria": "Beca Generación",
            "cantidad": 1234,
            "porcentaje": 12.5,
            "source_page": 7,
            "source_system": "pronabec_reports",
            "source_dataset": "synthetic_snapshot",
            "extraction_date": "2026-06-15",
            "ingestion_timestamp": "2026-06-16T00:00:00+00:00",
            "pipeline_run_id": "test-run",
        }
    ]


def test_unknown_report_router_raises_clear_error() -> None:
    with pytest.raises(ValueError, match="Unsupported PRONABEC report dataset"):
        transform_pronabec_report_record("dataset_no_soportado", {}, CONTEXT)


def test_dataflow_hook_falls_back_for_unmapped_report_dataset() -> None:
    transformed = transform_bronze_records(
        {"id": "1"},
        source_system="pronabec_reports",
        source_dataset="dataset_no_soportado",
        extraction_date="2026-06-15",
        ingestion_timestamp="2026-06-16T00:00:00+00:00",
        pipeline_run_id="test-run",
    )

    assert transformed[0]["id"] == "1"
    assert transformed[0]["source_system"] == "pronabec_reports"
    assert transformed[0]["source_dataset"] == "dataset_no_soportado"
    assert transformed[0]["extraction_date"] == "2026-06-15"
    assert transformed[0]["pipeline_run_id"] == "test-run"
    assert transformed[0]["ingestion_timestamp"]
