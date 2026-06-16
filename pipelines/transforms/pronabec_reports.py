"""Framework for PRONABEC official aggregate report transforms."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal

from pipelines.common.text_normalization import (
    fix_mojibake,
    normalize_whitespace,
    remove_control_characters,
)
from pipelines.transforms.base import parse_int_safe, parse_numeric_safe


ReportType = Literal["annual_wide", "snapshot"]
FieldType = Literal["string", "int", "numeric", "percent"]

DOCUMENT_METADATA_FIELDS = (
    "source_document_file",
    "source_document_title",
    "source_publication_url",
    "source_page",
    "source_figure",
    "source_table",
    "extraction_method",
)
TECHNICAL_METADATA_FIELDS = (
    "source_system",
    "source_dataset",
    "extraction_date",
    "ingestion_timestamp",
    "pipeline_run_id",
)
TOTAL_VALUES = {"TOTAL", "TOTAL GENERAL"}
NULL_LIKE_VALUES = {"", "-", "NULL", "N.D.", "N/D", "ND"}


@dataclass(frozen=True)
class ReportTransformSpec:
    """Declarative transform spec for a PRONABEC aggregate report."""

    source_dataset: str
    target_table: str
    report_type: ReportType
    dimension_columns: tuple[str, ...]
    field_types: dict[str, FieldType]
    metadata_fields: tuple[str, ...] = DOCUMENT_METADATA_FIELDS
    zero_dash_fields: frozenset[str] = frozenset()
    drop_total_rows: bool = True
    drop_total_columns: bool = True
    value_field: str | None = None
    year_field: str = "ano_convocatoria"
    preliminary_field: str = "es_anio_preliminar"
    output_fields: tuple[str, ...] = field(default_factory=tuple)


REPORT_SPECS: dict[str, ReportTransformSpec] = {}


def clean_report_text(value: Any) -> str | None:
    """Clean report text technically while preserving readable accents/case."""
    fixed = fix_mojibake(value)
    without_controls = remove_control_characters(fixed)
    return normalize_whitespace(without_controls)


def _clean_number_text(value: Any) -> str | None:
    text = clean_report_text(value)
    if text is None:
        return None
    return text.replace("\u00a0", " ").strip()


def _is_null_like(value: Any) -> bool:
    text = _clean_number_text(value)
    return text is None or text.upper() in NULL_LIKE_VALUES


def parse_report_int(value: Any, zero_dash: bool = False) -> int | None:
    """Parse integer values from reports, handling thousands commas."""
    text = _clean_number_text(value)
    if text is None:
        return None
    if text == "-":
        return 0 if zero_dash else None
    if text.upper() in NULL_LIKE_VALUES:
        return None
    return parse_int_safe(text)


def parse_report_numeric(value: Any) -> float | None:
    """Parse numeric values from reports, including comma decimals."""
    if _is_null_like(value):
        return None
    return parse_numeric_safe(value)


def parse_report_percent(value: Any) -> float | None:
    """Parse percentages in 0-100 scale, without dividing by 100."""
    if _is_null_like(value):
        return None
    text = _clean_number_text(value)
    if text is None:
        return None
    return parse_report_numeric(text.replace("%", "").strip())


def is_total_column(column_name: str) -> bool:
    cleaned = clean_report_text(column_name)
    return cleaned is not None and cleaned.upper() == "TOTAL"


def parse_year_column(column_name: str) -> tuple[int, bool] | None:
    cleaned = clean_report_text(column_name)
    if cleaned is None or is_total_column(cleaned):
        return None

    lowered = cleaned.lower()
    preliminary = "(*)" in lowered or "preliminar" in lowered
    match = re.search(r"(20\d{2}|19\d{2})", cleaned)
    if not match:
        return None
    return int(match.group(1)), preliminary


def is_preliminary_year_column(column_name: str) -> bool:
    parsed = parse_year_column(column_name)
    return bool(parsed and parsed[1])


def is_total_row(record: dict[str, Any]) -> bool:
    for key, value in record.items():
        if parse_year_column(str(key)) or is_total_column(str(key)):
            continue
        cleaned = clean_report_text(value)
        if cleaned is not None and cleaned.upper() in TOTAL_VALUES:
            return True
    return False


def build_report_metadata(
    source_dataset: str,
    context: dict[str, Any],
    record: dict[str, Any] | None = None,
    metadata_fields: tuple[str, ...] = DOCUMENT_METADATA_FIELDS,
) -> dict[str, Any]:
    source = record or {}
    ingestion_timestamp = context.get("ingestion_timestamp") or datetime.now(UTC).isoformat()
    metadata: dict[str, Any] = {}

    for field_name in metadata_fields:
        if field_name == "source_page":
            metadata[field_name] = parse_report_int(source.get(field_name))
        else:
            metadata[field_name] = clean_report_text(source.get(field_name))

    metadata.update(
        {
            "source_system": "pronabec_reports",
            "source_dataset": source_dataset,
            "extraction_date": context.get("extraction_date"),
            "ingestion_timestamp": ingestion_timestamp,
            "pipeline_run_id": context.get("pipeline_run_id"),
        }
    )
    return metadata


def _convert_field(value: Any, field_type: FieldType, zero_dash: bool = False) -> Any:
    if field_type == "int":
        return parse_report_int(value, zero_dash=zero_dash)
    if field_type == "numeric":
        return parse_report_numeric(value)
    if field_type == "percent":
        return parse_report_percent(value)
    return clean_report_text(value)


def _project_output(record: dict[str, Any], spec: ReportTransformSpec) -> dict[str, Any]:
    if not spec.output_fields:
        return record
    return {field_name: record.get(field_name) for field_name in spec.output_fields}


def unpivot_annual_report(
    record: dict[str, Any],
    spec: ReportTransformSpec,
    context: dict[str, Any],
) -> list[dict[str, Any]]:
    """Unpivot one wide annual report row into long Silver records."""
    if spec.drop_total_rows and is_total_row(record):
        return []
    if spec.value_field is None:
        raise ValueError(f"Annual report spec '{spec.source_dataset}' requires value_field.")

    dimensions = {
        column: clean_report_text(record.get(column))
        for column in spec.dimension_columns
    }
    metadata = build_report_metadata(
        spec.source_dataset,
        context,
        record=record,
        metadata_fields=spec.metadata_fields,
    )
    rows: list[dict[str, Any]] = []
    for column, value in record.items():
        if spec.drop_total_columns and is_total_column(str(column)):
            continue
        parsed_year = parse_year_column(str(column))
        if parsed_year is None:
            continue

        year, is_preliminary = parsed_year
        transformed = {
            **dimensions,
            spec.year_field: year,
            spec.value_field: _convert_field(
                value,
                spec.field_types[spec.value_field],
                zero_dash=spec.value_field in spec.zero_dash_fields,
            ),
            spec.preliminary_field: is_preliminary,
            **metadata,
        }
        rows.append(_project_output(transformed, spec))
    return rows


def transform_snapshot_report(
    record: dict[str, Any],
    spec: ReportTransformSpec,
    context: dict[str, Any],
) -> list[dict[str, Any]]:
    """Transform a row-shaped report into one Silver record."""
    if spec.drop_total_rows and is_total_row(record):
        return []

    transformed: dict[str, Any] = {}
    for field_name, field_type in spec.field_types.items():
        transformed[field_name] = _convert_field(
            record.get(field_name),
            field_type,
            zero_dash=field_name in spec.zero_dash_fields,
        )
    transformed.update(
        build_report_metadata(
            spec.source_dataset,
            context,
            record=record,
            metadata_fields=spec.metadata_fields,
        )
    )
    return [_project_output(transformed, spec)]


def transform_pronabec_report_record(
    source_dataset: str,
    record: dict[str, Any],
    context: dict[str, Any],
) -> list[dict[str, Any]]:
    """Route one PRONABEC aggregate report record through its configured spec."""
    spec = REPORT_SPECS.get(source_dataset)
    if spec is None:
        raise ValueError(f"Unsupported PRONABEC report dataset '{source_dataset}'.")
    if spec.report_type == "annual_wide":
        return unpivot_annual_report(record, spec, context)
    return transform_snapshot_report(record, spec, context)
