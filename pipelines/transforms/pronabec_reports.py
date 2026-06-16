"""Framework for PRONABEC official aggregate report transforms."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
import json
from pathlib import Path
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


def _schema_to_field_types(schema_fields: list[dict[str, Any]]) -> dict[str, FieldType]:
    field_types: dict[str, FieldType] = {}
    excluded = set(DOCUMENT_METADATA_FIELDS) | set(TECHNICAL_METADATA_FIELDS)
    for field_def in schema_fields:
        name = field_def["name"]
        if name in excluded or name == "es_anio_preliminar":
            continue
        field_type = str(field_def["type"]).upper()
        if field_type in {"INT64", "INTEGER"}:
            field_types[name] = "int"
        elif name.startswith("porcentaje_"):
            field_types[name] = "percent"
        elif field_type in {"NUMERIC", "FLOAT", "FLOAT64"}:
            field_types[name] = "numeric"
        else:
            field_types[name] = "string"
    return field_types


def _load_json_schema(path: Path) -> list[dict[str, Any]]:
    return json.loads(path.read_text(encoding="utf-8"))


def _build_spec_from_schema(
    source_dataset: str,
    bronze_schema: list[dict[str, Any]],
    silver_schema: list[dict[str, Any]],
) -> ReportTransformSpec:
    silver_fields = tuple(field_def["name"] for field_def in silver_schema)
    metadata_fields = tuple(
        field for field in DOCUMENT_METADATA_FIELDS if field in silver_fields
    )
    target_table = f"pronabec_{source_dataset}"
    bronze_field_names = {field_def["name"] for field_def in bronze_schema}
    is_wide = any(parse_year_column(field_name) for field_name in bronze_field_names)

    if is_wide:
        dimension_columns = tuple(
            field
            for field in bronze_field_names
            if field not in set(DOCUMENT_METADATA_FIELDS)
            and not is_total_column(field)
            and parse_year_column(field) is None
        )
        return ReportTransformSpec(
            source_dataset=source_dataset,
            target_table=target_table,
            report_type="annual_wide",
            dimension_columns=dimension_columns,
            field_types={"cantidad_becarios": "int"},
            metadata_fields=metadata_fields,
            zero_dash_fields=frozenset({"cantidad_becarios"}),
            value_field="cantidad_becarios",
            output_fields=silver_fields,
        )

    field_types = _schema_to_field_types(silver_schema)
    dimension_columns = tuple(
        field_name
        for field_name, field_type in field_types.items()
        if field_type == "string" or field_name.startswith("ano_") or field_name == "periodo"
    )
    return ReportTransformSpec(
        source_dataset=source_dataset,
        target_table=target_table,
        report_type="snapshot",
        dimension_columns=dimension_columns,
        field_types=field_types,
        metadata_fields=metadata_fields,
        output_fields=silver_fields,
    )


def _load_report_specs() -> dict[str, ReportTransformSpec]:
    repo_root = Path(__file__).resolve().parents[2]
    bronze_dir = repo_root / "config" / "schemas" / "bronze"
    silver_dir = repo_root / "config" / "schemas" / "silver"
    specs: dict[str, ReportTransformSpec] = {}

    for bronze_path in sorted(bronze_dir.glob("report_*_schema.json")):
        source_dataset = bronze_path.name.removesuffix("_schema.json")
        silver_path = silver_dir / f"pronabec_{source_dataset}_schema.json"
        if not silver_path.exists():
            continue
        specs[source_dataset] = _build_spec_from_schema(
            source_dataset=source_dataset,
            bronze_schema=_load_json_schema(bronze_path),
            silver_schema=_load_json_schema(silver_path),
        )
    return specs


REPORT_SPECS: dict[str, ReportTransformSpec] = {}


def _normalize_key_for_matching(k: str) -> str:
    # remove BOM
    k = k.replace("\ufeff", "")
    # lowercase
    k = k.lower()
    # treat underscores as spaces for word boundary splitting
    k = k.replace("_", " ")
    words = k.split()
    stop_words = {"de", "del", "la", "los", "las", "el", "y", "en", "para", "con", "segun", "a", "o", "u"}
    filtered_words = [w for w in words if w not in stop_words]
    return "".join(filtered_words)


def get_record_value_flexible(record: dict[str, Any], target_key: str) -> Any:
    """Retrieve value from record using flexible key matching (BOM, case, space/underscore insensitive)."""
    normalized_target = _normalize_key_for_matching(target_key)
    for k, v in record.items():
        if _normalize_key_for_matching(k) == normalized_target:
            return v
    return None


def clean_report_text(value: Any) -> str | None:
    """Clean report text technically while preserving readable accents/case."""
    fixed = fix_mojibake(value)
    without_controls = remove_control_characters(fixed)
    return normalize_whitespace(without_controls)


_CANONICAL_CATALOG = None


def get_canonical_catalog() -> dict[str, Any]:
    global _CANONICAL_CATALOG
    if _CANONICAL_CATALOG is None:
        from pathlib import Path
        from pipelines.common.canonical_mapping import load_canonical_mappings
        repo_root = Path(__file__).resolve().parents[2]
        catalog_path = repo_root / "config" / "reference" / "pronabec_canonical_mappings.yaml"
        _CANONICAL_CATALOG = load_canonical_mappings(catalog_path)
    return _CANONICAL_CATALOG


def _apply_report_canonical(
    record: dict[str, Any],
    output_fields: tuple[str, ...],
) -> None:
    from pipelines.common.canonical_mapping import lookup_canonical_value
    catalog = get_canonical_catalog()

    # Map carrera_estudio -> carrera
    if "carrera_estudio" in record and "carrera_estudio_canonical" in output_fields:
        domain = "carrera"
        val = record["carrera_estudio"]
        match_res = lookup_canonical_value(catalog, domain, val)
        record["carrera_estudio_canonical"] = match_res.canonical_value
        record["carrera_estudio_canonical_match_method"] = match_res.match_method
        record["carrera_estudio_canonical_review_required"] = match_res.review_required if match_res.matched else None

    # Map universidad -> institucion
    if "universidad" in record and "universidad_canonical" in output_fields:
        domain = "institucion"
        val = record["universidad"]
        if domain not in catalog.get("domains", {}):
            record["universidad_canonical"] = None
            record["universidad_canonical_match_method"] = None
            record["universidad_canonical_review_required"] = None
        else:
            match_res = lookup_canonical_value(catalog, domain, val)
            record["universidad_canonical"] = match_res.canonical_value
            record["universidad_canonical_match_method"] = match_res.match_method
            record["universidad_canonical_review_required"] = match_res.review_required if match_res.matched else None


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
            metadata[field_name] = parse_report_int(get_record_value_flexible(source, field_name))
        else:
            metadata[field_name] = clean_report_text(get_record_value_flexible(source, field_name))

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
        column: clean_report_text(get_record_value_flexible(record, column))
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
        _apply_report_canonical(transformed, spec.output_fields)
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
            get_record_value_flexible(record, field_name),
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
    _apply_report_canonical(transformed, spec.output_fields)
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


REPORT_SPECS.update(_load_report_specs())
