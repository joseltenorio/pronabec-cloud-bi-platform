"""INEI regional context Bronze to Silver transforms."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Callable

from pipelines.transforms.base import clean_text_basic, parse_int_safe, parse_numeric_safe

TECHNICAL_FIELDS = (
    "source_system",
    "source_dataset",
    "extraction_date",
    "ingestion_timestamp",
    "pipeline_run_id",
)


def _metadata(dataset_name: str, context: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_system": "INEI",
        "source_dataset": dataset_name,
        "extraction_date": context.get("extraction_date"),
        "ingestion_timestamp": context.get("ingestion_timestamp")
        or datetime.now(UTC).isoformat(),
        "pipeline_run_id": context.get("pipeline_run_id"),
    }


def _parse_required_year(record: dict[str, Any]) -> int:
    year = parse_int_safe(record.get("anio"))
    if year is None:
        raise ValueError("anio es obligatorio y debe ser numérico.")
    return year


def _parse_required_region(record: dict[str, Any]) -> str:
    region = clean_text_basic(record.get("region"))
    if region is None:
        raise ValueError("region es obligatoria.")
    return region


def _is_blank(value: Any) -> bool:
    return clean_text_basic(value) is None


def _parse_optional_int(record: dict[str, Any], field_name: str) -> int | None:
    value = record.get(field_name)
    if _is_blank(value):
        return None
    parsed = parse_int_safe(value)
    if parsed is None:
        raise ValueError(f"{field_name} debe ser entero.")
    return parsed


def _parse_optional_numeric(record: dict[str, Any], field_name: str) -> float | None:
    value = record.get(field_name)
    if _is_blank(value):
        return None
    parsed = parse_numeric_safe(value)
    if parsed is None:
        raise ValueError(f"{field_name} debe ser numérico.")
    return parsed


def _validate_non_negative(value: int | None, field_name: str) -> None:
    if value is not None and value < 0:
        raise ValueError(f"{field_name} no puede ser negativo.")


def _validate_range(
    value: float | None,
    field_name: str,
    min_value: float,
    max_value: float,
) -> None:
    if value is not None and not (min_value <= value <= max_value):
        raise ValueError(f"{field_name} debe estar entre {min_value} y {max_value}.")


def _source_field(
    record: dict[str, Any],
    field_name: str,
    default: str | None = None,
) -> str | None:
    return clean_text_basic(record.get(field_name)) or default


def _base_record(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "anio": _parse_required_year(record),
        "region": _parse_required_region(record),
    }


def transform_inei_population_youth_region(
    record: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    dataset_name = "inei_population_youth_region"
    transformed = _base_record(record)
    for field_name in (
        "poblacion_total",
        "poblacion_15_24",
        "poblacion_15_29",
        "poblacion_17_24",
        "poblacion_18_24",
    ):
        transformed[field_name] = _parse_optional_int(record, field_name)
        _validate_non_negative(transformed[field_name], field_name)

    transformed["share_15_24_total"] = _parse_optional_numeric(
        record,
        "share_15_24_total",
    )
    _validate_range(transformed["share_15_24_total"], "share_15_24_total", 0, 100)
    transformed.update(_metadata(dataset_name, context))
    return transformed


def transform_inei_demographic_indicators_region(
    record: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    dataset_name = "inei_demographic_indicators_region"
    transformed = _base_record(record)
    for field_name in (
        "tasa_bruta_natalidad",
        "tasa_global_fecundidad",
        "esperanza_vida_nacer",
        "tasa_mortalidad_infantil",
        "tasa_migracion_neta",
        "tasa_crecimiento_total_pct",
    ):
        transformed[field_name] = _parse_optional_numeric(record, field_name)
    if (
        transformed["esperanza_vida_nacer"] is not None
        and transformed["esperanza_vida_nacer"] <= 0
    ):
        raise ValueError("esperanza_vida_nacer debe ser mayor que 0.")
    transformed.update(_metadata(dataset_name, context))
    return transformed


def transform_inei_pobreza_departamental(
    record: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    dataset_name = "inei_pobreza_departamental"
    transformed = _base_record(record)
    transformed["pobreza_monetaria_pct"] = _parse_optional_numeric(
        record,
        "pobreza_monetaria_pct",
    )
    _validate_range(transformed["pobreza_monetaria_pct"], "pobreza_monetaria_pct", 0, 100)
    for field_name in ("source_name", "source_period", "source_type", "metric"):
        transformed[field_name] = clean_text_basic(record.get(field_name))
    transformed.update(_metadata(dataset_name, context))
    return transformed


def transform_inei_internet_acceso_region(
    record: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any] | list[dict[str, Any]]:
    dataset_name = "inei_internet_acceso_region"
    if "anio" in record or "internet_acceso_pct" in record:
        transformed = _base_record(record)
        transformed["internet_acceso_pct"] = _parse_optional_numeric(
            record,
            "internet_acceso_pct",
        )
        _validate_range(transformed["internet_acceso_pct"], "internet_acceso_pct", 0, 100)
        for field_name in ("source_name", "source_period", "source_type", "metric"):
            transformed[field_name] = clean_text_basic(record.get(field_name))
        transformed.update(_metadata(dataset_name, context))
        return transformed

    region = _parse_required_region(record)
    year_columns = sorted(field_name for field_name in record if field_name.isdigit())
    if not year_columns:
        raise ValueError("inei_internet_acceso_region requiere columnas anio/internet_acceso_pct o columnas anuales.")

    source_period = _source_field(record, "source_period") or f"{year_columns[0]}-{year_columns[-1]}"
    transformed_rows: list[dict[str, Any]] = []
    for year_column in year_columns:
        if _is_blank(record.get(year_column)):
            continue
        internet_pct = _parse_optional_numeric(record, year_column)
        _validate_range(internet_pct, "internet_acceso_pct", 0, 100)
        transformed = {
            "anio": int(year_column),
            "region": region,
            "internet_acceso_pct": internet_pct,
            "source_name": _source_field(record, "source_name", "INEI"),
            "source_period": source_period,
            "source_type": _source_field(record, "source_type", "manual_csv"),
            "metric": _source_field(record, "metric", "internet_acceso_pct"),
        }
        transformed.update(_metadata(dataset_name, context))
        transformed_rows.append(transformed)

    if not transformed_rows:
        raise ValueError("inei_internet_acceso_region no contiene valores anuales de internet.")
    return transformed_rows


INEI_TRANSFORMS: dict[
    str,
    Callable[[dict[str, Any], dict[str, Any]], dict[str, Any] | list[dict[str, Any]]],
] = {
    "inei_population_youth_region": transform_inei_population_youth_region,
    "inei_demographic_indicators_region": transform_inei_demographic_indicators_region,
    "inei_pobreza_departamental": transform_inei_pobreza_departamental,
    "inei_internet_acceso_region": transform_inei_internet_acceso_region,
}


def transform_inei_report_record(
    dataset_name: str,
    record: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any] | list[dict[str, Any]]:
    transform = INEI_TRANSFORMS.get(dataset_name)
    if transform is None:
        supported = ", ".join(sorted(INEI_TRANSFORMS))
        raise ValueError(
            f"Unsupported INEI report dataset '{dataset_name}'. Supported datasets: {supported}."
        )
    return transform(record, context)
