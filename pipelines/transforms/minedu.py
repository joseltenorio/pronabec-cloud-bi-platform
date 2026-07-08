"""MINEDU ESCALE Bronze to Silver transforms."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal, ROUND_HALF_UP
import unicodedata
from typing import Any

from pipelines.transforms.base import clean_text_basic, parse_int_safe

DATASET_NAME = "minedu_matricula_secundaria_departamental"
SOURCE_SYSTEM = "MINEDU_ESCALE"
GRADE_ORDER = {
    "PRIMER_GRADO": 1,
    "SEGUNDO_GRADO": 2,
    "TERCER_GRADO": 3,
    "CUARTO_GRADO": 4,
    "QUINTO_GRADO": 5,
}
METRIC_FIELDS = (
    "matricula_total",
    "matricula_publica",
    "matricula_privada",
    "matricula_urbana",
    "matricula_rural",
    "matricula_masculino",
    "matricula_femenino",
)


def normalize_region_name(value: Any) -> str:
    region = clean_text_basic(value)
    if region is None:
        raise ValueError("region es obligatoria.")
    normalized = unicodedata.normalize("NFKD", region)
    without_accents = "".join(char for char in normalized if not unicodedata.combining(char))
    return " ".join(without_accents.upper().split())


def _parse_required_int(record: dict[str, Any], field_name: str) -> int:
    parsed = parse_int_safe(record.get(field_name))
    if parsed is None:
        raise ValueError(f"{field_name} es obligatorio y debe ser entero.")
    if parsed < 0:
        raise ValueError(f"{field_name} no puede ser negativo.")
    return parsed


def _ratio(numerator: int | None, denominator: int | None) -> str | None:
    if numerator is None or denominator in (None, 0):
        return None
    value = Decimal(numerator) / Decimal(denominator)
    value = value.quantize(Decimal("0.000000001"), rounding=ROUND_HALF_UP)
    return format(value, "f")


def transform_minedu_matricula_secundaria_departamental(
    record: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    anio = _parse_required_int(record, "anio")
    if anio < 2012 or anio > 2025:
        raise ValueError("anio debe estar entre 2012 y 2025.")

    codigo_departamento = clean_text_basic(record.get("codigo_departamento"))
    if codigo_departamento is None:
        raise ValueError("codigo_departamento es obligatorio.")

    grado = clean_text_basic(record.get("grado"))
    if grado not in GRADE_ORDER:
        raise ValueError(f"grado invalido: {grado}")

    nivel_educativo = clean_text_basic(record.get("nivel_educativo"))
    if nivel_educativo != "SECUNDARIA":
        raise ValueError("nivel_educativo debe ser SECUNDARIA.")

    transformed = {
        "anio": anio,
        "codigo_departamento": codigo_departamento,
        "region": clean_text_basic(record.get("region")),
        "region_normalizada": normalize_region_name(record.get("region")),
        "nivel_educativo": nivel_educativo,
        "grado": grado,
        "grado_orden": GRADE_ORDER[grado],
    }

    for field_name in METRIC_FIELDS:
        transformed[field_name] = _parse_required_int(record, field_name)

    total = transformed["matricula_total"]
    transformed["publica_pct"] = _ratio(transformed["matricula_publica"], total)
    transformed["privada_pct"] = _ratio(transformed["matricula_privada"], total)
    transformed["urbana_pct"] = _ratio(transformed["matricula_urbana"], total)
    transformed["rural_pct"] = _ratio(transformed["matricula_rural"], total)
    transformed["masculino_pct"] = _ratio(transformed["matricula_masculino"], total)
    transformed["femenino_pct"] = _ratio(transformed["matricula_femenino"], total)

    for pct_field in (
        "publica_pct",
        "privada_pct",
        "urbana_pct",
        "rural_pct",
        "masculino_pct",
        "femenino_pct",
    ):
        value = transformed[pct_field]
        if value is not None:
            decimal_value = Decimal(value)
            if decimal_value < 0 or decimal_value > 1:
                raise ValueError(f"{pct_field} debe estar entre 0 y 1.")

    transformed.update(
        {
            "source_system": SOURCE_SYSTEM,
            "source_dataset": DATASET_NAME,
            "extraction_date": context.get("extraction_date"),
            "ingestion_timestamp": context.get("ingestion_timestamp")
            or datetime.now(UTC).isoformat(),
            "pipeline_run_id": context.get("pipeline_run_id"),
        }
    )
    return transformed
