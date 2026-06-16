"""MEF Bronze to Silver budget transforms."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

from pipelines.common.text_normalization import (
    fix_mojibake,
    normalize_whitespace,
    remove_control_characters,
)


FieldKind = Literal["int", "numeric", "percent", "code", "period", "text"]

TECHNICAL_FIELDS = (
    "source_system",
    "source_dataset",
    "extraction_date",
    "ingestion_timestamp",
    "pipeline_run_id",
)


@dataclass(frozen=True)
class MefTransformSpec:
    source_dataset: str
    target_dataset: str
    output_fields: tuple[str, ...]
    field_kinds: dict[str, FieldKind]
    source_aliases: dict[str, tuple[str, ...]]


def clean_mef_text(value: Any) -> str | None:
    fixed = fix_mojibake(value)
    without_controls = remove_control_characters(fixed)
    return normalize_whitespace(without_controls)


def clean_mef_code(value: Any) -> str | None:
    return clean_mef_text(value)


def clean_period_value(value: Any) -> str | None:
    return clean_mef_text(value)


def parse_mef_int(value: Any) -> int | None:
    cleaned = clean_mef_text(value)
    if cleaned is None:
        return None
    try:
        return int(cleaned)
    except ValueError:
        try:
            return int(float(cleaned))
        except ValueError:
            return None


def parse_mef_numeric(value: Any) -> float | None:
    cleaned = clean_mef_text(value)
    if cleaned is None or cleaned == "-":
        return None

    normalized = cleaned.replace(" ", "").replace("%", "")
    if not normalized or normalized == "-":
        return None

    if "," in normalized and "." in normalized:
        if normalized.rfind(",") > normalized.rfind("."):
            normalized = normalized.replace(".", "").replace(",", ".")
        else:
            normalized = normalized.replace(",", "")
    elif "," in normalized:
        parts = normalized.split(",")
        if len(parts) == 2 and len(parts[1]) in {1, 2}:
            normalized = normalized.replace(",", ".")
        else:
            normalized = normalized.replace(",", "")

    try:
        return float(normalized)
    except ValueError:
        return None


def parse_mef_percent(value: Any) -> float | None:
    return parse_mef_numeric(value)


def build_mef_metadata(source_dataset: str, context: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_system": "mef",
        "source_dataset": source_dataset,
        "extraction_date": context.get("extraction_date"),
        "ingestion_timestamp": context.get("ingestion_timestamp")
        or datetime.now(UTC).isoformat(),
        "pipeline_run_id": context.get("pipeline_run_id"),
    }


def _first_present(record: dict[str, Any], aliases: tuple[str, ...], field_name: str) -> Any:
    for alias in (*aliases, field_name):
        if alias in record:
            return record.get(alias)
    return None


def _convert(value: Any, kind: FieldKind) -> Any:
    if kind == "int":
        return parse_mef_int(value)
    if kind == "numeric":
        return parse_mef_numeric(value)
    if kind == "percent":
        return parse_mef_percent(value)
    if kind == "code":
        return clean_mef_code(value)
    if kind == "period":
        return clean_period_value(value)
    return clean_mef_text(value)


def _transform_with_spec(
    record: dict[str, Any],
    context: dict[str, Any],
    spec: MefTransformSpec,
) -> dict[str, Any]:
    transformed: dict[str, Any] = {}
    for field_name in spec.output_fields:
        if field_name in TECHNICAL_FIELDS:
            continue
        value = _first_present(record, spec.source_aliases.get(field_name, ()), field_name)
        transformed[field_name] = _convert(value, spec.field_kinds[field_name])

    transformed.update(build_mef_metadata(spec.source_dataset, context))
    return {field_name: transformed.get(field_name) for field_name in spec.output_fields}


BASE_METADATA = (
    "source_system",
    "source_dataset",
    "extraction_date",
    "ingestion_timestamp",
    "pipeline_run_id",
)

ANNUAL_METRICS = {
    "pia": "numeric",
    "pim": "numeric",
    "devengado": "numeric",
    "avance_porcentaje": "percent",
}
TEMPORAL_FIELDS = {
    "ano": "int",
    "periodo_tipo": "period",
    "periodo_valor": "period",
    "trimestre": "int",
    "mes_numero": "int",
    "mes_nombre": "text",
    "devengado": "numeric",
}


def _field_kinds(fields: tuple[str, ...]) -> dict[str, FieldKind]:
    kinds: dict[str, FieldKind] = {}
    for field_name in fields:
        if field_name in BASE_METADATA:
            continue
        if field_name == "ano" or field_name in {"trimestre", "mes_numero"}:
            kinds[field_name] = "int"
        elif field_name in {"periodo_tipo", "periodo_valor"}:
            kinds[field_name] = "period"
        elif field_name.startswith("codigo_"):
            kinds[field_name] = "code"
        elif field_name in {"pia", "pim", "devengado"}:
            kinds[field_name] = "numeric"
        elif field_name == "avance_porcentaje":
            kinds[field_name] = "percent"
        else:
            kinds[field_name] = "text"
    return kinds


MEF_SPECS: dict[str, MefTransformSpec] = {}


def _register(
    source_dataset: str,
    output_fields: tuple[str, ...],
    aliases: dict[str, tuple[str, ...]] | None = None,
) -> None:
    aliases = aliases or {}
    target_dataset = f"presupuesto_mef{source_dataset.removeprefix('presupuesto')}"
    spec = MefTransformSpec(
        source_dataset=source_dataset,
        target_dataset=target_dataset,
        output_fields=output_fields,
        field_kinds=_field_kinds(output_fields),
        source_aliases=aliases,
    )
    MEF_SPECS[source_dataset] = spec
    MEF_SPECS[target_dataset] = spec


_register(
    "presupuesto",
    (
        "ano",
        "codigo_entidad",
        "nombre_entidad",
        "pia",
        "pim",
        "devengado",
        "avance_porcentaje",
        *BASE_METADATA,
    ),
    {
        "codigo_entidad": ("ejecutora_codigo",),
        "nombre_entidad": ("ejecutora_nombre",),
    },
)
_register(
    "presupuesto_temporal",
    (
        "ano",
        "periodo_tipo",
        "periodo_valor",
        "trimestre",
        "mes_numero",
        "mes_nombre",
        "devengado",
        *BASE_METADATA,
    ),
)
_register(
    "presupuesto_producto",
    (
        "ano",
        "codigo_producto",
        "producto",
        "pia",
        "pim",
        "devengado",
        "avance_porcentaje",
        *BASE_METADATA,
    ),
    {"producto": ("producto_proyecto", "producto_nombre")},
)
_register(
    "presupuesto_producto_temporal",
    (
        "ano",
        "periodo_tipo",
        "periodo_valor",
        "trimestre",
        "mes_numero",
        "mes_nombre",
        "codigo_producto",
        "producto",
        "devengado",
        *BASE_METADATA,
    ),
    {"producto": ("producto_proyecto", "producto_nombre")},
)
_register(
    "presupuesto_actividad",
    (
        "ano",
        "codigo_producto",
        "producto",
        "codigo_actividad",
        "actividad",
        "pia",
        "pim",
        "devengado",
        "avance_porcentaje",
        *BASE_METADATA,
    ),
)
_register(
    "presupuesto_actividad_temporal",
    (
        "ano",
        "periodo_tipo",
        "periodo_valor",
        "trimestre",
        "mes_numero",
        "mes_nombre",
        "codigo_producto",
        "producto",
        "codigo_actividad",
        "actividad",
        "devengado",
        *BASE_METADATA,
    ),
)
_register(
    "presupuesto_generica",
    (
        "ano",
        "codigo_generica",
        "generica",
        "pia",
        "pim",
        "devengado",
        "avance_porcentaje",
        *BASE_METADATA,
    ),
)
_register(
    "presupuesto_generica_temporal",
    (
        "ano",
        "periodo_tipo",
        "periodo_valor",
        "trimestre",
        "mes_numero",
        "mes_nombre",
        "codigo_generica",
        "generica",
        "devengado",
        *BASE_METADATA,
    ),
)
_register(
    "presupuesto_hierarchy",
    (
        "ano",
        "nivel_jerarquia",
        "codigo_entidad",
        "nombre_entidad",
        "pia",
        "pim",
        "devengado",
        "avance_porcentaje",
        *BASE_METADATA,
    ),
    {
        "codigo_entidad": ("codigo", "ejecutora_codigo"),
        "nombre_entidad": ("descripcion", "ejecutora_nombre"),
    },
)


def transform_mef_presupuesto(record: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    return _transform_with_spec(record, context, MEF_SPECS["presupuesto"])


def transform_mef_presupuesto_temporal(
    record: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    return _transform_with_spec(record, context, MEF_SPECS["presupuesto_temporal"])


def transform_mef_presupuesto_producto(
    record: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    return _transform_with_spec(record, context, MEF_SPECS["presupuesto_producto"])


def transform_mef_presupuesto_producto_temporal(
    record: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    return _transform_with_spec(record, context, MEF_SPECS["presupuesto_producto_temporal"])


def transform_mef_presupuesto_actividad(
    record: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    return _transform_with_spec(record, context, MEF_SPECS["presupuesto_actividad"])


def transform_mef_presupuesto_actividad_temporal(
    record: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    return _transform_with_spec(record, context, MEF_SPECS["presupuesto_actividad_temporal"])


def transform_mef_presupuesto_generica(
    record: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    return _transform_with_spec(record, context, MEF_SPECS["presupuesto_generica"])


def transform_mef_presupuesto_generica_temporal(
    record: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    return _transform_with_spec(record, context, MEF_SPECS["presupuesto_generica_temporal"])


def transform_mef_presupuesto_hierarchy(
    record: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    return _transform_with_spec(record, context, MEF_SPECS["presupuesto_hierarchy"])


def transform_mef_record(
    source_dataset: str,
    record: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    spec = MEF_SPECS.get(source_dataset)
    if spec is None:
        supported = ", ".join(sorted(MEF_SPECS))
        raise ValueError(
            f"Unsupported MEF dataset '{source_dataset}'. Supported datasets: {supported}."
        )
    return _transform_with_spec(record, context, spec)
