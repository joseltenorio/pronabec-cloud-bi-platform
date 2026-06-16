"""PRONABEC Bronze to Silver transforms for selected datasets."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Callable

from pipelines.common.text_normalization import (
    fix_mojibake,
    normalize_whitespace,
    remove_control_characters,
)
from pipelines.transforms.base import parse_int_safe


TransformContext = dict[str, Any]
TransformFn = Callable[[dict[str, Any], TransformContext], dict[str, Any]]


SUPPORTED_PRONABEC_DATASETS = {
    "convocatorias",
    "ubigeo_postulacion",
    "becarios_pais_estudio",
    "colegios_habiles",
}


def clean_text_value(value: Any) -> str | None:
    """Apply technical-only text cleanup for Silver display values."""
    fixed = fix_mojibake(value)
    without_controls = remove_control_characters(fixed)
    return normalize_whitespace(without_controls)


def _metadata(dataset_name: str, context: TransformContext) -> dict[str, Any]:
    ingestion_timestamp = context.get("ingestion_timestamp")
    if not ingestion_timestamp:
        ingestion_timestamp = datetime.now(UTC).isoformat()

    return {
        "source_system": "pronabec",
        "source_dataset": dataset_name,
        "extraction_date": context.get("extraction_date"),
        "ingestion_timestamp": ingestion_timestamp,
        "pipeline_run_id": context.get("pipeline_run_id"),
    }


def _with_metadata(
    dataset_name: str,
    context: TransformContext,
    values: dict[str, Any],
) -> dict[str, Any]:
    return {**values, **_metadata(dataset_name, context)}


def transform_pronabec_convocatorias(
    record: dict[str, Any],
    context: TransformContext,
) -> dict[str, Any]:
    """Transform Bronze convocatorias into pronabec_convocatorias Silver shape."""
    dataset_name = "convocatorias"
    return _with_metadata(
        dataset_name,
        context,
        {
            "source_row_id": parse_int_safe(record.get("source_row_id")),
            "id_convocatoria": parse_int_safe(record.get("id_convocatoria")),
            "codigo_anual": clean_text_value(record.get("codigo_anual")),
            "descripcion_convocatoria": clean_text_value(record.get("description_conv")),
            "modalidad": clean_text_value(record.get("modalidad")),
            "programa": clean_text_value(record.get("programa")),
            "vacantes": parse_int_safe(record.get("vacantes")),
        },
    )


def transform_pronabec_ubigeo_postulacion(
    record: dict[str, Any],
    context: TransformContext,
) -> dict[str, Any]:
    """Transform Bronze ubigeo_postulacion into its selected Silver shape."""
    dataset_name = "ubigeo_postulacion"
    return _with_metadata(
        dataset_name,
        context,
        {
            "source_row_id": parse_int_safe(record.get("source_row_id")),
            "region": clean_text_value(record.get("region")),
            "provincia": clean_text_value(record.get("provincia")),
            "distrito": clean_text_value(record.get("distrito")),
            "codigo_ubigeo": clean_text_value(record.get("codigo_ubigeo")),
        },
    )


def transform_pronabec_becarios_pais_estudio(
    record: dict[str, Any],
    context: TransformContext,
) -> dict[str, Any]:
    """Transform Bronze becarios_pais_estudio into its selected Silver shape."""
    dataset_name = "becarios_pais_estudio"
    return _with_metadata(
        dataset_name,
        context,
        {
            "source_row_id": parse_int_safe(record.get("source_row_id")),
            "convocatoria": clean_text_value(record.get("convocatoria")),
            "modalidad": clean_text_value(record.get("modalidad")),
            "pais_estudio": clean_text_value(record.get("pais_estudio")),
            "institucion": clean_text_value(record.get("institucion")),
            "sexo": clean_text_value(record.get("sexo")),
        },
    )


def transform_pronabec_colegios_elegibles(
    record: dict[str, Any],
    context: TransformContext,
) -> dict[str, Any]:
    """Transform Bronze colegios_habiles into pronabec_colegios_elegibles."""
    dataset_name = "colegios_habiles"
    return _with_metadata(
        dataset_name,
        context,
        {
            "source_row_id": parse_int_safe(record.get("source_row_id")),
            "ugel": clean_text_value(record.get("ugel")),
            "institucion_educativa": clean_text_value(record.get("institucion_educativa")),
            "tipo_gestion_colegio": clean_text_value(record.get("tipo_gestion")),
            "nivel_modalidad": clean_text_value(record.get("nivel_modalidad")),
            "forma_atencion": clean_text_value(record.get("forma_atencion")),
            "distrito": clean_text_value(record.get("distrito")),
        },
    )


TRANSFORMS_BY_DATASET: dict[str, TransformFn] = {
    "convocatorias": transform_pronabec_convocatorias,
    "ubigeo_postulacion": transform_pronabec_ubigeo_postulacion,
    "becarios_pais_estudio": transform_pronabec_becarios_pais_estudio,
    "colegios_habiles": transform_pronabec_colegios_elegibles,
}


def transform_pronabec_record(
    dataset_name: str,
    record: dict[str, Any],
    context: TransformContext,
) -> dict[str, Any]:
    """Route a PRONABEC Bronze record to the selected dataset transform."""
    transform = TRANSFORMS_BY_DATASET.get(dataset_name)
    if transform is None:
        supported = ", ".join(sorted(SUPPORTED_PRONABEC_DATASETS))
        raise ValueError(
            f"Unsupported PRONABEC dataset '{dataset_name}'. "
            f"Supported selected datasets: {supported}."
        )
    return transform(record, context)
