"""Tests for MEF Bronze to Silver budget transforms."""

from __future__ import annotations

import pytest

from pipelines.dataflow_bronze_to_silver import transform_bronze_records
from pipelines.transforms.mef import (
    MEF_SPECS,
    clean_mef_code,
    clean_period_value,
    parse_mef_numeric,
    parse_mef_percent,
    transform_mef_presupuesto,
    transform_mef_presupuesto_actividad,
    transform_mef_presupuesto_actividad_temporal,
    transform_mef_presupuesto_generica,
    transform_mef_presupuesto_generica_temporal,
    transform_mef_presupuesto_hierarchy,
    transform_mef_presupuesto_producto,
    transform_mef_presupuesto_producto_temporal,
    transform_mef_presupuesto_temporal,
    transform_mef_record,
)


CONTEXT = {
    "extraction_date": "2026-06-15",
    "ingestion_timestamp": "2026-06-16T00:00:00+00:00",
    "pipeline_run_id": "test-run",
}


def assert_metadata(row: dict[str, object], source_dataset: str) -> None:
    assert row["source_system"] == "mef"
    assert row["source_dataset"] == source_dataset
    assert row["extraction_date"] == "2026-06-15"
    assert row["ingestion_timestamp"] == "2026-06-16T00:00:00+00:00"
    assert row["pipeline_run_id"] == "test-run"


def test_parse_mef_numeric() -> None:
    assert parse_mef_numeric("1,234,567") == 1234567
    assert parse_mef_numeric("-1,098,046") == -1098046
    assert parse_mef_numeric("0") == 0
    assert parse_mef_numeric("") is None
    assert parse_mef_numeric("-") is None


def test_parse_mef_percent() -> None:
    assert parse_mef_percent("86.5") == 86.5
    assert parse_mef_percent("86,5") == 86.5
    assert parse_mef_percent("86.5%") == 86.5
    assert parse_mef_percent("") is None
    assert parse_mef_percent("-") is None


def test_clean_mef_codes_preserve_punctuation() -> None:
    assert clean_mef_code(" 117-1438 ") == "117-1438"
    assert clean_mef_code("003000001") == "003000001"
    assert clean_mef_code("2.3") == "2.3"


def test_clean_period_value_preserves_period_format() -> None:
    assert clean_period_value(" 2026-01 ") == "2026-01"
    assert clean_period_value("2026-Q1") == "2026-Q1"
    assert clean_period_value("") is None


def test_transform_mef_presupuesto() -> None:
    row = transform_mef_presupuesto(
        {
            "ano": "2026",
            "ejecutora_codigo": "117-1438",
            "ejecutora_nombre": " PROGRAMA NACIONAL DE BECAS Y CREDITO EDUCATIVO ",
            "pia": "1,000",
            "pim": "2,000",
            "devengado": "1,500",
            "avance_porcentaje": "75.0",
            "certificacion": "NO DEBE SALIR",
            "compromiso_anual": "NO DEBE SALIR",
            "girado": "NO DEBE SALIR",
        },
        CONTEXT,
    )

    assert row == {
        "ano": 2026,
        "codigo_entidad": "117-1438",
        "nombre_entidad": "PROGRAMA NACIONAL DE BECAS Y CREDITO EDUCATIVO",
        "pia": 1000,
        "pim": 2000,
        "devengado": 1500,
        "avance_porcentaje": 75.0,
        "source_system": "mef",
        "source_dataset": "presupuesto",
        "extraction_date": "2026-06-15",
        "ingestion_timestamp": "2026-06-16T00:00:00+00:00",
        "pipeline_run_id": "test-run",
    }
    assert "certificacion" not in row
    assert "compromiso_anual" not in row
    assert "girado" not in row


def test_transform_mef_presupuesto_temporal() -> None:
    row = transform_mef_presupuesto_temporal(
        {
            "ano": "2026",
            "periodo_tipo": "MENSUAL",
            "periodo_valor": "2026-01",
            "mes_numero": "01",
            "trimestre": "1",
            "mes_nombre": "ENERO",
            "ejecutora_codigo": "117-1438",
            "ejecutora_nombre": "PROGRAMA NACIONAL DE BECAS Y CREDITO EDUCATIVO",
            "devengado": "123,456",
            "avance_porcentaje": "",
            "certificacion": "NO DEBE SALIR",
        },
        CONTEXT,
    )

    assert row["periodo_valor"] == "2026-01"
    assert row["mes_numero"] == 1
    assert row["trimestre"] == 1
    assert row["devengado"] == 123456
    assert "certificacion" not in row
    assert "ejecutora_codigo" not in row


def test_transform_mef_presupuesto_producto() -> None:
    row = transform_mef_presupuesto_producto(
        {
            "ano": "2026",
            "codigo_producto": "003000001",
            "producto_proyecto": " ACCIONES   COMUNES ",
            "pia": "1,000",
            "pim": "2,000",
            "devengado": "1,500",
            "avance_porcentaje": "75,5",
        },
        CONTEXT,
    )

    assert row["codigo_producto"] == "003000001"
    assert row["producto"] == "ACCIONES COMUNES"
    assert row["pia"] == 1000
    assert row["avance_porcentaje"] == 75.5


def test_transform_mef_presupuesto_actividad() -> None:
    row = transform_mef_presupuesto_actividad(
        {
            "ano": "2026",
            "codigo_producto": "3000001",
            "producto": "ACCIONES   COMUNES",
            "codigo_actividad": "5000653",
            "actividad": " GESTION   DEL PROGRAMA ",
            "pia": "1,000",
            "pim": "2,000",
            "devengado": "1,500",
            "avance_porcentaje": "75.0",
        },
        CONTEXT,
    )

    assert row["codigo_actividad"] == "5000653"
    assert row["actividad"] == "GESTION DEL PROGRAMA"
    assert row["codigo_producto"] == "3000001"
    assert row["devengado"] == 1500


def test_transform_mef_presupuesto_generica() -> None:
    row = transform_mef_presupuesto_generica(
        {
            "ano": "2026",
            "codigo_generica": "2.3",
            "generica": " BIENES   Y SERVICIOS ",
            "pia": "1,000",
            "pim": "2,000",
            "devengado": "1,500",
            "avance_porcentaje": "75.0",
        },
        CONTEXT,
    )

    assert row["codigo_generica"] == "2.3"
    assert row["generica"] == "BIENES Y SERVICIOS"
    assert row["pim"] == 2000


def test_transform_temporal_product_activity_and_generica() -> None:
    producto = transform_mef_presupuesto_producto_temporal(
        {
            "ano": "2026",
            "periodo_tipo": "MENSUAL",
            "periodo_valor": "2026-01",
            "mes_numero": "01",
            "codigo_producto": "3000001",
            "producto": "ACCIONES COMUNES",
            "devengado": "123,456",
        },
        CONTEXT,
    )
    actividad = transform_mef_presupuesto_actividad_temporal(
        {
            "ano": "2026",
            "periodo_tipo": "MENSUAL",
            "periodo_valor": "2026-01",
            "mes_numero": "01",
            "codigo_producto": "3000001",
            "producto": "ACCIONES COMUNES",
            "codigo_actividad": "5000653",
            "actividad": "GESTION",
            "devengado": "123,456",
        },
        CONTEXT,
    )
    generica = transform_mef_presupuesto_generica_temporal(
        {
            "ano": "2026",
            "periodo_tipo": "MENSUAL",
            "periodo_valor": "2026-01",
            "mes_numero": "01",
            "codigo_generica": "2.3",
            "generica": "BIENES Y SERVICIOS",
            "devengado": "123,456",
        },
        CONTEXT,
    )

    assert producto["periodo_valor"] == "2026-01"
    assert producto["devengado"] == 123456
    assert actividad["codigo_actividad"] == "5000653"
    assert actividad["devengado"] == 123456
    assert generica["codigo_generica"] == "2.3"
    assert generica["devengado"] == 123456


def test_transform_mef_hierarchy_does_not_aggregate() -> None:
    row = transform_mef_presupuesto_hierarchy(
        {
            "ano": "2026",
            "nivel_jerarquia": "NIVEL DE GOBIERNO",
            "codigo": "E",
            "descripcion": " GOBIERNO   NACIONAL ",
            "pia": "1,000",
            "pim": "2,000",
            "devengado": "1,500",
            "avance_porcentaje": "75.0",
        },
        CONTEXT,
    )

    assert row["nivel_jerarquia"] == "NIVEL DE GOBIERNO"
    assert row["codigo_entidad"] == "E"
    assert row["nombre_entidad"] == "GOBIERNO NACIONAL"
    assert row["devengado"] == 1500
    assert_metadata(row, "presupuesto_hierarchy")


@pytest.mark.parametrize(
    "source_dataset",
    [
        "presupuesto",
        "presupuesto_mef",
        "presupuesto_temporal",
        "presupuesto_mef_temporal",
        "presupuesto_producto",
        "presupuesto_mef_producto",
        "presupuesto_producto_temporal",
        "presupuesto_mef_producto_temporal",
        "presupuesto_actividad",
        "presupuesto_mef_actividad",
        "presupuesto_actividad_temporal",
        "presupuesto_mef_actividad_temporal",
        "presupuesto_generica",
        "presupuesto_mef_generica",
        "presupuesto_generica_temporal",
        "presupuesto_mef_generica_temporal",
        "presupuesto_hierarchy",
        "presupuesto_mef_hierarchy",
    ],
)
def test_transform_mef_router_supported_datasets(source_dataset: str) -> None:
    row = transform_mef_record({"source_dataset": "not-used"} and source_dataset, {}, CONTEXT)

    assert row["source_system"] == "mef"
    assert row["source_dataset"] == source_dataset


def test_transform_mef_router_unknown_dataset() -> None:
    with pytest.raises(ValueError, match="Unsupported MEF dataset 'presupuesto_fuente'"):
        transform_mef_record("presupuesto_fuente", {}, CONTEXT)


def test_punctuation_is_not_normalized_for_codes_or_periods() -> None:
    row = transform_mef_record(
        "presupuesto_mef_generica_temporal",
        {
            "ano": "2026",
            "periodo_valor": "2026-01",
            "codigo_generica": "2.3",
        },
        CONTEXT,
    )

    assert row["periodo_valor"] == "2026-01"
    assert row["codigo_generica"] == "2.3"
    assert row["periodo_valor"] != "2026 01"
    assert row["codigo_generica"] != "23"


def test_dataflow_hook_routes_mef_without_bigquery() -> None:
    rows = transform_bronze_records(
        {
            "ano": "2026",
            "ejecutora_codigo": "117-1438",
            "ejecutora_nombre": "PRONABEC",
            "pia": "1,000",
            "pim": "2,000",
            "devengado": "1,500",
            "avance_porcentaje": "75.0",
        },
        source_system="mef",
        source_dataset="presupuesto_mef",
        extraction_date="2026-06-15",
        ingestion_timestamp="2026-06-16T00:00:00+00:00",
        pipeline_run_id="test-run",
    )

    assert rows[0]["source_system"] == "mef"
    assert rows[0]["source_dataset"] == "presupuesto_mef"
    assert rows[0]["codigo_entidad"] == "117-1438"
    assert rows[0]["devengado"] == 1500


def test_specs_cover_only_approved_silver_datasets() -> None:
    unsupported = {
        "presupuesto_fuente",
        "presupuesto_rubro",
        "presupuesto_departamento",
    }

    assert unsupported.isdisjoint(MEF_SPECS)
