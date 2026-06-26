"""Tests for PRONABEC selected Bronze to Silver transforms."""

from __future__ import annotations

from datetime import date

import pytest

from pipelines.dataflow_bronze_to_silver import transform_bronze_record
from pipelines.transforms.pronabec import (
    transform_pronabec_becarios_provincia,
    transform_pronabec_becarios_pais_estudio,
    transform_pronabec_colegios_elegibles,
    transform_pronabec_convocatorias,
    transform_pronabec_record,
    transform_pronabec_ubigeo_postulacion,
)


CONTEXT = {
    "extraction_date": "2026-06-15",
    "ingestion_timestamp": "2026-06-16T00:00:00+00:00",
    "pipeline_run_id": "test-run",
}
METADATA_FIELDS = {
    "source_system",
    "source_dataset",
    "extraction_date",
    "ingestion_timestamp",
    "pipeline_run_id",
}


def assert_common_metadata(record: dict[str, object], dataset: str) -> None:
    assert METADATA_FIELDS <= set(record)
    assert record["source_system"] == "pronabec"
    assert record["source_dataset"] == dataset
    assert record["extraction_date"] == "2026-06-15"
    assert record["ingestion_timestamp"] == "2026-06-16T00:00:00+00:00"
    assert record["pipeline_run_id"] == "test-run"


def test_transform_pronabec_convocatorias() -> None:
    record = {
        "source_row_id": "10",
        "nro_fila": "999",
        "id_convocatoria": "123",
        "codigo_anual": " 2021-02 ",
        "description_conv": " Beca   Especial ",
        "modalidad": " ACADÃ‰MICA ",
        "programa": "BECA CENTENARIO                                ",
        "vacantes": "100",
        "etapas": "NO DEBE SALIR",
        "resolucion": "NO DEBE SALIR",
        "fecha_carga": "NO DEBE SALIR",
    }

    transformed = transform_pronabec_convocatorias(record, CONTEXT)

    assert transformed == {
        "source_row_id": 10,
        "id_convocatoria": 123,
        "codigo_anual": "2021-02",
        "descripcion_convocatoria": "Beca Especial",
        "modalidad": "ACADÉMICA",
        "programa": "BECA CENTENARIO",
        "vacantes": 100,
        "modalidad_canonical": None,
        "modalidad_canonical_match_method": None,
        "modalidad_canonical_review_required": None,
        "programa_canonical": None,
        "programa_canonical_match_method": None,
        "programa_canonical_review_required": None,
        "source_system": "pronabec",
        "source_dataset": "convocatorias",
        "extraction_date": "2026-06-15",
        "ingestion_timestamp": "2026-06-16T00:00:00+00:00",
        "pipeline_run_id": "test-run",
    }
    assert "nro_fila" not in transformed
    assert "etapas" not in transformed
    assert "resolucion" not in transformed
    assert "fecha_carga" not in transformed
    assert "programa_canonical" in transformed


def test_transform_pronabec_ubigeo_postulacion() -> None:
    record = {
        "source_row_id": "20",
        "nro_fila": "1",
        "codigo_ubigeo": "010101",
        "region": " AMAZONAS ",
        "provincia": " CHACHAPOYAS ",
        "distrito": " SAN   FRANCISCO ",
        "fecha_carga": "NO DEBE SALIR",
    }

    transformed = transform_pronabec_ubigeo_postulacion(record, CONTEXT)

    assert transformed["source_row_id"] == 20
    assert transformed["codigo_ubigeo"] == "010101"
    assert transformed["region"] == "AMAZONAS"
    assert transformed["provincia"] == "CHACHAPOYAS"
    assert transformed["distrito"] == "SAN FRANCISCO"
    assert "nro_fila" not in transformed
    assert "fecha_carga" not in transformed
    assert_common_metadata(transformed, "ubigeo_postulacion")


def test_transform_pronabec_becarios_pais_estudio() -> None:
    record = {
        "source_row_id": "30",
        "nro_fila": "5",
        "modalidad": " Beca   18 ",
        "convocatoria": "BECA DE PERMANENCIA - CONVOCATORIA 2024",
        "pais_estudio": " PERÚ ",
        "tipo_institucion": "NO DEBE SALIR",
        "institucion": " UNIVERSIDAD   NACIONAL ",
        "sexo": " F ",
        "fecha_carga": "NO DEBE SALIR",
    }

    transformed = transform_pronabec_becarios_pais_estudio(record, CONTEXT)

    assert transformed["modalidad"] == "Beca 18"
    assert transformed["convocatoria"] == "BECA DE PERMANENCIA - CONVOCATORIA 2024"
    assert transformed["pais_estudio"] == "PERÚ"
    assert transformed["institucion"] == "UNIVERSIDAD NACIONAL"
    assert transformed["sexo"] == "F"
    assert "tipo_institucion" not in transformed
    assert "nro_fila" not in transformed
    assert "fecha_carga" not in transformed
    assert_common_metadata(transformed, "becarios_pais_estudio")


def test_transform_pronabec_becarios_provincia_detail_row() -> None:
    record = {
        "source_row_id": "2",
        "nro_fila": "2",
        "region": " AMAZONAS ",
        "provincia": " CHACHAPOYAS ",
        "b18_n": "72",
        "b18_pct": "0,27",
        "permanencia_n": "NO DEBE SALIR",
        "permanencia_pct": "NO DEBE SALIR",
        "bicentenario_n": "NO DEBE SALIR",
        "bicentenario_pct": "NO DEBE SALIR",
        "especial_n": "NO DEBE SALIR",
        "especial_pct": "NO DEBE SALIR",
        "ffaa_n": "NO DEBE SALIR",
        "ffaa_pct": "NO DEBE SALIR",
        "vraem_n": "NO DEBE SALIR",
        "vraem_pct": "NO DEBE SALIR",
        "repec_n": "NO DEBE SALIR",
        "repec_pct": "NO DEBE SALIR",
        "internacional_n": "NO DEBE SALIR",
        "internacional_pct": "NO DEBE SALIR",
        "otros_n": "NO DEBE SALIR",
        "otros_pct": "NO DEBE SALIR",
        "fecha_carga": "21/09/2016 00:00:00",
    }

    transformed = transform_pronabec_becarios_provincia(record, CONTEXT)

    assert transformed == {
        "source_row_id": 2,
        "region": "AMAZONAS",
        "provincia": "CHACHAPOYAS",
        "becarios_b18_count": 72,
        "source_snapshot_date": date(2016, 9, 21),
        "source_system": "PRONABEC",
        "source_dataset": "becarios_provincia",
        "extraction_date": "2026-06-15",
        "ingestion_timestamp": "2026-06-16T00:00:00+00:00",
        "pipeline_run_id": "test-run",
    }
    assert "aggregation_scope" not in transformed
    assert "nro_fila" not in transformed
    assert "b18_pct" not in transformed
    assert "permanencia_n" not in transformed
    assert "permanencia_pct" not in transformed
    assert "bicentenario_n" not in transformed
    assert "bicentenario_pct" not in transformed
    assert "especial_n" not in transformed
    assert "especial_pct" not in transformed
    assert "ffaa_n" not in transformed
    assert "ffaa_pct" not in transformed
    assert "vraem_n" not in transformed
    assert "vraem_pct" not in transformed
    assert "repec_n" not in transformed
    assert "repec_pct" not in transformed
    assert "internacional_n" not in transformed
    assert "internacional_pct" not in transformed
    assert "otros_n" not in transformed
    assert "otros_pct" not in transformed


@pytest.mark.parametrize(
    "record",
    [
        {"region": "AMAZONAS", "provincia": "TOTAL"},
        {"region": None, "provincia": "TOTAL DE BENEFICIARIOS"},
        {"region": None, "provincia": "TOTAL GLOBAL"},
        {"region": "AMAZONAS", "provincia": "TOTAL AMAZONAS"},
        {"region": "", "provincia": "CHACHAPOYAS"},
        {"region": "AMAZONAS", "provincia": "   "},
    ],
)
def test_transform_pronabec_becarios_provincia_discards_aggregates(
    record: dict[str, object],
) -> None:
    assert transform_pronabec_becarios_provincia(record, CONTEXT) is None


def test_transform_pronabec_becarios_provincia_empty_b18_count_is_none() -> None:
    transformed = transform_pronabec_becarios_provincia(
        {
            "source_row_id": "3",
            "region": "AMAZONAS",
            "provincia": "BAGUA",
            "b18_n": "",
            "fecha_carga": "21/09/2016 00:00:00",
        },
        CONTEXT,
    )

    assert transformed is not None
    assert transformed["becarios_b18_count"] is None
    assert transformed["source_snapshot_date"] == date(2016, 9, 21)


def test_transform_pronabec_colegios_elegibles() -> None:
    record = {
        "source_row_id": "40",
        "nro_fila": "7",
        "ugel": " UGEL   CHACHAPOYAS ",
        "institucion_educativa": " I.E.   12345 ",
        "tipo_gestion": " Pública ",
        "nivel_modalidad": " Secundaria ",
        "forma_atencion": " Escolarizada ",
        "centro_poblado": "NO DEBE SALIR",
        "distrito": " CHACHAPOYAS ",
        "direccion": "NO DEBE SALIR",
        "telefono": "NO DEBE SALIR",
        "fecha_carga": "NO DEBE SALIR",
    }

    transformed = transform_pronabec_colegios_elegibles(record, CONTEXT)

    assert transformed["ugel"] == "UGEL CHACHAPOYAS"
    assert transformed["institucion_educativa"] == "I.E. 12345"
    assert transformed["tipo_gestion_colegio"] == "Pública"
    assert transformed["nivel_modalidad"] == "Secundaria"
    assert transformed["forma_atencion"] == "Escolarizada"
    assert transformed["distrito"] == "CHACHAPOYAS"
    assert "centro_poblado" not in transformed
    assert "direccion" not in transformed
    assert "telefono" not in transformed
    assert_common_metadata(transformed, "colegios_habiles")


@pytest.mark.parametrize(
    ("dataset_name", "record"),
    [
        ("convocatorias", {"source_row_id": "1"}),
        ("ubigeo_postulacion", {"source_row_id": "2"}),
        (
            "becarios_provincia",
            {
                "source_row_id": "3",
                "region": "AMAZONAS",
                "provincia": "CHACHAPOYAS",
                "b18_n": "72",
                "fecha_carga": "21/09/2016 00:00:00",
            },
        ),
        ("becarios_pais_estudio", {"source_row_id": "3"}),
        ("colegios_habiles", {"source_row_id": "4"}),
    ],
)
def test_transform_pronabec_router_supported_datasets(
    dataset_name: str,
    record: dict[str, object],
) -> None:
    transformed = transform_pronabec_record(dataset_name, record, CONTEXT)

    assert transformed is not None
    assert transformed["source_dataset"] == dataset_name
    assert transformed["source_system"] in {"pronabec", "PRONABEC"}


def test_transform_pronabec_router_unsupported_dataset() -> None:
    with pytest.raises(ValueError, match="Unsupported PRONABEC dataset 'perdida_becas'"):
        transform_pronabec_record("perdida_becas", {}, CONTEXT)


def test_common_metadata_for_all_dataset_outputs() -> None:
    transforms = [
        ("convocatorias", transform_pronabec_convocatorias),
        ("ubigeo_postulacion", transform_pronabec_ubigeo_postulacion),
        ("becarios_pais_estudio", transform_pronabec_becarios_pais_estudio),
        ("colegios_habiles", transform_pronabec_colegios_elegibles),
    ]

    for dataset_name, transform in transforms:
        assert_common_metadata(transform({}, CONTEXT), dataset_name)


def test_invalid_and_missing_values_are_defensive() -> None:
    transformed = transform_pronabec_convocatorias(
        {
            "source_row_id": "abc",
            "id_convocatoria": "x",
            "codigo_anual": "   ",
            "vacantes": "no-numero",
        },
        CONTEXT,
    )

    assert transformed["source_row_id"] is None
    assert transformed["id_convocatoria"] is None
    assert transformed["codigo_anual"] is None
    assert transformed["descripcion_convocatoria"] is None
    assert transformed["modalidad"] is None
    assert transformed["programa"] is None
    assert transformed["vacantes"] is None


def test_no_business_canonicalization() -> None:
    value = "BECA DE PERMANENCIA DE ESTUDIOS NACIONAL - CONVOCATORIA 2024"
    transformed = transform_pronabec_becarios_pais_estudio(
        {"convocatoria": value},
        CONTEXT,
    )

    assert transformed["convocatoria"] == value
    assert transformed["convocatoria"] != "BECA PERMANENCIA"
    assert "convocatoria_canonical" not in transformed
    assert "match_score" not in transformed


def test_dataflow_skeleton_hook_applies_pronabec_transform() -> None:
    transformed = transform_bronze_record(
        {
            "source_row_id": "10",
            "id_convocatoria": "123",
            "description_conv": " Beca   Especial ",
            "modalidad": " ACADÃ‰MICA ",
            "programa": "BECA CENTENARIO                                ",
            "vacantes": "100",
            "nro_fila": "NO DEBE SALIR",
        },
        source_system="pronabec",
        source_dataset="convocatorias",
        extraction_date="2026-06-15",
        ingestion_timestamp="2026-06-16T00:00:00+00:00",
        pipeline_run_id="test-run",
    )

    assert transformed["source_dataset"] == "convocatorias"
    assert transformed["descripcion_convocatoria"] == "Beca Especial"
    assert transformed["modalidad"] == "ACADÉMICA"
    assert "nro_fila" not in transformed


def test_dataflow_skeleton_discards_becarios_provincia_total() -> None:
    from pipelines.dataflow_bronze_to_silver import transform_bronze_records

    transformed = transform_bronze_records(
        {
            "source_row_id": "1",
            "region": "AMAZONAS",
            "provincia": "TOTAL",
            "b18_n": "100",
            "fecha_carga": "21/09/2016 00:00:00",
        },
        source_system="pronabec",
        source_dataset="becarios_provincia",
        extraction_date="2026-06-15",
        ingestion_timestamp="2026-06-16T00:00:00+00:00",
        pipeline_run_id="test-run",
    )

    assert transformed == []
