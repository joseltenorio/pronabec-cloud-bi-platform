from __future__ import annotations

import pytest

from pipelines.dataflow_bronze_to_silver import transform_bronze_records
from pipelines.transforms.inei_reports import (
    INEI_TRANSFORMS,
    transform_inei_demographic_indicators_region,
    transform_inei_internet_acceso_region,
    transform_inei_pobreza_departamental,
    transform_inei_population_youth_region,
    transform_inei_report_record,
)


CONTEXT = {
    "extraction_date": "2026-07-07",
    "ingestion_timestamp": "2026-07-07T10:00:00+00:00",
    "pipeline_run_id": "run-1",
}


def assert_metadata(row: dict[str, object], dataset: str) -> None:
    assert row["source_system"] == "INEI"
    assert row["source_dataset"] == dataset
    assert row["extraction_date"] == "2026-07-07"
    assert row["ingestion_timestamp"] == "2026-07-07T10:00:00+00:00"
    assert row["pipeline_run_id"] == "run-1"


def test_population_parses_integers_and_metadata() -> None:
    row = transform_inei_population_youth_region(
        {
            "anio": "2025",
            "region": " Lima ",
            "poblacion_total": "10,000",
            "poblacion_15_24": "1200",
            "poblacion_15_29": "1800",
            "poblacion_17_24": "900",
            "poblacion_18_24": "800",
            "share_15_24_total": "0.12",
        },
        CONTEXT,
    )

    assert row["anio"] == 2025
    assert row["region"] == "Lima"
    assert row["poblacion_total"] == 10000
    assert row["poblacion_15_24"] == 1200
    assert row["share_15_24_total"] == 0.12
    assert_metadata(row, "inei_population_youth_region")


def test_population_share_accepts_ratio_or_percent_scale() -> None:
    ratio = transform_inei_population_youth_region(
        {"anio": "2025", "region": "Lima", "share_15_24_total": "0.18"},
        CONTEXT,
    )
    percent = transform_inei_population_youth_region(
        {"anio": "2025", "region": "Lima", "share_15_24_total": "18.0"},
        CONTEXT,
    )

    assert ratio["share_15_24_total"] == 0.18
    assert percent["share_15_24_total"] == 18.0


def test_demographic_parses_decimals_and_accepts_negative_migration() -> None:
    row = transform_inei_demographic_indicators_region(
        {
            "anio": "2025",
            "region": "Cusco",
            "tasa_bruta_natalidad": "15.2",
            "tasa_global_fecundidad": "2.1",
            "esperanza_vida_nacer": "75.5",
            "tasa_mortalidad_infantil": "8.4",
            "tasa_migracion_neta": "-1.3",
            "tasa_crecimiento_total_pct": "0.9",
        },
        CONTEXT,
    )

    assert row["tasa_bruta_natalidad"] == 15.2
    assert row["tasa_migracion_neta"] == -1.3
    assert_metadata(row, "inei_demographic_indicators_region")


def test_poverty_parses_percentage_and_preserves_source_fields() -> None:
    row = transform_inei_pobreza_departamental(
        {
            "anio": "2025",
            "region": "Puno",
            "pobreza_monetaria_pct": "42.5",
            "source_name": " ENAHO ",
            "source_period": "2012-2025",
            "source_type": "manual",
            "metric": "pobreza",
        },
        CONTEXT,
    )

    assert row["pobreza_monetaria_pct"] == 42.5
    assert row["source_name"] == "ENAHO"
    assert row["source_period"] == "2012-2025"
    assert_metadata(row, "inei_pobreza_departamental")


def test_internet_parses_percentage_and_preserves_source_fields() -> None:
    row = transform_inei_internet_acceso_region(
        {
            "anio": "2025",
            "region": "Arequipa",
            "internet_acceso_pct": "73.4",
            "source_name": "INEI",
            "source_period": "2012-2025",
            "source_type": "manual",
            "metric": "internet",
        },
        CONTEXT,
    )

    assert row["internet_acceso_pct"] == 73.4
    assert row["metric"] == "internet"
    assert_metadata(row, "inei_internet_acceso_region")


def test_internet_wide_source_row_expands_year_columns_to_silver_rows() -> None:
    rows = transform_inei_internet_acceso_region(
        {
            "region": "Amazonas",
            "2012": "18.2",
            "2013": "16.8",
            "2025": "67.7",
        },
        CONTEXT,
    )

    assert isinstance(rows, list)
    assert [row["anio"] for row in rows] == [2012, 2013, 2025]
    assert rows[0]["region"] == "Amazonas"
    assert rows[0]["internet_acceso_pct"] == 18.2
    assert rows[0]["source_name"] == "INEI"
    assert rows[0]["source_period"] == "2012-2025"
    assert rows[0]["source_type"] == "manual_csv"
    assert rows[0]["metric"] == "internet_acceso_pct"
    assert_metadata(rows[0], "inei_internet_acceso_region")


@pytest.mark.parametrize(
    ("dataset", "record"),
    [
        ("inei_pobreza_departamental", {"anio": "2025", "region": "Lima", "pobreza_monetaria_pct": "101"}),
        ("inei_internet_acceso_region", {"anio": "2025", "region": "Lima", "internet_acceso_pct": "120"}),
        ("inei_population_youth_region", {"anio": "2025", "region": "Lima", "share_15_24_total": "120"}),
    ],
)
def test_rejects_percentage_out_of_range(dataset: str, record: dict[str, str]) -> None:
    with pytest.raises(ValueError, match="debe estar entre"):
        transform_inei_report_record(dataset, record, CONTEXT)


def test_rejects_invalid_year() -> None:
    with pytest.raises(ValueError, match="anio"):
        transform_inei_report_record(
            "inei_population_youth_region",
            {"anio": "bad", "region": "Lima"},
            CONTEXT,
        )


def test_rejects_empty_region() -> None:
    with pytest.raises(ValueError, match="region"):
        transform_inei_report_record(
            "inei_population_youth_region",
            {"anio": "2025", "region": " "},
            CONTEXT,
        )


def test_dispatcher_recognizes_all_datasets() -> None:
    assert set(INEI_TRANSFORMS) == {
        "inei_population_youth_region",
        "inei_demographic_indicators_region",
        "inei_pobreza_departamental",
        "inei_internet_acceso_region",
    }


def test_dispatcher_fails_unknown_dataset() -> None:
    with pytest.raises(ValueError, match="Unsupported INEI report dataset"):
        transform_inei_report_record("inei_unknown", {}, CONTEXT)


def test_dataflow_hook_routes_inei_reports() -> None:
    rows = transform_bronze_records(
        {"region": "Lima", "2024": "71", "2025": "73"},
        source_system="inei_reports",
        source_dataset="inei_internet_acceso_region",
        extraction_date="2026-07-07",
        ingestion_timestamp="2026-07-07T10:00:00+00:00",
        pipeline_run_id="run-1",
    )

    assert rows[0]["source_system"] == "INEI"
    assert rows[0]["source_dataset"] == "inei_internet_acceso_region"
    assert [row["anio"] for row in rows] == [2024, 2025]
    assert rows[1]["internet_acceso_pct"] == 73.0
