from __future__ import annotations

import pytest
from pipelines.dataflow_bronze_to_silver import expand_env_placeholders as expand_ds
from pipelines.quality_checks import expand_env_placeholders as expand_qc


def test_recursive_placeholder_nested_bq_output_table(monkeypatch):
    monkeypatch.setenv("PROJECT_ID", "pronabec-cloud-bi-platform")
    monkeypatch.setenv("SILVER_DATASET", "silver")
    monkeypatch.setenv("SOURCE_DATASET", "inei_pobreza_departamental")

    # BQ_OUTPUT_TABLE referencing other variables
    monkeypatch.setenv("BQ_OUTPUT_TABLE", "${PROJECT_ID}:${SILVER_DATASET}.${SOURCE_DATASET}")

    expected = "pronabec-cloud-bi-platform:silver.inei_pobreza_departamental"
    assert expand_ds("${BQ_OUTPUT_TABLE}") == expected
    assert expand_qc("${BQ_OUTPUT_TABLE}") == expected


def test_recursive_placeholder_nested_bronze_input_path(monkeypatch):
    monkeypatch.setenv("GCS_BUCKET_NAME", "pronabec-data-bucket")
    monkeypatch.setenv("INEI_REPORTS_BRONZE_PREFIX", "bronze/inei_reports")
    monkeypatch.setenv("SOURCE_DATASET", "inei_population_youth_region")
    monkeypatch.setenv("BRONZE_EXTRACTION_DATE", "2026-07-08")

    # BRONZE_INPUT_PATH referencing other variables
    monkeypatch.setenv(
        "BRONZE_INPUT_PATH",
        "gs://${GCS_BUCKET_NAME}/${INEI_REPORTS_BRONZE_PREFIX}/${SOURCE_DATASET}/extraction_date=${BRONZE_EXTRACTION_DATE}/data.csv",
    )

    expected = (
        "gs://pronabec-data-bucket/bronze/inei_reports/inei_population_youth_region/"
        "extraction_date=2026-07-08/data.csv"
    )
    assert expand_ds("${BRONZE_INPUT_PATH}") == expected
    assert expand_qc("${BRONZE_INPUT_PATH}") == expected


def test_recursive_placeholder_missing_env_var(monkeypatch):
    monkeypatch.delenv("MISSING_VAR_IN_CHAIN", raising=False)
    monkeypatch.setenv("CHAIN_VAR", "${MISSING_VAR_IN_CHAIN}")

    with pytest.raises(ValueError, match="Variable de entorno requerida no definida: MISSING_VAR_IN_CHAIN"):
        expand_ds("${CHAIN_VAR}")

    with pytest.raises(ValueError, match="Variable de entorno requerida no definida: MISSING_VAR_IN_CHAIN"):
        expand_qc("${CHAIN_VAR}")


def test_recursive_placeholder_max_recursion_depth(monkeypatch):
    # Circular reference to trigger recursion depth / loop error
    monkeypatch.setenv("CIRCULAR_A", "${CIRCULAR_B}")
    monkeypatch.setenv("CIRCULAR_B", "${CIRCULAR_A}")

    with pytest.raises(ValueError, match="Excedido el nivel maximo de recursion"):
        expand_ds("${CIRCULAR_A}")

    with pytest.raises(ValueError, match="Excedido el nivel maximo de recursion"):
        expand_qc("${CIRCULAR_A}")


def test_recursive_placeholder_too_deep_nesting(monkeypatch):
    # Nested variables of depth 6 (exceeding depth 5 limit)
    monkeypatch.setenv("VAR_1", "final_val")
    monkeypatch.setenv("VAR_2", "${VAR_1}")
    monkeypatch.setenv("VAR_3", "${VAR_2}")
    monkeypatch.setenv("VAR_4", "${VAR_3}")
    monkeypatch.setenv("VAR_5", "${VAR_4}")
    monkeypatch.setenv("VAR_6", "${VAR_5}")

    with pytest.raises(ValueError, match="Excedido el nivel maximo de recursion"):
        expand_ds("${VAR_6}")

    with pytest.raises(ValueError, match="Excedido el nivel maximo de recursion"):
        expand_qc("${VAR_6}")
