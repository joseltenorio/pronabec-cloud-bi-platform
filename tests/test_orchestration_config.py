from __future__ import annotations

from pathlib import Path

import pytest

from pipelines.common.config import ConfigError
from pipelines.common.orchestration_config import (
    build_bq_table_ref,
    build_gcs_uri,
    load_endpoints_config,
    load_orchestration_config,
    resolve_airflow_var_name,
    resolve_pronabec_report_datasets,
    resolve_pronabec_report_groups,
    validate_orchestration_config,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
ORCHESTRATION_CONFIG_PATH = REPO_ROOT / "config" / "orchestration.yaml"
ENDPOINTS_CONFIG_PATH = REPO_ROOT / "config" / "endpoints.yaml"


def test_load_orchestration_config_reads_manifest() -> None:
    config = load_orchestration_config(ORCHESTRATION_CONFIG_PATH)

    assert config["dag"]["id"] == "pronabec_medallion_batch"
    assert config["dag"]["schedule"] == "0 5 * * 6"
    assert config["runtime"]["project_id_var"] == "gcp_project_id"
    assert config["datasets"]["pronabec_reports"]["landing_path_template"] == "landing/pronabec_reports/{source_subset}"
    assert config["datasets"]["pronabec_reports"]["bronze_path_template"] == "bronze/pronabec_reports/{dataset}/extraction_date={extraction_date}/data.csv"


def test_validate_orchestration_config_accepts_current_manifest() -> None:
    config = load_orchestration_config(ORCHESTRATION_CONFIG_PATH)
    validate_orchestration_config(config)


def test_resolve_airflow_var_name_reads_runtime_and_jobs() -> None:
    config = load_orchestration_config(ORCHESTRATION_CONFIG_PATH)

    assert resolve_airflow_var_name(config, "project_id_var") == "gcp_project_id"
    assert resolve_airflow_var_name(config, "gold_publish_job_name_var") == "gold_publish_job_name"


def test_build_gcs_uri_returns_valid_uri() -> None:
    assert build_gcs_uri("bucket", "landing/pronabec_reports/pes_2025") == "gs://bucket/landing/pronabec_reports/pes_2025"


def test_build_bq_table_ref_returns_project_dataset_table() -> None:
    assert build_bq_table_ref("project", "silver", "table") == "project:silver.table"


def test_orchestration_manifest_enforces_reports_path_rules() -> None:
    config = load_orchestration_config(ORCHESTRATION_CONFIG_PATH)
    reports = config["datasets"]["pronabec_reports"]

    assert "{extraction_date}" not in reports["landing_path_template"]
    assert "{extraction_date}" in reports["bronze_path_template"]
    assert "{source_subset}" not in reports["bronze_path_template"]
    assert reports["landing_documents_path_template"].startswith("landing/")
    assert "/_documents" in reports["landing_documents_path_template"]
    assert "bronze/" not in reports["landing_documents_path_template"]


def test_orchestration_manifest_gold_queries_are_renderable() -> None:
    config = load_orchestration_config(ORCHESTRATION_CONFIG_PATH)
    queries = config["gold"]["validation_queries"]

    assert queries
    for query in queries:
        rendered = query["query"].format(project_id="project", gold_dataset="gold")
        assert "{project_id}" not in rendered
        assert "{gold_dataset}" not in rendered
        assert "project.gold." in rendered


def test_resolve_pronabec_report_groups_and_datasets() -> None:
    orchestration_config = load_orchestration_config(ORCHESTRATION_CONFIG_PATH)
    endpoints_config = load_endpoints_config(ENDPOINTS_CONFIG_PATH)

    groups = resolve_pronabec_report_groups(orchestration_config, endpoints_config)
    datasets = resolve_pronabec_report_datasets(orchestration_config, endpoints_config)

    assert len(groups) == 2
    assert len(datasets) == 23
    assert {item["source_dataset"] for item in datasets} >= {
        "report_beca18_universitarios_universidad_anual",
        "report_beca18_universitarios_carrera_anual",
    }


def test_invalid_manifest_rejected(tmp_path: Path) -> None:
    invalid_config = tmp_path / "orchestration.yaml"
    invalid_config.write_text("dag: {}\n", encoding="utf-8")

    with pytest.raises(ConfigError):
        load_orchestration_config(invalid_config)
