from __future__ import annotations

from pathlib import Path

import pytest

from pipelines.common.config import ConfigError
from pipelines.common.orchestration_config import (
    DatasetExtractionPolicy,
    build_bq_table_ref,
    build_gcs_uri,
    get_chunked_pronabec_datasets,
    get_enabled_pronabec_datasets,
    get_pronabec_dataset_policies,
    get_required_pronabec_datasets,
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
    assert resolve_airflow_var_name(config, "pronabec_discovery_job_name_var") == "pronabec_discovery_job_name"
    assert resolve_airflow_var_name(config, "pronabec_build_plan_job_name_var") == "pronabec_build_plan_job_name"
    assert resolve_airflow_var_name(config, "pronabec_extract_chunk_job_name_var") == "pronabec_extract_chunk_job_name"
    assert resolve_airflow_var_name(config, "pronabec_finalize_dataset_job_name_var") == "pronabec_finalize_dataset_job_name"
    assert (
        resolve_airflow_var_name(config, "bronze_manifest_validation_job_name_var")
        == "bronze_manifest_validation_job_name"
    )


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
    assert {group["source_subset"] for group in groups} == {
        "pes_2025",
        "beca18_universitarios_2012_2026",
    }
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

def test_bronze_manifest_validation_job_is_configured() -> None:
    config = load_orchestration_config(ORCHESTRATION_CONFIG_PATH)

    assert (
        config["jobs"]["bronze_manifest_validation_job_name_var"]
        == "bronze_manifest_validation_job_name"
    )
    assert (
        resolve_airflow_var_name(config, "bronze_manifest_validation_job_name_var")
        == "bronze_manifest_validation_job_name"
    )


def test_pronabec_dataset_policies_are_loaded_from_manifest() -> None:
    config = load_orchestration_config(ORCHESTRATION_CONFIG_PATH)
    policies = get_pronabec_dataset_policies(config)

    assert policies
    assert all(isinstance(policy, DatasetExtractionPolicy) for policy in policies)
    assert {policy.source_dataset for policy in policies} >= {
        "convocatorias",
        "becarios_pais_estudio",
        "notas_becarios",
        "convocatorias_carrera_sede",
    }


def test_pronabec_dataset_policy_fields_are_typed() -> None:
    config = load_orchestration_config(ORCHESTRATION_CONFIG_PATH)

    for policy in get_pronabec_dataset_policies(config):
        assert policy.source_dataset
        assert isinstance(policy.extraction_enabled, bool)
        assert isinstance(policy.silver_enabled, bool)
        assert isinstance(policy.required_for_e2e, bool)
        assert policy.extraction_mode in {"single", "chunked"}
        assert policy.max_parallel_chunks > 0
        assert policy.recommended_page_size > 0
        assert policy.fallback_page_sizes
        assert all(value > 0 for value in policy.fallback_page_sizes)
        assert policy.fallback_page_sizes == sorted(policy.fallback_page_sizes, reverse=True)
        assert all(value <= policy.recommended_page_size for value in policy.fallback_page_sizes)
        if policy.max_page_size_tested_ok is not None:
            assert policy.max_page_size_tested_ok > 0
            assert policy.recommended_page_size <= policy.max_page_size_tested_ok
        assert policy.page_size_policy == "dataset_safe_default"
        assert isinstance(policy.allow_record_count_mismatch, bool)
        if policy.extraction_mode == "chunked":
            assert policy.chunk_size_pages is not None
            assert policy.chunk_size_pages > 0
            assert policy.max_parallel_chunks > 0
        else:
            assert policy.chunk_size_pages is None
            assert policy.max_parallel_chunks == 1


def test_pronabec_dataset_policies_use_real_endpoint_names() -> None:
    orchestration_config = load_orchestration_config(ORCHESTRATION_CONFIG_PATH)
    endpoints_config = load_endpoints_config(ENDPOINTS_CONFIG_PATH)

    endpoint_names = {
        endpoint["name"]
        for endpoint in endpoints_config["pronabec"]["endpoints"]
    }
    policy_names = {
        policy.source_dataset
        for policy in get_pronabec_dataset_policies(orchestration_config)
    }

    assert policy_names <= endpoint_names


def test_pronabec_dataset_policy_helpers_filter_by_flags() -> None:
    config = load_orchestration_config(ORCHESTRATION_CONFIG_PATH)

    assert get_enabled_pronabec_datasets(config) == [
        "convocatorias",
        "becarios_provincia",
        "ubigeo_postulacion",
        "colegios_habiles",
        "becarios_pais_estudio",
    ]
    assert get_required_pronabec_datasets(config) == [
        "convocatorias",
        "becarios_provincia",
        "ubigeo_postulacion",
        "colegios_habiles",
        "becarios_pais_estudio",
    ]
    assert get_chunked_pronabec_datasets(config) == ["convocatorias_carrera_sede"]


def test_pronabec_bronze_only_datasets_are_not_required_for_e2e() -> None:
    config = load_orchestration_config(ORCHESTRATION_CONFIG_PATH)
    policies = {
        policy.source_dataset: policy
        for policy in get_pronabec_dataset_policies(config)
    }

    for dataset in [
        "notas_becarios",
        "convocatorias_carrera_sede",
        "perdida_becas",
        "concepto_pago",
        "periodos_academicos",
        "nota_postulante_region",
    ]:
        assert policies[dataset].extraction_enabled is False
        assert policies[dataset].silver_enabled is False
        assert policies[dataset].required_for_e2e is False


def test_invalid_pronabec_policy_mode_is_rejected() -> None:
    config = load_orchestration_config(ORCHESTRATION_CONFIG_PATH)
    config["datasets"]["pronabec_api"]["extraction_policies"][0]["extraction_mode"] = "full"

    with pytest.raises(ConfigError, match="extraction_mode invalido"):
        validate_orchestration_config(config)


def test_chunked_pronabec_policy_requires_positive_chunk_size() -> None:
    config = load_orchestration_config(ORCHESTRATION_CONFIG_PATH)

    for policy in config["datasets"]["pronabec_api"]["extraction_policies"]:
        if policy["source_dataset"] == "convocatorias_carrera_sede":
            policy["chunk_size_pages"] = 0
            break

    with pytest.raises(ConfigError, match="chunk_size_pages debe ser entero positivo"):
        validate_orchestration_config(config)


def test_pronabec_policy_requires_positive_max_parallel_chunks() -> None:
    config = load_orchestration_config(ORCHESTRATION_CONFIG_PATH)
    config["datasets"]["pronabec_api"]["extraction_policies"][0]["max_parallel_chunks"] = 0

    with pytest.raises(ConfigError, match="max_parallel_chunks debe ser entero positivo"):
        validate_orchestration_config(config)


def test_pronabec_policy_requires_boolean_flags() -> None:
    config = load_orchestration_config(ORCHESTRATION_CONFIG_PATH)
    config["datasets"]["pronabec_api"]["extraction_policies"][0]["extraction_enabled"] = "yes"

    with pytest.raises(ConfigError, match="extraction_enabled debe ser boolean"):
        validate_orchestration_config(config)


def test_pronabec_page_size_policy_values_are_calibrated() -> None:
    config = load_orchestration_config(ORCHESTRATION_CONFIG_PATH)
    policies = {
        policy.source_dataset: policy
        for policy in get_pronabec_dataset_policies(config)
    }

    assert policies["notas_becarios"].extraction_mode == "single"
    assert policies["notas_becarios"].recommended_page_size == 10000
    assert policies["notas_becarios"].fallback_page_sizes == [5000, 2000, 1000, 500, 100]
    assert policies["notas_becarios"].max_page_size_tested_ok == 20000
    assert policies["becarios_pais_estudio"].recommended_page_size == 10000
    assert policies["becarios_pais_estudio"].extraction_mode == "single"
    assert policies["colegios_habiles"].extraction_mode == "single"
    assert policies["convocatorias_carrera_sede"].extraction_mode == "chunked"
    assert policies["convocatorias_carrera_sede"].chunk_size_pages == 10
    assert policies["convocatorias_carrera_sede"].recommended_page_size == 5000
    assert policies["ubigeo_postulacion"].recommended_page_size == 2000
    assert policies["becarios_provincia"].recommended_page_size == 500
    assert policies["becarios_provincia"].allow_record_count_mismatch is True


def test_pronabec_policy_requires_positive_recommended_page_size() -> None:
    config = load_orchestration_config(ORCHESTRATION_CONFIG_PATH)
    config["datasets"]["pronabec_api"]["extraction_policies"][0]["recommended_page_size"] = 0

    with pytest.raises(ConfigError, match="recommended_page_size debe ser entero positivo"):
        validate_orchestration_config(config)


def test_pronabec_policy_requires_positive_fallback_page_sizes() -> None:
    config = load_orchestration_config(ORCHESTRATION_CONFIG_PATH)
    config["datasets"]["pronabec_api"]["extraction_policies"][0]["fallback_page_sizes"] = [100, 0]

    with pytest.raises(ConfigError, match="fallback_page_sizes debe contener enteros positivos"):
        validate_orchestration_config(config)


def test_pronabec_policy_requires_descending_fallback_page_sizes() -> None:
    config = load_orchestration_config(ORCHESTRATION_CONFIG_PATH)
    config["datasets"]["pronabec_api"]["extraction_policies"][0]["fallback_page_sizes"] = [100, 500]

    with pytest.raises(ConfigError, match="fallback_page_sizes debe estar ordenado"):
        validate_orchestration_config(config)


def test_pronabec_policy_rejects_fallback_above_recommended_page_size() -> None:
    config = load_orchestration_config(ORCHESTRATION_CONFIG_PATH)
    config["datasets"]["pronabec_api"]["extraction_policies"][0]["fallback_page_sizes"] = [1000, 100]

    with pytest.raises(ConfigError, match="fallback_page_sizes no puede contener valores mayores"):
        validate_orchestration_config(config)


def test_pronabec_policy_rejects_recommended_above_tested_max() -> None:
    config = load_orchestration_config(ORCHESTRATION_CONFIG_PATH)
    config["datasets"]["pronabec_api"]["extraction_policies"][0]["recommended_page_size"] = 20000

    with pytest.raises(ConfigError, match="recommended_page_size no puede ser mayor"):
        validate_orchestration_config(config)


def test_pronabec_policy_requires_non_empty_page_size_policy() -> None:
    config = load_orchestration_config(ORCHESTRATION_CONFIG_PATH)
    config["datasets"]["pronabec_api"]["extraction_policies"][0]["page_size_policy"] = ""

    with pytest.raises(ConfigError, match="page_size_policy debe ser string no vacio"):
        validate_orchestration_config(config)


def test_pronabec_policy_requires_boolean_allow_record_count_mismatch() -> None:
    config = load_orchestration_config(ORCHESTRATION_CONFIG_PATH)
    config["datasets"]["pronabec_api"]["extraction_policies"][4]["allow_record_count_mismatch"] = "yes"

    with pytest.raises(ConfigError, match="allow_record_count_mismatch debe ser boolean"):
        validate_orchestration_config(config)
