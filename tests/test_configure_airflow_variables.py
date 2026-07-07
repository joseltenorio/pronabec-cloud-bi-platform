from __future__ import annotations

from pathlib import Path


SCRIPT_PATH = Path("scripts/configure_airflow_variables.sh")


def test_configure_airflow_variables_script_exists():
    assert SCRIPT_PATH.exists()


def test_configure_airflow_variables_sets_required_core_values():
    content = SCRIPT_PATH.read_text(encoding="utf-8")

    required_variables = [
        "gcp_project_id",
        "gcp_region",
        "cloud_run_region",
        "gcs_bucket_name",
        "bq_bronze_dataset",
        "bq_silver_dataset",
        "bq_gold_dataset",
        "bq_audit_dataset",
        "dataflow_sdk_container_image",
    ]

    for variable in required_variables:
        assert variable in content


def test_configure_airflow_variables_sets_job_names():
    content = SCRIPT_PATH.read_text(encoding="utf-8")

    required_variables = [
        "pronabec_discovery_job_name",
        "pronabec_build_plan_job_name",
        "pronabec_run_plan_job_name",
        "pronabec_finalize_dataset_job_name",
        "mef_extract_job_name",
        "pronabec_reports_stage_job_name",
        "bronze_manifest_validation_job_name",
        "dataflow_pronabec_report_job_name",
        "dataflow_pronabec_convocatorias_job_name",
        "dataflow_pronabec_ubigeo_postulacion_job_name",
        "dataflow_pronabec_becarios_pais_estudio_job_name",
        "dataflow_pronabec_colegios_habiles_job_name",
        "dataflow_pronabec_becarios_provincia_job_name",
        "dataflow_mef_presupuesto_job_name",
        "dataflow_mef_presupuesto_temporal_job_name",
        "dataflow_mef_producto_job_name",
        "dataflow_mef_producto_temporal_job_name",
        "dataflow_mef_actividad_job_name",
        "dataflow_mef_actividad_temporal_job_name",
        "dataflow_mef_generica_job_name",
        "dataflow_mef_generica_temporal_job_name",
        "dataflow_mef_hierarchy_job_name",
        "gold_publish_job_name",
        "gold_validate_job_name",
        "quality_checks_job_name",
    ]

    for variable in required_variables:
        assert variable in content


def test_configure_airflow_variables_omits_legacy_pronabec_jobs():
    content = SCRIPT_PATH.read_text(encoding="utf-8")

    forbidden_variables = [
        "pronabec_" + "extract_job_name",
        "pronabec_" + "extract_chunk_job_name",
        "PRONABEC_" + "EXTRACT_JOB_NAME",
        "PRONABEC_" + "EXTRACT_CHUNK_JOB_NAME",
    ]

    for variable in forbidden_variables:
        assert variable not in content


def test_configure_airflow_variables_uses_strict_bash_and_variables_set():
    content = SCRIPT_PATH.read_text(encoding="utf-8")

    assert "set -euo pipefail" in content
    assert 'variables set -- "$key" "$value"' in content


def test_configure_airflow_variables_sets_dataflow_worker_image() -> None:
    content = SCRIPT_PATH.read_text(encoding="utf-8")

    assert "DATAFLOW_SDK_CONTAINER_IMAGE" in content
    assert "require_env DATAFLOW_SDK_CONTAINER_IMAGE" in content
    assert 'set_airflow_variable dataflow_sdk_container_image "$DATAFLOW_SDK_CONTAINER_IMAGE"' in content
