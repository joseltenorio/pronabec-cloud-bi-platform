# -*- coding: utf-8 -*-
"""Pruebas unitarias para validar los jobs de Cloud Run registrados."""

from pathlib import Path
import re

REPO_ROOT = Path(__file__).resolve().parents[1]
DEPLOY_SCRIPT_PATH = REPO_ROOT / "scripts" / "deploy_cloud_run_jobs.ps1"


def _read_deploy_script() -> str:
    return DEPLOY_SCRIPT_PATH.read_text(encoding="utf-8")


def test_deploy_script_exists():
    assert DEPLOY_SCRIPT_PATH.exists(), f"El script {DEPLOY_SCRIPT_PATH} no existe"


def test_selected_pronabec_api_jobs_are_defined():
    content = _read_deploy_script()

    required_jobs = [
        "pronabec-extract-job",
        "pronabec-discovery-job",
        "pronabec-build-plan-job",
        "pronabec-extract-chunk-job",
        "pronabec-finalize-dataset-job",
        "pronabec-stage-reports-job",
        "bronze-manifest-validation-job",
        "gold-publish-job",
        "gold-validate-job",
        "dataflow-pronabec-convocatorias-job",
        "dataflow-pronabec-ubigeo-postulacion-job",
        "dataflow-pronabec-becarios-pais-estudio-job",
        "dataflow-pronabec-colegios-habiles-job",
        "dataflow-pronabec-becarios-provincia-job",
    ]

    for job_name in required_jobs:
        assert job_name in content, f"Falta el job PRONABEC esperado: {job_name}"


def test_selected_mef_jobs_are_defined():
    content = _read_deploy_script()

    required_jobs = [
        "mef-extract-job",
        "dataflow-mef-presupuesto-job",
        "dataflow-mef-presupuesto-temporal-job",
        "dataflow-mef-producto-job",
        "dataflow-mef-producto-temporal-job",
        "dataflow-mef-actividad-job",
        "dataflow-mef-actividad-temporal-job",
        "dataflow-mef-generica-job",
        "dataflow-mef-generica-temporal-job",
        "dataflow-mef-hierarchy-job",
    ]

    for job_name in required_jobs:
        assert job_name in content, f"Falta el job MEF esperado: {job_name}"


def test_bronze_manifest_validation_job_is_defined():
    content = _read_deploy_script()

    assert "bronze-manifest-validation-job" in content
    assert "BronzeManifestValidationJobName" in content
    assert "BRONZE_MANIFEST_VALIDATION_JOB_NAME" in content
    assert "pipelines.validate_bronze_manifests" in content
    assert "Validacion de manifests Bronze antes de promover a Silver" in content
    assert "bronze-manifest-validation-job" in content


def test_pronabec_api_retry_env_vars_are_defined():
    content = _read_deploy_script()

    required_env_vars = [
        "PRONABEC_REQUEST_TIMEOUT_SECONDS",
        "PRONABEC_MAX_RETRIES",
        "PRONABEC_BACKOFF_BASE_SECONDS",
        "PRONABEC_BACKOFF_MAX_SECONDS",
    ]

    for env_var in required_env_vars:
        assert env_var in content, f"Falta variable de resiliencia PRONABEC: {env_var}"


def test_parameterized_pronabec_report_job_is_the_only_report_job():
    content = _read_deploy_script()

    assert "dataflow-pronabec-report-job" in content
    assert "DataflowPronabecReportJobName" in content
    assert "SOURCE_DATASET=placeholder_dataset" in content
    assert "INPUT_PATH=gs://" in content
    assert "OUTPUT_TABLE=" in content

    forbidden_report_references = [
        "dataflow-report-universitarios-job",
        "DataflowReportUniversitariosJobName",
    ]

    for forbidden_reference in forbidden_report_references:
        assert forbidden_reference not in content, (
            "No debe existir un job dedicado para reportes universitarios. "
            "Los 23 reportes deben usar dataflow-pronabec-report-job."
        )


def test_gold_jobs_and_env_vars_are_defined():
    content = _read_deploy_script()

    assert "PRONABEC_DISCOVERY_JOB_NAME" in content
    assert "PRONABEC_BUILD_PLAN_JOB_NAME" in content
    assert "PRONABEC_EXTRACT_CHUNK_JOB_NAME" in content
    assert "PRONABEC_FINALIZE_DATASET_JOB_NAME" in content
    assert "GOLD_PUBLISH_JOB_NAME" in content
    assert "GOLD_VALIDATE_JOB_NAME" in content
    assert "BQ_LOCATION=$Location" in content
    assert "CLOUD_RUN_JOBS_SERVICE_ACCOUNT" in content
    assert "CLOUD_RUN_SERVICE_ACCOUNT" in content
    assert "CLOUD_RUN_JOBS_REGION" in content
    assert "CLOUD_RUN_REGION" in content
    assert "pipelines.publish_gold_views" in content
    assert "pipelines.validate_gold" in content
    assert "pipelines.discover_pronabec" in content
    assert "pipelines.build_pronabec_extraction_plan" in content
    assert "pipelines.extract_pronabec" in content
    assert "pipelines.finalize_pronabec_dataset" in content


def test_pronabec_chunked_jobs_have_expected_names_and_no_dataset_specific_jobs():
    content = _read_deploy_script()

    forbidden_job_names = [
        "pronabec-extract-notas-becarios-job",
        "pronabec-extract-becarios-pais-estudio-job",
        "pronabec-extract-convocatorias-carrera-sede-job",
        "dataflow-pronabec-convocatorias-carrera-sede-job",
    ]

    for job_name in forbidden_job_names:
        assert job_name not in content, f"Nombre de job específico por dataset detectado: {job_name}"

    assert "OUTPUT_MODE=chunk" in content


def test_pronabec_chunked_job_modules_are_correct():
    content = _read_deploy_script()

    assert "pronabec-discovery-job" in content
    assert "pronabec-build-plan-job" in content
    assert "pronabec-extract-chunk-job" in content
    assert "pronabec-finalize-dataset-job" in content
    assert "pipelines.discover_pronabec" in content
    assert "pipelines.build_pronabec_extraction_plan" in content
    assert "pipelines.extract_pronabec" in content
    assert "pipelines.finalize_pronabec_dataset" in content


def test_bronze_only_jobs_are_not_defined():
    content = _read_deploy_script()

    forbidden_patterns = [
        r"dataflow-pronabec-convocatorias-carrera-sede-job",
        r"dataflow-mef-departamento-job",
        r"dataflow-mef-fuente-job",
        r"dataflow-mef-rubro-job",
        r"presupuesto_departamento",
        r"presupuesto_fuente",
        r"presupuesto_rubro",
        r"convocatorias_carrera_sede",
    ]

    for pattern in forbidden_patterns:
        assert re.search(pattern, content) is None, (
            f"Se detectó una referencia operativa prohibida en Cloud Run: {pattern}"
        )


def test_wrong_cloud_run_job_names_are_not_documented_in_script():
    content = _read_deploy_script()

    wrong_job_names = [
        "dataflow-pronabec-colegios-elegibles-job",
        "dataflow-mef-presupuesto-producto-job",
        "dataflow-mef-presupuesto-producto-temporal-job",
        "dataflow-mef-presupuesto-actividad-job",
        "dataflow-mef-presupuesto-actividad-temporal-job",
        "dataflow-mef-presupuesto-generica-job",
        "dataflow-mef-presupuesto-generica-temporal-job",
    ]

    for job_name in wrong_job_names:
        assert job_name not in content, f"Nombre de job incorrecto detectado: {job_name}"
