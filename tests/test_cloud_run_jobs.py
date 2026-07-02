# -*- coding: utf-8 -*-
"""Pruebas unitarias para validar los jobs de Cloud Run registrados."""

from pathlib import Path
import re

REPO_ROOT = Path(__file__).resolve().parents[1]
DEPLOY_SCRIPT_PATH = REPO_ROOT / "scripts" / "deploy_cloud_run_jobs.ps1"
ENV_EXAMPLE_PATH = REPO_ROOT / ".env.example"
GCP_EXAMPLE_PATH = REPO_ROOT / "config" / "gcp.example.yaml"


def _read_deploy_script() -> str:
    return DEPLOY_SCRIPT_PATH.read_text(encoding="utf-8")


def test_deploy_script_exists():
    assert DEPLOY_SCRIPT_PATH.exists(), f"El script {DEPLOY_SCRIPT_PATH} no existe"


def test_selected_pronabec_api_jobs_are_defined():
    content = _read_deploy_script()

    required_jobs = [
        "pronabec-discovery-job",
        "pronabec-build-plan-job",
        "pronabec-run-plan-job",
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


def test_mef_extract_job_has_complete_runtime_configuration():
    content = _read_deploy_script()

    required_env_vars = [
        "MEF_SOURCE_MODE",
        "MEF_CONSULTA_AMIGABLE_BASE_URL",
        "MEF_START_YEAR",
        "MEF_END_YEAR",
        "MEF_TEXT_FILTER",
        "MEF_TIMEOUT_SECONDS",
        "MEF_PRONABEC_EXECUTORA_CODE",
        "MEF_PRONABEC_EXECUTORA_NAME",
        "MEF_INCLUDE_HIERARCHY",
        "MEF_INCLUDE_SPENDING_BREAKDOWNS",
        "MEF_BREAKDOWN_SLICES",
    ]

    for env_var in required_env_vars:
        assert env_var in content, f"Falta variable MEF en Cloud Run deploy: {env_var}"

    assert "Assert-RequiredValue -Name \"MEF_SOURCE_MODE\"" in content
    assert "Assert-RequiredValue -Name \"MEF_START_YEAR\"" in content
    assert "Assert-RequiredValue -Name \"MEF_END_YEAR\"" in content
    assert "Assert-RequiredValue -Name \"MEF_TEXT_FILTER\"" in content
    assert "Assert-RequiredValue -Name \"MEF_PRONABEC_EXECUTORA_CODE\"" in content
    assert "MEF_START_YEAR=2026" not in content
    assert "MEF_END_YEAR=2026" not in content


def test_mef_breakdown_slices_are_complete_and_safe_as_single_env_var():
    content = _read_deploy_script()

    expected_slices = [
        "producto",
        "generica",
        "fuente",
        "rubro",
        "departamento",
        "temporal",
        "producto_temporal",
        "actividad",
        "actividad_temporal",
        "generica_temporal",
    ]

    for slice_name in expected_slices:
        assert slice_name in content

    assert "MEF_BREAKDOWN_SLICES=$MefBreakdownSlices" in content
    assert "Join-CloudRunEnvVars" in content
    assert "return \"^|^\" + ($EnvVars -join \"|\")" in content


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

    assert "PRONABEC_EXTRACTION_SCOPE" not in content
    assert "PRONABEC_DISCOVERY_JOB_NAME" in content
    assert "PRONABEC_BUILD_PLAN_JOB_NAME" in content
    assert "PRONABEC_RUN_PLAN_JOB_NAME" in content
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
    assert "pipelines.run_pronabec_extraction_plan" in content
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
    assert "pronabec-run-plan-job" in content
    assert "pronabec-extract-chunk-job" in content
    assert "pronabec-finalize-dataset-job" in content
    assert "pipelines.discover_pronabec" in content
    assert "pipelines.build_pronabec_extraction_plan" in content
    assert "pipelines.run_pronabec_extraction_plan" in content
    assert "pipelines.extract_pronabec" in content
    assert "pipelines.finalize_pronabec_dataset" in content


def test_pronabec_reports_stage_job_runs_as_module():
    content = _read_deploy_script()
    stage_section = content[
        content.index("-JobName $PronabecReportsStageJobName"):
        content.index("-JobName $BronzeManifestValidationJobName")
    ]

    assert "\"-m\"" in stage_section
    assert "tools.stage_pronabec_manual_reports" in stage_section
    assert "--strict" in stage_section
    assert "--overwrite" in stage_section
    assert "tools/stage_pronabec_manual_reports.py" not in stage_section
    assert "PRONABEC_REPORTS_LANDING_PREFIX=$PronabecReportsLandingPrefix" in content
    assert "PRONABEC_REPORTS_BRONZE_PREFIX=$PronabecReportsBronzePrefix" in content


def test_pronabec_finalize_job_does_not_hardcode_source_dataset():
    content = _read_deploy_script()
    finalize_section = content[
        content.index("-JobName $PronabecFinalizeDatasetJobName"):
        content.index("-JobName $MefJobName")
    ]

    assert "pipelines.finalize_pronabec_dataset" in finalize_section
    assert "SOURCE_DATASET=" not in finalize_section
    assert "--source-dataset" not in finalize_section


def test_pronabec_plan_runner_job_does_not_hardcode_chunk_ranges():
    content = _read_deploy_script()

    assert "pronabec-run-plan-job" in content
    assert "pipelines.run_pronabec_extraction_plan" in content
    assert "PAGE_START" not in content
    assert "PAGE_END" not in content


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


def test_upsert_cloud_run_job_has_memory_and_cpu_defaults():
    content = _read_deploy_script()
    assert '[string]$Memory = "512Mi"' in content
    assert '[string]$Cpu = "1"' in content


def test_pronabec_finalize_job_memory_and_cpu():
    content = _read_deploy_script()
    finalize_section = content[
        content.index("-JobName $PronabecFinalizeDatasetJobName"):
        content.index("-JobName $MefJobName")
    ]
    assert '-Memory "2Gi"' in finalize_section
    assert '-Cpu "1"' in finalize_section


def test_dataflow_jobs_use_dedicated_worker_service_account():
    content = _read_deploy_script()

    assert "$DataflowServiceAccount = $env:DATAFLOW_SERVICE_ACCOUNT" in content
    assert "Assert-RequiredValue -Name \"DATAFLOW_SERVICE_ACCOUNT\"" in content
    assert "DATAFLOW_SERVICE_ACCOUNT=$DataflowServiceAccount" in content
    assert "--service-account-email" in content
    assert "$DataflowServiceAccount" in content
    assert "1030103187284-compute@developer.gserviceaccount.com" not in content
    assert "compute@developer.gserviceaccount.com" not in content


def test_dataflow_jobs_package_pipeline_modules_for_workers():
    content = _read_deploy_script()

    assert "$DataflowSdkContainerImage = $env:DATAFLOW_SDK_CONTAINER_IMAGE" in content
    assert "Resolve-DataflowSdkContainerImage" in content
    assert "DATAFLOW_SDK_CONTAINER_IMAGE=$ResolvedDataflowSdkContainerImage" in content
    assert "--sdk-container-image" in content
    assert "$ResolvedDataflowSdkContainerImage" in content
    assert "DATAFLOW_SETUP_FILE" not in content
    assert "--setup-file" not in content
    assert "DATAFLOW_REQUIREMENTS_FILE" not in content
    assert "--requirements-file" not in content


def test_non_dataflow_jobs_do_not_receive_sdk_container_arg():
    content = _read_deploy_script()

    non_dataflow_sections = [
        content[content.index("-JobName $PronabecDiscoveryJobName"):content.index("-JobName $PronabecBuildPlanJobName")],
        content[content.index("-JobName $PronabecBuildPlanJobName"):content.index("-JobName $PronabecRunPlanJobName")],
        content[content.index("-JobName $PronabecRunPlanJobName"):content.index("-JobName $PronabecExtractChunkJobName")],
        content[content.index("-JobName $PronabecExtractChunkJobName"):content.index("-JobName $PronabecFinalizeDatasetJobName")],
        content[content.index("-JobName $PronabecFinalizeDatasetJobName"):content.index("-JobName $MefJobName")],
        content[content.index("-JobName $MefJobName"):content.index("-JobName $PronabecReportsStageJobName")],
        content[content.index("-JobName $GoldPublishJobName"):content.index("-JobName $GoldValidateJobName")],
        content[content.index("-JobName $GoldValidateJobName"):content.index("-JobName $DataflowPronabecConvocatoriasJobName")],
    ]

    for section in non_dataflow_sections:
        assert "--sdk-container-image" not in section


def test_dataflow_service_account_is_documented_in_examples():
    env_example = ENV_EXAMPLE_PATH.read_text(encoding="utf-8")
    gcp_example = GCP_EXAMPLE_PATH.read_text(encoding="utf-8")

    expected_service_account = (
        "DATAFLOW_SERVICE_ACCOUNT=pronabec-dataflow-sa@"
        "pronabec-cloud-bi-platform.iam.gserviceaccount.com"
    )

    assert expected_service_account in env_example
    assert "DATAFLOW_WORKER_MACHINE_TYPE=n1-standard-2" in env_example
    assert "DATAFLOW_MAX_NUM_WORKERS=2" in env_example
    assert "DATAFLOW_SDK_CONTAINER_IMAGE=us-central1-docker.pkg.dev/your-gcp-project-id/pronabec-containers/pronabec-dataflow-worker:latest" in env_example
    assert "DATAFLOW_WORKER_IMAGE_NAME=pronabec-dataflow-worker" in env_example
    assert "DATAFLOW_WORKER_IMAGE_TAG=latest" in env_example
    assert "service_account: pronabec-dataflow-sa@pronabec-cloud-bi-platform.iam.gserviceaccount.com" in gcp_example
    assert "sdk_container_image: us-central1-docker.pkg.dev/your-gcp-project-id/pronabec-containers/pronabec-dataflow-worker:latest" in gcp_example


def test_pyproject_packages_pipeline_modules():
    pyproject = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")

    assert '[tool.setuptools.packages.find]' in pyproject
    assert '"pipelines"' in pyproject
    assert '"pipelines.common"' in pyproject
    assert '"pipelines.transforms"' in pyproject
