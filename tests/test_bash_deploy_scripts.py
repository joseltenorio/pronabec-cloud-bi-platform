from __future__ import annotations

from pathlib import Path


EXPECTED_BASH_SCRIPTS = [
    "scripts/check_composer_environment.sh",
    "scripts/create_composer_environment.sh",
    "scripts/delete_composer_environment.sh",
    "scripts/build_and_push_image.sh",
    "scripts/build_and_push_dataflow_worker_image.sh",
    "scripts/deploy_bigquery_sql.sh",
    "scripts/deploy_cloud_run_jobs.sh",
    "scripts/upload_composer_dag.sh",
    "scripts/configure_airflow_variables.sh",
    "scripts/run_cloud_deploy.sh",
]

FORBIDDEN_COMPOSER_UPLOAD_REFERENCES = [
    ".venv",
    "tmp/",
    "data/",
    "credentials",
    "secrets",
    "repomix-output",
    "build/generated",
    ".env",
]


def _read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def test_expected_bash_deploy_scripts_exist():
    for script in EXPECTED_BASH_SCRIPTS:
        assert Path(script).exists(), f"Missing Bash deployment script: {script}"


def test_bash_deploy_scripts_have_required_header_and_strict_mode():
    for script in EXPECTED_BASH_SCRIPTS:
        content = _read(script)
        assert content.startswith("#!/usr/bin/env bash\n")
        assert "set -euo pipefail" in content


def test_bash_deploy_scripts_do_not_call_powershell_or_ps1():
    for script in EXPECTED_BASH_SCRIPTS:
        content = _read(script)
        assert "pwsh" not in content
        assert ".ps1" not in content


def test_deploy_bigquery_invokes_generate_bigquery_ddl_python_tool():
    content = _read("scripts/deploy_bigquery_sql.sh") + _read("scripts/generate_bigquery_ddl.sh")

    assert "tools/generate_bigquery_ddl.py" in content


def test_generate_bigquery_ddl_script_supports_ci_and_deploy_modes():
    content = _read("scripts/generate_bigquery_ddl.sh")

    assert "--ci" in content
    assert "--deploy" in content
    assert "--generation-mode" in content
    assert '"$GENERATION_MODE"' in content
    assert "BRONZE_EXTRACTION_DATE is required in deploy generation mode" in content


def test_composer_lifecycle_scripts_validate_required_environment():
    check_content = _read("scripts/check_composer_environment.sh")
    create_content = _read("scripts/create_composer_environment.sh")
    delete_content = _read("scripts/delete_composer_environment.sh")

    for required in ["GCP_PROJECT_ID", "COMPOSER_LOCATION", "COMPOSER_ENVIRONMENT_NAME"]:
        assert required in check_content
    assert "--allow-missing" in check_content
    assert "gcloud composer environments describe" in check_content

    for required in [
        "GCP_PROJECT_ID",
        "COMPOSER_LOCATION",
        "COMPOSER_ENVIRONMENT_NAME",
        "COMPOSER_SERVICE_ACCOUNT",
    ]:
        assert required in create_content
    assert "./scripts/check_composer_environment.sh --allow-missing" in create_content
    assert "gcloud composer environments create" in create_content

    for required in ["GCP_PROJECT_ID", "COMPOSER_LOCATION", "COMPOSER_ENVIRONMENT_NAME"]:
        assert required in delete_content
    assert "./scripts/check_composer_environment.sh --allow-missing" in delete_content
    assert "gcloud composer environments delete" in delete_content


def test_deploy_cloud_run_jobs_references_main_and_dataflow_images():
    content = _read("scripts/deploy_cloud_run_jobs.sh")

    assert "CLOUD_RUN_IMAGE" in content
    assert "DATAFLOW_WORKER_IMAGE" in content
    assert "DATAFLOW_SDK_CONTAINER_IMAGE" in content


def test_run_cloud_deploy_supports_expected_flags():
    content = _read("scripts/run_cloud_deploy.sh")

    for flag in ["--bigquery", "--images", "--jobs", "--composer", "--all"]:
        assert flag in content


def test_upload_composer_dag_does_not_reference_forbidden_paths():
    content = _read("scripts/upload_composer_dag.sh")

    for forbidden in FORBIDDEN_COMPOSER_UPLOAD_REFERENCES:
        assert forbidden not in content


def test_upload_composer_dag_resolves_dag_bucket_from_composer():
    content = _read("scripts/upload_composer_dag.sh")

    assert "./scripts/check_composer_environment.sh --allow-missing" in content
    assert "Composer environment '" in content
    assert 'gcloud composer environments describe "$COMPOSER_ENVIRONMENT_NAME"' in content
    assert '--format="value(config.dagGcsPrefix)"' in content


def test_configure_airflow_variables_uses_composer_variables_set():
    content = _read("scripts/configure_airflow_variables.sh")

    assert "./scripts/check_composer_environment.sh --allow-missing" in content
    assert "Composer environment '" in content
    assert 'variables set -- "$key" "$value"' in content
    for variable_name in [
        "gcp_project_id",
        "gcs_bucket_name",
        "bq_bronze_dataset",
        "pronabec_discovery_job_name",
        "dataflow_pronabec_report_job_name",
        "dataflow_inei_report_job_name",
        "quality_checks_job_name",
    ]:
        assert variable_name in content


def test_ci_workflow_uses_static_ddl_generation_without_hardcoded_date():
    content = _read(".github/workflows/ci.yml")

    assert "./scripts/generate_bigquery_ddl.sh --ci" in content
    assert "--bronze-extraction-date" not in content
    assert "2026-07-06" not in content


def test_deploy_workflow_exposes_optional_bronze_extraction_date():
    content = _read(".github/workflows/deploy.yml")

    assert "bronze_extraction_date:" in content
    assert 'default: ""' in content
    assert "export BRONZE_EXTRACTION_DATE" in content
    assert "create_composer_environment:" in content
    assert "delete_composer_environment:" in content
    assert "Cannot create and delete Composer in the same deployment run." in content
    assert "Create Composer environment" in content
    assert "Delete Composer environment" in content
    assert "Validate Composer import errors" in content
