from __future__ import annotations

from pathlib import Path


EXPECTED_BASH_SCRIPTS = [
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

    assert 'gcloud composer environments describe "$COMPOSER_ENVIRONMENT_NAME"' in content
    assert '--format="value(config.dagGcsPrefix)"' in content


def test_configure_airflow_variables_uses_composer_variables_set():
    content = _read("scripts/configure_airflow_variables.sh")

    assert 'variables set -- "$key" "$value"' in content
    for variable_name in [
        "gcp_project_id",
        "gcs_bucket_name",
        "bq_bronze_dataset",
        "pronabec_discovery_job_name",
        "dataflow_pronabec_report_job_name",
        "quality_checks_job_name",
    ]:
        assert variable_name in content
