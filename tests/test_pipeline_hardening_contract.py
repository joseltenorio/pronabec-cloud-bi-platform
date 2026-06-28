from __future__ import annotations

from pathlib import Path


DEPLOY_SCRIPT = Path("scripts/deploy_cloud_run_jobs.ps1")
DAG_FILE = Path("dags/pronabec_medallion_batch_dag.py")
RUNBOOK = Path("docs/cloud/final_e2e_runbook.md")
README = Path("README.md")


def test_bronze_manifest_validation_is_deployable_and_orchestrated():
    deploy_content = DEPLOY_SCRIPT.read_text(encoding="utf-8")
    dag_content = DAG_FILE.read_text(encoding="utf-8")

    assert "bronze-manifest-validation-job" in deploy_content
    assert "pipelines.validate_bronze_manifests" in deploy_content

    assert "validate_bronze_manifests" in dag_content
    assert "run_bronze_manifest_validation" in dag_content
    assert "validate_bronze_manifests >> pronabec_api_tasks" in dag_content
    assert "validate_bronze_manifests >> mef_tasks" in dag_content
    assert "validate_bronze_manifests >> report_tasks" in dag_content


def test_cost_controlled_composer_operations_are_documented():
    runbook_content = RUNBOOK.read_text(encoding="utf-8")
    readme_content = README.read_text(encoding="utf-8")

    assert "gcloud composer environments delete" in runbook_content
    assert "scripts/configure_airflow_variables.ps1" in runbook_content
    assert "bronze-manifest-validation-job" in readme_content