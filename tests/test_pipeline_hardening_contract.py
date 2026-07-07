from __future__ import annotations

from pathlib import Path


DEPLOY_SCRIPT = Path("scripts/deploy_cloud_run_jobs.sh")
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
    assert "run_cloud_run_job_with_polling" in dag_content
    assert "--wait" not in dag_content
    assert "--async" not in dag_content
    assert "gcloud" not in dag_content
    assert "subprocess" not in dag_content
    assert "AuthorizedSession" in dag_content
    assert "google.auth.default" in dag_content
    assert "timeout_seconds=180" not in dag_content
    assert "execution_timeout=timedelta(seconds=timeout_seconds + 600)" in dag_content
    assert "schedule_interval=None" in dag_content
    assert "dag_run.conf.get('pipeline_run_id', run_id)" in dag_content
    assert "silver_parallel = [pronabec_api_silver, mef_silver, pronabec_reports_silver]" in dag_content
    assert "validate_bronze_manifests >> silver_parallel" in dag_content
    assert "silver_parallel >> publish_gold_views" in dag_content


def test_cost_controlled_composer_operations_are_documented():
    runbook_content = RUNBOOK.read_text(encoding="utf-8")
    readme_content = README.read_text(encoding="utf-8")

    assert "gcloud composer environments delete" in runbook_content
    assert "bronze-manifest-validation-job" in readme_content
