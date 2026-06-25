# dags/pronabec_medallion_batch_dag.py

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


PROJECT_ID = "{{ var.value.gcp_project_id }}"
REGION = "{{ var.value.gcp_region }}"

PRONABEC_EXTRACT_JOB = "{{ var.value.pronabec_extract_job_name }}"
MEF_EXTRACT_JOB = "{{ var.value.mef_extract_job_name }}"
QUALITY_CHECKS_JOB = "{{ var.value.quality_checks_job_name }}"


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="pronabec_medallion_batch",
    description="Orquestación batch Medallion para extracción, validación y control operativo de PRONABEC Cloud BI Platform.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["pronabec", "medallion", "batch", "cloud-run"],
) as dag:
    start = EmptyOperator(task_id="start")

    run_pronabec_extract = BashOperator(
        task_id="run_pronabec_extract",
        bash_command=(
            "gcloud run jobs execute "
            f"{PRONABEC_EXTRACT_JOB} "
            f"--project {PROJECT_ID} "
            f"--region {REGION} "
            "--wait"
        ),
    )

    run_mef_extract = BashOperator(
        task_id="run_mef_extract",
        bash_command=(
            "gcloud run jobs execute "
            f"{MEF_EXTRACT_JOB} "
            f"--project {PROJECT_ID} "
            f"--region {REGION} "
            "--wait"
        ),
    )

    run_quality_checks = BashOperator(
        task_id="run_quality_checks",
        bash_command=(
            "gcloud run jobs execute "
            f"{QUALITY_CHECKS_JOB} "
            f"--project {PROJECT_ID} "
            f"--region {REGION} "
            "--wait"
        ),
    )

    end = EmptyOperator(task_id="end")

    start >> run_pronabec_extract >> run_mef_extract >> run_quality_checks >> end