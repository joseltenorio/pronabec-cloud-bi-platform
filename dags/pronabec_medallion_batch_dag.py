# dags/pronabec_medallion_batch_dag.py

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


PROJECT_ID = "{{ var.value.gcp_project_id }}"
REGION = "{{ var.value.gcp_region }}"

PRONABEC_EXTRACT_JOB = "{{ var.value.pronabec_extract_job_name }}"
MEF_EXTRACT_JOB = "{{ var.value.mef_extract_job_name }}"
QUALITY_CHECKS_JOB = "{{ var.value.quality_checks_job_name }}"

DATAFLOW_PRONABEC_CONVOCATORIAS_JOB = "{{ var.value.dataflow_pronabec_convocatorias_job_name }}"
DATAFLOW_MEF_PRESUPUESTO_JOB = "{{ var.value.dataflow_mef_presupuesto_job_name }}"
DATAFLOW_REPORT_UNIVERSITARIOS_JOB = "{{ var.value.dataflow_report_universitarios_job_name }}"

EXTRACTION_DATE = "{{ dag_run.conf.get('extraction_date', ds) }}"
RUN_PRONABEC = "{{ dag_run.conf.get('run_pronabec', true) }}"
RUN_MEF = "{{ dag_run.conf.get('run_mef', true) }}"
RUN_DATAFLOW_PRONABEC = "{{ dag_run.conf.get('run_dataflow_pronabec', true) }}"
RUN_DATAFLOW_MEF = "{{ dag_run.conf.get('run_dataflow_mef', true) }}"
RUN_DATAFLOW_REPORTS = "{{ dag_run.conf.get('run_dataflow_reports', true) }}"
RUN_QUALITY = "{{ dag_run.conf.get('run_quality', true) }}"


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


def cloud_run_execute_command(job_name: str, enabled_expression: str) -> str:
    return f"""
if [ "{enabled_expression}" = "True" ] || [ "{enabled_expression}" = "true" ]; then
  gcloud run jobs execute {job_name} \
    --project {PROJECT_ID} \
    --region {REGION} \
    --update-env-vars BRONZE_EXTRACTION_DATE={EXTRACTION_DATE},PIPELINE_RUN_ID={{{{ run_id }}}} \
    --wait
else
  echo "Task disabled by DAG configuration."
fi
""".strip()


with DAG(
    dag_id="pronabec_medallion_batch",
    description="Orquestación batch Medallion para extracción, transformación Bronze a Silver y calidad de PRONABEC Cloud BI Platform.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["pronabec", "medallion", "batch", "cloud-run", "dataflow", "composer"],
    params={
        "extraction_date": Param(
            default="",
            type="string",
            description="Fecha lógica de extracción. Si no se envía, se usa la fecha de ejecución del DAG.",
        ),
        "run_pronabec": Param(
            default=True,
            type="boolean",
            description="Controla la ejecución del job de extracción PRONABEC.",
        ),
        "run_mef": Param(
            default=True,
            type="boolean",
            description="Controla la ejecución del job de extracción MEF.",
        ),
        "run_dataflow_pronabec": Param(
            default=True,
            type="boolean",
            description="Controla la transformación PRONABEC Bronze a Silver.",
        ),
        "run_dataflow_mef": Param(
            default=True,
            type="boolean",
            description="Controla la transformación MEF Bronze a Silver.",
        ),
        "run_dataflow_reports": Param(
            default=True,
            type="boolean",
            description="Controla la transformación de reportes PRONABEC Bronze a Silver.",
        ),
        "run_quality": Param(
            default=True,
            type="boolean",
            description="Controla la ejecución del job de calidad.",
        ),
    },
) as dag:
    start = EmptyOperator(task_id="start")

    run_pronabec_extract = BashOperator(
        task_id="run_pronabec_extract",
        bash_command=cloud_run_execute_command(
            job_name=PRONABEC_EXTRACT_JOB,
            enabled_expression=RUN_PRONABEC,
        ),
    )

    run_mef_extract = BashOperator(
        task_id="run_mef_extract",
        bash_command=cloud_run_execute_command(
            job_name=MEF_EXTRACT_JOB,
            enabled_expression=RUN_MEF,
        ),
    )

    run_dataflow_pronabec_convocatorias = BashOperator(
        task_id="run_dataflow_pronabec_convocatorias",
        bash_command=cloud_run_execute_command(
            job_name=DATAFLOW_PRONABEC_CONVOCATORIAS_JOB,
            enabled_expression=RUN_DATAFLOW_PRONABEC,
        ),
    )

    run_dataflow_mef_presupuesto = BashOperator(
        task_id="run_dataflow_mef_presupuesto",
        bash_command=cloud_run_execute_command(
            job_name=DATAFLOW_MEF_PRESUPUESTO_JOB,
            enabled_expression=RUN_DATAFLOW_MEF,
        ),
    )

    run_dataflow_report_universitarios = BashOperator(
        task_id="run_dataflow_report_universitarios",
        bash_command=cloud_run_execute_command(
            job_name=DATAFLOW_REPORT_UNIVERSITARIOS_JOB,
            enabled_expression=RUN_DATAFLOW_REPORTS,
        ),
    )

    run_quality_checks = BashOperator(
        task_id="run_quality_checks",
        bash_command=cloud_run_execute_command(
            job_name=QUALITY_CHECKS_JOB,
            enabled_expression=RUN_QUALITY,
        ),
        trigger_rule="all_done",
    )

    end = EmptyOperator(task_id="end")

    start >> [run_pronabec_extract, run_mef_extract]
    [run_pronabec_extract, run_mef_extract] >> run_dataflow_pronabec_convocatorias
    run_dataflow_pronabec_convocatorias >> run_dataflow_mef_presupuesto
    run_dataflow_mef_presupuesto >> run_dataflow_report_universitarios
    run_dataflow_report_universitarios >> run_quality_checks >> end