from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from pipelines.common.orchestration_config import (
    build_bq_table_ref,
    load_endpoints_config,
    load_orchestration_config,
    resolve_airflow_var_name,
    resolve_pronabec_report_datasets,
    resolve_pronabec_report_groups,
)


def resolve_repo_root() -> Path:
    """Resuelve la raíz del repositorio localmente y cuando se sincroniza a Composer."""
    dag_dir = Path(__file__).resolve().parent
    repo_root = Path(__file__).resolve().parents[1]

    for candidate in (dag_dir, repo_root):
        if (candidate / "config" / "orchestration.yaml").exists() and (
            candidate / "config" / "endpoints.yaml"
        ).exists():
            return candidate

    return repo_root


REPO_ROOT = resolve_repo_root()
ORCHESTRATION_CONFIG = load_orchestration_config(REPO_ROOT / "config" / "orchestration.yaml")
ENDPOINTS_CONFIG = load_endpoints_config(REPO_ROOT / "config" / "endpoints.yaml")

PRONABEC_API_ITEMS = [
    item
    for item in ORCHESTRATION_CONFIG["datasets"]["pronabec_api"]["items"]
    if item.get("source_dataset")
]
MEF_ITEMS = [
    item
    for item in ORCHESTRATION_CONFIG["datasets"]["mef"]["items"]
    if item.get("source_dataset")
]
REPORT_GROUPS = resolve_pronabec_report_groups(ORCHESTRATION_CONFIG, ENDPOINTS_CONFIG)
REPORT_DATASETS = resolve_pronabec_report_datasets(ORCHESTRATION_CONFIG, ENDPOINTS_CONFIG)
REPORT_DATASET_TO_SUBSET = {
    item["source_dataset"]: item["source_subset"]
    for item in REPORT_DATASETS
}


def airflow_var_template(var_name: str, default: str | None = None) -> str:
    if default is None:
        return f"{{{{ var.value.{var_name} }}}}"
    return f"{{{{ var.value.get('{var_name}', '{default}') }}}}"


def resolve_job_name(item: dict[str, str], default_job_name: str) -> str:
    """Resuelve el nombre de un Cloud Run Job desde la configuración de orquestación."""
    job_name_var = item.get("job_name_var")
    job_name = item.get("job_name") or default_job_name
    if not job_name_var:
        return job_name
    return airflow_var_template(job_name_var, job_name)


PROJECT_ID = airflow_var_template(resolve_airflow_var_name(ORCHESTRATION_CONFIG, "project_id_var"))
REGION = airflow_var_template(resolve_airflow_var_name(ORCHESTRATION_CONFIG, "region_var"))
BUCKET_NAME = airflow_var_template(resolve_airflow_var_name(ORCHESTRATION_CONFIG, "bucket_name_var"))
BRONZE_DATASET = airflow_var_template(resolve_airflow_var_name(ORCHESTRATION_CONFIG, "bronze_dataset_var"))
SILVER_DATASET = airflow_var_template(resolve_airflow_var_name(ORCHESTRATION_CONFIG, "silver_dataset_var"))
GOLD_DATASET = airflow_var_template(resolve_airflow_var_name(ORCHESTRATION_CONFIG, "gold_dataset_var"))
AUDIT_DATASET = airflow_var_template(resolve_airflow_var_name(ORCHESTRATION_CONFIG, "audit_dataset_var"))

PRONABEC_EXTRACT_JOB = airflow_var_template(
    resolve_airflow_var_name(ORCHESTRATION_CONFIG, "pronabec_extract_job_name_var"),
    "pronabec-extract-job",
)
MEF_EXTRACT_JOB = airflow_var_template(
    resolve_airflow_var_name(ORCHESTRATION_CONFIG, "mef_extract_job_name_var"),
    "mef-extract-job",
)
PRONABEC_REPORTS_STAGE_JOB = airflow_var_template(
    resolve_airflow_var_name(ORCHESTRATION_CONFIG, "pronabec_reports_stage_job_name_var"),
    "pronabec-stage-reports-job",
)
BRONZE_MANIFEST_VALIDATION_JOB = airflow_var_template(
    resolve_airflow_var_name(
        ORCHESTRATION_CONFIG,
        "bronze_manifest_validation_job_name_var",
    ),
    "bronze-manifest-validation-job",
)
DATAFLOW_PRONABEC_REPORT_JOB = airflow_var_template(
    resolve_airflow_var_name(ORCHESTRATION_CONFIG, "pronabec_reports_dataflow_job_name_var"),
    "dataflow-pronabec-report-job",
)
GOLD_PUBLISH_JOB = airflow_var_template(
    resolve_airflow_var_name(ORCHESTRATION_CONFIG, "gold_publish_job_name_var"),
    "gold-publish-job",
)
GOLD_VALIDATE_JOB = airflow_var_template(
    resolve_airflow_var_name(ORCHESTRATION_CONFIG, "gold_validate_job_name_var"),
    "gold-validate-job",
)
QUALITY_CHECKS_JOB = airflow_var_template(
    resolve_airflow_var_name(ORCHESTRATION_CONFIG, "quality_checks_job_name_var"),
    "quality-checks-job",
)

EXTRACTION_DATE = "{{ dag_run.conf.get('extraction_date') or ds }}"
RUN_PRONABEC = "{{ dag_run.conf.get('run_pronabec', true) }}"
RUN_MEF = "{{ dag_run.conf.get('run_mef', true) }}"
RUN_PRONABEC_REPORTS_STAGING = "{{ dag_run.conf.get('run_pronabec_reports_staging', true) }}"
RUN_BRONZE_MANIFEST_VALIDATION = "{{ dag_run.conf.get('run_bronze_manifest_validation', true) }}"
RUN_DATAFLOW_PRONABEC = "{{ dag_run.conf.get('run_dataflow_pronabec', true) }}"
RUN_DATAFLOW_MEF = "{{ dag_run.conf.get('run_dataflow_mef', true) }}"
RUN_DATAFLOW_REPORTS = "{{ dag_run.conf.get('run_dataflow_reports', true) }}"
RUN_GOLD_PUBLISH = "{{ dag_run.conf.get('run_gold_publish', true) }}"
RUN_GOLD_VALIDATION = "{{ dag_run.conf.get('run_gold_validation', true) }}"
RUN_QUALITY = "{{ dag_run.conf.get('run_quality', true) }}"


def cloud_run_execute_command(
    job_name: str,
    enabled_expression: str,
    extra_env_vars: dict[str, str] | None = None,
) -> str:
    env_vars = [
        f"BRONZE_EXTRACTION_DATE={EXTRACTION_DATE}",
        "PIPELINE_RUN_ID={{ run_id }}",
    ]
    if extra_env_vars:
        env_vars.extend(f"{key}={value}" for key, value in extra_env_vars.items())

    joined_env_vars = ",".join(env_vars)

    return f"""
if [ "{enabled_expression}" = "True" ] || [ "{enabled_expression}" = "true" ]; then
  gcloud run jobs execute {job_name} \
    --project {PROJECT_ID} \
    --region {REGION} \
    --update-env-vars {joined_env_vars} \
    --wait
else
  echo "Tarea deshabilitada por la configuración del DAG."
fi
""".strip()


def render_gcs_path(template: str, **values: str) -> str:
    return template.format(**values)


def build_api_input_path(dataset: str) -> str:
    template = ORCHESTRATION_CONFIG["datasets"]["pronabec_api"]["bronze_path_template"]
    return f"gs://{BUCKET_NAME}/{render_gcs_path(template, dataset=dataset, extraction_date=EXTRACTION_DATE)}"


def build_api_output_table(dataset: str) -> str:
    silver_table = f"pronabec_{dataset}"
    return build_bq_table_ref(PROJECT_ID, SILVER_DATASET, silver_table)


def build_mef_output_table(silver_table: str) -> str:
    return build_bq_table_ref(PROJECT_ID, SILVER_DATASET, silver_table)


def build_report_landing_uri(source_subset: str) -> str:
    template = ORCHESTRATION_CONFIG["datasets"]["pronabec_reports"]["landing_path_template"]
    return f"gs://{BUCKET_NAME}/{render_gcs_path(template, source_subset=source_subset)}"


def build_report_bronze_uri(dataset: str) -> str:
    template = ORCHESTRATION_CONFIG["datasets"]["pronabec_reports"]["bronze_path_template"]
    return f"gs://{BUCKET_NAME}/{render_gcs_path(template, dataset=dataset, extraction_date=EXTRACTION_DATE)}"


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": ORCHESTRATION_CONFIG["dag"]["retries"],
    "retry_delay": timedelta(minutes=ORCHESTRATION_CONFIG["dag"]["retry_delay_minutes"]),
    "execution_timeout": timedelta(hours=2),
}


with DAG(
    dag_id=ORCHESTRATION_CONFIG["dag"]["id"],
    description="Orquesta el pipeline batch medallion de PRONABEC usando configuración declarativa.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=ORCHESTRATION_CONFIG["dag"]["schedule"],
    catchup=False,
    max_active_runs=ORCHESTRATION_CONFIG["dag"]["max_active_runs"],
    tags=["pronabec", "medallion", "batch", "cloud-run", "dataflow", "composer"],
    params={
        "extraction_date": Param(
            default="",
            type="string",
            description="Fecha lógica de extracción. Si se omite, usa la fecha de ejecución del DAG.",
        ),
        "run_pronabec": Param(default=True, type="boolean"),
        "run_mef": Param(default=True, type="boolean"),
        "run_pronabec_reports_staging": Param(default=True, type="boolean"),
        "run_bronze_manifest_validation": Param(default=True, type="boolean"),
        "run_dataflow_pronabec": Param(default=True, type="boolean"),
        "run_dataflow_mef": Param(default=True, type="boolean"),
        "run_dataflow_reports": Param(default=True, type="boolean"),
        "run_gold_publish": Param(default=True, type="boolean"),
        "run_gold_validation": Param(default=True, type="boolean"),
        "run_quality": Param(default=True, type="boolean"),
    },
) as dag:
    init_run = EmptyOperator(task_id="init_run")

    extract_pronabec_api = BashOperator(
        task_id="extract_pronabec_api",
        bash_command=cloud_run_execute_command(
            job_name=PRONABEC_EXTRACT_JOB,
            enabled_expression=RUN_PRONABEC,
        ),
    )

    extract_mef = BashOperator(
        task_id="extract_mef",
        bash_command=cloud_run_execute_command(
            job_name=MEF_EXTRACT_JOB,
            enabled_expression=RUN_MEF,
        ),
    )

    report_stage_tasks = []
    for group in REPORT_GROUPS:
        source_subset = group["source_subset"]
        report_stage_tasks.append(
            BashOperator(
                task_id=f"stage_pronabec_reports_{source_subset}",
                bash_command=cloud_run_execute_command(
                    job_name=PRONABEC_REPORTS_STAGE_JOB,
                    enabled_expression=RUN_PRONABEC_REPORTS_STAGING,
                    extra_env_vars={
                        "SOURCE_SUBSET": source_subset,
                    },
                ),
            )
        )

    validate_bronze_manifests = BashOperator(
        task_id="validate_bronze_manifests",
        bash_command=cloud_run_execute_command(
            job_name=BRONZE_MANIFEST_VALIDATION_JOB,
            enabled_expression=RUN_BRONZE_MANIFEST_VALIDATION,
        ),
    )

    pronabec_api_tasks = []
    for item in PRONABEC_API_ITEMS:
        source_dataset = item["source_dataset"]
        job_name = resolve_job_name(item, f"dataflow-pronabec-{source_dataset.replace('_', '-')}-job")
        pronabec_api_tasks.append(
            BashOperator(
                task_id=f"bronze_to_silver_pronabec_api_{source_dataset}",
                bash_command=cloud_run_execute_command(
                    job_name=job_name,
                    enabled_expression=RUN_DATAFLOW_PRONABEC,
                    extra_env_vars={
                        "SOURCE_SYSTEM": "pronabec",
                        "SOURCE_DATASET": source_dataset,
                        "INPUT_FORMAT": "jsonl",
                        "INPUT_PATH": build_api_input_path(source_dataset),
                        "OUTPUT_TABLE": build_api_output_table(source_dataset),
                    },
                ),
            )
        )

    mef_tasks = []
    for item in MEF_ITEMS:
        source_dataset = item["source_dataset"]
        input_path_template = ORCHESTRATION_CONFIG["datasets"]["mef"]["bronze_path_template"]
        input_path = f"gs://{BUCKET_NAME}/{render_gcs_path(input_path_template, dataset=source_dataset, extraction_date=EXTRACTION_DATE)}"
        job_name = resolve_job_name(item, f"dataflow-mef-{source_dataset.replace('_', '-')}-job")
        mef_tasks.append(
            BashOperator(
                task_id=f"bronze_to_silver_mef_{source_dataset}",
                bash_command=cloud_run_execute_command(
                    job_name=job_name,
                    enabled_expression=RUN_DATAFLOW_MEF,
                    extra_env_vars={
                        "SOURCE_SYSTEM": "mef",
                        "SOURCE_DATASET": source_dataset,
                        "INPUT_FORMAT": "csv",
                        "INPUT_PATH": input_path,
                        "OUTPUT_TABLE": build_mef_output_table(item["silver_table"]),
                    },
                ),
            )
        )

    report_tasks = []
    for item in REPORT_DATASETS:
        source_dataset = item["source_dataset"]
        report_tasks.append(
            BashOperator(
                task_id=f"bronze_to_silver_pronabec_reports_{source_dataset}",
                bash_command=cloud_run_execute_command(
                    job_name=DATAFLOW_PRONABEC_REPORT_JOB,
                    enabled_expression=RUN_DATAFLOW_REPORTS,
                    extra_env_vars={
                        "SOURCE_SYSTEM": "pronabec_reports",
                        "SOURCE_DATASET": source_dataset,
                        "INPUT_FORMAT": "csv",
                        "INPUT_PATH": build_report_bronze_uri(source_dataset),
                        "OUTPUT_TABLE": build_bq_table_ref(
                            PROJECT_ID,
                            SILVER_DATASET,
                            f"pronabec_{source_dataset}",
                        ),
                    },
                ),
            )
        )

    publish_gold_views = BashOperator(
        task_id="publish_gold_views",
        bash_command=cloud_run_execute_command(
            job_name=GOLD_PUBLISH_JOB,
            enabled_expression=RUN_GOLD_PUBLISH,
        ),
    )

    validate_gold_contracts = BashOperator(
        task_id="validate_gold_contracts",
        bash_command=cloud_run_execute_command(
            job_name=GOLD_VALIDATE_JOB,
            enabled_expression=RUN_GOLD_VALIDATION,
        ),
    )

    run_quality_checks = BashOperator(
        task_id="run_quality_checks",
        bash_command=cloud_run_execute_command(
            job_name=QUALITY_CHECKS_JOB,
            enabled_expression=RUN_QUALITY,
        ),
    )

    finish_run = EmptyOperator(task_id="finish_run")

    init_run >> extract_pronabec_api >> extract_mef
    extract_pronabec_api >> report_stage_tasks
    extract_mef >> report_stage_tasks

    extract_pronabec_api >> validate_bronze_manifests
    extract_mef >> validate_bronze_manifests
    for stage_task in report_stage_tasks:
        stage_task >> validate_bronze_manifests

    validate_bronze_manifests >> pronabec_api_tasks
    validate_bronze_manifests >> mef_tasks
    validate_bronze_manifests >> report_tasks

    all_silver_tasks = pronabec_api_tasks + mef_tasks + report_tasks
    all_silver_tasks >> publish_gold_views >> validate_gold_contracts >> run_quality_checks >> finish_run
