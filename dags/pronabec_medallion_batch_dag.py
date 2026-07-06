from __future__ import annotations

import time
from datetime import datetime, timedelta
from pathlib import Path

import google.auth
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from google.auth.transport.requests import AuthorizedSession

from pipelines.common.orchestration_config import (
    build_bq_table_ref,
    get_pronabec_dataset_policies,
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
PRONABEC_EXTRACTION_POLICIES = [
    policy
    for policy in get_pronabec_dataset_policies(ORCHESTRATION_CONFIG)
    if policy.bronze_enabled
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
DATAFLOW_SDK_CONTAINER_IMAGE = airflow_var_template(
    resolve_airflow_var_name(ORCHESTRATION_CONFIG, "dataflow_sdk_container_image_var")
)

PRONABEC_DISCOVERY_JOB = airflow_var_template(
    resolve_airflow_var_name(ORCHESTRATION_CONFIG, "pronabec_discovery_job_name_var"),
    "pronabec-discovery-job",
)
PRONABEC_BUILD_PLAN_JOB = airflow_var_template(
    resolve_airflow_var_name(ORCHESTRATION_CONFIG, "pronabec_build_plan_job_name_var"),
    "pronabec-build-plan-job",
)
PRONABEC_RUN_PLAN_JOB = airflow_var_template(
    resolve_airflow_var_name(ORCHESTRATION_CONFIG, "pronabec_run_plan_job_name_var"),
    "pronabec-run-plan-job",
)
PRONABEC_FINALIZE_DATASET_JOB = airflow_var_template(
    resolve_airflow_var_name(ORCHESTRATION_CONFIG, "pronabec_finalize_dataset_job_name_var"),
    "pronabec-finalize-dataset-job",
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
PIPELINE_RUN_ID = "{{ dag_run.conf.get('pipeline_run_id', run_id) }}"
RUN_PRONABEC = "{{ dag_run.conf.get('run_pronabec', true) }}"
RUN_PRONABEC_DISCOVERY = "{{ dag_run.conf.get('run_pronabec', true) and dag_run.conf.get('run_pronabec_discovery', true) }}"
RUN_PRONABEC_BUILD_PLAN = "{{ dag_run.conf.get('run_pronabec', true) and dag_run.conf.get('run_pronabec_build_plan', true) }}"
RUN_PRONABEC_PLAN_EXECUTION = "{{ dag_run.conf.get('run_pronabec', true) and dag_run.conf.get('run_pronabec_plan_execution', true) }}"
RUN_PRONABEC_FINALIZE = "{{ dag_run.conf.get('run_pronabec', true) and dag_run.conf.get('run_pronabec_finalize', true) }}"
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
    raise NotImplementedError(
        "Cloud Run Jobs must be created with cloud_run_job_operator and "
        "run_cloud_run_job_with_polling."
    )


def _is_enabled(value: bool | str) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _authorized_session() -> AuthorizedSession:
    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    return AuthorizedSession(credentials)


def _cloud_run_job_url(project_id: str, region: str, job_name: str) -> str:
    return f"https://run.googleapis.com/v2/projects/{project_id}/locations/{region}/jobs/{job_name}:run"


def _cloud_run_operation_url(operation_name: str) -> str:
    return f"https://run.googleapis.com/v2/{operation_name}"


def _request_json(method: str, session: AuthorizedSession, url: str, **kwargs) -> dict:
    response = getattr(session, method)(url, **kwargs)
    if response.status_code >= 400:
        try:
            payload = response.json()
        except ValueError:
            payload = response.text
        raise RuntimeError(f"Cloud Run API request failed method={method} url={url} payload={payload!r}")
    if not response.text:
        return {}
    return response.json()


def _env_overrides(env_vars: dict[str, str]) -> dict:
    return {
        "overrides": {
            "containerOverrides": [
                {
                    "env": [
                        {"name": key, "value": value}
                        for key, value in env_vars.items()
                    ]
                }
            ]
        }
    }


def _extract_execution_name(operation: dict) -> str | None:
    response = operation.get("response") or {}
    metadata = operation.get("metadata") or {}
    for candidate in (
        response.get("name"),
        response.get("execution"),
        response.get("executionName"),
        metadata.get("target"),
    ):
        if isinstance(candidate, str) and "/executions/" in candidate:
            return candidate.split("/")[-1]
        if isinstance(candidate, str) and candidate:
            return candidate.split("/")[-1]
    return None


def _raise_operation_error(operation: dict, job_name: str) -> None:
    error = operation.get("error")
    if not error:
        return
    raise RuntimeError(
        "Cloud Run operation failed "
        f"job={job_name} code={error.get('code')} "
        f"message={error.get('message')} details={error.get('details')}"
    )


def run_cloud_run_job_with_polling(
    job_name: str,
    project_id: str,
    region: str,
    env_vars: dict[str, str],
    enabled: bool | str = True,
    poll_interval_seconds: int = 30,
    timeout_seconds: int | None = None,
) -> None:
    if not _is_enabled(enabled):
        print(f"Cloud Run job disabled by DAG configuration: {job_name}")
        return

    joined_env_vars = ",".join(f"{key}={value}" for key, value in env_vars.items())
    print(f"Launching Cloud Run job through REST API. job={job_name} project={project_id} region={region}")
    print(f"Cloud Run env vars: {joined_env_vars}")

    session = _authorized_session()
    operation = _request_json(
        "post",
        session,
        _cloud_run_job_url(project_id, region, job_name),
        json=_env_overrides(env_vars),
        timeout=60,
    )
    operation_name = operation.get("name")
    if not operation_name:
        raise RuntimeError(f"Cloud Run run API did not return operation name. response={operation!r}")
    print(f"Cloud Run operation: {operation_name}")

    started_at = time.monotonic()
    while True:
        elapsed = int(time.monotonic() - started_at)
        if timeout_seconds is not None and elapsed > timeout_seconds:
            raise TimeoutError(
                f"Timed out waiting for Cloud Run operation={operation_name} job={job_name} "
                f"after {elapsed} seconds."
            )

        operation = _request_json(
            "get",
            session,
            _cloud_run_operation_url(operation_name),
            timeout=60,
        )
        print(
            f"Cloud Run operation={operation_name} job={job_name} "
            f"elapsed={elapsed} done={operation.get('done', False)}"
        )

        if operation.get("done"):
            _raise_operation_error(operation, job_name)
            print("Cloud Run operation completed successfully.")
            execution_name = _extract_execution_name(operation)
            if execution_name:
                print(f"Cloud Run execution: {execution_name}")
            return

        time.sleep(poll_interval_seconds)


def cloud_run_job_operator(
    task_id: str,
    job_name: str,
    enabled_expression: str,
    timeout_seconds: int,
    extra_env_vars: dict[str, str] | None = None,
) -> PythonOperator:
    env_vars = {
        "BRONZE_EXTRACTION_DATE": EXTRACTION_DATE,
        "PIPELINE_RUN_ID": PIPELINE_RUN_ID,
    }
    if extra_env_vars:
        env_vars.update(extra_env_vars)

    return PythonOperator(
        task_id=task_id,
        python_callable=run_cloud_run_job_with_polling,
        op_kwargs={
            "job_name": job_name,
            "project_id": PROJECT_ID,
            "region": REGION,
            "env_vars": env_vars,
            "enabled": enabled_expression,
            "timeout_seconds": timeout_seconds,
        },
        retries=0,
        execution_timeout=timedelta(seconds=timeout_seconds + 600),
    )


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
}


with DAG(
    dag_id=ORCHESTRATION_CONFIG["dag"]["id"],
    description="Orquesta el pipeline batch medallion de PRONABEC usando configuración declarativa.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=ORCHESTRATION_CONFIG["dag"]["max_active_runs"],
    max_active_tasks=8,
    tags=["pronabec", "medallion", "batch", "cloud-run", "dataflow", "composer"],
    params={
        "extraction_date": Param(
            default="",
            type="string",
            description="Fecha lógica de extracción. Si se omite, usa la fecha de ejecución del DAG.",
        ),
        "run_pronabec": Param(default=True, type="boolean"),
        "run_pronabec_discovery": Param(default=True, type="boolean"),
        "run_pronabec_build_plan": Param(default=True, type="boolean"),
        "run_pronabec_plan_execution": Param(default=True, type="boolean"),
        "run_pronabec_finalize": Param(default=True, type="boolean"),
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

    with TaskGroup(group_id="pronabec_api_bronze") as pronabec_api_bronze:
        discover_pronabec_datasets = cloud_run_job_operator(
            task_id="discover_pronabec_datasets",
            job_name=PRONABEC_DISCOVERY_JOB,
            enabled_expression=RUN_PRONABEC_DISCOVERY,
            timeout_seconds=10800,
        )

        build_pronabec_extraction_plan = cloud_run_job_operator(
            task_id="build_pronabec_extraction_plan",
            job_name=PRONABEC_BUILD_PLAN_JOB,
            enabled_expression=RUN_PRONABEC_BUILD_PLAN,
            timeout_seconds=3600,
        )

        run_pronabec_extraction_plan = cloud_run_job_operator(
            task_id="run_pronabec_extraction_plan",
            job_name=PRONABEC_RUN_PLAN_JOB,
            enabled_expression=RUN_PRONABEC_PLAN_EXECUTION,
            timeout_seconds=14400,
        )

        pronabec_finalize_tasks = []
        for policy in PRONABEC_EXTRACTION_POLICIES:
            source_dataset = policy.source_dataset
            pronabec_finalize_tasks.append(
                cloud_run_job_operator(
                    task_id=f"finalize_pronabec_{source_dataset}",
                    job_name=PRONABEC_FINALIZE_DATASET_JOB,
                    enabled_expression=RUN_PRONABEC_FINALIZE,
                    timeout_seconds=3600,
                    extra_env_vars={
                        "SOURCE_DATASET": source_dataset,
                    },
                )
            )

        discover_pronabec_datasets >> build_pronabec_extraction_plan >> run_pronabec_extraction_plan
        run_pronabec_extraction_plan >> pronabec_finalize_tasks

    with TaskGroup(group_id="mef_bronze") as mef_bronze:
        extract_mef = cloud_run_job_operator(
            task_id="extract_mef",
            job_name=MEF_EXTRACT_JOB,
            enabled_expression=RUN_MEF,
            timeout_seconds=7200,
        )

    with TaskGroup(group_id="pronabec_reports_bronze") as pronabec_reports_bronze:
        report_stage_tasks = []
        for group in REPORT_GROUPS:
            source_subset = group["source_subset"]
            report_stage_tasks.append(
                cloud_run_job_operator(
                    task_id=f"stage_pronabec_reports_{source_subset}",
                    job_name=PRONABEC_REPORTS_STAGE_JOB,
                    enabled_expression=RUN_PRONABEC_REPORTS_STAGING,
                    timeout_seconds=3600,
                    extra_env_vars={
                        "SOURCE_SUBSET": source_subset,
                    },
                )
            )

    validate_bronze_manifests = cloud_run_job_operator(
        task_id="validate_bronze_manifests",
        job_name=BRONZE_MANIFEST_VALIDATION_JOB,
        enabled_expression=RUN_BRONZE_MANIFEST_VALIDATION,
        timeout_seconds=3600,
    )

    with TaskGroup(group_id="pronabec_api_silver") as pronabec_api_silver:
        pronabec_api_tasks = []
        for item in PRONABEC_API_ITEMS:
            source_dataset = item["source_dataset"]
            job_name = resolve_job_name(item, f"dataflow-pronabec-{source_dataset.replace('_', '-')}-job")
            pronabec_api_tasks.append(
                cloud_run_job_operator(
                    task_id=f"bronze_to_silver_pronabec_api_{source_dataset}",
                    job_name=job_name,
                    enabled_expression=RUN_DATAFLOW_PRONABEC,
                    timeout_seconds=7200,
                    extra_env_vars={
                        "SOURCE_SYSTEM": "pronabec",
                        "SOURCE_DATASET": source_dataset,
                        "INPUT_FORMAT": "jsonl",
                        "INPUT_PATH": build_api_input_path(source_dataset),
                        "OUTPUT_TABLE": build_api_output_table(source_dataset),
                    },
                )
            )

    with TaskGroup(group_id="mef_silver") as mef_silver:
        mef_tasks = []
        for item in MEF_ITEMS:
            source_dataset = item["source_dataset"]
            input_path_template = ORCHESTRATION_CONFIG["datasets"]["mef"]["bronze_path_template"]
            input_path = f"gs://{BUCKET_NAME}/{render_gcs_path(input_path_template, dataset=source_dataset, extraction_date=EXTRACTION_DATE)}"
            job_name = resolve_job_name(item, f"dataflow-mef-{source_dataset.replace('_', '-')}-job")
            mef_tasks.append(
                cloud_run_job_operator(
                    task_id=f"bronze_to_silver_mef_{source_dataset}",
                    job_name=job_name,
                    enabled_expression=RUN_DATAFLOW_MEF,
                    timeout_seconds=7200,
                    extra_env_vars={
                        "SOURCE_SYSTEM": "mef",
                        "SOURCE_DATASET": source_dataset,
                        "INPUT_FORMAT": "csv",
                        "INPUT_PATH": input_path,
                        "OUTPUT_TABLE": build_mef_output_table(item["silver_table"]),
                    },
                )
            )

    with TaskGroup(group_id="pronabec_reports_silver") as pronabec_reports_silver:
        report_tasks = []
        for item in REPORT_DATASETS:
            source_dataset = item["source_dataset"]
            report_tasks.append(
                cloud_run_job_operator(
                    task_id=f"bronze_to_silver_pronabec_reports_{source_dataset}",
                    job_name=DATAFLOW_PRONABEC_REPORT_JOB,
                    enabled_expression=RUN_DATAFLOW_REPORTS,
                    timeout_seconds=7200,
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
                        "DATAFLOW_SDK_CONTAINER_IMAGE": DATAFLOW_SDK_CONTAINER_IMAGE,
                    },
                )
            )

    publish_gold_views = cloud_run_job_operator(
        task_id="publish_gold_views",
        job_name=GOLD_PUBLISH_JOB,
        enabled_expression=RUN_GOLD_PUBLISH,
        timeout_seconds=3600,
    )

    validate_gold_contracts = cloud_run_job_operator(
        task_id="validate_gold_contracts",
        job_name=GOLD_VALIDATE_JOB,
        enabled_expression=RUN_GOLD_VALIDATION,
        timeout_seconds=3600,
    )

    run_quality_checks = cloud_run_job_operator(
        task_id="run_quality_checks",
        job_name=QUALITY_CHECKS_JOB,
        enabled_expression=RUN_QUALITY,
        timeout_seconds=3600,
    )

    finish_run = EmptyOperator(task_id="finish_run")

    bronze_parallel = [pronabec_api_bronze, mef_bronze, pronabec_reports_bronze]
    init_run >> bronze_parallel
    bronze_parallel >> validate_bronze_manifests

    silver_parallel = [pronabec_api_silver, mef_silver, pronabec_reports_silver]
    validate_bronze_manifests >> silver_parallel
    silver_parallel >> publish_gold_views
    publish_gold_views >> validate_gold_contracts >> run_quality_checks >> finish_run
