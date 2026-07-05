from __future__ import annotations

import json
import re
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

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


def _run_gcloud(command: list[str], timeout_seconds: int = 120) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
    )
    if result.returncode != 0:
        print("gcloud command failed:", " ".join(command))
        if result.stdout:
            print("stdout:")
            print(result.stdout)
        if result.stderr:
            print("stderr:")
            print(result.stderr)
        raise RuntimeError(f"gcloud command failed with return code {result.returncode}")
    return result


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _execution_name_from_text(text: str, job_name: str) -> str | None:
    execution_path_match = re.search(rf"(?:executions/)?({re.escape(job_name)}-[A-Za-z0-9-]+)", text)
    if execution_path_match:
        return execution_path_match.group(1)
    return None


def _find_execution_name_in_json(value: object, job_name: str) -> str | None:
    if isinstance(value, dict):
        for key in ("name", "execution", "executionName"):
            candidate = value.get(key)
            if isinstance(candidate, str):
                execution_name = _execution_name_from_text(candidate, job_name)
                if execution_name:
                    return execution_name
        for nested in value.values():
            execution_name = _find_execution_name_in_json(nested, job_name)
            if execution_name:
                return execution_name
    elif isinstance(value, list):
        for nested in value:
            execution_name = _find_execution_name_in_json(nested, job_name)
            if execution_name:
                return execution_name
    elif isinstance(value, str):
        return _execution_name_from_text(value, job_name)
    return None


def _list_recent_executions_for_job(job_name: str, project_id: str, region: str) -> list[dict]:
    result = _run_gcloud(
        [
            "gcloud",
            "run",
            "jobs",
            "executions",
            "list",
            "--job",
            job_name,
            "--project",
            project_id,
            "--region",
            region,
            "--format=json",
            "--limit",
            "10",
        ],
        timeout_seconds=60,
    )
    if not result.stdout.strip():
        return []
    return json.loads(result.stdout)


def _execution_creation_timestamp(execution: dict) -> datetime | None:
    metadata = execution.get("metadata", {})
    return _parse_timestamp(
        metadata.get("creationTimestamp")
        or execution.get("createTime")
        or execution.get("creationTimestamp")
    )


def _execution_name(execution: dict) -> str | None:
    metadata = execution.get("metadata", {})
    candidate = metadata.get("name") or execution.get("name")
    if isinstance(candidate, str):
        return candidate.split("/")[-1]
    return None


def _resolve_execution_name_from_launch(
    job_name: str,
    project_id: str,
    region: str,
    launch_started_at: datetime,
    stdout: str,
    stderr: str,
) -> str:
    for payload in (stdout, stderr):
        if payload.strip():
            try:
                parsed = json.loads(payload)
            except json.JSONDecodeError:
                parsed = None
            if parsed is not None:
                execution_name = _find_execution_name_in_json(parsed, job_name)
                if execution_name:
                    return execution_name

            execution_name = _execution_name_from_text(payload, job_name)
            if execution_name:
                return execution_name

    tolerance_start = launch_started_at - timedelta(minutes=2)
    candidates = _list_recent_executions_for_job(job_name, project_id, region)
    recent_candidates = []
    for execution in candidates:
        created_at = _execution_creation_timestamp(execution)
        execution_name = _execution_name(execution)
        if execution_name and (created_at is None or created_at >= tolerance_start):
            recent_candidates.append((created_at or datetime.min.replace(tzinfo=timezone.utc), execution_name))

    if recent_candidates:
        recent_candidates.sort(key=lambda item: item[0], reverse=True)
        return recent_candidates[0][1]

    raise RuntimeError(
        "Could not resolve Cloud Run execution name after async launch. "
        f"job={job_name} stdout={stdout!r} stderr={stderr!r} candidates={candidates!r}"
    )


def _condition_summary(execution: dict) -> str:
    conditions = execution.get("status", {}).get("conditions") or execution.get("conditions") or []
    summary = []
    for condition in conditions:
        condition_type = condition.get("type")
        status = condition.get("status")
        reason = condition.get("reason")
        if condition_type:
            value = f"{condition_type}={status}"
            if reason:
                value = f"{value}:{reason}"
            summary.append(value)
    return ";".join(summary) or "none"


def _condition_is_true(execution: dict, *condition_types: str) -> bool:
    expected = {condition_type.lower() for condition_type in condition_types}
    conditions = execution.get("status", {}).get("conditions") or execution.get("conditions") or []
    return any(
        str(condition.get("type", "")).lower() in expected
        and str(condition.get("status", "")).lower() == "true"
        for condition in conditions
    )


def _execution_counts(execution: dict) -> tuple[int, int, int, int]:
    status = execution.get("status", {})
    task_count = int(status.get("taskCount") or execution.get("taskCount") or 0)
    running_count = int(status.get("runningCount") or execution.get("runningCount") or 0)
    succeeded_count = int(status.get("succeededCount") or execution.get("succeededCount") or 0)
    failed_count = int(status.get("failedCount") or execution.get("failedCount") or 0)
    return task_count, running_count, succeeded_count, failed_count


def _execution_succeeded(execution: dict) -> bool:
    task_count, _, succeeded_count, failed_count = _execution_counts(execution)
    if failed_count > 0:
        return False
    if task_count > 0 and succeeded_count >= task_count:
        return True
    return _condition_is_true(execution, "Completed", "Succeeded")


def _execution_failed(execution: dict) -> bool:
    _, _, _, failed_count = _execution_counts(execution)
    return failed_count > 0 or _condition_is_true(execution, "Failed", "Cancelled")


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
    print(f"Launching Cloud Run job asynchronously job={job_name} project={project_id} region={region}")
    print(f"Cloud Run env vars: {joined_env_vars}")

    launch_started_at = datetime.now(timezone.utc)
    execute_result = _run_gcloud(
        [
            "gcloud",
            "run",
            "jobs",
            "execute",
            job_name,
            "--project",
            project_id,
            "--region",
            region,
            "--update-env-vars",
            joined_env_vars,
            "--async",
            "--format=json",
        ],
        timeout_seconds=60,
    )
    print("Cloud Run launch command completed.")
    print(f"stdout: {execute_result.stdout}")
    print(f"stderr: {execute_result.stderr}")

    execution_name = _resolve_execution_name_from_launch(
        job_name=job_name,
        project_id=project_id,
        region=region,
        launch_started_at=launch_started_at,
        stdout=execute_result.stdout,
        stderr=execute_result.stderr,
    )
    print(f"Resolved Cloud Run execution: {execution_name}")
    print(f"Polling Cloud Run execution: {execution_name}")
    started_at = time.monotonic()
    while True:
        elapsed = int(time.monotonic() - started_at)
        if timeout_seconds is not None and elapsed > timeout_seconds:
            raise TimeoutError(
                f"Timed out waiting for Cloud Run execution={execution_name} job={job_name} "
                f"after {elapsed} seconds."
            )

        describe_result = _run_gcloud(
            [
                "gcloud",
                "run",
                "jobs",
                "executions",
                "describe",
                execution_name,
                "--project",
                project_id,
                "--region",
                region,
                "--format=json",
            ],
            timeout_seconds=60,
        )
        execution = json.loads(describe_result.stdout)
        task_count, running_count, succeeded_count, failed_count = _execution_counts(execution)
        condition_summary = _condition_summary(execution)
        print(
            "Cloud Run execution="
            f"{execution_name} job={job_name} elapsed={elapsed} "
            f"running={running_count} succeeded={succeeded_count} failed={failed_count} "
            f"taskCount={task_count} condition={condition_summary}"
        )

        if _execution_succeeded(execution):
            print(f"Cloud Run execution succeeded: {execution_name}")
            return
        if _execution_failed(execution):
            raise RuntimeError(
                f"Cloud Run execution failed: execution={execution_name} "
                f"job={job_name} condition={condition_summary}"
            )

        print(f"Polling Cloud Run execution... execution={execution_name}")
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
    "execution_timeout": timedelta(hours=2),
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
