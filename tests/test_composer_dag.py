"""Pruebas unitarias para validar la configuracion del DAG de Composer."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock


sys.modules["airflow"] = MagicMock()
sys.modules["airflow.models"] = MagicMock()
sys.modules["airflow.models.param"] = MagicMock()
sys.modules["airflow.operators"] = MagicMock()
sys.modules["airflow.operators.bash"] = MagicMock()
sys.modules["airflow.operators.empty"] = MagicMock()
sys.modules["airflow.operators.python"] = MagicMock()
sys.modules["airflow.utils"] = MagicMock()
sys.modules["airflow.utils.task_group"] = MagicMock()

import dags.pronabec_medallion_batch_dag as dag_mod  # noqa: E402


def _read_dag_source() -> str:
    return Path(dag_mod.__file__).read_text(encoding="utf-8")


def test_dag_uses_declarative_configuration() -> None:
    content = _read_dag_source()

    assert dag_mod.ORCHESTRATION_CONFIG["dag"]["id"] == "pronabec_medallion_batch"
    assert dag_mod.ORCHESTRATION_CONFIG["datasets"]["pronabec_reports"]["landing_path_template"] == "landing/pronabec_reports/{source_subset}"
    assert dag_mod.ORCHESTRATION_CONFIG["datasets"]["pronabec_reports"]["bronze_path_template"] == "bronze/pronabec_reports/{dataset}/extraction_date={extraction_date}/data.csv"
    assert "resolve_repo_root" in content
    assert 'candidate / "config" / "orchestration.yaml"' in content
    assert 'candidate / "config" / "endpoints.yaml"' in content
    assert "REPORT_SUBSETS" not in content
    assert "PRONABEC_SILVER_DATASETS" not in content
    assert "MEF_SILVER_DATASETS" not in content
    assert "PRONABEC_REPORT_SILVER_DATASETS" not in content


def test_pronabec_api_and_mef_items_are_declarative() -> None:
    pronabec_datasets = [item["source_dataset"] for item in dag_mod.PRONABEC_API_ITEMS]
    mef_datasets = [item["source_dataset"] for item in dag_mod.MEF_ITEMS]

    assert pronabec_datasets == [
        "convocatorias",
        "ubigeo_postulacion",
        "becarios_pais_estudio",
        "colegios_habiles",
        "becarios_provincia",
    ]
    assert mef_datasets == [
        "presupuesto",
        "presupuesto_temporal",
        "presupuesto_producto",
        "presupuesto_producto_temporal",
        "presupuesto_actividad",
        "presupuesto_actividad_temporal",
        "presupuesto_generica",
        "presupuesto_generica_temporal",
        "presupuesto_hierarchy",
    ]

    assert "convocatorias_carrera_sede" not in pronabec_datasets
    assert "presupuesto_departamento" not in mef_datasets


def test_report_groups_are_resolved_from_endpoints() -> None:
    subsets = [group["source_subset"] for group in dag_mod.REPORT_GROUPS]
    datasets = [item["source_dataset"] for item in dag_mod.REPORT_DATASETS]

    assert subsets == [
        "pes_2025",
        "beca18_universitarios_2012_2026",
    ]
    assert len(datasets) == 23
    assert "report_beca18_becas_otorgadas_modalidad_anual" in datasets
    assert "report_beca18_universitarios_carrera_anual" in datasets


def test_reports_path_templates_are_correct() -> None:
    assert dag_mod.build_report_landing_uri("pes_2025") == "gs://{{ var.value.gcs_bucket_name }}/landing/pronabec_reports/pes_2025"
    assert (
        dag_mod.build_report_bronze_uri("report_beca18_becas_otorgadas_modalidad_anual")
        == "gs://{{ var.value.gcs_bucket_name }}/bronze/pronabec_reports/report_beca18_becas_otorgadas_modalidad_anual/extraction_date={{ dag_run.conf.get('extraction_date') or ds }}/data.csv"
    )


def test_dag_contains_gold_publication_and_validation_tasks() -> None:
    content = _read_dag_source()

    assert "publish_gold_views" in content
    assert "validate_gold_contracts" in content
    assert "run_quality_checks" in content
    assert "publish_gold_views >> validate_gold_contracts >> run_quality_checks" in content


def test_dag_does_not_route_reports_through_source_subset_in_bronze() -> None:
    assert dag_mod.build_report_bronze_uri("report_beca18_becas_otorgadas_modalidad_anual").startswith(
        "gs://{{ var.value.gcs_bucket_name }}/bronze/pronabec_reports/report_beca18_becas_otorgadas_modalidad_anual/extraction_date="
    )
    assert "{source_subset}" not in dag_mod.build_report_bronze_uri("report_beca18_becas_otorgadas_modalidad_anual")
    assert "extraction_date" not in dag_mod.build_report_landing_uri("pes_2025")


def test_dag_uses_non_empty_extraction_date_fallback() -> None:
    content = _read_dag_source()

    assert "dag_run.conf.get('extraction_date') or ds" in content
    assert "dag_run.conf.get('extraction_date', ds)" not in content


def test_dag_is_manual_only_without_catchup() -> None:
    content = _read_dag_source()

    assert "schedule_interval=None" in content
    assert "catchup=False" in content
    assert "max_active_tasks=8" in content
    assert 'max_active_runs=ORCHESTRATION_CONFIG["dag"]["max_active_runs"]' in content


def test_composer_upload_script_syncs_support_files() -> None:
    upload_script = Path(dag_mod.__file__).resolve().parents[1] / "scripts" / "upload_composer_dag.ps1"
    content = upload_script.read_text(encoding="utf-8")

    assert "Sync-ComposerSupportFiles" in content
    assert "git ls-files config pipelines" in content


def test_dag_contains_bronze_manifest_validation_gate() -> None:
    content = _read_dag_source()

    assert "BRONZE_MANIFEST_VALIDATION_JOB" in content
    assert "bronze-manifest-validation-job" in content
    assert "validate_bronze_manifests" in content
    assert "run_bronze_manifest_validation" in content
    assert "RUN_BRONZE_MANIFEST_VALIDATION" in content


def test_bronze_manifest_validation_runs_after_bronze_tasks() -> None:
    content = _read_dag_source()

    assert 'TaskGroup(group_id="pronabec_api_bronze")' in content
    assert 'TaskGroup(group_id="mef_bronze")' in content
    assert 'TaskGroup(group_id="pronabec_reports_bronze")' in content
    assert "discover_pronabec_datasets >> build_pronabec_extraction_plan >> run_pronabec_extraction_plan" in content
    assert "run_pronabec_extraction_plan >> pronabec_finalize_tasks" in content
    assert "chain_tasks(pronabec_finalize_tasks)" in content
    assert "chain_tasks(report_stage_tasks)" in content
    assert "bronze_parallel = [pronabec_api_bronze, mef_bronze, pronabec_reports_bronze]" in content
    assert "init_run >> bronze_parallel" in content
    assert "bronze_parallel >> validate_bronze_manifests" in content
    assert "extract_mef >> validate_bronze_manifests" not in content
    assert "stage_task >> validate_bronze_manifests" not in content


def test_bronze_manifest_validation_runs_before_silver_tasks() -> None:
    content = _read_dag_source()

    assert 'TaskGroup(group_id="pronabec_api_silver")' in content
    assert 'TaskGroup(group_id="mef_silver")' in content
    assert 'TaskGroup(group_id="pronabec_reports_silver")' in content
    assert "silver_parallel = [pronabec_api_silver, mef_silver, pronabec_reports_silver]" in content
    assert "validate_bronze_manifests >> silver_parallel" in content
    assert "silver_parallel >> publish_gold_views" in content


def test_dag_exposes_bronze_manifest_validation_param() -> None:
    content = _read_dag_source()

    assert '"run_bronze_manifest_validation": Param(default=True, type="boolean")' in content


def test_dag_contains_plan_driven_pronabec_job_refs() -> None:
    content = _read_dag_source()

    assert "PRONABEC_DISCOVERY_JOB" in content
    assert "PRONABEC_BUILD_PLAN_JOB" in content
    assert "PRONABEC_RUN_PLAN_JOB" in content
    assert "PRONABEC_FINALIZE_DATASET_JOB" in content
    assert "pronabec-discovery-job" in content
    assert "pronabec-build-plan-job" in content
    assert "pronabec-run-plan-job" in content
    assert "pronabec-finalize-dataset-job" in content


def test_dag_contains_chunked_pronabec_task_ids() -> None:
    content = _read_dag_source()

    assert "discover_pronabec_datasets" in content
    assert "build_pronabec_extraction_plan" in content
    assert "run_pronabec_extraction_plan" in content
    assert "finalize_pronabec_" in content


def test_dag_passes_chunk_runtime_env_vars() -> None:
    content = _read_dag_source()

    assert "BRONZE_EXTRACTION_DATE" in content
    assert "dag_run.conf.get('pipeline_run_id', run_id)" in content
    assert "PIPELINE_RUN_ID={{ run_id }}" not in content
    assert '"SOURCE_DATASET": source_dataset' in content
    assert '"PAGE_START": str(page_start)' not in content
    assert '"PAGE_END": str(page_end)' not in content
    assert '"OUTPUT_MODE": "chunk"' not in content


def test_dag_uses_pronabec_flags_for_chunked_tasks() -> None:
    content = _read_dag_source()

    assert "PRONABEC_EXTRACTION_SCOPE" not in content
    assert "pronabec_extraction_scope" not in content
    assert "policy.bronze_enabled and policy.required_for_e2e" not in content
    assert "if policy.bronze_enabled" in content
    assert "RUN_PRONABEC_DISCOVERY" in content
    assert "RUN_PRONABEC_BUILD_PLAN" in content
    assert "RUN_PRONABEC_PLAN_EXECUTION" in content
    assert "RUN_PRONABEC_FINALIZE" in content
    assert "dag_run.conf.get('run_pronabec', true) and dag_run.conf.get('run_pronabec_discovery', true)" in content
    assert "dag_run.conf.get('run_pronabec_plan_execution', true)" in content
    assert "run_pronabec_" + "chunk_extraction" not in content
    assert '"run_pronabec_plan_execution": Param(default=True, type="boolean")' in content


def test_dag_keeps_dataflow_on_final_bronze_paths() -> None:
    content = _read_dag_source()

    assert "bronze_work" not in content
    assert "bronze/pronabec/{dataset}/extraction_date={extraction_date}/data.jsonl" in dag_mod.ORCHESTRATION_CONFIG["datasets"]["pronabec_api"]["bronze_path_template"]
    assert "build_api_input_path(source_dataset)" in content
    assert "PAGE_END=999999" not in content
    assert "extract_pronabec_" not in content


def test_dag_creates_finalizers_for_all_bronze_enabled_datasets() -> None:
    pronabec_policies = {policy.source_dataset for policy in dag_mod.PRONABEC_EXTRACTION_POLICIES}

    assert pronabec_policies == {
        "perdida_becas",
        "notas_becarios",
        "concepto_pago",
        "convocatorias",
        "becarios_provincia",
        "ubigeo_postulacion",
        "periodos_academicos",
        "colegios_habiles",
        "becarios_pais_estudio",
        "convocatorias_carrera_sede",
        "nota_postulante_region",
    }


def test_dag_does_not_use_dynamic_task_mapping() -> None:
    content = _read_dag_source()

    assert ".expand(" not in content
    assert ".partial(" not in content


def test_dag_keeps_existing_non_pronabec_sections() -> None:
    content = _read_dag_source()

    assert "extract_mef" in content
    assert "mef_tasks" in content
    assert "stage_pronabec_reports_" in content
    assert "report_tasks" in content
    assert "publish_gold_views" in content
    assert "validate_gold_contracts" in content
    assert "run_quality_checks" in content


def test_silver_dataflow_groups_are_parallel_and_complete() -> None:
    content = _read_dag_source()

    pronabec_jobs = {item["job_name"] for item in dag_mod.PRONABEC_API_ITEMS}
    mef_jobs = {item["job_name"] for item in dag_mod.MEF_ITEMS}

    assert pronabec_jobs == {
        "dataflow-pronabec-becarios-pais-estudio-job",
        "dataflow-pronabec-becarios-provincia-job",
        "dataflow-pronabec-colegios-habiles-job",
        "dataflow-pronabec-convocatorias-job",
        "dataflow-pronabec-ubigeo-postulacion-job",
    }
    assert mef_jobs == {
        "dataflow-mef-presupuesto-job",
        "dataflow-mef-presupuesto-temporal-job",
        "dataflow-mef-producto-job",
        "dataflow-mef-producto-temporal-job",
        "dataflow-mef-actividad-job",
        "dataflow-mef-actividad-temporal-job",
        "dataflow-mef-generica-job",
        "dataflow-mef-generica-temporal-job",
        "dataflow-mef-hierarchy-job",
    }
    assert len(dag_mod.REPORT_DATASETS) == 23
    assert "all_silver_tasks" not in content
    assert "chain_tasks(report_tasks)" in content
    assert "chain_tasks(mef_tasks)" not in content
    assert "chain_tasks(pronabec_api_tasks)" not in content


def test_shared_cloud_run_job_groups_are_serialized() -> None:
    content = _read_dag_source()

    assert "def chain_tasks(tasks: list) -> None:" in content
    assert "for upstream, downstream in zip(tasks, tasks[1:]):" in content
    assert "chain_tasks(report_stage_tasks)" in content
    assert "chain_tasks(pronabec_finalize_tasks)" in content
    assert "chain_tasks(report_tasks)" in content


def test_distinct_cloud_run_job_silver_groups_stay_parallel() -> None:
    content = _read_dag_source()

    assert "mef_tasks = []" in content
    assert "pronabec_api_tasks = []" in content
    assert "chain_tasks(mef_tasks)" not in content
    assert "chain_tasks(pronabec_api_tasks)" not in content


def test_report_dataflow_tasks_use_real_dataset_parameters() -> None:
    content = _read_dag_source()

    assert "DATAFLOW_SDK_CONTAINER_IMAGE" in content
    assert '"DATAFLOW_SDK_CONTAINER_IMAGE": DATAFLOW_SDK_CONTAINER_IMAGE' in content
    assert '"SOURCE_DATASET": source_dataset' in content
    assert '"INPUT_PATH": build_report_bronze_uri(source_dataset)' in content
    assert 'f"pronabec_{source_dataset}"' in content
    assert "placeholder" not in content.lower()
    assert "TODO" not in content
    assert "SOURCE_DATASET=report_dataset" not in content
    assert "INPUT_PATH=report_dataset" not in content
    assert "OUTPUT_TABLE=report_dataset" not in content


def test_report_dataflow_paths_and_tables_are_bound_per_report() -> None:
    dataset = "report_beca18_region_postulacion_2025"

    assert (
        dag_mod.build_report_bronze_uri(dataset)
        == "gs://{{ var.value.gcs_bucket_name }}/bronze/pronabec_reports/"
        "report_beca18_region_postulacion_2025/"
        "extraction_date={{ dag_run.conf.get('extraction_date') or ds }}/data.csv"
    )
    assert (
        dag_mod.build_bq_table_ref(
            dag_mod.PROJECT_ID,
            dag_mod.SILVER_DATASET,
            f"pronabec_{dataset}",
        )
        == "{{ var.value.gcp_project_id }}:{{ var.value.bq_silver_dataset }}."
        "pronabec_report_beca18_region_postulacion_2025"
    )


def test_dag_uses_cloud_run_polling_helper_instead_of_bash_wait() -> None:
    content = _read_dag_source()

    assert "run_cloud_run_job_with_polling" in content
    assert "PythonOperator" in content
    assert "BashOperator" not in content
    assert "subprocess" not in content
    assert "gcloud" not in content
    assert "--wait" not in content
    assert "--async" not in content
    assert "AuthorizedSession" in content
    assert "google.auth.default" in content
    assert '"overrides"' in content
    assert '"containerOverrides"' in content
    assert "retries=0" in content
    assert "execution_timeout=timedelta(seconds=timeout_seconds + 600)" in content


class _FakeResponse:
    def __init__(self, payload: dict | None = None, status_code: int = 200, text: str | None = None) -> None:
        self.payload = payload or {}
        self.status_code = status_code
        self.text = json.dumps(self.payload) if text is None else text

    def json(self) -> dict:
        return self.payload


class _FakeSession:
    def __init__(self, post_payload: dict, get_payloads: list[dict]) -> None:
        self.post_payload = post_payload
        self.get_payloads = get_payloads
        self.post_calls = []
        self.get_calls = []

    def post(self, url: str, **kwargs) -> _FakeResponse:
        self.post_calls.append((url, kwargs))
        return _FakeResponse(self.post_payload)

    def get(self, url: str, **kwargs) -> _FakeResponse:
        self.get_calls.append((url, kwargs))
        return _FakeResponse(self.get_payloads.pop(0))


def _patch_api_session(monkeypatch, session: _FakeSession) -> None:
    monkeypatch.setattr(dag_mod.google.auth, "default", lambda scopes: ("credentials", "project"))
    monkeypatch.setattr(dag_mod, "AuthorizedSession", lambda credentials: session)


def test_cloud_run_api_polling_helper_succeeds(monkeypatch, capsys) -> None:
    session = _FakeSession(
        post_payload={
            "name": "projects/project/locations/us-central1/operations/op-1",
            "response": {
                "name": (
                    "projects/project/locations/us-central1/jobs/test-job/"
                    "executions/test-job-execution"
                )
            },
        },
        get_payloads=[
            {
                "name": "projects/project/locations/us-central1/jobs/test-job/executions/test-job-execution",
                "taskCount": 1,
                "runningCount": 0,
                "succeededCount": 1,
                "failedCount": 0,
                "conditions": [{"type": "Completed", "state": "CONDITION_SUCCEEDED"}],
            },
        ],
    )
    _patch_api_session(monkeypatch, session)
    monkeypatch.setattr(dag_mod.time, "sleep", lambda _: None)

    dag_mod.run_cloud_run_job_with_polling(
        job_name="test-job",
        project_id="project",
        region="us-central1",
        env_vars={"BRONZE_EXTRACTION_DATE": "2026-07-02", "PIPELINE_RUN_ID": "manual_20260702"},
        poll_interval_seconds=1,
        timeout_seconds=30,
    )

    output = capsys.readouterr().out
    assert "Launching Cloud Run job through REST API." in output
    assert "Cloud Run operation: projects/project/locations/us-central1/operations/op-1" in output
    assert "Resolved Cloud Run execution: test-job-execution" in output
    assert "Cloud Run execution succeeded: test-job-execution" in output
    assert session.post_calls[0][0] == "https://run.googleapis.com/v2/projects/project/locations/us-central1/jobs/test-job:run"
    post_body = session.post_calls[0][1]["json"]
    assert post_body["overrides"]["containerOverrides"][0]["env"] == [
        {"name": "BRONZE_EXTRACTION_DATE", "value": "2026-07-02"},
        {"name": "PIPELINE_RUN_ID", "value": "manual_20260702"},
    ]
    assert session.get_calls


def test_cloud_run_api_polling_helper_resolves_execution_from_list(monkeypatch) -> None:
    session = _FakeSession(
        post_payload={"name": "projects/project/locations/us-central1/operations/op-list"},
        get_payloads=[
            {
                "executions": [
                    {
                        "name": (
                            "projects/project/locations/us-central1/jobs/test-job/"
                            "executions/test-job-listed"
                        ),
                        "createTime": datetime.now(timezone.utc).isoformat(),
                    }
                ]
            },
            {
                "name": "projects/project/locations/us-central1/jobs/test-job/executions/test-job-listed",
                "taskCount": 1,
                "succeededCount": 1,
                "failedCount": 0,
            },
        ],
    )
    _patch_api_session(monkeypatch, session)

    dag_mod.run_cloud_run_job_with_polling(
        job_name="test-job",
        project_id="project",
        region="us-central1",
        env_vars={"BRONZE_EXTRACTION_DATE": "2026-07-02", "PIPELINE_RUN_ID": "manual_20260702"},
        poll_interval_seconds=1,
        timeout_seconds=30,
    )

    assert session.get_calls[0][0].endswith("/jobs/test-job/executions")
    assert session.get_calls[1][0].endswith("/executions/test-job-listed")


def test_cloud_run_api_polling_succeeds_when_done_without_resolved_execution(
    monkeypatch,
    capsys,
) -> None:
    session = _FakeSession(
        post_payload={"name": "projects/project/locations/us-central1/operations/op-done"},
        get_payloads=[
            {"executions": []},
            {"name": "projects/project/locations/us-central1/operations/op-done", "done": True},
            {"executions": []},
        ],
    )
    _patch_api_session(monkeypatch, session)

    dag_mod.run_cloud_run_job_with_polling(
        job_name="test-job",
        project_id="project",
        region="us-central1",
        env_vars={"BRONZE_EXTRACTION_DATE": "2026-07-02", "PIPELINE_RUN_ID": "manual_20260702"},
        poll_interval_seconds=1,
        timeout_seconds=30,
    )

    output = capsys.readouterr().out
    assert (
        "Cloud Run operation completed successfully but execution could not be resolved "
        "unambiguously"
    ) in output


def test_cloud_run_api_polling_does_not_select_ambiguous_execution_candidate(
    monkeypatch,
    capsys,
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    first_candidate = {
        "name": "projects/project/locations/us-central1/jobs/test-job/executions/test-job-first",
        "createTime": now,
        "taskCount": 1,
        "succeededCount": 0,
        "failedCount": 1,
        "conditions": [{"type": "Failed", "state": "CONDITION_SUCCEEDED"}],
    }
    second_candidate = {
        "name": "projects/project/locations/us-central1/jobs/test-job/executions/test-job-second",
        "createTime": now,
        "taskCount": 1,
        "succeededCount": 1,
        "failedCount": 0,
        "conditions": [{"type": "Completed", "state": "CONDITION_SUCCEEDED"}],
    }
    session = _FakeSession(
        post_payload={"name": "projects/project/locations/us-central1/operations/op-ambiguous"},
        get_payloads=[
            {"executions": [first_candidate, second_candidate]},
            {"name": "projects/project/locations/us-central1/operations/op-ambiguous", "done": True},
            {"executions": [first_candidate, second_candidate]},
        ],
    )
    _patch_api_session(monkeypatch, session)

    dag_mod.run_cloud_run_job_with_polling(
        job_name="test-job",
        project_id="project",
        region="us-central1",
        env_vars={"BRONZE_EXTRACTION_DATE": "2026-07-02", "PIPELINE_RUN_ID": "manual_20260702"},
        poll_interval_seconds=1,
        timeout_seconds=30,
    )

    output = capsys.readouterr().out
    assert "Ambiguous Cloud Run execution candidates for job=test-job:" in output
    assert "test-job-first" in output
    assert "test-job-second" in output
    assert not any(call[0].endswith("/executions/test-job-first") for call in session.get_calls)
    assert not any(call[0].endswith("/executions/test-job-second") for call in session.get_calls)


def test_cloud_run_api_polling_helper_fails_on_operation_error(monkeypatch) -> None:
    session = _FakeSession(
        post_payload={"name": "operations/op-error"},
        get_payloads=[
            {"executions": []},
            {
                "name": "operations/op-error",
                "done": True,
                "error": {"code": 13, "message": "job failed", "details": [{"reason": "FAILED"}]},
            }
        ],
    )
    _patch_api_session(monkeypatch, session)

    try:
        dag_mod.run_cloud_run_job_with_polling(
            job_name="test-job",
            project_id="project",
            region="us-central1",
            env_vars={"BRONZE_EXTRACTION_DATE": "2026-07-02", "PIPELINE_RUN_ID": "manual_20260702"},
            poll_interval_seconds=1,
            timeout_seconds=30,
        )
    except RuntimeError as exc:
        message = str(exc)
        assert "Cloud Run operation failed" in message
        assert "code=13" in message
        assert "job failed" in message
    else:
        raise AssertionError("Expected RuntimeError for Cloud Run operation error")


def test_cloud_run_api_polling_helper_fails_on_execution_failed_count(monkeypatch) -> None:
    session = _FakeSession(
        post_payload={
            "name": "operations/op-execution-failed",
            "response": {
                "name": (
                    "projects/project/locations/us-central1/jobs/test-job/"
                    "executions/test-job-failed"
                )
            },
        },
        get_payloads=[
            {
                "name": "projects/project/locations/us-central1/jobs/test-job/executions/test-job-failed",
                "taskCount": 1,
                "succeededCount": 0,
                "failedCount": 1,
                "conditions": [{"type": "Failed", "state": "CONDITION_SUCCEEDED"}],
            }
        ],
    )
    _patch_api_session(monkeypatch, session)
    monkeypatch.setattr(
        dag_mod,
        "_log_bigquery_success_evidence",
        lambda env_vars, project_id: (_ for _ in ()).throw(
            AssertionError("BigQuery evidence must not hide failed Cloud Run executions")
        ),
    )

    try:
        dag_mod.run_cloud_run_job_with_polling(
            job_name="test-job",
            project_id="project",
            region="us-central1",
            env_vars={"BRONZE_EXTRACTION_DATE": "2026-07-02", "PIPELINE_RUN_ID": "manual_20260702"},
            poll_interval_seconds=1,
            timeout_seconds=30,
        )
    except RuntimeError as exc:
        assert "Cloud Run execution failed" in str(exc)
    else:
        raise AssertionError("Expected RuntimeError for failed Cloud Run execution")


def test_cloud_run_api_polling_logs_bigquery_evidence(monkeypatch, capsys) -> None:
    session = _FakeSession(
        post_payload={
            "name": "operations/op-evidence",
            "response": {
                "name": (
                    "projects/project/locations/us-central1/jobs/test-job/"
                    "executions/test-job-evidence"
                )
            },
        },
        get_payloads=[
            {
                "name": "projects/project/locations/us-central1/jobs/test-job/executions/test-job-evidence",
                "taskCount": 1,
                "succeededCount": 1,
                "failedCount": 0,
            }
        ],
    )
    _patch_api_session(monkeypatch, session)

    class FakeQueryJob:
        def result(self):
            return [type("Row", (), {"rows_count": 42})()]

    class FakeClient:
        def __init__(self, project: str) -> None:
            self.project = project

        def query(self, query, job_config=None):
            return FakeQueryJob()

    fake_bigquery = MagicMock()
    fake_bigquery.Client = FakeClient
    fake_bigquery.QueryJobConfig = lambda query_parameters: {"query_parameters": query_parameters}
    fake_bigquery.ScalarQueryParameter = lambda name, parameter_type, value: (name, parameter_type, value)
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", fake_bigquery)

    dag_mod.run_cloud_run_job_with_polling(
        job_name="test-job",
        project_id="project",
        region="us-central1",
        env_vars={
            "BRONZE_EXTRACTION_DATE": "2026-07-02",
            "PIPELINE_RUN_ID": "manual_20260702",
            "SOURCE_SYSTEM": "mef",
            "SOURCE_DATASET": "presupuesto",
            "OUTPUT_TABLE": "project:silver.presupuesto_mef",
        },
        poll_interval_seconds=1,
        timeout_seconds=30,
    )

    output = capsys.readouterr().out
    assert "BigQuery Silver evidence:" in output
    assert "rows=42" in output


def test_cloud_run_api_polling_helper_times_out(monkeypatch) -> None:
    session = _FakeSession(
        post_payload={"name": "operations/op-running"},
        get_payloads=[
            {"executions": []},
            {"name": "operations/op-running", "done": False},
            {"executions": []},
        ],
    )
    _patch_api_session(monkeypatch, session)
    monotonic_values = iter([0, 0, 31])
    monkeypatch.setattr(dag_mod.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(dag_mod.time, "sleep", lambda _: None)

    try:
        dag_mod.run_cloud_run_job_with_polling(
            job_name="test-job",
            project_id="project",
            region="us-central1",
            env_vars={"BRONZE_EXTRACTION_DATE": "2026-07-02", "PIPELINE_RUN_ID": "manual_20260702"},
            poll_interval_seconds=1,
            timeout_seconds=30,
        )
    except TimeoutError as exc:
        assert "Timed out waiting for Cloud Run execution" in str(exc)
    else:
        raise AssertionError("Expected TimeoutError for long running Cloud Run operation")


def test_cloud_run_api_request_error_is_clear(monkeypatch) -> None:
    class ErrorSession:
        def post(self, url: str, **kwargs) -> _FakeResponse:
            return _FakeResponse({"error": {"message": "forbidden"}}, status_code=403)

    _patch_api_session(monkeypatch, ErrorSession())

    try:
        dag_mod.run_cloud_run_job_with_polling(
            job_name="test-job",
            project_id="project",
            region="us-central1",
            env_vars={"BRONZE_EXTRACTION_DATE": "2026-07-02", "PIPELINE_RUN_ID": "manual_20260702"},
            poll_interval_seconds=1,
            timeout_seconds=30,
        )
    except RuntimeError as exc:
        assert "Cloud Run API request failed" in str(exc)
        assert "forbidden" in str(exc)
    else:
        raise AssertionError("Expected RuntimeError for Cloud Run API request error")
