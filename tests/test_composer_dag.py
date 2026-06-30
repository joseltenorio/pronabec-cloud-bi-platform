"""Pruebas unitarias para validar la configuracion del DAG de Composer."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock


sys.modules["airflow"] = MagicMock()
sys.modules["airflow.models"] = MagicMock()
sys.modules["airflow.models.param"] = MagicMock()
sys.modules["airflow.operators"] = MagicMock()
sys.modules["airflow.operators.bash"] = MagicMock()
sys.modules["airflow.operators.empty"] = MagicMock()

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


def test_dag_schedule_is_weekly_without_catchup() -> None:
    content = _read_dag_source()

    assert 'schedule_interval=ORCHESTRATION_CONFIG["dag"]["schedule"]' in content
    assert "catchup=False" in content


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

    assert "finalize_task >> validate_bronze_manifests" in content
    assert "extract_mef >> validate_bronze_manifests" in content
    assert "stage_task >> validate_bronze_manifests" in content


def test_bronze_manifest_validation_runs_before_silver_tasks() -> None:
    content = _read_dag_source()

    assert "validate_bronze_manifests >> pronabec_api_tasks" in content
    assert "validate_bronze_manifests >> mef_tasks" in content
    assert "validate_bronze_manifests >> report_tasks" in content


def test_dag_exposes_bronze_manifest_validation_param() -> None:
    content = _read_dag_source()

    assert '"run_bronze_manifest_validation": Param(default=True, type="boolean")' in content


def test_dag_contains_chunked_pronabec_job_refs() -> None:
    content = _read_dag_source()

    assert "PRONABEC_DISCOVERY_JOB" in content
    assert "PRONABEC_BUILD_PLAN_JOB" in content
    assert "PRONABEC_RUN_PLAN_JOB" in content
    assert "PRONABEC_EXTRACT_CHUNK_JOB" in content
    assert "PRONABEC_FINALIZE_DATASET_JOB" in content
    assert "pronabec-discovery-job" in content
    assert "pronabec-build-plan-job" in content
    assert "pronabec-run-plan-job" in content
    assert "pronabec-extract-chunk-job" in content
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
    assert "PIPELINE_RUN_ID={{ run_id }}" in content
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
    assert "RUN_PRONABEC_CHUNK_EXTRACTION" in content
    assert "RUN_PRONABEC_FINALIZE" in content
    assert "dag_run.conf.get('run_pronabec', true) and dag_run.conf.get('run_pronabec_discovery', true)" in content
    assert "dag_run.conf.get('run_pronabec_plan_execution', dag_run.conf.get('run_pronabec_chunk_extraction', true))" in content
    assert '"run_pronabec_chunk_extraction": Param(default=True, type="boolean")' in content
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
