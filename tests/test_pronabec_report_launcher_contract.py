"""Contract tests for the parameterized PRONABEC reports Dataflow launcher."""

from __future__ import annotations

from pathlib import Path

import pytest

from pipelines.common.orchestration_config import (
    build_bq_table_ref,
    load_endpoints_config,
    load_orchestration_config,
    resolve_pronabec_report_datasets,
)
from pipelines.dataflow_bronze_to_silver import (
    PRONABEC_REPORTS_INVALID_CONFIG_MESSAGE,
    parse_arguments,
    validate_arguments,
)


def _validate(argv: list[str]) -> None:
    args, _ = parse_arguments(argv)
    validate_arguments(args)


def _base_args(**overrides: str) -> list[str]:
    values = {
        "source_system": "pronabec_reports",
        "source_dataset": "report_beca18_universitarios_universidad_anual",
        "extraction_date": "2026-06-15",
        "input_path": (
            "gs://bucket/bronze/pronabec_reports/"
            "report_beca18_universitarios_universidad_anual/"
            "extraction_date=2026-06-15/data.csv"
        ),
        "input_format": "csv",
        "output_table": (
            "test-project:silver."
            "pronabec_report_beca18_universitarios_universidad_anual"
        ),
        "runner": "DataflowRunner",
    }
    values.update(overrides)
    return [
        "--source-system", values["source_system"],
        "--source-dataset", values["source_dataset"],
        "--extraction-date", values["extraction_date"],
        "--input-path", values["input_path"],
        "--input-format", values["input_format"],
        "--output-table", values["output_table"],
        "--runner", values["runner"],
        "--project", "test-project",
        "--region", "us-central1",
        "--temp-location", "gs://bucket/temp",
        "--staging-location", "gs://bucket/staging",
        "--service-account-email", "test-dataflow-sa@test-project.iam.gserviceaccount.com",
        "--sdk-container-image",
        "us-central1-docker.pkg.dev/test-project/repo/dataflow-worker:latest",
    ]


@pytest.mark.parametrize(
    "overrides",
    [
        {"source_dataset": "placeholder_dataset"},
        {"input_path": "gs://bucket/bronze/pronabec_reports/placeholder_path/data.csv"},
        {"output_table": "test-project:silver.placeholder_table"},
        {"input_path": "tmp/bronze/pronabec_reports/report/data.csv"},
        {"output_table": "test-project.silver.pronabec_report"},
    ],
)
def test_pronabec_reports_rejects_unbound_launcher_values(overrides: dict[str, str]) -> None:
    with pytest.raises(ValueError, match=PRONABEC_REPORTS_INVALID_CONFIG_MESSAGE):
        _validate(_base_args(**overrides))


def test_pronabec_reports_accepts_real_parameters() -> None:
    _validate(_base_args())


def test_pronabec_api_is_not_affected_by_report_launcher_contract() -> None:
    _validate(
        _base_args(
            source_system="pronabec",
            source_dataset="becarios_pais_estudio",
            input_path="tmp/bronze/pronabec/becarios_pais_estudio/data.jsonl",
            input_format="jsonl",
            output_table="test-project:silver.pronabec_becarios_pais_estudio",
        )
    )


def test_mef_is_not_affected_by_report_launcher_contract() -> None:
    _validate(
        _base_args(
            source_system="mef",
            source_dataset="presupuesto",
            input_path="tmp/bronze/mef/presupuesto/data.csv",
            output_table="test-project:silver.presupuesto_mef",
        )
    )


def test_composer_defines_one_parameterized_report_launch_per_dataset() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    orchestration_config = load_orchestration_config(repo_root / "config" / "orchestration.yaml")
    endpoints_config = load_endpoints_config(repo_root / "config" / "endpoints.yaml")
    report_datasets = resolve_pronabec_report_datasets(orchestration_config, endpoints_config)
    dag_source = (repo_root / "dags" / "pronabec_medallion_batch_dag.py").read_text(encoding="utf-8")

    assert len(report_datasets) == 23
    assert 'TaskGroup(group_id="pronabec_reports_silver")' in dag_source
    assert "for item in REPORT_DATASETS" in dag_source
    assert '"SOURCE_DATASET": source_dataset' in dag_source
    assert '"INPUT_PATH": build_report_bronze_uri(source_dataset)' in dag_source
    assert '"OUTPUT_TABLE": build_bq_table_ref(' in dag_source
    assert '"DATAFLOW_SDK_CONTAINER_IMAGE": DATAFLOW_SDK_CONTAINER_IMAGE' in dag_source

    dataset = "report_beca18_universitarios_universidad_anual"
    bronze_template = orchestration_config["datasets"]["pronabec_reports"]["bronze_path_template"]
    input_path = "gs://bucket/" + bronze_template.format(
        dataset=dataset,
        extraction_date="2026-07-02",
    )
    output_table = build_bq_table_ref("project", "silver", f"pronabec_{dataset}")

    assert input_path == (
        "gs://bucket/bronze/pronabec_reports/"
        "report_beca18_universitarios_universidad_anual/"
        "extraction_date=2026-07-02/data.csv"
    )
    assert output_table == "project:silver.pronabec_report_beca18_universitarios_universidad_anual"
