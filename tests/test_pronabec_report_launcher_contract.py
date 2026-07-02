"""Contract tests for the parameterized PRONABEC reports Dataflow launcher."""

from __future__ import annotations

import pytest

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
