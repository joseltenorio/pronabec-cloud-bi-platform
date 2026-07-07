"""Tests for Bronze input path expansion before Dataflow reads files."""

from __future__ import annotations

import re
from pathlib import Path
from types import SimpleNamespace

import pytest

from pipelines.dataflow_bronze_to_silver import (
    expand_input_paths,
    parse_arguments,
    validate_arguments,
)


def test_concrete_input_path_returns_single_path() -> None:
    input_path = "gs://bucket/bronze/pronabec/becarios/extraction_date=2026-07-02/data.jsonl"

    assert expand_input_paths(input_path) == [input_path]


def test_wildcard_input_path_expands_to_sorted_real_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_match(patterns: list[str]) -> list[SimpleNamespace]:
        assert patterns == [
            "gs://bucket/bronze/mef/presupuesto/extraction_date=2026-07-02/year=*/data.csv"
        ]
        return [
            SimpleNamespace(
                metadata_list=[
                    SimpleNamespace(
                        path="gs://bucket/bronze/mef/presupuesto/extraction_date=2026-07-02/year=2014/data.csv"
                    ),
                    SimpleNamespace(
                        path="gs://bucket/bronze/mef/presupuesto/extraction_date=2026-07-02/year=2012/data.csv"
                    ),
                    SimpleNamespace(
                        path="gs://bucket/bronze/mef/presupuesto/extraction_date=2026-07-02/year=2013/data.csv"
                    ),
                ]
            )
        ]

    monkeypatch.setattr("apache_beam.io.filesystems.FileSystems.match", fake_match)

    assert expand_input_paths(
        "gs://bucket/bronze/mef/presupuesto/extraction_date=2026-07-02/year=*/data.csv"
    ) == [
        "gs://bucket/bronze/mef/presupuesto/extraction_date=2026-07-02/year=2012/data.csv",
        "gs://bucket/bronze/mef/presupuesto/extraction_date=2026-07-02/year=2013/data.csv",
        "gs://bucket/bronze/mef/presupuesto/extraction_date=2026-07-02/year=2014/data.csv",
    ]


def test_wildcard_input_path_without_matches_raises_clear_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "apache_beam.io.filesystems.FileSystems.match",
        lambda patterns: [SimpleNamespace(metadata_list=[])],
    )

    input_path = "gs://bucket/bronze/mef/presupuesto/extraction_date=2026-07-02/year=*/data.csv"

    with pytest.raises(
        ValueError,
        match=re.escape(f"Input path pattern did not match any files: {input_path}"),
    ):
        expand_input_paths(input_path)


def test_wildcard_expansion_returns_real_paths_not_literal_pattern(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_path = "gs://bucket/bronze/mef/presupuesto/extraction_date=2026-07-02/year=*/data.csv"
    real_path = "gs://bucket/bronze/mef/presupuesto/extraction_date=2026-07-02/year=2026/data.csv"

    monkeypatch.setattr(
        "apache_beam.io.filesystems.FileSystems.match",
        lambda patterns: [SimpleNamespace(metadata_list=[SimpleNamespace(path=real_path)])],
    )

    expanded_paths = expand_input_paths(input_path)

    assert expanded_paths == [real_path]
    assert input_path not in expanded_paths


def test_pronabec_report_concrete_input_path_still_validates() -> None:
    args, _ = parse_arguments(
        [
            "--source-system", "pronabec_reports",
            "--source-dataset", "report_beca18_universitarios_universidad_anual",
            "--extraction-date", "2026-07-02",
            "--input-path", (
                "gs://bucket/bronze/pronabec_reports/"
                "report_beca18_universitarios_universidad_anual/"
                "extraction_date=2026-07-02/data.csv"
            ),
            "--input-format", "csv",
            "--output-table", "project:silver.pronabec_report_beca18_universitarios_universidad_anual",
            "--runner", "DirectRunner",
            "--dry-run",
        ]
    )

    validate_arguments(args)
    assert args.source_dataset == "report_beca18_universitarios_universidad_anual"
    assert args.output_table == "project:silver.pronabec_report_beca18_universitarios_universidad_anual"


def test_pronabec_api_concrete_input_path_still_validates() -> None:
    args, _ = parse_arguments(
        [
            "--source-system", "pronabec",
            "--source-dataset", "becarios_pais_estudio",
            "--extraction-date", "2026-07-02",
            "--input-path", (
                "gs://bucket/bronze/pronabec/becarios_pais_estudio/"
                "extraction_date=2026-07-02/data.jsonl"
            ),
            "--input-format", "jsonl",
            "--output-table", "project:silver.pronabec_becarios_pais_estudio",
            "--runner", "DirectRunner",
            "--dry-run",
        ]
    )

    validate_arguments(args)
    assert args.source_dataset == "becarios_pais_estudio"
    assert args.output_table == "project:silver.pronabec_becarios_pais_estudio"


def test_mef_year_wildcard_input_path_is_valid() -> None:
    args, _ = parse_arguments(
        [
            "--source-system", "mef",
            "--source-dataset", "presupuesto",
            "--extraction-date", "2026-07-02",
            "--input-path",
            "gs://bucket/bronze/mef/presupuesto/extraction_date=2026-07-02/year=*/data.csv",
            "--input-format", "csv",
            "--output-table", "project:silver.presupuesto_mef",
            "--runner", "DirectRunner",
            "--dry-run",
        ]
    )

    validate_arguments(args)
    assert args.source_dataset == "presupuesto"
    assert args.output_table == "project:silver.presupuesto_mef"


def test_mef_deploy_keeps_year_wildcard_for_partitioned_inputs() -> None:
    deploy_script = Path("scripts/deploy_cloud_run_jobs.sh")
    content = deploy_script.read_text(encoding="utf-8")

    assert "bronze/mef/presupuesto/extraction_date=\\${BRONZE_EXTRACTION_DATE}/year=*/data.csv" in content
