from __future__ import annotations

import json
from pathlib import Path

import pytest

from tools.stage_inei_reports import (
    build_bronze_data_uri,
    build_landing_uri,
    build_manifest,
    expected_dataset_names,
    stage_inei_reports_gcs,
    stage_inei_reports_local,
)


def test_builds_landing_and_bronze_paths() -> None:
    assert (
        build_landing_uri("bucket", "landing/inei_reports", "file.csv")
        == "gs://bucket/landing/inei_reports/file.csv"
    )
    assert (
        build_bronze_data_uri(
            "bucket",
            "bronze/inei_reports",
            "inei_population_youth_region",
            "2026-07-07",
        )
        == "gs://bucket/bronze/inei_reports/inei_population_youth_region/extraction_date=2026-07-07/data.csv"
    )


def test_recognizes_expected_datasets() -> None:
    assert expected_dataset_names() == [
        "inei_population_youth_region",
        "inei_demographic_indicators_region",
        "inei_pobreza_departamental",
        "inei_internet_acceso_region",
    ]


def test_strict_mode_fails_when_expected_file_is_missing(tmp_path: Path) -> None:
    input_dir = tmp_path / "landing"
    output_dir = tmp_path / "bronze"
    input_dir.mkdir()

    with pytest.raises(FileNotFoundError):
        stage_inei_reports_local(
            input_dir=str(input_dir),
            output_dir=str(output_dir),
            extraction_date="2026-07-07",
            strict=True,
        )


def test_can_process_single_dataset(tmp_path: Path) -> None:
    input_dir = tmp_path / "landing"
    output_dir = tmp_path / "bronze"
    input_dir.mkdir()
    (input_dir / "inei_population_youth_region.csv").write_text(
        "anio,region,poblacion_total\n2025,Lima,100\n",
        encoding="utf-8",
    )

    staged, skipped, missing = stage_inei_reports_local(
        input_dir=str(input_dir),
        output_dir=str(output_dir),
        extraction_date="2026-07-07",
        dataset_name="inei_population_youth_region",
        strict=True,
    )

    assert (staged, skipped, missing) == (1, 0, 0)
    assert (
        output_dir
        / "inei_population_youth_region"
        / "extraction_date=2026-07-07"
        / "data.csv"
    ).exists()
    assert not (output_dir / "inei_demographic_indicators_region").exists()


def test_manifest_contains_expected_fields() -> None:
    manifest = build_manifest(
        dataset_name="inei_population_youth_region",
        source_uri="gs://bucket/landing/inei_reports/inei_population_youth_region.csv",
        bronze_uri="gs://bucket/bronze/inei_reports/inei_population_youth_region/extraction_date=2026-07-07/data.csv",
        extraction_date="2026-07-07",
        pipeline_run_id="run-1",
    )

    assert manifest["source_system"] == "INEI"
    assert manifest["source_dataset"] == "inei_population_youth_region"
    assert manifest["dataset"] == "inei_population_youth_region"
    assert manifest["pipeline_run_id"] == "run-1"
    assert manifest["status"] == "SUCCESS"


def test_gcs_staging_writes_data_success_and_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    written: dict[str, bytes | str] = {}

    monkeypatch.setattr(
        "tools.stage_inei_reports.read_gcs_bytes",
        lambda uri: b"anio,region\n2025,Lima\n",
    )
    monkeypatch.setattr("tools.stage_inei_reports.list_gcs_objects", lambda uri: [])
    monkeypatch.setattr(
        "tools.stage_inei_reports.write_gcs_bytes",
        lambda uri, content, content_type=None: written.update({uri: content}),
    )
    monkeypatch.setattr(
        "tools.stage_inei_reports.write_gcs_text",
        lambda uri, content: written.update({uri: content}),
    )

    staged, skipped, missing = stage_inei_reports_gcs(
        bucket="bucket",
        landing_prefix="landing/inei_reports",
        bronze_prefix="bronze/inei_reports",
        extraction_date="2026-07-07",
        dataset_name="inei_population_youth_region",
        strict=True,
        pipeline_run_id="run-1",
    )

    base = "gs://bucket/bronze/inei_reports/inei_population_youth_region/extraction_date=2026-07-07"
    assert (staged, skipped, missing) == (1, 0, 0)
    assert written[f"{base}/data.csv"] == b"anio,region\n2025,Lima\n"
    assert json.loads(written[f"{base}/manifest.json"])["source_system"] == "INEI"
    assert json.loads(written[f"{base}/_SUCCESS"])["status"] == "SUCCESS"


def test_does_not_touch_pronabec_reports_paths(tmp_path: Path) -> None:
    input_dir = tmp_path / "landing"
    output_dir = tmp_path / "bronze"
    input_dir.mkdir()
    (input_dir / "inei_population_youth_region.csv").write_text(
        "anio,region,poblacion_total\n2025,Lima,100\n",
        encoding="utf-8",
    )

    stage_inei_reports_local(
        input_dir=str(input_dir),
        output_dir=str(output_dir),
        extraction_date="2026-07-07",
        dataset_name="inei_population_youth_region",
    )

    assert not (output_dir / "pronabec_reports").exists()
