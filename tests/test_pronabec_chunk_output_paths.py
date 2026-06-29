from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pipelines.extract_pronabec import (
    PronabecExtractionError,
    build_local_pronabec_chunk_output_paths,
    build_pronabec_chunk_base_path,
    build_chunk_manifest,
    resolve_output_mode,
    resolve_pipeline_run_id,
    validate_chunk_output_contract,
    write_chunk_dataset_to_local,
)


def test_output_mode_default_is_final(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OUTPUT_MODE", raising=False)

    assert resolve_output_mode(None) == "final"


def test_chunk_mode_requires_source_dataset() -> None:
    with pytest.raises(PronabecExtractionError, match="requiere SOURCE_DATASET"):
        validate_chunk_output_contract("chunk", None, 1, 10, "run_1")


def test_pipeline_run_id_can_come_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PIPELINE_RUN_ID", "manual_20260629")

    assert resolve_pipeline_run_id(None) == "manual_20260629"


def test_chunk_mode_requires_page_range() -> None:
    with pytest.raises(PronabecExtractionError, match="requiere PAGE_START y PAGE_END"):
        validate_chunk_output_contract("chunk", "convocatorias", None, None, "run_1")


def test_chunk_mode_requires_pipeline_run_id() -> None:
    with pytest.raises(PronabecExtractionError, match="requiere PIPELINE_RUN_ID"):
        validate_chunk_output_contract("chunk", "convocatorias", 1, 10, None)


def test_chunk_base_path_uses_extraction_date_and_run_id() -> None:
    path = build_pronabec_chunk_base_path(
        dataset_name="convocatorias_carrera_sede",
        extraction_date="2026-06-29",
        pipeline_run_id="manual_20260629",
        page_start=1,
        page_end=10,
    )

    assert path == (
        "bronze_work/pronabec/convocatorias_carrera_sede/"
        "extraction_date=2026-06-29/run_id=manual_20260629/"
        "chunk_start=1_chunk_end=10"
    )


def test_chunk_mode_rejects_invalid_dataset_path_component() -> None:
    with pytest.raises(PronabecExtractionError, match="caracteres no permitidos"):
        build_pronabec_chunk_base_path(
            dataset_name="../bad",
            extraction_date="2026-06-29",
            pipeline_run_id="manual_20260629",
            page_start=1,
            page_end=10,
        )


def test_local_chunk_paths_write_under_bronze_work(tmp_path: Path) -> None:
    paths = build_local_pronabec_chunk_output_paths(
        output_dir=tmp_path,
        dataset_name="convocatorias_carrera_sede",
        extraction_date="2026-06-29",
        pipeline_run_id="manual_20260629",
        page_start=1,
        page_end=10,
    )

    assert "bronze_work/pronabec/convocatorias_carrera_sede" in str(
        paths["normalized_path"]
    ).replace("\\", "/")
    assert paths["normalized_path"].name == "data.jsonl"
    assert paths["chunk_manifest_path"].name == "chunk_manifest.json"


def test_write_chunk_dataset_to_local_does_not_create_final_success(tmp_path: Path) -> None:
    uris = write_chunk_dataset_to_local(
        dataset_name="convocatorias_carrera_sede",
        raw_payload={
            "requested_page_size": 5000,
            "effective_page_size": 2000,
            "reported_records": "10000",
            "total_pages": "5",
        },
        normalized_records=[{"source_row_id": "1"}],
        extraction_date="2026-06-29",
        output_dir=tmp_path,
        pipeline_run_id="manual_20260629",
        page_start=1,
        page_end=10,
        started_at=__import__("datetime").datetime.fromisoformat("2026-06-29T00:00:00+00:00"),
        logger=MagicMock(),
    )

    data_path = Path(uris["normalized_uri"])
    manifest_path = Path(uris["chunk_manifest_uri"])

    assert data_path.exists()
    assert manifest_path.exists()
    assert not (manifest_path.parent / "_SUCCESS").exists()
    assert not (manifest_path.parent / "manifest.json").exists()

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["effective_page_size"] == 2000
    assert manifest["requested_page_size"] == 5000
    assert manifest["status"] == "SUCCESS"


def test_chunk_manifest_contains_required_fields() -> None:
    manifest = build_chunk_manifest(
        dataset_name="notas_becarios",
        extraction_date="2026-06-29",
        pipeline_run_id="run_1",
        page_start=1,
        page_end=10,
        raw_payload={
            "requested_page_size": 10000,
            "effective_page_size": 5000,
            "reported_records": "103230",
            "total_pages": "21",
        },
        records_written=1000,
        started_at=__import__("datetime").datetime.fromisoformat("2026-06-29T00:00:00+00:00"),
        finished_at=__import__("datetime").datetime.fromisoformat("2026-06-29T00:05:00+00:00"),
    )

    assert manifest["source_system"] == "pronabec"
    assert manifest["source_dataset"] == "notas_becarios"
    assert manifest["pipeline_run_id"] == "run_1"
    assert manifest["page_start"] == 1
    assert manifest["page_end"] == 10
    assert manifest["effective_page_size"] == 5000
    assert manifest["records_written"] == 1000
