# -*- coding: utf-8 -*-
"""Pruebas unitarias para finalize_pronabec_dataset.py."""

import json
import pytest
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

from pipelines.finalize_pronabec_dataset import parse_args, run_finalize
from pipelines.common.config import ConfigError


@pytest.fixture
def mock_pipeline_settings():
    return {
        "log_level": "INFO",
        "bucket_name": "test-bucket",
    }


@pytest.fixture
def mock_orchestration():
    return {
        "dag": {},
        "runtime": {},
        "jobs": {},
        "gold": {"sql_template": "...", "validation_queries": [{"name": "q", "query": "..."}]},
        "datasets": {
            "pronabec_api": {
                "extraction_policies": [
                    {
                        "source_dataset": "dataset_chunked",
                        "extraction_enabled": True,
                        "silver_enabled": True,
                        "extraction_mode": "chunked",
                        "required_for_e2e": True,
                        "chunk_size_pages": 10,
                        "max_parallel_chunks": 2,
                        "recommended_page_size": 1000,
                        "fallback_page_sizes": [500],
                        "page_size_policy": "safe",
                        "allow_record_count_mismatch": False,
                    },
                    {
                        "source_dataset": "dataset_single",
                        "extraction_enabled": True,
                        "silver_enabled": True,
                        "extraction_mode": "single",
                        "required_for_e2e": False,
                        "recommended_page_size": 1000,
                        "fallback_page_sizes": [500],
                        "page_size_policy": "safe",
                        "allow_record_count_mismatch": False,
                    },
                    {
                        "source_dataset": "dataset_mismatch_allowed",
                        "extraction_enabled": True,
                        "silver_enabled": True,
                        "extraction_mode": "single",
                        "required_for_e2e": False,
                        "recommended_page_size": 1000,
                        "fallback_page_sizes": [500],
                        "page_size_policy": "safe",
                        "allow_record_count_mismatch": True,
                    }
                ]
            },
            "pronabec_reports": {"enabled": False},
        },
    }


class DummyArgs:
    def __init__(self, **kwargs):
        self.pipeline_config = "config/pipeline.yaml"
        self.endpoints_config = "config/endpoints.yaml"
        self.orchestration_config = "config/orchestration.yaml"
        self.source_dataset = "dataset_chunked"
        self.dataset = None
        self.bucket = "test-bucket"
        self.extraction_date = "2026-06-29"
        self.allow_default_date = False
        self.run_id = None
        self.pipeline_run_id = "test-run"
        self.dry_run = True
        self.output_dir = "tmp"
        for k, v in kwargs.items():
            setattr(self, k, v)


def test_parse_args_resolves_source_dataset_from_env(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["finalize_pronabec_dataset.py"])
    monkeypatch.setenv("SOURCE_DATASET", "dataset_from_env")

    args = parse_args()

    assert args.source_dataset is None


def test_parse_args_cli_source_dataset_has_priority_over_env(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        ["finalize_pronabec_dataset.py", "--source-dataset", "dataset_from_cli"],
    )
    monkeypatch.setenv("SOURCE_DATASET", "dataset_from_env")

    args = parse_args()

    assert args.source_dataset == "dataset_from_cli"


def test_parse_args_fails_without_source_dataset_or_env(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["finalize_pronabec_dataset.py"])
    monkeypatch.delenv("SOURCE_DATASET", raising=False)

    with pytest.raises(SystemExit):
        parse_args()

    assert "Missing required configuration: --source-dataset or SOURCE_DATASET" in capsys.readouterr().err


def create_mock_plan(tmp_path, dataset_name, expected_chunks, total_records, total_pages, chunk_size, chunks_list):
    plan_dir = tmp_path / "bronze_work" / "pronabec" / "_plans" / "extraction_date=2026-06-29" / "run_id=test-run"
    plan_dir.mkdir(parents=True, exist_ok=True)
    plan_data = {
        "source_system": "pronabec",
        "extraction_date": "2026-06-29",
        "pipeline_run_id": "test-run",
        "source_snapshot_observed_at": "2026-06-29T20:30:00Z",
        "status": "READY",
        "datasets": [
            {
                "source_dataset": dataset_name,
                "extraction_mode": "chunked" if chunk_size else "single",
                "effective_page_size": 1000,
                "total_records": total_records,
                "total_pages": total_pages,
                "chunk_size_pages": chunk_size,
                "max_parallel_chunks": 2,
                "expected_chunks": expected_chunks,
            }
        ],
        "chunks": chunks_list,
    }
    with open(plan_dir / "plan.json", "w", encoding="utf-8") as f:
        json.dump(plan_data, f)


def create_mock_chunk(tmp_path, dataset_name, chunk_id, page_start, page_end, records, status="SUCCESS", page_size=1000, data_content='{"id": 1}\n'):
    chunk_dir = (
        tmp_path
        / "bronze_work"
        / "pronabec"
        / dataset_name
        / "extraction_date=2026-06-29"
        / "run_id=test-run"
        / f"chunk_start={page_start}_chunk_end={page_end}"
    )
    chunk_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "source_system": "pronabec",
        "source_dataset": dataset_name,
        "extraction_date": "2026-06-29",
        "pipeline_run_id": "test-run",
        "page_start": page_start,
        "page_end": page_end,
        "effective_page_size": page_size,
        "records_written": records,
        "started_at": "2026-06-29T20:30:00Z",
        "finished_at": "2026-06-29T20:31:00Z",
        "status": status,
    }
    with open(chunk_dir / "chunk_manifest.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f)
    if data_content is not None:
        with open(chunk_dir / "data.jsonl", "w", encoding="utf-8") as f:
            f.write(data_content)


@patch("pipelines.finalize_pronabec_dataset.read_plan_json")
@patch("pipelines.finalize_pronabec_dataset.get_pipeline_settings")
@patch("pipelines.finalize_pronabec_dataset.load_orchestration_config")
def test_finalizer_resolves_runtime_values_from_env(
    mock_load_orch,
    mock_settings,
    mock_read_plan,
    mock_pipeline_settings,
    mock_orchestration,
    monkeypatch,
):
    mock_settings.return_value = {**mock_pipeline_settings, "bucket_name": None}
    mock_load_orch.return_value = mock_orchestration
    mock_read_plan.side_effect = RuntimeError("stop after config resolution")
    monkeypatch.setenv("SOURCE_DATASET", "dataset_chunked")
    monkeypatch.setenv("GCS_BUCKET_NAME", "bucket-from-env")
    monkeypatch.setenv("BRONZE_EXTRACTION_DATE", "2026-06-29")
    monkeypatch.setenv("PIPELINE_RUN_ID", "test-run")

    args = DummyArgs(
        dry_run=False,
        source_dataset=None,
        bucket=None,
        extraction_date=None,
        pipeline_run_id=None,
        run_id=None,
    )
    with pytest.raises(RuntimeError, match="stop after config resolution"):
        run_finalize(args)

    assert mock_read_plan.call_args.kwargs["bucket_name"] == "bucket-from-env"
    assert mock_read_plan.call_args.kwargs["extraction_date"] == "2026-06-29"
    assert mock_read_plan.call_args.kwargs["run_id"] == "test-run"


@patch("pipelines.finalize_pronabec_dataset.read_plan_json")
@patch("pipelines.finalize_pronabec_dataset.get_pipeline_settings")
@patch("pipelines.finalize_pronabec_dataset.load_orchestration_config")
def test_finalizer_cli_values_override_env(
    mock_load_orch,
    mock_settings,
    mock_read_plan,
    mock_pipeline_settings,
    mock_orchestration,
    monkeypatch,
):
    mock_settings.return_value = {**mock_pipeline_settings, "bucket_name": None}
    mock_load_orch.return_value = mock_orchestration
    mock_read_plan.side_effect = RuntimeError("stop after config resolution")
    monkeypatch.setenv("SOURCE_DATASET", "dataset_from_env")
    monkeypatch.setenv("GCS_BUCKET_NAME", "bucket-from-env")
    monkeypatch.setenv("BRONZE_EXTRACTION_DATE", "2026-06-28")
    monkeypatch.setenv("PIPELINE_RUN_ID", "run-from-env")

    args = DummyArgs(
        dry_run=False,
        source_dataset="dataset_chunked",
        bucket="bucket-from-cli",
        extraction_date="2026-06-29",
        pipeline_run_id="run-from-cli",
        run_id=None,
    )
    with pytest.raises(RuntimeError, match="stop after config resolution"):
        run_finalize(args)

    assert mock_read_plan.call_args.kwargs["bucket_name"] == "bucket-from-cli"
    assert mock_read_plan.call_args.kwargs["extraction_date"] == "2026-06-29"
    assert mock_read_plan.call_args.kwargs["run_id"] == "run-from-cli"


@patch("pipelines.finalize_pronabec_dataset.read_plan_json")
@patch("pipelines.finalize_pronabec_dataset.get_pipeline_settings")
@patch("pipelines.finalize_pronabec_dataset.load_orchestration_config")
def test_finalizer_resolves_bucket_from_legacy_gcs_bucket_env(
    mock_load_orch,
    mock_settings,
    mock_read_plan,
    mock_pipeline_settings,
    mock_orchestration,
    monkeypatch,
):
    mock_settings.return_value = {**mock_pipeline_settings, "bucket_name": None}
    mock_load_orch.return_value = mock_orchestration
    mock_read_plan.side_effect = RuntimeError("stop after config resolution")
    monkeypatch.setenv("SOURCE_DATASET", "dataset_chunked")
    monkeypatch.delenv("GCS_BUCKET_NAME", raising=False)
    monkeypatch.setenv("GCS_BUCKET", "legacy-bucket")
    monkeypatch.setenv("BRONZE_EXTRACTION_DATE", "2026-06-29")
    monkeypatch.setenv("PIPELINE_RUN_ID", "test-run")

    args = DummyArgs(
        dry_run=False,
        source_dataset=None,
        bucket=None,
        extraction_date=None,
        pipeline_run_id=None,
        run_id=None,
    )
    with pytest.raises(RuntimeError, match="stop after config resolution"):
        run_finalize(args)

    assert mock_read_plan.call_args.kwargs["bucket_name"] == "legacy-bucket"


@patch("pipelines.finalize_pronabec_dataset.get_pipeline_settings")
@patch("pipelines.finalize_pronabec_dataset.load_orchestration_config")
def test_finalizer_fails_if_plan_missing(
    mock_load_orch, mock_settings,
    mock_pipeline_settings, mock_orchestration, tmp_path
):
    mock_settings.return_value = mock_pipeline_settings
    mock_load_orch.return_value = mock_orchestration

    args = DummyArgs(dry_run=True, output_dir=str(tmp_path), source_dataset="dataset_chunked")
    # Plan missing throws FileNotFoundError
    with pytest.raises(FileNotFoundError, match="No se encontró plan.json"):
        run_finalize(args)


@patch("pipelines.finalize_pronabec_dataset.get_pipeline_settings")
@patch("pipelines.finalize_pronabec_dataset.load_orchestration_config")
def test_finalizer_fails_if_dataset_not_in_plan(
    mock_load_orch, mock_settings,
    mock_pipeline_settings, mock_orchestration, tmp_path
):
    mock_settings.return_value = mock_pipeline_settings
    mock_load_orch.return_value = mock_orchestration

    create_mock_plan(tmp_path, "dataset_single", 1, 100, 1, None, [])

    args = DummyArgs(dry_run=True, output_dir=str(tmp_path), source_dataset="dataset_chunked")
    with pytest.raises(ConfigError, match="no se encuentra en plan.json"):
        run_finalize(args)


@patch("pipelines.finalize_pronabec_dataset.get_pipeline_settings")
@patch("pipelines.finalize_pronabec_dataset.load_orchestration_config")
def test_finalizer_fails_if_chunk_manifest_missing(
    mock_load_orch, mock_settings,
    mock_pipeline_settings, mock_orchestration, tmp_path
):
    mock_settings.return_value = mock_pipeline_settings
    mock_load_orch.return_value = mock_orchestration

    chunks_list = [
        {
            "chunk_id": "dataset_chunked_0001",
            "source_dataset": "dataset_chunked",
            "page_start": 1,
            "page_end": 10,
            "effective_page_size": 1000,
            "required_for_e2e": True,
            "output_mode": "chunk"
        }
    ]
    create_mock_plan(tmp_path, "dataset_chunked", 1, 100, 10, 10, chunks_list)

    args = DummyArgs(dry_run=True, output_dir=str(tmp_path), source_dataset="dataset_chunked")
    with pytest.raises(FileNotFoundError, match="Falta manifest de chunk"):
        run_finalize(args)


@patch("pipelines.finalize_pronabec_dataset.get_pipeline_settings")
@patch("pipelines.finalize_pronabec_dataset.load_orchestration_config")
def test_finalizer_fails_if_chunk_failed(
    mock_load_orch, mock_settings,
    mock_pipeline_settings, mock_orchestration, tmp_path
):
    mock_settings.return_value = mock_pipeline_settings
    mock_load_orch.return_value = mock_orchestration

    chunks_list = [
        {
            "chunk_id": "dataset_chunked_0001",
            "source_dataset": "dataset_chunked",
            "page_start": 1,
            "page_end": 10,
            "effective_page_size": 1000,
            "required_for_e2e": True,
            "output_mode": "chunk"
        }
    ]
    create_mock_plan(tmp_path, "dataset_chunked", 1, 100, 10, 10, chunks_list)
    create_mock_chunk(tmp_path, "dataset_chunked", "dataset_chunked_0001", 1, 10, 100, status="FAILED")

    args = DummyArgs(dry_run=True, output_dir=str(tmp_path), source_dataset="dataset_chunked")
    with pytest.raises(ConfigError, match="falló durante la extracción"):
        run_finalize(args)


@patch("pipelines.finalize_pronabec_dataset.get_pipeline_settings")
@patch("pipelines.finalize_pronabec_dataset.load_orchestration_config")
def test_finalizer_fails_if_data_jsonl_missing(
    mock_load_orch, mock_settings,
    mock_pipeline_settings, mock_orchestration, tmp_path
):
    mock_settings.return_value = mock_pipeline_settings
    mock_load_orch.return_value = mock_orchestration

    chunks_list = [
        {
            "chunk_id": "dataset_chunked_0001",
            "source_dataset": "dataset_chunked",
            "page_start": 1,
            "page_end": 10,
            "effective_page_size": 1000,
            "required_for_e2e": True,
            "output_mode": "chunk"
        }
    ]
    create_mock_plan(tmp_path, "dataset_chunked", 1, 100, 10, 10, chunks_list)
    create_mock_chunk(tmp_path, "dataset_chunked", "dataset_chunked_0001", 1, 10, 100, status="SUCCESS", data_content=None)

    args = DummyArgs(dry_run=True, output_dir=str(tmp_path), source_dataset="dataset_chunked")
    with pytest.raises(FileNotFoundError, match="Falta archivo de datos de chunk"):
        run_finalize(args)


@patch("pipelines.finalize_pronabec_dataset.get_pipeline_settings")
@patch("pipelines.finalize_pronabec_dataset.load_orchestration_config")
def test_finalizer_fails_if_different_effective_page_size(
    mock_load_orch, mock_settings,
    mock_pipeline_settings, mock_orchestration, tmp_path
):
    mock_settings.return_value = mock_pipeline_settings
    mock_load_orch.return_value = mock_orchestration

    chunks_list = [
        {
            "chunk_id": "dataset_chunked_0001", "source_dataset": "dataset_chunked",
            "page_start": 1, "page_end": 10, "effective_page_size": 1000, "required_for_e2e": True, "output_mode": "chunk"
        },
        {
            "chunk_id": "dataset_chunked_0002", "source_dataset": "dataset_chunked",
            "page_start": 11, "page_end": 20, "effective_page_size": 1000, "required_for_e2e": True, "output_mode": "chunk"
        }
    ]
    create_mock_plan(tmp_path, "dataset_chunked", 2, 200, 20, 10, chunks_list)
    create_mock_chunk(tmp_path, "dataset_chunked", "dataset_chunked_0001", 1, 10, 100, page_size=1000)
    create_mock_chunk(tmp_path, "dataset_chunked", "dataset_chunked_0002", 11, 20, 100, page_size=500) # Different!

    args = DummyArgs(dry_run=True, output_dir=str(tmp_path), source_dataset="dataset_chunked")
    with pytest.raises(ConfigError, match="Diferentes effective_page_size detectados"):
        run_finalize(args)


@patch("pipelines.finalize_pronabec_dataset.get_pipeline_settings")
@patch("pipelines.finalize_pronabec_dataset.load_orchestration_config")
def test_finalizer_fails_on_gaps_or_overlaps(
    mock_load_orch, mock_settings,
    mock_pipeline_settings, mock_orchestration, tmp_path
):
    mock_settings.return_value = mock_pipeline_settings
    mock_load_orch.return_value = mock_orchestration

    # Gap: chunk 1 de 1 a 10, chunk 2 de 12 a 20
    chunks_list = [
        {"chunk_id": "c1", "source_dataset": "dataset_chunked", "page_start": 1, "page_end": 10, "effective_page_size": 1000, "required_for_e2e": True, "output_mode": "chunk"},
        {"chunk_id": "c2", "source_dataset": "dataset_chunked", "page_start": 12, "page_end": 20, "effective_page_size": 1000, "required_for_e2e": True, "output_mode": "chunk"}
    ]
    create_mock_plan(tmp_path, "dataset_chunked", 2, 200, 20, 10, chunks_list)

    args = DummyArgs(dry_run=True, output_dir=str(tmp_path), source_dataset="dataset_chunked")
    with pytest.raises(ConfigError, match="Detectado gap de páginas entre chunks"):
        run_finalize(args)

    # Overlap / rango duplicado en el plan
    chunks_list_dup = [
        {"chunk_id": "c1", "source_dataset": "dataset_chunked", "page_start": 1, "page_end": 10, "effective_page_size": 1000, "required_for_e2e": True, "output_mode": "chunk"},
        {"chunk_id": "c2", "source_dataset": "dataset_chunked", "page_start": 1, "page_end": 10, "effective_page_size": 1000, "required_for_e2e": True, "output_mode": "chunk"}
    ]
    create_mock_plan(tmp_path, "dataset_chunked", 2, 200, 20, 10, chunks_list_dup)
    with pytest.raises(ConfigError, match="Rango de páginas duplicado en el plan"):
        run_finalize(args)


@patch("pipelines.finalize_pronabec_dataset.get_pipeline_settings")
@patch("pipelines.finalize_pronabec_dataset.load_orchestration_config")
def test_finalizer_concatenation_manifest_and_success(
    mock_load_orch, mock_settings,
    mock_pipeline_settings, mock_orchestration, tmp_path
):
    mock_settings.return_value = mock_pipeline_settings
    mock_load_orch.return_value = mock_orchestration

    chunks_list = [
        {"chunk_id": "c1", "source_dataset": "dataset_chunked", "page_start": 1, "page_end": 10, "effective_page_size": 1000, "required_for_e2e": True, "output_mode": "chunk"},
        {"chunk_id": "c2", "source_dataset": "dataset_chunked", "page_start": 11, "page_end": 20, "effective_page_size": 1000, "required_for_e2e": True, "output_mode": "chunk"}
    ]
    create_mock_plan(tmp_path, "dataset_chunked", 2, 200, 20, 10, chunks_list)
    create_mock_chunk(tmp_path, "dataset_chunked", "c1", 1, 10, 100, data_content='{"id": 1}\n')
    create_mock_chunk(tmp_path, "dataset_chunked", "c2", 11, 20, 100, data_content='{"id": 2}\n')

    args = DummyArgs(dry_run=True, output_dir=str(tmp_path), source_dataset="dataset_chunked")
    run_finalize(args)

    final_dir = tmp_path / "bronze" / "pronabec" / "dataset_chunked" / "extraction_date=2026-06-29"
    final_data_p = final_dir / "data.jsonl"
    final_manifest_p = final_dir / "manifest.json"
    final_success_p = final_dir / "_SUCCESS"

    assert final_data_p.exists()
    assert final_manifest_p.exists()
    assert final_success_p.exists()

    # Validar concatenación ordenada por page_start
    with open(final_data_p, "r", encoding="utf-8") as f:
        lines = f.readlines()
    assert len(lines) == 2
    assert json.loads(lines[0]) == {"id": 1}
    assert json.loads(lines[1]) == {"id": 2}

    with open(final_manifest_p, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    assert manifest["expected_chunks"] == 2
    assert manifest["completed_chunks"] == 2
    assert manifest["records_written"] == 200
    assert manifest["status"] == "SUCCESS"


@patch("pipelines.finalize_pronabec_dataset.get_pipeline_settings")
@patch("pipelines.finalize_pronabec_dataset.load_orchestration_config")
def test_finalizer_records_written_mismatch(
    mock_load_orch, mock_settings,
    mock_pipeline_settings, mock_orchestration, tmp_path
):
    mock_settings.return_value = mock_pipeline_settings
    mock_load_orch.return_value = mock_orchestration

    chunks_list = [
        {"chunk_id": "c1", "source_dataset": "dataset_chunked", "page_start": 1, "page_end": 10, "effective_page_size": 1000, "required_for_e2e": True, "output_mode": "chunk"},
        {"chunk_id": "c2", "source_dataset": "dataset_chunked", "page_start": 11, "page_end": 20, "effective_page_size": 1000, "required_for_e2e": True, "output_mode": "chunk"}
    ]
    create_mock_plan(tmp_path, "dataset_chunked", 2, 200, 20, 10, chunks_list)
    # Suma de records is 150, but total_records is 200 in plan
    create_mock_chunk(tmp_path, "dataset_chunked", "c1", 1, 10, 100)
    create_mock_chunk(tmp_path, "dataset_chunked", "c2", 11, 20, 50)

    args = DummyArgs(dry_run=True, output_dir=str(tmp_path), source_dataset="dataset_chunked")
    # Debiera lanzar error de discrepancia
    with pytest.raises(ConfigError, match="Discrepancia en cantidad de registros"):
        run_finalize(args)


@patch("pipelines.finalize_pronabec_dataset.get_pipeline_settings")
@patch("pipelines.finalize_pronabec_dataset.load_orchestration_config")
def test_finalizer_records_written_mismatch_allowed(
    mock_load_orch, mock_settings,
    mock_pipeline_settings, mock_orchestration, tmp_path
):
    mock_settings.return_value = mock_pipeline_settings
    mock_load_orch.return_value = mock_orchestration

    chunks_list = [
        {"chunk_id": "c1", "source_dataset": "dataset_mismatch_allowed", "page_start": 1, "page_end": 1, "effective_page_size": 1000, "required_for_e2e": False, "output_mode": "chunk"}
    ]
    create_mock_plan(tmp_path, "dataset_mismatch_allowed", 1, 221, 1, None, chunks_list)
    create_mock_chunk(tmp_path, "dataset_mismatch_allowed", "c1", 1, 1, 223)

    args = DummyArgs(dry_run=True, output_dir=str(tmp_path), source_dataset="dataset_mismatch_allowed")
    # Permite continuar porque allow_record_count_mismatch es True en mock_orchestration
    run_finalize(args)

    final_dir = tmp_path / "bronze" / "pronabec" / "dataset_mismatch_allowed" / "extraction_date=2026-06-29"
    final_manifest_p = final_dir / "manifest.json"
    with open(final_manifest_p, "r") as f:
        manifest = json.load(f)
    assert manifest["records_written"] == 223


@patch("pipelines.finalize_pronabec_dataset.get_pipeline_settings")
@patch("pipelines.finalize_pronabec_dataset.load_orchestration_config")
def test_finalizer_empty_dataset(
    mock_load_orch, mock_settings,
    mock_pipeline_settings, mock_orchestration, tmp_path
):
    mock_settings.return_value = mock_pipeline_settings
    mock_load_orch.return_value = mock_orchestration

    chunks_list = [
        {"chunk_id": "c1", "source_dataset": "dataset_single", "page_start": 1, "page_end": 0, "effective_page_size": 1000, "required_for_e2e": False, "output_mode": "chunk"}
    ]
    create_mock_plan(tmp_path, "dataset_single", 1, 0, 0, None, chunks_list)
    # total_pages=0 -> chunk de 1 a 0, 0 records
    create_mock_chunk(tmp_path, "dataset_single", "c1", 1, 0, 0, data_content="")

    args = DummyArgs(dry_run=True, output_dir=str(tmp_path), source_dataset="dataset_single")
    run_finalize(args)

    final_dir = tmp_path / "bronze" / "pronabec" / "dataset_single" / "extraction_date=2026-06-29"
    final_data_p = final_dir / "data.jsonl"
    final_manifest_p = final_dir / "manifest.json"
    final_success_p = final_dir / "_SUCCESS"

    assert final_data_p.exists()
    assert final_manifest_p.exists()
    assert final_success_p.exists()

    with open(final_data_p, "r", encoding="utf-8") as f:
        content = f.read()
    assert content == ""

    with open(final_manifest_p, "r", encoding="utf-8") as f:
        manifest = json.load(f)
    assert manifest["records_written"] == 0
    assert manifest["total_records_observed"] == 0


@patch("pipelines.finalize_pronabec_dataset.get_pipeline_settings")
@patch("pipelines.finalize_pronabec_dataset.load_orchestration_config")
def test_finalizer_writes_bronze_only_manifest_flags(
    mock_load_orch, mock_settings,
    mock_pipeline_settings, mock_orchestration, tmp_path
):
    mock_settings.return_value = mock_pipeline_settings
    mock_load_orch.return_value = mock_orchestration

    chunks_list = [
        {
            "chunk_id": "c1",
            "source_dataset": "dataset_single",
            "page_start": 1,
            "page_end": 1,
            "effective_page_size": 1000,
            "bronze_enabled": True,
            "silver_enabled": False,
            "required_for_e2e": False,
            "output_mode": "chunk",
        }
    ]
    create_mock_plan(tmp_path, "dataset_single", 1, 1, 1, None, chunks_list)
    plan_file = tmp_path / "bronze_work" / "pronabec" / "_plans" / "extraction_date=2026-06-29" / "run_id=test-run" / "plan.json"
    with open(plan_file, "r", encoding="utf-8") as f:
        plan = json.load(f)
    plan["scope"] = "bronze_full"
    plan["datasets"][0]["bronze_enabled"] = True
    plan["datasets"][0]["silver_enabled"] = False
    plan["datasets"][0]["required_for_e2e"] = False
    with open(plan_file, "w", encoding="utf-8") as f:
        json.dump(plan, f)
    create_mock_chunk(tmp_path, "dataset_single", "c1", 1, 1, 1)

    args = DummyArgs(dry_run=True, output_dir=str(tmp_path), source_dataset="dataset_single")
    run_finalize(args)

    final_dir = tmp_path / "bronze" / "pronabec" / "dataset_single" / "extraction_date=2026-06-29"
    with open(final_dir / "manifest.json", "r", encoding="utf-8") as f:
        manifest = json.load(f)
    with open(final_dir / "_SUCCESS", "r", encoding="utf-8") as f:
        success = json.load(f)

    assert manifest["scope"] == "bronze_full"
    assert manifest["bronze_enabled"] is True
    assert manifest["silver_enabled"] is False
    assert manifest["required_for_e2e"] is False
    assert manifest["bronze_only"] is True
    assert success["bronze_only"] is True


def test_finalizer_structural_no_chunk_contents():
    code_path = Path(__file__).resolve().parents[1] / "pipelines" / "finalize_pronabec_dataset.py"
    code = code_path.read_text(encoding="utf-8")
    assert "chunk_contents" not in code, "Debe evitarse el uso de la variable chunk_contents para no acumular chunks en memoria."
    # Check that join on chunk_contents is not used
    assert "".join(["final_content", "=", '"".join']) not in code.replace(" ", "")


@patch("pipelines.finalize_pronabec_dataset.get_pipeline_settings")
@patch("pipelines.finalize_pronabec_dataset.load_orchestration_config")
def test_finalizer_handles_missing_trailing_newline_and_ordering(
    mock_load_orch, mock_settings,
    mock_pipeline_settings, mock_orchestration, tmp_path
):
    mock_settings.return_value = mock_pipeline_settings
    mock_load_orch.return_value = mock_orchestration

    chunks_list = [
        {"chunk_id": "c1", "source_dataset": "dataset_chunked", "page_start": 1, "page_end": 10, "effective_page_size": 1000, "required_for_e2e": True, "output_mode": "chunk"},
        {"chunk_id": "c2", "source_dataset": "dataset_chunked", "page_start": 11, "page_end": 20, "effective_page_size": 1000, "required_for_e2e": True, "output_mode": "chunk"}
    ]
    create_mock_plan(tmp_path, "dataset_chunked", 2, 200, 20, 10, chunks_list)
    create_mock_chunk(tmp_path, "dataset_chunked", "c1", 1, 10, 100, data_content='{"id": 1}')
    create_mock_chunk(tmp_path, "dataset_chunked", "c2", 11, 20, 100, data_content='{"id": 2}\n')

    args = DummyArgs(dry_run=True, output_dir=str(tmp_path), source_dataset="dataset_chunked")
    run_finalize(args)

    final_dir = tmp_path / "bronze" / "pronabec" / "dataset_chunked" / "extraction_date=2026-06-29"
    final_data_p = final_dir / "data.jsonl"
    final_manifest_p = final_dir / "manifest.json"

    assert final_data_p.exists()
    with open(final_data_p, "r", encoding="utf-8") as f:
        content = f.read()

    lines = content.splitlines(keepends=True)
    assert len(lines) == 2
    assert lines[0] == '{"id": 1}\n'
    assert lines[1] == '{"id": 2}\n'

    with open(final_manifest_p, "r", encoding="utf-8") as f:
        manifest = json.load(f)
    assert manifest["records_written"] == 200
    assert manifest["expected_chunks"] == 2
    assert manifest["completed_chunks"] == 2


@patch("pipelines.finalize_pronabec_dataset.get_pipeline_settings")
@patch("pipelines.finalize_pronabec_dataset.load_orchestration_config")
@patch("pipelines.finalize_pronabec_dataset.read_plan_json")
@patch("pipelines.finalize_pronabec_dataset.read_gcs_bytes")
@patch("pipelines.finalize_pronabec_dataset.upload_file")
@patch("pipelines.finalize_pronabec_dataset.upload_json")
def test_finalize_gcs_paths_and_uploads(
    mock_upload_json,
    mock_upload_file,
    mock_read_gcs_bytes,
    mock_read_plan,
    mock_load_orch,
    mock_settings,
    mock_pipeline_settings,
    mock_orchestration,
):
    mock_settings.return_value = mock_pipeline_settings
    mock_load_orch.return_value = mock_orchestration

    mock_read_plan.return_value = {
        "source_system": "pronabec",
        "extraction_date": "2026-06-29",
        "pipeline_run_id": "test-run",
        "source_snapshot_observed_at": "2026-06-29T20:30:00Z",
        "status": "READY",
        "datasets": [
            {
                "source_dataset": "dataset_chunked",
                "extraction_mode": "chunked",
                "effective_page_size": 1000,
                "total_records": 100,
                "total_pages": 10,
                "chunk_size_pages": 10,
                "max_parallel_chunks": 1,
                "expected_chunks": 1,
            }
        ],
        "chunks": [
            {"chunk_id": "c1", "source_dataset": "dataset_chunked", "page_start": 1, "page_end": 10, "effective_page_size": 1000, "required_for_e2e": True, "output_mode": "chunk"}
        ],
    }

    def mock_read_gcs(uri):
        if "chunk_manifest.json" in uri:
            manifest = {
                "status": "SUCCESS",
                "extraction_date": "2026-06-29",
                "pipeline_run_id": "test-run",
                "effective_page_size": 1000,
                "records_written": 100,
                "started_at": "2026-06-29T20:30:00Z",
                "finished_at": "2026-06-29T20:35:00Z",
            }
            return json.dumps(manifest).encode("utf-8")
        elif "data.jsonl" in uri:
            return b'{"id": 1}\n'
        raise FileNotFoundError(uri)

    mock_read_gcs_bytes.side_effect = mock_read_gcs

    args = DummyArgs(dry_run=False, bucket="test-bucket", source_dataset="dataset_chunked")
    run_finalize(args)

    mock_upload_file.assert_called_once()
    call_kwargs = mock_upload_file.call_args.kwargs
    assert call_kwargs["bucket_name"] == "test-bucket"
    assert call_kwargs["object_path"] == "bronze/pronabec/dataset_chunked/extraction_date=2026-06-29/data.jsonl"

    assert mock_upload_json.call_count == 2
    manifest_calls = [call.kwargs for call in mock_upload_json.call_args_list]
    paths = [c["object_path"] for c in manifest_calls]
    assert "bronze/pronabec/dataset_chunked/extraction_date=2026-06-29/manifest.json" in paths
    assert "bronze/pronabec/dataset_chunked/extraction_date=2026-06-29/_SUCCESS" in paths

