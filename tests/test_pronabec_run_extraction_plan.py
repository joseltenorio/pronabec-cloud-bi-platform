# -*- coding: utf-8 -*-
"""Pruebas para el runner plan-driven de PRONABEC."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipelines.common.config import ConfigError
from pipelines.run_pronabec_extraction_plan import run_extraction_plan


def _base_args(**overrides):
    args = SimpleNamespace(
        pipeline_config="config/pipeline.yaml",
        endpoints_config="config/endpoints.yaml",
        orchestration_config="config/orchestration.yaml",
        source_dataset=None,
        dataset=None,
        bucket="test-bucket",
        extraction_date="2026-06-29",
        allow_default_date=False,
        run_id=None,
        pipeline_run_id="manual-run",
        timeout=None,
        max_retries=None,
        backoff_base_seconds=None,
        backoff_max_seconds=None,
        dry_run=True,
        output_dir="tmp",
        max_workers=1,
    )
    for key, value in overrides.items():
        setattr(args, key, value)
    return args


def _base_settings():
    return {"bucket_name": "test-bucket", "log_level": "INFO"}


def _base_endpoints():
    return {
        "pronabec": {
            "base_url": "https://example.invalid",
            "endpoints": [
                {"name": "convocatorias_carrera_sede", "path": "/convocatorias_carrera_sede"},
                {"name": "becarios_pais_estudio", "path": "/becarios_pais_estudio"},
            ],
        }
    }


def _base_plan():
    return {
        "source_system": "pronabec",
        "extraction_date": "2026-06-29",
        "pipeline_run_id": "manual-run",
        "source_snapshot_observed_at": "2026-06-29T00:00:00Z",
        "status": "READY",
        "datasets": [
            {
                "source_dataset": "convocatorias_carrera_sede",
                "extraction_mode": "chunked",
                "effective_page_size": 5000,
                "total_records": 395633,
                "total_pages": 80,
                "chunk_size_pages": 10,
                "max_parallel_chunks": 2,
                "expected_chunks": 2,
            },
            {
                "source_dataset": "becarios_pais_estudio",
                "extraction_mode": "single",
                "effective_page_size": 10000,
                "total_records": 91000,
                "total_pages": 9,
                "chunk_size_pages": None,
                "max_parallel_chunks": 1,
                "expected_chunks": 1,
            },
        ],
        "chunks": [
            {
                "chunk_id": "convocatorias_carrera_sede_0001",
                "source_dataset": "convocatorias_carrera_sede",
                "page_start": 1,
                "page_end": 10,
                "effective_page_size": 5000,
                "requested_page_size": 10000,
                "required_for_e2e": False,
            },
            {
                "chunk_id": "convocatorias_carrera_sede_0002",
                "source_dataset": "convocatorias_carrera_sede",
                "page_start": 11,
                "page_end": 20,
                "effective_page_size": 5000,
                "requested_page_size": 10000,
                "required_for_e2e": False,
            },
            {
                "chunk_id": "becarios_pais_estudio_0001",
                "source_dataset": "becarios_pais_estudio",
                "page_start": 1,
                "page_end": 9,
                "effective_page_size": 10000,
                "requested_page_size": 10000,
                "required_for_e2e": True,
            },
        ],
    }


@patch("pipelines.run_pronabec_extraction_plan.get_pipeline_settings")
@patch("pipelines.run_pronabec_extraction_plan.load_yaml_config")
@patch("pipelines.run_pronabec_extraction_plan.load_orchestration_config")
@patch("pipelines.run_pronabec_extraction_plan.read_plan_json")
@patch("pipelines.run_pronabec_extraction_plan.setup_structured_logger")
@patch("pipelines.run_pronabec_extraction_plan.extract_dataset")
@patch("pipelines.run_pronabec_extraction_plan.write_chunk_dataset_to_local")
@patch("pipelines.run_pronabec_extraction_plan.write_chunk_dataset_to_gcs")
@patch("pipelines.run_pronabec_extraction_plan._read_chunk_manifest")
def test_runner_executes_exact_plan_chunks(
    mock_read_chunk_manifest,
    mock_write_gcs,
    mock_write_local,
    mock_extract_dataset,
    mock_logger_factory,
    mock_read_plan,
    mock_load_orch,
    mock_load_yaml,
    mock_settings,
):
    mock_settings.return_value = _base_settings()
    mock_load_yaml.return_value = _base_endpoints()
    mock_load_orch.return_value = {"datasets": {"pronabec_api": {"extraction_policies": []}}}
    mock_read_plan.return_value = _base_plan()
    mock_logger_factory.return_value = MagicMock()
    mock_read_chunk_manifest.return_value = None
    mock_extract_dataset.side_effect = [
        ({"requested_page_size": 10000, "effective_page_size": 5000, "reported_records": 395633, "total_pages": 80}, [{"id": 1}]),
        ({"requested_page_size": 10000, "effective_page_size": 5000, "reported_records": 395633, "total_pages": 80}, [{"id": 2}]),
        ({"requested_page_size": 10000, "effective_page_size": 10000, "reported_records": 91000, "total_pages": 9}, [{"id": 3}]),
    ]
    mock_write_local.return_value = {"normalized_uri": "local/data.jsonl", "chunk_manifest_uri": "local/chunk_manifest.json"}

    summary = run_extraction_plan(_base_args())

    assert summary["total_chunks"] == 3
    assert summary["completed_chunks"] == 3
    assert summary["skipped_chunks"] == 0
    assert summary["failed_chunks"] == 0
    assert summary["datasets_processed"] == ["becarios_pais_estudio", "convocatorias_carrera_sede"]
    assert mock_write_gcs.call_count == 0
    assert mock_extract_dataset.call_count == 3
    assert mock_extract_dataset.call_args_list[0].kwargs["rows_per_page"] == 10000
    assert mock_extract_dataset.call_args_list[0].kwargs["page_start"] == 1
    assert mock_extract_dataset.call_args_list[0].kwargs["page_end"] == 9
    assert mock_extract_dataset.call_args_list[1].kwargs["rows_per_page"] == 5000
    assert mock_extract_dataset.call_args_list[1].kwargs["page_start"] == 1
    assert mock_extract_dataset.call_args_list[1].kwargs["page_end"] == 10
    assert mock_extract_dataset.call_args_list[2].kwargs["rows_per_page"] == 5000
    assert mock_extract_dataset.call_args_list[2].kwargs["page_start"] == 11
    assert mock_extract_dataset.call_args_list[2].kwargs["page_end"] == 20


@patch("pipelines.run_pronabec_extraction_plan.get_pipeline_settings")
@patch("pipelines.run_pronabec_extraction_plan.load_yaml_config")
@patch("pipelines.run_pronabec_extraction_plan.load_orchestration_config")
@patch("pipelines.run_pronabec_extraction_plan.read_plan_json")
@patch("pipelines.run_pronabec_extraction_plan.setup_structured_logger")
@patch("pipelines.run_pronabec_extraction_plan.extract_dataset")
@patch("pipelines.run_pronabec_extraction_plan.write_chunk_dataset_to_local")
@patch("pipelines.run_pronabec_extraction_plan._read_chunk_manifest")
def test_runner_skips_already_completed_chunks(
    mock_read_chunk_manifest,
    mock_write_local,
    mock_extract_dataset,
    mock_logger_factory,
    mock_read_plan,
    mock_load_orch,
    mock_load_yaml,
    mock_settings,
):
    plan = _base_plan()
    mock_settings.return_value = _base_settings()
    mock_load_yaml.return_value = _base_endpoints()
    mock_load_orch.return_value = {"datasets": {"pronabec_api": {"extraction_policies": []}}}
    mock_read_plan.return_value = plan
    mock_logger_factory.return_value = MagicMock()
    mock_read_chunk_manifest.side_effect = [
        {"status": "SUCCESS"},
        None,
        None,
    ]
    mock_extract_dataset.return_value = (
        {"requested_page_size": 10000, "effective_page_size": 5000, "reported_records": 395633, "total_pages": 80},
        [{"id": 1}],
    )
    mock_write_local.return_value = {"normalized_uri": "local/data.jsonl", "chunk_manifest_uri": "local/chunk_manifest.json"}

    summary = run_extraction_plan(_base_args(source_dataset="convocatorias_carrera_sede"))

    assert summary["total_chunks"] == 2
    assert summary["skipped_chunks"] == 1
    assert summary["completed_chunks"] == 1
    assert mock_extract_dataset.call_count == 1


@patch("pipelines.run_pronabec_extraction_plan.get_pipeline_settings")
@patch("pipelines.run_pronabec_extraction_plan.load_yaml_config")
@patch("pipelines.run_pronabec_extraction_plan.load_orchestration_config")
@patch("pipelines.run_pronabec_extraction_plan.read_plan_json")
@patch("pipelines.run_pronabec_extraction_plan.setup_structured_logger")
def test_runner_fails_when_plan_missing(
    mock_logger_factory,
    mock_read_plan,
    mock_load_orch,
    mock_load_yaml,
    mock_settings,
):
    mock_settings.return_value = _base_settings()
    mock_load_yaml.return_value = _base_endpoints()
    mock_load_orch.return_value = {"datasets": {"pronabec_api": {"extraction_policies": []}}}
    mock_logger_factory.return_value = MagicMock()
    mock_read_plan.side_effect = FileNotFoundError("No se encontro plan.json")

    with pytest.raises(FileNotFoundError, match="plan.json"):
        run_extraction_plan(_base_args())


@patch("pipelines.run_pronabec_extraction_plan.get_pipeline_settings")
@patch("pipelines.run_pronabec_extraction_plan.load_yaml_config")
@patch("pipelines.run_pronabec_extraction_plan.load_orchestration_config")
@patch("pipelines.run_pronabec_extraction_plan.read_plan_json")
@patch("pipelines.run_pronabec_extraction_plan.setup_structured_logger")
def test_runner_fails_when_plan_is_not_ready(
    mock_logger_factory,
    mock_read_plan,
    mock_load_orch,
    mock_load_yaml,
    mock_settings,
):
    mock_settings.return_value = _base_settings()
    mock_load_yaml.return_value = _base_endpoints()
    mock_load_orch.return_value = {"datasets": {"pronabec_api": {"extraction_policies": []}}}
    mock_logger_factory.return_value = MagicMock()
    plan = _base_plan()
    plan["status"] = "DRAFT"
    mock_read_plan.return_value = plan

    with pytest.raises(ConfigError, match="status READY"):
        run_extraction_plan(_base_args())


@patch("pipelines.run_pronabec_extraction_plan.get_pipeline_settings")
@patch("pipelines.run_pronabec_extraction_plan.load_yaml_config")
@patch("pipelines.run_pronabec_extraction_plan.load_orchestration_config")
@patch("pipelines.run_pronabec_extraction_plan.read_plan_json")
@patch("pipelines.run_pronabec_extraction_plan.setup_structured_logger")
def test_runner_fails_when_dataset_filter_matches_no_chunks(
    mock_logger_factory,
    mock_read_plan,
    mock_load_orch,
    mock_load_yaml,
    mock_settings,
):
    mock_settings.return_value = _base_settings()
    mock_load_yaml.return_value = _base_endpoints()
    mock_load_orch.return_value = {"datasets": {"pronabec_api": {"extraction_policies": []}}}
    mock_logger_factory.return_value = MagicMock()
    mock_read_plan.return_value = _base_plan()

    with pytest.raises(ConfigError, match="No hay chunks para ejecutar"):
        run_extraction_plan(_base_args(source_dataset="dataset_inexistente"))
