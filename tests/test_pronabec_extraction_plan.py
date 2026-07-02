# -*- coding: utf-8 -*-
"""Pruebas de integración para build_pronabec_extraction_plan.py."""

import json
import pytest
from unittest.mock import MagicMock, patch

from pipelines.build_pronabec_extraction_plan import build_plan, run_build_plan
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
                        "source_dataset": "becarios_pais_estudio",
                        "extraction_enabled": True,
                        "bronze_enabled": True,
                        "silver_enabled": True,
                        "extraction_mode": "chunked",
                        "required_for_e2e": True,
                        "chunk_size_pages": 10,
                        "max_parallel_chunks": 2,
                        "recommended_page_size": 1000,
                        "fallback_page_sizes": [500, 100],
                        "page_size_policy": "safe",
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
        self.source_dataset = None
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


def stable_discovery_fields(total_pages: int) -> dict:
    return {
        "page_size_validation_mode": "full_pages",
        "validation_status": "SUCCESS",
        "validated_pages": total_pages,
    }


@patch("pipelines.build_pronabec_extraction_plan.get_pipeline_settings")
@patch("pipelines.build_pronabec_extraction_plan.load_orchestration_config")
@patch("pipelines.build_pronabec_extraction_plan.read_discovery_json")
def test_build_plan_pipeline_integration(
    mock_read_discovery, mock_load_orch, mock_settings,
    mock_pipeline_settings, mock_orchestration, tmp_path
):
    mock_settings.return_value = mock_pipeline_settings
    mock_load_orch.return_value = mock_orchestration

    # Mock discovery data
    mock_read_discovery.return_value = {
        "source_system": "pronabec",
        "extraction_date": "2026-06-29",
        "pipeline_run_id": "test-run",
        "source_snapshot_observed_at": "2026-06-29T20:30:00Z",
        "status": "SUCCESS",
        "datasets": [
            {
                "source_dataset": "becarios_pais_estudio",
                "extraction_enabled": True,
                "bronze_enabled": True,
                "silver_enabled": True,
                "required_for_e2e": True,
                "extraction_mode": "chunked",
                "recommended_page_size": 1000,
                "fallback_page_sizes": [500, 100],
                "effective_page_size": 500,
                "total_records": 1200,
                "total_pages": 3,
                "actual_records_returned": 500,
                "status": "SUCCESS",
                "elapsed_seconds": 1.0,
                **stable_discovery_fields(3),
            }
        ]
    }

    args = DummyArgs(dry_run=True, output_dir=str(tmp_path))
    run_build_plan(args)

    plan_file = tmp_path / "bronze_work" / "pronabec" / "_plans" / "extraction_date=2026-06-29" / "run_id=test-run" / "plan.json"
    assert plan_file.exists()

    with open(plan_file, "r") as f:
        plan = json.load(f)

    assert plan["source_system"] == "pronabec"
    assert plan["extraction_date"] == "2026-06-29"
    assert plan["pipeline_run_id"] == "test-run"
    assert plan["source_snapshot_observed_at"] == "2026-06-29T20:30:00Z"
    assert plan["status"] == "READY"
    assert len(plan["datasets"]) == 1
    assert len(plan["chunks"]) == 1 # 3 pages with chunk size 10 -> 1 chunk
    assert plan["datasets"][0]["bronze_enabled"] is True
    assert plan["datasets"][0]["silver_enabled"] is True
    assert plan["datasets"][0]["required_for_e2e"] is True
    assert plan["chunks"][0]["bronze_enabled"] is True
    assert plan["chunks"][0]["silver_enabled"] is True
    assert plan["chunks"][0]["required_for_e2e"] is True
    assert plan["datasets"][0]["validation_status"] == "SUCCESS"
    assert plan["datasets"][0]["page_size_validation_mode"] == "full_pages"
    assert plan["chunks"][0]["effective_page_size"] == 500


def test_build_plan_fails_when_bronze_dataset_failed() -> None:
    orchestration = {
        "datasets": {
            "pronabec_api": {
                "extraction_policies": [
                    {
                        "source_dataset": "notas_becarios",
                        "extraction_enabled": True,
                        "bronze_enabled": True,
                        "silver_enabled": False,
                        "extraction_mode": "single",
                        "required_for_e2e": False,
                        "chunk_size_pages": None,
                        "max_parallel_chunks": 1,
                        "recommended_page_size": 10000,
                        "fallback_page_sizes": [5000, 2000, 1000, 500, 100],
                        "max_page_size_tested_ok": 20000,
                        "page_size_policy": "dataset_safe_default",
                    }
                ]
            }
        }
    }
    discovery = {
        "source_system": "pronabec",
        "extraction_date": "2026-06-29",
        "pipeline_run_id": "test-run",
        "source_snapshot_observed_at": "2026-06-29T20:30:00Z",
        "status": "FAILED",
        "datasets": [
            {
                "source_dataset": "notas_becarios",
                "extraction_enabled": True,
                "bronze_enabled": True,
                "silver_enabled": False,
                "required_for_e2e": False,
                "extraction_mode": "single",
                "recommended_page_size": 10000,
                "fallback_page_sizes": [5000, 2000, 1000, 500, 100],
                "effective_page_size": 10000,
                "total_records": 0,
                "total_pages": 0,
                "actual_records_returned": 0,
                "status": "FAILED",
                "error": "HTTP 500",
            }
        ],
    }

    with pytest.raises(ConfigError, match="datasets Bronze habilitados"):
        build_plan(discovery, orchestration, None)


def test_build_plan_fails_when_convocatorias_carrera_sede_failed() -> None:
    orchestration = {
        "datasets": {
            "pronabec_api": {
                "extraction_policies": [
                    {
                        "source_dataset": "convocatorias_carrera_sede",
                        "extraction_enabled": True,
                        "bronze_enabled": True,
                        "silver_enabled": False,
                        "extraction_mode": "chunked",
                        "required_for_e2e": False,
                        "chunk_size_pages": 10,
                        "max_parallel_chunks": 2,
                        "recommended_page_size": 5000,
                        "fallback_page_sizes": [2000, 1000, 500, 100],
                        "max_page_size_tested_ok": 5000,
                        "page_size_policy": "dataset_safe_default",
                    }
                ]
            }
        }
    }
    discovery = {
        "source_system": "pronabec",
        "extraction_date": "2026-06-29",
        "pipeline_run_id": "test-run",
        "source_snapshot_observed_at": "2026-06-29T20:30:00Z",
        "status": "FAILED",
        "datasets": [
            {
                "source_dataset": "convocatorias_carrera_sede",
                "extraction_enabled": True,
                "bronze_enabled": True,
                "silver_enabled": False,
                "required_for_e2e": False,
                "extraction_mode": "chunked",
                "recommended_page_size": 5000,
                "fallback_page_sizes": [2000, 1000, 500, 100],
                "effective_page_size": 5000,
                "total_records": 0,
                "total_pages": 0,
                "actual_records_returned": 0,
                "status": "FAILED",
                "error": "HTTP 500",
            }
        ],
    }

    with pytest.raises(ConfigError, match="datasets Bronze habilitados"):
        build_plan(discovery, orchestration, None)


def test_build_plan_includes_bronze_only_successful_datasets() -> None:
    orchestration = {
        "datasets": {
            "pronabec_api": {
                "extraction_policies": [
                    {
                        "source_dataset": "convocatorias_carrera_sede",
                        "extraction_enabled": True,
                        "bronze_enabled": True,
                        "silver_enabled": False,
                        "extraction_mode": "chunked",
                        "required_for_e2e": False,
                        "chunk_size_pages": 10,
                        "max_parallel_chunks": 2,
                        "recommended_page_size": 5000,
                        "fallback_page_sizes": [2000, 1000, 500, 100],
                        "max_page_size_tested_ok": 5000,
                        "page_size_policy": "dataset_safe_default",
                    }
                ]
            }
        }
    }
    discovery = {
        "source_system": "pronabec",
        "extraction_date": "2026-06-29",
        "pipeline_run_id": "test-run",
        "source_snapshot_observed_at": "2026-06-29T20:30:00Z",
        "status": "SUCCESS",
        "datasets": [
            {
                "source_dataset": "convocatorias_carrera_sede",
                "extraction_enabled": True,
                "bronze_enabled": True,
                "silver_enabled": False,
                "required_for_e2e": False,
                "extraction_mode": "chunked",
                "recommended_page_size": 5000,
                "fallback_page_sizes": [2000, 1000, 500, 100],
                "effective_page_size": 5000,
                "total_records": 395633,
                "total_pages": 80,
                "actual_records_returned": 5000,
                "status": "SUCCESS",
                "elapsed_seconds": 1.0,
                **stable_discovery_fields(80),
            }
        ],
    }

    plan = build_plan(discovery, orchestration, None)

    assert plan["status"] == "READY"
    assert plan["datasets"][0]["source_dataset"] == "convocatorias_carrera_sede"
    assert plan["datasets"][0]["bronze_enabled"] is True
    assert plan["datasets"][0]["silver_enabled"] is False
    assert plan["datasets"][0]["required_for_e2e"] is False
    assert len(plan["chunks"]) == 8


def test_build_plan_requires_stable_discovery() -> None:
    orchestration = {
        "datasets": {
            "pronabec_api": {
                "extraction_policies": [
                    {
                        "source_dataset": "notas_becarios",
                        "extraction_enabled": True,
                        "bronze_enabled": True,
                        "silver_enabled": False,
                        "extraction_mode": "single",
                        "required_for_e2e": False,
                        "chunk_size_pages": None,
                        "max_parallel_chunks": 1,
                        "recommended_page_size": 10000,
                        "fallback_page_sizes": [5000, 2000, 1000, 500, 100],
                        "max_page_size_tested_ok": 20000,
                        "page_size_policy": "dataset_safe_default",
                    }
                ]
            }
        }
    }
    discovery = {
        "source_system": "pronabec",
        "extraction_date": "2026-06-29",
        "pipeline_run_id": "test-run",
        "source_snapshot_observed_at": "2026-06-29T20:30:00Z",
        "status": "SUCCESS",
        "datasets": [
            {
                "source_dataset": "notas_becarios",
                "extraction_enabled": True,
                "bronze_enabled": True,
                "silver_enabled": False,
                "required_for_e2e": False,
                "extraction_mode": "single",
                "recommended_page_size": 10000,
                "fallback_page_sizes": [5000, 2000, 1000, 500, 100],
                "effective_page_size": 10000,
                "total_records": 103230,
                "total_pages": 11,
                "actual_records_returned": 10000,
                "status": "SUCCESS",
                "elapsed_seconds": 1.0,
            }
        ],
    }

    with pytest.raises(ConfigError, match="validation_status"):
        build_plan(discovery, orchestration, None)


def test_build_plan_uses_effective_page_size_from_stable_discovery() -> None:
    orchestration = {
        "datasets": {
            "pronabec_api": {
                "extraction_policies": [
                    {
                        "source_dataset": "colegios_habiles",
                        "extraction_enabled": True,
                        "bronze_enabled": True,
                        "silver_enabled": True,
                        "extraction_mode": "single",
                        "required_for_e2e": True,
                        "chunk_size_pages": None,
                        "max_parallel_chunks": 1,
                        "recommended_page_size": 5000,
                        "fallback_page_sizes": [3000, 2000, 1000, 500, 100],
                        "max_page_size_tested_ok": 5000,
                        "page_size_policy": "dataset_safe_default",
                    }
                ]
            }
        }
    }
    discovery = {
        "source_system": "pronabec",
        "extraction_date": "2026-06-29",
        "pipeline_run_id": "test-run",
        "source_snapshot_observed_at": "2026-06-29T20:30:00Z",
        "status": "SUCCESS",
        "datasets": [
            {
                "source_dataset": "colegios_habiles",
                "extraction_enabled": True,
                "bronze_enabled": True,
                "silver_enabled": True,
                "required_for_e2e": True,
                "extraction_mode": "single",
                "recommended_page_size": 5000,
                "fallback_page_sizes": [3000, 2000, 1000, 500, 100],
                "effective_page_size": 3000,
                "total_records": 71605,
                "total_pages": 24,
                "actual_records_returned": 3000,
                "status": "SUCCESS",
                "elapsed_seconds": 1.0,
                **stable_discovery_fields(24),
            }
        ],
    }

    plan = build_plan(discovery, orchestration, None)

    assert plan["datasets"][0]["effective_page_size"] == 3000
    assert plan["chunks"][0]["effective_page_size"] == 3000
