# -*- coding: utf-8 -*-
"""Pruebas de integración para build_pronabec_extraction_plan.py."""

import json
import pytest
from unittest.mock import MagicMock, patch

from pipelines.build_pronabec_extraction_plan import run_build_plan


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
