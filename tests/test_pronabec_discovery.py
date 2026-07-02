# -*- coding: utf-8 -*-
"""Pruebas unitarias para discover_pronabec.py."""

import json
from copy import deepcopy
import pytest
import requests
from unittest.mock import MagicMock, patch

from pipelines.common.config import ConfigError, load_yaml_config
from pipelines.common.orchestration_config import get_pronabec_dataset_policies
from pipelines.discover_pronabec import discover_dataset, run_discovery


@pytest.fixture
def mock_pipeline_settings():
    return {
        "log_level": "INFO",
        "bucket_name": "test-bucket",
    }


@pytest.fixture
def mock_endpoints():
    return {
        "pronabec": {
            "base_url": "https://fake.url",
            "endpoints": [
                {
                    "name": "becarios_pais_estudio",
                    "path": "/becarios-pais",
                    "expected_columns": [],
                },
                {
                    "name": "perdida_becas",
                    "path": "/perdida-becas",
                    "expected_columns": [],
                },
            ],
        }
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
                "discovery": {
                    "page_size_validation_mode": "full_pages",
                    "page_size_candidates": [5000, 3000, 2000, 1000, 500, 100],
                    "max_validation_pages": 1000,
                    "stop_on_first_stable_page_size": True,
                },
                "extraction_policies": [
                    {
                        "source_dataset": "becarios_pais_estudio",
                        "extraction_enabled": True,
                        "silver_enabled": True,
                        "extraction_mode": "chunked",
                        "required_for_e2e": True,
                        "chunk_size_pages": 10,
                        "max_parallel_chunks": 2,
                        "recommended_page_size": 1000,
                        "fallback_page_sizes": [500, 100],
                        "page_size_policy": "safe",
                    },
                    {
                        "source_dataset": "perdida_becas",
                        "extraction_enabled": False,
                        "silver_enabled": False,
                        "extraction_mode": "single",
                        "required_for_e2e": False,
                        "recommended_page_size": 1000,
                        "fallback_page_sizes": [500, 100],
                        "page_size_policy": "safe",
                    },
                ]
            },
            "pronabec_reports": {
                "enabled": False,
                "landing_path_template": "...",
                "landing_documents_path_template": "landing/reports/_documents",
                "bronze_path_template": "bronze/{extraction_date}",
                "silver_table_template": "p:d.t",
                "items_from": "...",
            },
        },
    }


class DummyArgs:
    def __init__(self, **kwargs):
        self.pipeline_config = "config/pipeline.yaml"
        self.endpoints_config = "config/endpoints.yaml"
        self.orchestration_config = "config/orchestration.yaml"
        self.source_dataset = None
        self.dataset = None
        self.allow_disabled_dataset = False
        self.scope = None
        self.bucket = "test-bucket"
        self.extraction_date = "2026-06-29"
        self.allow_default_date = False
        self.run_id = None
        self.pipeline_run_id = "test-run"
        self.dry_run = True
        self.output_dir = "tmp"
        self.timeout = None
        self.max_retries = None
        self.backoff_base_seconds = None
        self.backoff_max_seconds = None
        self.sleep_seconds = 0.2
        for k, v in kwargs.items():
            setattr(self, k, v)


@patch("pipelines.discover_pronabec.get_pipeline_settings")
@patch("pipelines.discover_pronabec.load_yaml_config")
@patch("pipelines.discover_pronabec.load_orchestration_config")
@patch("pipelines.discover_pronabec.fetch_pronabec_page")
def test_bronze_only_dataset_failure_aborts_discovery(
    mock_fetch, mock_load_orch, mock_load_endpoints, mock_settings,
    mock_pipeline_settings, mock_endpoints, mock_orchestration, tmp_path
):
    mock_settings.return_value = mock_pipeline_settings
    mock_load_endpoints.return_value = mock_endpoints
    mock_orch = dict(mock_orchestration)
    mock_orch["datasets"]["pronabec_api"]["extraction_policies"][1]["bronze_enabled"] = True
    mock_orch["datasets"]["pronabec_api"]["extraction_policies"][1]["extraction_enabled"] = True
    mock_load_orch.return_value = mock_orch

    mock_fetch.side_effect = requests.exceptions.HTTPError("HTTP 500", response=MagicMock(status_code=500))

    args = DummyArgs(
        dry_run=True,
        output_dir=str(tmp_path),
        source_dataset="perdida_becas"
    )
    with pytest.raises(SystemExit):
        run_discovery(args)

    plan_file = tmp_path / "bronze_work" / "pronabec" / "_plans" / "extraction_date=2026-06-29" / "run_id=test-run" / "discovery.json"
    assert plan_file.exists()
    with open(plan_file, "r") as f:
        data = json.load(f)
    assert data["status"] == "FAILED"
    assert data["datasets"][0]["status"] == "FAILED"
    assert data["datasets"][0]["validation_status"] == "FAILED"
    assert "error" in data["datasets"][0]


@patch("pipelines.discover_pronabec.get_pipeline_settings")
@patch("pipelines.discover_pronabec.load_yaml_config")
@patch("pipelines.discover_pronabec.load_orchestration_config")
@patch("pipelines.discover_pronabec.fetch_pronabec_page")
def test_discovery_includes_bronze_only_datasets_when_bronze_enabled(
    mock_fetch, mock_load_orch, mock_load_endpoints, mock_settings,
    mock_pipeline_settings, mock_endpoints, mock_orchestration, tmp_path
):
    mock_settings.return_value = mock_pipeline_settings
    mock_load_endpoints.return_value = mock_endpoints
    mock_orch = dict(mock_orchestration)
    policies = mock_orch["datasets"]["pronabec_api"]["extraction_policies"]
    policies[1]["bronze_enabled"] = True
    policies[1]["extraction_enabled"] = True
    mock_load_orch.return_value = mock_orch
    mock_fetch.return_value = {
        "records": 100,
        "total": 1,
        "rows": [{"id": 1, "cell": []}] * 100,
    }

    args = DummyArgs(dry_run=True, output_dir=str(tmp_path))
    run_discovery(args)

    plan_file = tmp_path / "bronze_work" / "pronabec" / "_plans" / "extraction_date=2026-06-29" / "run_id=test-run" / "discovery.json"
    with open(plan_file, "r") as f:
        data = json.load(f)

    assert [dataset["source_dataset"] for dataset in data["datasets"]] == [
        "becarios_pais_estudio",
        "perdida_becas",
    ]
    assert data["datasets"][1]["bronze_enabled"] is True
    assert data["datasets"][1]["silver_enabled"] is False
    assert data["datasets"][1]["required_for_e2e"] is False


def _retry_settings() -> dict:
    return {
        "timeout": 30,
        "max_retries": 1,
        "backoff_base_seconds": 0,
        "backoff_max_seconds": 0,
    }


def _payload(total_records: int, total_pages: int, rows: int = 1) -> dict:
    return {
        "records": total_records,
        "total": total_pages,
        "rows": [{"id": index} for index in range(rows)],
    }


@patch("pipelines.discover_pronabec.fetch_pronabec_page")
def test_discovery_rejects_candidate_when_later_page_fails(
    mock_fetch,
    mock_orchestration,
):
    orchestration = deepcopy(mock_orchestration)
    raw_policy = orchestration["datasets"]["pronabec_api"]["extraction_policies"][0]
    raw_policy["recommended_page_size"] = 5000
    raw_policy["fallback_page_sizes"] = [3000, 2000, 1000, 500, 100]
    policy = get_pronabec_dataset_policies(orchestration)[0]
    endpoint = {"name": "becarios_pais_estudio", "path": "/becarios-pais"}

    def fake_fetch(**kwargs):
        rows = kwargs["rows"]
        page = kwargs["page"]
        if rows == 5000:
            if page == 6:
                raise requests.exceptions.HTTPError(
                    "HTTP 500",
                    response=MagicMock(status_code=500),
                )
            return _payload(total_records=30000, total_pages=6, rows=1)
        if rows == 3000:
            return _payload(total_records=12000, total_pages=4, rows=1)
        raise AssertionError(f"unexpected rows={rows}")

    mock_fetch.side_effect = fake_fetch

    result = discover_dataset(
        endpoint=endpoint,
        policy=policy,
        orchestration_config=orchestration,
        base_url="https://fake.url",
        retry_settings=_retry_settings(),
        logger=MagicMock(),
    )

    assert result["effective_page_size"] == 3000
    assert result["validation_status"] == "SUCCESS"
    assert result["page_size_validation_mode"] == "full_pages"
    assert result["validated_pages"] == 4
    assert result["rejected_page_sizes"][0]["page_size"] == 5000
    assert result["rejected_page_sizes"][0]["failed_page"] == 6
    assert result["rejected_page_sizes"][0]["status_code"] == 500


@patch("pipelines.discover_pronabec.fetch_pronabec_page")
def test_discovery_validates_all_pages_before_accepting_page_size(
    mock_fetch,
    mock_orchestration,
):
    orchestration = deepcopy(mock_orchestration)
    raw_policy = orchestration["datasets"]["pronabec_api"]["extraction_policies"][0]
    raw_policy["recommended_page_size"] = 5000
    raw_policy["fallback_page_sizes"] = [3000, 2000, 1000, 500, 100]
    policy = get_pronabec_dataset_policies(orchestration)[0]
    endpoint = {"name": "becarios_pais_estudio", "path": "/becarios-pais"}
    pages_seen: list[int] = []

    def fake_fetch(**kwargs):
        assert kwargs["rows"] == 5000
        pages_seen.append(kwargs["page"])
        return _payload(total_records=25000, total_pages=5, rows=1)

    mock_fetch.side_effect = fake_fetch

    result = discover_dataset(
        endpoint=endpoint,
        policy=policy,
        orchestration_config=orchestration,
        base_url="https://fake.url",
        retry_settings=_retry_settings(),
        logger=MagicMock(),
    )

    assert pages_seen == [1, 2, 3, 4, 5]
    assert result["effective_page_size"] == 5000
    assert result["validated_pages"] == 5


@patch("pipelines.discover_pronabec.fetch_pronabec_page")
def test_discovery_fails_dataset_when_all_page_sizes_fail(
    mock_fetch,
    mock_orchestration,
    mock_pipeline_settings,
    mock_endpoints,
    tmp_path,
):
    mock_orch = deepcopy(mock_orchestration)
    policy = mock_orch["datasets"]["pronabec_api"]["extraction_policies"][0]
    policy["bronze_enabled"] = True
    policy["extraction_enabled"] = True

    mock_fetch.side_effect = requests.exceptions.HTTPError(
        "HTTP 500",
        response=MagicMock(status_code=500),
    )

    with patch("pipelines.discover_pronabec.get_pipeline_settings", return_value=mock_pipeline_settings), \
        patch("pipelines.discover_pronabec.load_yaml_config", return_value=mock_endpoints), \
        patch("pipelines.discover_pronabec.load_orchestration_config", return_value=mock_orch):
        args = DummyArgs(
            dry_run=True,
            output_dir=str(tmp_path),
            source_dataset="becarios_pais_estudio",
        )
        with pytest.raises(SystemExit):
            run_discovery(args)

    discovery_file = tmp_path / "bronze_work" / "pronabec" / "_plans" / "extraction_date=2026-06-29" / "run_id=test-run" / "discovery.json"
    with open(discovery_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    dataset = data["datasets"][0]
    assert data["status"] == "FAILED"
    assert dataset["status"] == "FAILED"
    assert dataset["validation_status"] == "FAILED"
    assert dataset["rejected_page_sizes"]
