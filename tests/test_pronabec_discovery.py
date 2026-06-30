# -*- coding: utf-8 -*-
"""Pruebas unitarias para discover_pronabec.py."""

import json
import pytest
import requests
from unittest.mock import MagicMock, patch

from pipelines.common.config import ConfigError, load_yaml_config
from pipelines.discover_pronabec import run_discovery


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
def test_discovery_includes_only_enabled_datasets(
    mock_fetch, mock_load_orch, mock_load_endpoints, mock_settings,
    mock_pipeline_settings, mock_endpoints, mock_orchestration, tmp_path
):
    mock_settings.return_value = mock_pipeline_settings
    mock_load_endpoints.return_value = mock_endpoints
    mock_load_orch.return_value = mock_orchestration

    # Mock response for successful fetch
    mock_fetch.return_value = {
        "records": 2500,
        "total": 3,
        "rows": [{"id": 1, "cell": []}] * 1000
    }

    args = DummyArgs(dry_run=True, output_dir=str(tmp_path))
    run_discovery(args)

    plan_file = tmp_path / "bronze_work" / "pronabec" / "_plans" / "extraction_date=2026-06-29" / "run_id=test-run" / "discovery.json"
    assert plan_file.exists()

    with open(plan_file, "r") as f:
        data = json.load(f)

    assert data["source_system"] == "pronabec"
    assert data["extraction_date"] == "2026-06-29"
    assert data["pipeline_run_id"] == "test-run"
    assert "source_snapshot_observed_at" in data
    assert data["status"] == "SUCCESS"

    datasets = data["datasets"]
    # Only becarios_pais_estudio is enabled in mock_orchestration
    assert len(datasets) == 1
    assert datasets[0]["source_dataset"] == "becarios_pais_estudio"
    assert datasets[0]["extraction_enabled"] is True
    assert datasets[0]["bronze_enabled"] is True
    assert datasets[0]["total_records"] == 2500
    assert datasets[0]["total_pages"] == 3
    assert datasets[0]["effective_page_size"] == 1000
    assert datasets[0]["actual_records_returned"] == 1000


@patch("pipelines.discover_pronabec.get_pipeline_settings")
@patch("pipelines.discover_pronabec.load_yaml_config")
@patch("pipelines.discover_pronabec.load_orchestration_config")
@patch("pipelines.discover_pronabec.fetch_pronabec_page")
def test_discovery_with_source_dataset_filter(
    mock_fetch, mock_load_orch, mock_load_endpoints, mock_settings,
    mock_pipeline_settings, mock_endpoints, mock_orchestration, tmp_path
):
    mock_settings.return_value = mock_pipeline_settings
    mock_load_endpoints.return_value = mock_endpoints
    mock_load_orch.return_value = mock_orchestration

    mock_fetch.return_value = {
        "records": 500,
        "total": 1,
        "rows": [{"id": 1, "cell": []}] * 500
    }

    args = DummyArgs(
        dry_run=True,
        output_dir=str(tmp_path),
        source_dataset="becarios_pais_estudio"
    )
    run_discovery(args)

    plan_file = tmp_path / "bronze_work" / "pronabec" / "_plans" / "extraction_date=2026-06-29" / "run_id=test-run" / "discovery.json"
    with open(plan_file, "r") as f:
        data = json.load(f)

    assert len(data["datasets"]) == 1
    assert data["datasets"][0]["source_dataset"] == "becarios_pais_estudio"


@patch("pipelines.discover_pronabec.get_pipeline_settings")
@patch("pipelines.discover_pronabec.load_yaml_config")
@patch("pipelines.discover_pronabec.load_orchestration_config")
def test_discovery_disabled_fails_without_allow_disabled(
    mock_load_orch, mock_load_endpoints, mock_settings,
    mock_pipeline_settings, mock_endpoints, mock_orchestration, tmp_path
):
    mock_settings.return_value = mock_pipeline_settings
    mock_load_endpoints.return_value = mock_endpoints
    mock_load_orch.return_value = mock_orchestration

    args = DummyArgs(
        dry_run=True,
        output_dir=str(tmp_path),
        source_dataset="perdida_becas",
        allow_disabled_dataset=False
    )
    # Debiera lanzar un error de configuración porque está deshabilitado
    with pytest.raises(ConfigError, match="deshabilitado para extraccion"):
        run_discovery(args)


@patch("pipelines.discover_pronabec.get_pipeline_settings")
@patch("pipelines.discover_pronabec.load_yaml_config")
@patch("pipelines.discover_pronabec.load_orchestration_config")
@patch("pipelines.discover_pronabec.fetch_pronabec_page")
def test_discovery_disabled_works_with_allow_disabled(
    mock_fetch, mock_load_orch, mock_load_endpoints, mock_settings,
    mock_pipeline_settings, mock_endpoints, mock_orchestration, tmp_path
):
    mock_settings.return_value = mock_pipeline_settings
    mock_load_endpoints.return_value = mock_endpoints
    mock_load_orch.return_value = mock_orchestration

    mock_fetch.return_value = {
        "records": 50,
        "total": 1,
        "rows": [{"id": 1, "cell": []}] * 50
    }

    args = DummyArgs(
        dry_run=True,
        output_dir=str(tmp_path),
        source_dataset="perdida_becas",
        allow_disabled_dataset=True
    )
    run_discovery(args)

    plan_file = tmp_path / "bronze_work" / "pronabec" / "_plans" / "extraction_date=2026-06-29" / "run_id=test-run" / "discovery.json"
    with open(plan_file, "r") as f:
        data = json.load(f)

    assert len(data["datasets"]) == 1
    assert data["datasets"][0]["source_dataset"] == "perdida_becas"
    assert data["datasets"][0]["status"] == "SUCCESS"


@patch("pipelines.discover_pronabec.get_pipeline_settings")
@patch("pipelines.discover_pronabec.load_yaml_config")
@patch("pipelines.discover_pronabec.load_orchestration_config")
@patch("pipelines.discover_pronabec.fetch_pronabec_page")
def test_discovery_falls_back_page_sizes_on_http_500(
    mock_fetch, mock_load_orch, mock_load_endpoints, mock_settings,
    mock_pipeline_settings, mock_endpoints, mock_orchestration, tmp_path
):
    mock_settings.return_value = mock_pipeline_settings
    mock_load_endpoints.return_value = mock_endpoints
    mock_load_orch.return_value = mock_orchestration

    # Primer fetch (1000) falla con HTTP 500
    # Segundo fetch (500) es exitoso
    def side_effect(session, url, page, rows, **kwargs):
        if rows == 1000:
            raise requests.exceptions.HTTPError("HTTP 500 Internal Server Error", response=MagicMock(status_code=500))
        return {
            "records": 1200,
            "total": 3,
            "rows": [{"id": 1, "cell": []}] * 500
        }

    mock_fetch.side_effect = side_effect

    args = DummyArgs(
        dry_run=True,
        output_dir=str(tmp_path),
        source_dataset="becarios_pais_estudio"
    )
    run_discovery(args)

    plan_file = tmp_path / "bronze_work" / "pronabec" / "_plans" / "extraction_date=2026-06-29" / "run_id=test-run" / "discovery.json"
    with open(plan_file, "r") as f:
        data = json.load(f)

    dataset_res = data["datasets"][0]
    assert dataset_res["effective_page_size"] == 500
    assert dataset_res["total_records"] == 1200
    assert dataset_res["total_pages"] == 3


@patch("pipelines.discover_pronabec.get_pipeline_settings")
@patch("pipelines.discover_pronabec.load_yaml_config")
@patch("pipelines.discover_pronabec.load_orchestration_config")
@patch("pipelines.discover_pronabec.fetch_pronabec_page")
def test_required_dataset_failure_aborts_process(
    mock_fetch, mock_load_orch, mock_load_endpoints, mock_settings,
    mock_pipeline_settings, mock_endpoints, mock_orchestration, tmp_path
):
    mock_settings.return_value = mock_pipeline_settings
    mock_load_endpoints.return_value = mock_endpoints
    mock_load_orch.return_value = mock_orchestration

    # Todos los fetch fallan
    mock_fetch.side_effect = requests.exceptions.HTTPError("HTTP 500", response=MagicMock(status_code=500))

    args = DummyArgs(
        dry_run=True,
        output_dir=str(tmp_path),
        source_dataset="becarios_pais_estudio" # Requerido en la orquestacion
    )
    with pytest.raises(SystemExit):
        run_discovery(args)

    # Debe haber escrito el discovery.json indicando status FAILED antes de abortar
    plan_file = tmp_path / "bronze_work" / "pronabec" / "_plans" / "extraction_date=2026-06-29" / "run_id=test-run" / "discovery.json"
    assert plan_file.exists()
    with open(plan_file, "r") as f:
        data = json.load(f)
    assert data["status"] == "FAILED"
    assert data["datasets"][0]["status"] == "FAILED"


@patch("pipelines.discover_pronabec.get_pipeline_settings")
@patch("pipelines.discover_pronabec.load_yaml_config")
@patch("pipelines.discover_pronabec.load_orchestration_config")
@patch("pipelines.discover_pronabec.fetch_pronabec_page")
def test_non_required_dataset_failure_does_not_abort_by_default(
    mock_fetch, mock_load_orch, mock_load_endpoints, mock_settings,
    mock_pipeline_settings, mock_endpoints, mock_orchestration, tmp_path
):
    mock_settings.return_value = mock_pipeline_settings
    mock_load_endpoints.return_value = mock_endpoints
    
    # Hacer que becarios_pais_estudio NO sea requerido, pero perdida_becas sea el único descubierto y falle
    # Y que no aborte porque no es requerido
    mock_orch = dict(mock_orchestration)
    mock_orch["datasets"]["pronabec_api"]["extraction_policies"][1]["extraction_enabled"] = True
    mock_orch["datasets"]["pronabec_api"]["extraction_policies"][0]["extraction_enabled"] = False
    mock_load_orch.return_value = mock_orch

    mock_fetch.side_effect = requests.exceptions.HTTPError("HTTP 500", response=MagicMock(status_code=500))

    args = DummyArgs(
        dry_run=True,
        output_dir=str(tmp_path),
        source_dataset="perdida_becas"
    )
    # Ejecuta sin SystemExit porque required_for_e2e es False y fail_on_error no existe
    run_discovery(args)

    plan_file = tmp_path / "bronze_work" / "pronabec" / "_plans" / "extraction_date=2026-06-29" / "run_id=test-run" / "discovery.json"
    assert plan_file.exists()
    with open(plan_file, "r") as f:
        data = json.load(f)
    assert data["status"] == "SUCCESS" # El discovery general fue exitoso a pesar de la falla del opcional
    assert data["datasets"][0]["status"] == "FAILED"
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
