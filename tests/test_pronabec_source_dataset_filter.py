from __future__ import annotations

import pytest

from pipelines.common.config import ConfigError
from pipelines.extract_pronabec import (
    resolve_source_dataset,
    select_pronabec_endpoints,
)


ENDPOINTS_CONFIG = {
    "pronabec": {
        "endpoints": [
            {"name": "convocatorias", "path": "Convocatorias", "enabled": True},
            {"name": "notas_becarios", "path": "NotasDeBecarios", "enabled": True},
        ]
    }
}


ORCHESTRATION_CONFIG = {
    "datasets": {
        "pronabec_api": {
            "extraction_policies": [
                {
                    "source_dataset": "convocatorias",
                    "extraction_enabled": True,
                    "silver_enabled": True,
                    "extraction_mode": "single",
                    "required_for_e2e": True,
                    "chunk_size_pages": None,
                    "max_parallel_chunks": 1,
                    "recommended_page_size": 1000,
                    "fallback_page_sizes": [500, 100],
                    "max_page_size_tested_ok": 10000,
                    "page_size_policy": "dataset_safe_default",
                },
                {
                    "source_dataset": "notas_becarios",
                    "extraction_enabled": False,
                    "silver_enabled": False,
                    "extraction_mode": "chunked",
                    "required_for_e2e": False,
                    "chunk_size_pages": 100,
                    "max_parallel_chunks": 2,
                    "recommended_page_size": 10000,
                    "fallback_page_sizes": [5000, 1000, 100],
                    "max_page_size_tested_ok": 20000,
                    "page_size_policy": "dataset_safe_default",
                },
            ]
        }
    }
}


def test_without_source_dataset_keeps_legacy_enabled_endpoint_selection() -> None:
    endpoints = select_pronabec_endpoints(
        endpoints_config=ENDPOINTS_CONFIG,
        orchestration_config=ORCHESTRATION_CONFIG,
        source_dataset=None,
    )

    assert [endpoint["name"] for endpoint in endpoints] == [
        "convocatorias",
        "notas_becarios",
    ]


def test_with_source_dataset_selects_only_that_endpoint() -> None:
    endpoints = select_pronabec_endpoints(
        endpoints_config=ENDPOINTS_CONFIG,
        orchestration_config=ORCHESTRATION_CONFIG,
        source_dataset="convocatorias",
    )

    assert [endpoint["name"] for endpoint in endpoints] == ["convocatorias"]


def test_unknown_source_dataset_fails() -> None:
    with pytest.raises(ConfigError, match="Dataset PRONABEC no encontrado"):
        select_pronabec_endpoints(
            endpoints_config=ENDPOINTS_CONFIG,
            orchestration_config=ORCHESTRATION_CONFIG,
            source_dataset="no_existe",
        )


def test_cli_source_dataset_has_priority_over_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOURCE_DATASET", "notas_becarios")

    assert resolve_source_dataset("convocatorias") == "convocatorias"


def test_env_source_dataset_is_used_when_cli_is_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOURCE_DATASET", "notas_becarios")

    assert resolve_source_dataset(None) == "notas_becarios"


def test_disabled_dataset_fails_by_default() -> None:
    with pytest.raises(ConfigError, match="Dataset PRONABEC deshabilitado"):
        select_pronabec_endpoints(
            endpoints_config=ENDPOINTS_CONFIG,
            orchestration_config=ORCHESTRATION_CONFIG,
            source_dataset="notas_becarios",
        )


def test_disabled_dataset_runs_when_explicitly_allowed() -> None:
    endpoints = select_pronabec_endpoints(
        endpoints_config=ENDPOINTS_CONFIG,
        orchestration_config=ORCHESTRATION_CONFIG,
        source_dataset="notas_becarios",
        allow_disabled_dataset=True,
    )

    assert [endpoint["name"] for endpoint in endpoints] == ["notas_becarios"]
