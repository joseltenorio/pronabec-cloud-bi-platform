from __future__ import annotations

import requests
import pytest

from pipelines.extract_pronabec import (
    PronabecExtractionError,
    resolve_effective_page_size,
    resolve_page_size_override,
    resolve_requested_page_size,
)


ORCHESTRATION_CONFIG = {
    "datasets": {
        "pronabec_api": {
            "extraction_policies": [
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
                }
            ]
        }
    }
}


def http_500_error(page_size: int) -> PronabecExtractionError:
    response = requests.Response()
    response.status_code = 500
    http_error = requests.exceptions.HTTPError(f"HTTP 500 rows={page_size}")
    http_error.response = response
    try:
        raise http_error
    except requests.exceptions.HTTPError as exc:
        raise PronabecExtractionError(f"fallo page_size={page_size}") from exc


def test_uses_recommended_page_size_when_cli_is_absent() -> None:
    assert (
        resolve_requested_page_size(
            dataset_name="notas_becarios",
            requested_page_size=None,
            orchestration_config=ORCHESTRATION_CONFIG,
        )
        == 10000
    )


def test_cli_page_size_has_priority() -> None:
    assert (
        resolve_requested_page_size(
            dataset_name="notas_becarios",
            requested_page_size=7500,
            orchestration_config=ORCHESTRATION_CONFIG,
        )
        == 7500
    )


def test_env_page_size_override_is_supported(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PRONABEC_PAGE_SIZE", "5000")

    assert resolve_page_size_override(None) == 5000


def test_http_500_uses_first_successful_fallback() -> None:
    calls: list[tuple[int, int]] = []

    def fetch_page(page_number: int, page_size: int) -> None:
        calls.append((page_number, page_size))
        if page_size == 10000:
            http_500_error(page_size)

    effective_page_size = resolve_effective_page_size(
        dataset="notas_becarios",
        requested_page_size=10000,
        fallback_page_sizes=[5000, 1000, 100],
        page_start=1,
        fetch_page=fetch_page,
    )

    assert effective_page_size == 5000
    assert calls == [(1, 10000), (1, 5000)]


def test_all_fallbacks_fail_with_clear_error() -> None:
    def fetch_page(page_number: int, page_size: int) -> None:
        http_500_error(page_size)

    with pytest.raises(PronabecExtractionError, match="Todos los page_size fallaron"):
        resolve_effective_page_size(
            dataset="notas_becarios",
            requested_page_size=10000,
            fallback_page_sizes=[5000, 1000],
            page_start=1,
            fetch_page=fetch_page,
        )


def test_effective_page_size_is_resolved_once_for_range() -> None:
    calls: list[tuple[int, int]] = []

    def fetch_page(page_number: int, page_size: int) -> None:
        calls.append((page_number, page_size))

    effective_page_size = resolve_effective_page_size(
        dataset="notas_becarios",
        requested_page_size=7500,
        fallback_page_sizes=[5000, 1000],
        page_start=3,
        fetch_page=fetch_page,
    )

    assert effective_page_size == 7500
    assert calls == [(3, 7500)]
