from __future__ import annotations

import pytest
from unittest.mock import MagicMock

from pipelines.extract_pronabec import (
    PronabecExtractionError,
    consolidate_raw_payload,
    extract_dataset,
    resolve_page_range,
)


def test_page_range_limits_pages_extracted() -> None:
    assert resolve_page_range(2, 5) == (2, 5)


def test_page_end_before_page_start_fails() -> None:
    with pytest.raises(PronabecExtractionError, match="PAGE_END debe ser mayor"):
        resolve_page_range(5, 2)


def test_only_page_start_fails() -> None:
    with pytest.raises(PronabecExtractionError, match="PAGE_START y PAGE_END"):
        resolve_page_range(1, None)


def test_only_page_end_fails() -> None:
    with pytest.raises(PronabecExtractionError, match="PAGE_START y PAGE_END"):
        resolve_page_range(None, 5)


def test_raw_payload_metadata_reflects_effective_page_size_and_range() -> None:
    payload = consolidate_raw_payload(
        dataset_name="convocatorias",
        url="https://example.test",
        pages=[{"total": "3", "records": "300", "rows": []}],
        effective_page_size=5000,
        page_start=1,
        page_end=3,
    )

    assert payload["effective_page_size"] == 5000
    assert payload["page_start"] == 1
    assert payload["page_end"] == 3


def test_page_end_above_total_pages_processes_until_total_pages(monkeypatch: pytest.MonkeyPatch) -> None:
    import pipelines.extract_pronabec as extract_mod

    pages_requested: list[int] = []

    def fake_fetch_pronabec_page(**kwargs):
        pages_requested.append(kwargs["page"])
        return {
            "total": "3",
            "records": "3",
            "rows": [{"id": str(kwargs["page"]), "cell": [str(kwargs["page"])]}],
        }

    monkeypatch.setattr(extract_mod, "fetch_pronabec_page", fake_fetch_pronabec_page)

    raw_payload, records = extract_dataset(
        endpoint={"name": "convocatorias", "path": "Convocatorias", "expected_columns": ["valor"]},
        base_url="https://example.test/Dataset",
        rows_per_page=500,
        requested_page_size=500,
        max_pages=None,
        page_start=1,
        page_end=10,
        timeout=1,
        max_retries=1,
        backoff_base_seconds=1.0,
        backoff_max_seconds=1.0,
        sleep_seconds=0,
        extraction_date="2026-06-28",
        run_id="test-run",
        logger=MagicMock(),
    )

    assert pages_requested == [1, 2, 3]
    assert len(records) == 3
    assert raw_payload["page_end"] == 3
