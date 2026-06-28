from __future__ import annotations

import pytest

from pipelines.extract_pronabec import (
    PronabecExtractionError,
    resolve_extraction_date,
)


def test_resolve_extraction_date_prefers_cli_value(monkeypatch):
    monkeypatch.setenv("BRONZE_EXTRACTION_DATE", "2026-06-29")

    value = resolve_extraction_date(
        cli_value="2026-06-28",
        dry_run=False,
    )

    assert value == "2026-06-28"


def test_resolve_extraction_date_uses_environment(monkeypatch):
    monkeypatch.setenv("BRONZE_EXTRACTION_DATE", "2026-06-28")

    value = resolve_extraction_date(
        cli_value=None,
        dry_run=False,
    )

    assert value == "2026-06-28"


def test_resolve_extraction_date_fails_without_cloud_date(monkeypatch):
    monkeypatch.delenv("BRONZE_EXTRACTION_DATE", raising=False)

    with pytest.raises(PronabecExtractionError):
        resolve_extraction_date(
            cli_value=None,
            dry_run=False,
        )


def test_resolve_extraction_date_rejects_invalid_date():
    with pytest.raises(PronabecExtractionError):
        resolve_extraction_date(
            cli_value="2026/06/28",
            dry_run=False,
        )


def test_resolve_extraction_date_allows_default_only_for_local_dry_run(monkeypatch):
    monkeypatch.delenv("BRONZE_EXTRACTION_DATE", raising=False)

    value = resolve_extraction_date(
        cli_value=None,
        dry_run=True,
        allow_default_date=True,
    )

    assert len(value) == 10