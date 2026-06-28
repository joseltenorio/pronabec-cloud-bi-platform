from __future__ import annotations

from unittest.mock import Mock

import pytest
import requests

from pipelines.extract_pronabec import (
    PronabecExtractionError,
    calculate_backoff_seconds,
    fetch_pronabec_page,
)


def test_calculate_backoff_seconds_respects_maximum():
    value = calculate_backoff_seconds(
        attempt=10,
        base_seconds=10,
        max_seconds=30,
    )

    assert value <= 30


def test_fetch_pronabec_page_retries_on_read_timeout(monkeypatch):
    monkeypatch.setattr("pipelines.extract_pronabec.time.sleep", lambda _: None)

    session = Mock()
    success_response = Mock()
    success_response.raise_for_status.return_value = None
    success_response.json.return_value = {
        "total": 1,
        "records": 1,
        "rows": [{"id": "1", "cell": ["A"]}],
    }

    session.get.side_effect = [
        requests.exceptions.ReadTimeout("timeout"),
        success_response,
    ]

    payload = fetch_pronabec_page(
        session=session,
        url="https://example.test/ListarDemo",
        page=1,
        rows=100,
        timeout=1,
        max_retries=2,
        backoff_base_seconds=0.01,
        backoff_max_seconds=0.01,
        dataset_name="demo",
        extraction_date="2026-06-28",
        run_id="test_run",
    )

    assert payload["records"] == 1
    assert session.get.call_count == 2


def test_fetch_pronabec_page_fails_after_max_retries(monkeypatch):
    monkeypatch.setattr("pipelines.extract_pronabec.time.sleep", lambda _: None)

    session = Mock()
    session.get.side_effect = requests.exceptions.ReadTimeout("timeout")

    with pytest.raises(PronabecExtractionError):
        fetch_pronabec_page(
            session=session,
            url="https://example.test/ListarDemo",
            page=1,
            rows=100,
            timeout=1,
            max_retries=2,
            backoff_base_seconds=0.01,
            backoff_max_seconds=0.01,
            dataset_name="demo",
            extraction_date="2026-06-28",
            run_id="test_run",
        )

    assert session.get.call_count == 2


def test_fetch_pronabec_page_retries_on_http_503(monkeypatch):
    monkeypatch.setattr("pipelines.extract_pronabec.time.sleep", lambda _: None)

    failed_response = Mock()
    failed_response.status_code = 503

    http_error = requests.exceptions.HTTPError("service unavailable")
    http_error.response = failed_response

    success_response = Mock()
    success_response.raise_for_status.return_value = None
    success_response.json.return_value = {
        "total": 1,
        "records": 1,
        "rows": [{"id": "1", "cell": ["A"]}],
    }

    first_response = Mock()
    first_response.raise_for_status.side_effect = http_error

    session = Mock()
    session.get.side_effect = [first_response, success_response]

    payload = fetch_pronabec_page(
        session=session,
        url="https://example.test/ListarDemo",
        page=1,
        rows=100,
        timeout=1,
        max_retries=2,
        backoff_base_seconds=0.01,
        backoff_max_seconds=0.01,
        dataset_name="demo",
        extraction_date="2026-06-28",
        run_id="test_run",
    )

    assert payload["records"] == 1
    assert session.get.call_count == 2