from __future__ import annotations

import json
import sys
from typing import Any

import pytest

from tools import probe_sources


ENDPOINTS_CONFIG = {
    "pronabec": {
        "base_url": "https://datosabiertos.pronabec.gob.pe/Dataset",
        "endpoints": [
            {
                "name": "notas_becarios",
                "path": "NotasDeBecarios",
                "enabled": True,
            }
        ],
    }
}


class FakeResponse:
    def __init__(
        self,
        payload: Any | None,
        status_code: int = 200,
        url: str = "https://example.test/ListarNotasDeBecarios",
        json_error: Exception | None = None,
    ) -> None:
        self.payload = payload
        self.status_code = status_code
        self.url = url
        self.headers = {"Content-Type": "application/json"}
        self.content = b"{}"
        self.text = "{}"
        self.json_error = json_error

    def json(self) -> Any:
        if self.json_error:
            raise self.json_error
        return self.payload


def make_payload(row_count: int, total_records: int, total_pages: int = 10) -> dict[str, Any]:
    return {
        "rows": [{"id": str(index), "cell": []} for index in range(row_count)],
        "records": str(total_records),
        "total": str(total_pages),
    }


def test_page_size_probe_detects_server_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        probe_sources.requests,
        "get",
        lambda *args, **kwargs: FakeResponse(make_payload(row_count=100, total_records=1000)),
    )

    result = probe_sources.probe_pronabec_page_size(
        dataset="notas_becarios",
        page=1,
        page_size=500,
        endpoints_config=ENDPOINTS_CONFIG,
    )

    assert result["actual_records_returned"] == 100
    assert result["total_records"] == 1000
    assert result["server_capped_page_size"] is True


def test_page_size_probe_accepts_full_requested_page(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        probe_sources.requests,
        "get",
        lambda *args, **kwargs: FakeResponse(make_payload(row_count=500, total_records=1000)),
    )

    result = probe_sources.probe_pronabec_page_size(
        dataset="notas_becarios",
        page=1,
        page_size=500,
        endpoints_config=ENDPOINTS_CONFIG,
    )

    assert result["actual_records_returned"] == 500
    assert result["server_capped_page_size"] is False


def test_page_size_probe_does_not_mark_small_dataset_as_capped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        probe_sources.requests,
        "get",
        lambda *args, **kwargs: FakeResponse(make_payload(row_count=200, total_records=200)),
    )

    result = probe_sources.probe_pronabec_page_size(
        dataset="notas_becarios",
        page=1,
        page_size=500,
        endpoints_config=ENDPOINTS_CONFIG,
    )

    assert result["actual_records_returned"] == 200
    assert result["server_capped_page_size"] is False


def test_page_size_probe_reports_http_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        probe_sources.requests,
        "get",
        lambda *args, **kwargs: FakeResponse({"error": "bad"}, status_code=503),
    )

    result = probe_sources.probe_pronabec_page_size(
        dataset="notas_becarios",
        page=1,
        page_size=500,
        endpoints_config=ENDPOINTS_CONFIG,
    )

    assert result["status_code"] == 503
    assert result["error"] == "HTTP error 503"


def test_page_size_probe_reports_invalid_json(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        probe_sources.requests,
        "get",
        lambda *args, **kwargs: FakeResponse(None, json_error=ValueError("invalid")),
    )

    result = probe_sources.probe_pronabec_page_size(
        dataset="notas_becarios",
        page=1,
        page_size=500,
        endpoints_config=ENDPOINTS_CONFIG,
    )

    assert result["status_code"] == 200
    assert result["error"] == "Respuesta PRONABEC no es JSON valido"


def test_cli_accepts_page_and_page_size(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(probe_sources, "load_yaml", lambda path: ENDPOINTS_CONFIG)
    monkeypatch.setattr(
        probe_sources,
        "probe_pronabec_page_size",
        lambda **kwargs: {
            "source": "pronabec",
            "dataset": kwargs["dataset"],
            "requested_page": kwargs["page"],
            "requested_page_size": kwargs["page_size"],
        },
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "probe_sources",
            "--source",
            "pronabec",
            "--dataset",
            "notas_becarios",
            "--page",
            "1",
            "--page-size",
            "500",
            "--output",
            "json",
        ],
    )

    probe_sources.main()
    payload = json.loads(capsys.readouterr().out)

    assert payload["dataset"] == "notas_becarios"
    assert payload["requested_page"] == 1
    assert payload["requested_page_size"] == 500


def test_cli_without_page_size_uses_existing_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = []
    monkeypatch.setattr(probe_sources, "load_yaml", lambda path: ENDPOINTS_CONFIG)
    monkeypatch.setattr(
        probe_sources,
        "probe_pronabec_endpoint",
        lambda config, dataset, timeout=60: calls.append((dataset, timeout)),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "probe_sources",
            "--source",
            "pronabec",
            "--dataset",
            "notas_becarios",
        ],
    )

    probe_sources.main()

    assert calls == [("notas_becarios", 60)]
