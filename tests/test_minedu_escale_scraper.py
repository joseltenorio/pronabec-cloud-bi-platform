from __future__ import annotations

from pathlib import Path

import pytest
import requests

from pipelines.scrape_minedu_escale import (
    build_minedu_escale_url,
    clean_number,
    extract_total_secundaria_rows,
    normalize_grade,
    scrape_year_department,
    validate_enrollment_row,
)


FIXTURE_PATH = Path("tests/fixtures/minedu_escale_secondary_enrollment.html")


def _fixture_html() -> str:
    return FIXTURE_PATH.read_text(encoding="utf-8")


def test_build_url_uses_year_and_department_params() -> None:
    url = build_minedu_escale_url(
        "https://escale.minedu.gob.pe/magnitudes-portlet/reporte/cuadro",
        anio_param=39,
        cuadro=733,
        codigo_departamento="25",
    )

    assert "anio=39" in url
    assert "cuadro=733" in url
    assert "dpto=25" in url
    assert "tipo_ambito=ambito-ubigeo" in url


@pytest.mark.parametrize(
    ("value", "expected"),
    [("1 234", 1234), (" - ", 0), ("1,050", 1050), ("\xa0", 0)],
)
def test_clean_number_handles_spaces_and_dashes(value: str, expected: int) -> None:
    assert clean_number(value) == expected


def test_normalize_grade_maps_all_five_grades() -> None:
    assert normalize_grade("Primer grado") == "PRIMER_GRADO"
    assert normalize_grade("Segundo grado") == "SEGUNDO_GRADO"
    assert normalize_grade("Tercer grado") == "TERCER_GRADO"
    assert normalize_grade("Cuarto grado") == "CUARTO_GRADO"
    assert normalize_grade("Quinto grado") == "QUINTO_GRADO"


def test_extract_total_secundaria_rows_from_fixture() -> None:
    rows = extract_total_secundaria_rows(_fixture_html())

    assert len(rows) == 5
    assert rows[0]["grado"] == "PRIMER_GRADO"
    assert rows[-1]["matricula_total"] == 800


def test_extract_ignores_presencial_distancia_alternancia() -> None:
    rows = extract_total_secundaria_rows(_fixture_html())

    assert all(row["matricula_total"] != 999 for row in rows)


def test_validate_row_checks_public_private_total() -> None:
    with pytest.raises(ValueError, match="publica_privada"):
        validate_enrollment_row(
            {
                "matricula_total": "100",
                "matricula_publica": "70",
                "matricula_privada": "20",
                "matricula_urbana": "60",
                "matricula_rural": "40",
                "matricula_masculino": "50",
                "matricula_femenino": "50",
            }
        )


def test_scraper_returns_expected_5_rows_for_department_year_fixture(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeResponse:
        status_code = 200
        text = _fixture_html()

        def raise_for_status(self) -> None:
            return None

    session = requests.Session()
    monkeypatch.setattr(session, "get", lambda *args, **kwargs: FakeResponse())

    rows = scrape_year_department(
        session=session,
        base_url="https://escale.minedu.gob.pe/magnitudes-portlet/reporte/cuadro",
        year=2025,
        year_config={"anio_param": 39, "cuadro": 733},
        codigo_departamento="25",
        region="UCAYALI",
        extraction_date="2026-07-08",
        pipeline_run_id="manual-20260708",
        timeout=30,
    )

    assert len(rows) == 5
    assert rows[0]["anio"] == "2025"
    assert rows[0]["codigo_departamento"] == "25"
    assert rows[0]["source_url"].endswith("dpto=25&prov=&dre=&tipo_ambito=ambito-ubigeo")
