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
GROUPED_HEADER_FIXTURE_PATH = Path(
    "tests/fixtures/minedu_escale_secondary_enrollment_grouped_header.html"
)


def _fixture_html() -> str:
    return FIXTURE_PATH.read_text(encoding="utf-8")


def _grouped_header_fixture_html() -> str:
    return GROUPED_HEADER_FIXTURE_PATH.read_text(encoding="utf-8")


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


def test_extract_total_secundaria_rows_supports_grouped_header_fixture() -> None:
    rows = extract_total_secundaria_rows(_grouped_header_fixture_html())

    assert len(rows) == 5
    assert rows[0]["grado"] == "PRIMER_GRADO"
    assert rows[0]["matricula_total"] == 1000
    assert rows[0]["matricula_publica"] == 800
    assert rows[0]["matricula_privada"] == 200
    assert rows[0]["matricula_urbana"] == 700
    assert rows[0]["matricula_rural"] == 300
    assert rows[0]["matricula_masculino"] == 510
    assert rows[0]["matricula_femenino"] == 490


def test_extract_total_secundaria_rows_uses_fallback_positional_for_incomplete_header() -> None:
    html = """
    <table>
      <tr>
        <th>Nivel educativo</th>
        <th>Total</th>
        <th>Gestión</th>
        <th>Área</th>
        <th>Sexo</th>
      </tr>
      <tr><td>Total Secundaria</td><td></td><td></td><td></td><td></td><td></td><td></td><td></td></tr>
      <tr><td>Primer grado</td><td>1000</td><td>800</td><td>200</td><td>700</td><td>300</td><td>510</td><td>490</td></tr>
      <tr><td>Segundo grado</td><td>950</td><td>760</td><td>190</td><td>650</td><td>300</td><td>490</td><td>460</td></tr>
      <tr><td>Tercer grado</td><td>900</td><td>720</td><td>180</td><td>620</td><td>280</td><td>460</td><td>440</td></tr>
      <tr><td>Cuarto grado</td><td>850</td><td>680</td><td>170</td><td>600</td><td>250</td><td>430</td><td>420</td></tr>
      <tr><td>Quinto grado</td><td>800</td><td>640</td><td>160</td><td>560</td><td>240</td><td>390</td><td>410</td></tr>
    </table>
    """

    rows = extract_total_secundaria_rows(html)

    assert len(rows) == 5
    assert rows[0]["matricula_publica"] == 800
    assert rows[0]["matricula_femenino"] == 490


def test_extract_total_secundaria_rows_fails_when_incomplete_structure_has_less_than_8_columns() -> None:
    html = """
    <table>
      <tr>
        <th>Nivel educativo</th>
        <th>Total</th>
        <th>Gestión</th>
        <th>Área</th>
        <th>Sexo</th>
      </tr>
      <tr><td>Total Secundaria</td><td></td><td></td><td></td><td></td></tr>
      <tr><td>Primer grado</td><td>1000</td><td>800</td><td>200</td><td>700</td></tr>
    </table>
    """

    with pytest.raises(ValueError, match="No se pudieron resolver columnas MINEDU ESCALE"):
        extract_total_secundaria_rows(html)


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
