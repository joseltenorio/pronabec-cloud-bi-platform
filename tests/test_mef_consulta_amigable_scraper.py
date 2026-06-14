import json
from pathlib import Path

from bs4 import BeautifulSoup

from pipelines import scrape_mef_budget
from pipelines.scrape_mef_budget import (
    CONSULTA_AMIGABLE_BASE_URL,
    build_mef_form_payload,
    clean_mef_number,
    extract_mef_budget_row,
    find_mef_grp1_value,
    scrape_consulta_amigable_year,
    write_mef_to_local,
)


PRONABEC_EXECUTORA = (
    "117-1438: PROGRAMA NACIONAL DE BECAS Y CREDITO EDUCATIVO"
)


def soup_from_html(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


def base_form(body: str) -> str:
    return f"""
    <html>
      <body>
        <form method="post">
          <input type="hidden" name="__VIEWSTATE" value="viewstate" />
          <input type="hidden" name="__EVENTVALIDATION" value="validation" />
          {body}
        </form>
      </body>
    </html>
    """


def radio_row(value: str, label: str) -> str:
    return f"""
    <tr>
      <td><input type="radio" name="grp1" value="{value}" /></td>
      <td>{label}</td>
    </tr>
    """


def final_budget_table() -> str:
    return base_form(
        f"""
        <table class="Data">
          <tr>
            <th>Unidad Ejecutora</th>
            <th>PIA</th>
            <th>PIM</th>
            <th>Certificacion</th>
            <th>Compromiso Anual</th>
            <th>Atencion de Compromiso Mensual</th>
            <th>Devengado</th>
            <th>Girado</th>
            <th>Avance %</th>
          </tr>
          <tr>
            <td>{PRONABEC_EXECUTORA}</td>
            <td>1,429,676,488</td>
            <td>1,607,711,495</td>
            <td>1,590,100,549</td>
            <td>1,138,467,562</td>
            <td>675,591,756</td>
            <td>662,693,665</td>
            <td>660,194,362</td>
            <td>41.2</td>
          </tr>
        </table>
        """
    )


def test_clean_mef_number_handles_portal_formats() -> None:
    assert clean_mef_number("1,429,676,488") == 1429676488.0
    assert clean_mef_number("41.2") == 41.2
    assert clean_mef_number("41.2%") == 41.2
    assert clean_mef_number("-") == 0.0
    assert clean_mef_number("") == 0.0
    assert clean_mef_number(None) == 0.0


def test_build_mef_form_payload_preserves_state_and_selected_radio() -> None:
    soup = soup_from_html(
        """
        <form>
          <input type="hidden" name="__VIEWSTATE" value="abc" />
          <input type="hidden" name="__EVENTVALIDATION" value="xyz" />
          <input type="submit" name="ctl00$CPH1$BtnSector" value="Sector" />
          <input type="submit" name="ctl00$CPH1$BtnPliego" value="Pliego" />
          <input type="radio" name="grp1" value="E" checked />
        </form>
        """
    )

    payload = build_mef_form_payload(soup, "ctl00$CPH1$BtnSector")

    assert payload["__VIEWSTATE"] == "abc"
    assert payload["__EVENTVALIDATION"] == "xyz"
    assert payload["grp1"] == "E"
    assert payload["ctl00$CPH1$BtnSector"] == ""
    assert "ctl00$CPH1$BtnPliego" not in payload


def test_find_mef_grp1_value_finds_navigation_options() -> None:
    soup = soup_from_html(
        f"""
        <table>
          {radio_row("E", "Nivel de Gobierno E: GOBIERNO NACIONAL")}
          {radio_row("10", "Sector 10: EDUCACION")}
          {radio_row("010", "Pliego 010: M. DE EDUCACION")}
        </table>
        """
    )

    assert find_mef_grp1_value(soup, ["GOBIERNO NACIONAL"])[0] == "E"
    assert find_mef_grp1_value(soup, ["EDUCACION"])[0] == "10"
    assert find_mef_grp1_value(soup, ["010", "M. DE EDUCACION"])[0] == "010"


def test_extract_mef_budget_row_finds_pronabec_primary_and_fallback() -> None:
    soup = soup_from_html(final_budget_table())

    primary_row = extract_mef_budget_row(
        soup,
        ["117-1438", "PROGRAMA NACIONAL DE BECAS"],
    )
    fallback_row = extract_mef_budget_row(soup, ["BECAS", "CREDITO"])

    assert primary_row is not None
    assert fallback_row == primary_row
    assert primary_row[0] == PRONABEC_EXECUTORA
    assert primary_row[-8:] == [
        "1,429,676,488",
        "1,607,711,495",
        "1,590,100,549",
        "1,138,467,562",
        "675,591,756",
        "662,693,665",
        "660,194,362",
        "41.2",
    ]


class FakeResponse:
    def __init__(self, text: str, url: str = "https://example.test/Navegar.aspx"):
        self.text = text
        self.url = url

    def raise_for_status(self) -> None:
        return None


class FakeSession:
    def __init__(self) -> None:
        self.headers: dict[str, str] = {}
        self.get_calls: list[dict[str, object]] = []
        self.post_calls: list[dict[str, object]] = []
        self.post_responses = [
            base_form(
                f"""
                <input type="submit" name="ctl00$CPH1$BtnSector" value="Sector" />
                <table>{radio_row("E", "E: GOBIERNO NACIONAL")}</table>
                """
            ),
            base_form(
                f"""
                <input type="submit" name="ctl00$CPH1$BtnPliego" value="Pliego" />
                <table>{radio_row("10", "10: EDUCACION")}</table>
                """
            ),
            base_form(
                f"""
                <input type="submit" name="ctl00$CPH1$BtnEjecutora" value="Ejecutora" />
                <table>{radio_row("010", "010: M. DE EDUCACION")}</table>
                """
            ),
            final_budget_table(),
        ]

    def __enter__(self) -> "FakeSession":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def get(self, url: str, params: dict[str, str], timeout: int) -> FakeResponse:
        self.get_calls.append({"url": url, "params": params, "timeout": timeout})
        return FakeResponse(
            base_form(
                """
                <input type="submit"
                       name="ctl00$CPH1$BtnTipoGobierno"
                       value="Nivel de Gobierno" />
                """
            ),
            url="https://example.test/Navegar_7.aspx?y=2026&ap=ActProy",
        )

    def post(self, url: str, data: dict[str, str], timeout: int) -> FakeResponse:
        self.post_calls.append({"url": url, "data": data, "timeout": timeout})
        return FakeResponse(self.post_responses.pop(0), url=url)


def test_scrape_consulta_amigable_year_uses_mocked_session(
    monkeypatch,
) -> None:
    fake_session = FakeSession()
    monkeypatch.setattr(
        scrape_mef_budget.requests,
        "Session",
        lambda: fake_session,
    )

    record = scrape_consulta_amigable_year(2026, timeout=15)

    assert record == {
        "ano": 2026,
        "ejecutora_nombre": PRONABEC_EXECUTORA,
        "pia": 1429676488.0,
        "pim": 1607711495.0,
        "certificacion": 1590100549.0,
        "compromiso_anual": 1138467562.0,
        "compromiso_mensual": 675591756.0,
        "devengado": 662693665.0,
        "girado": 660194362.0,
        "avance_porcentaje": 41.2,
    }
    assert len(fake_session.get_calls) == 2
    assert len(fake_session.post_calls) == 4
    assert fake_session.post_calls[0]["data"]["ctl00$CPH1$BtnTipoGobierno"] == ""
    assert fake_session.post_calls[1]["data"]["grp1"] == "E"
    assert fake_session.post_calls[2]["data"]["grp1"] == "10"
    assert fake_session.post_calls[3]["data"]["grp1"] == "010"


def test_write_mef_to_local_includes_consulta_amigable_metadata(
    tmp_path: Path,
) -> None:
    logger_mock = type("MockLogger", (), {"log": lambda *args, **kwargs: None})()
    records = [
        {
            "ano": "2026",
            "ejecutora_nombre": PRONABEC_EXECUTORA,
            "pia": "1429676488.0",
            "pim": "1607711495.0",
            "certificacion": "1590100549.0",
            "compromiso_anual": "1138467562.0",
            "compromiso_mensual": "675591756.0",
            "devengado": "662693665.0",
            "girado": "660194362.0",
            "avance_porcentaje": "41.2",
        }
    ]
    fieldnames = list(records[0].keys())

    result = write_mef_to_local(
        records=records,
        fieldnames=fieldnames,
        extraction_date="2026-06-14",
        output_dir=tmp_path,
        run_id="test_run",
        records_read=1,
        source_url=CONSULTA_AMIGABLE_BASE_URL,
        source_file=None,
        logger=logger_mock,
        source_mode="consulta_amigable",
    )

    csv_path = Path(result["output_uri"])
    metadata_path = Path(result["metadata_path"])

    assert str(csv_path.parent).replace("\\", "/").endswith(
        "bronze/mef/presupuesto/extraction_date=2026-06-14"
    )
    assert csv_path.exists()
    assert metadata_path.exists()

    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert metadata["metadata"] == {
        "mode": "dry_run",
        "source_mode": "consulta_amigable",
        "source_url": CONSULTA_AMIGABLE_BASE_URL,
        "source_file": None,
    }
