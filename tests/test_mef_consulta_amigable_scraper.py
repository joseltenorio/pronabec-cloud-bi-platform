import json
from pathlib import Path

from bs4 import BeautifulSoup

from pipelines import scrape_mef_budget
from pipelines.scrape_mef_budget import (
    CONSULTA_AMIGABLE_BASE_URL,
    MEF_HIERARCHY_FIELDNAMES,
    build_mef_form_payload,
    clean_mef_number,
    extract_mef_breakdown_rows,
    extract_mef_hierarchy_rows,
    extract_mef_budget_row,
    find_mef_grp1_value,
    parse_breakdown_slices,
    parse_mef_temporal_period,
    scrape_consulta_amigable_year,
    split_mef_code_description,
    write_mef_breakdown_to_local,
    write_mef_hierarchy_to_local,
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


def hierarchy_table() -> str:
    return base_form(
        f"""
        <table class="History">
          <tr>
            <th>Nivel</th>
            <th>Descripcion</th>
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
            <td>TOTAL</td>
            <td>TOTAL</td>
            <td>10,000</td>
            <td>11,000</td>
            <td>9,000</td>
            <td>8,000</td>
            <td>7,000</td>
            <td>6,000</td>
            <td>5,000</td>
            <td>54.5</td>
          </tr>
          <tr>
            <td>PLIEGO</td>
            <td>010: M. DE EDUCACION</td>
            <td>1,429,676,488</td>
            <td>1,607,711,495</td>
            <td>1,590,100,549</td>
            <td>1,138,467,562</td>
            <td>675,591,756</td>
            <td>662,693,665</td>
            <td>660,194,362</td>
            <td>41.2</td>
          </tr>
          <tr>
            <td>UNIDAD EJECUTORA</td>
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


def breakdown_table(*labels: str) -> str:
    rows = "\n".join(
        f"""
        <tr>
          <td>{label}</td>
          <td>1,000</td>
          <td>2,000</td>
          <td>1,900</td>
          <td>1,500</td>
          <td>1,100</td>
          <td>1,000</td>
          <td>900</td>
          <td>50.0</td>
        </tr>
        """
        for label in labels
    )
    return base_form(
        f"""
        <table class="Data">
          <tr>
            <th>Descripcion</th>
            <th>PIA</th>
            <th>PIM</th>
            <th>Certificacion</th>
            <th>Compromiso Anual</th>
            <th>Atencion de Compromiso Mensual</th>
            <th>Devengado</th>
            <th>Girado</th>
            <th>Avance %</th>
          </tr>
          {rows}
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


def test_split_mef_code_description_handles_hierarchy_labels() -> None:
    assert split_mef_code_description("010: M. DE EDUCACION") == (
        "010",
        "M. DE EDUCACION",
    )
    assert split_mef_code_description(PRONABEC_EXECUTORA) == (
        "117-1438",
        "PROGRAMA NACIONAL DE BECAS Y CREDITO EDUCATIVO",
    )
    assert split_mef_code_description("TOTAL") == ("", "TOTAL")
    assert split_mef_code_description(
        "3000885: ENTREGA DE BECA DE EDUCACION SUPERIOR"
    ) == (
        "3000885",
        "ENTREGA DE BECA DE EDUCACION SUPERIOR",
    )
    assert split_mef_code_description("2.3: BIENES Y SERVICIOS") == (
        "2.3",
        "BIENES Y SERVICIOS",
    )
    assert split_mef_code_description("1: RECURSOS ORDINARIOS") == (
        "1",
        "RECURSOS ORDINARIOS",
    )
    assert split_mef_code_description("00: RECURSOS ORDINARIOS") == (
        "00",
        "RECURSOS ORDINARIOS",
    )
    assert split_mef_code_description("15: LIMA") == ("15", "LIMA")
    assert split_mef_code_description("SIN CODIGO") == ("", "SIN CODIGO")


def test_extract_mef_hierarchy_rows_from_fake_html() -> None:
    soup = soup_from_html(hierarchy_table())

    records = extract_mef_hierarchy_rows(
        soup=soup,
        ano=2026,
        periodo_tipo="ANUAL",
        periodo_valor="2026",
    )

    assert len(records) == 3
    assert records[0] == {
        "ano": "2026",
        "periodo_tipo": "ANUAL",
        "periodo_valor": "2026",
        "nivel_jerarquia": "TOTAL",
        "codigo": "",
        "descripcion": "TOTAL",
        "pia": "10,000",
        "pim": "11,000",
        "certificacion": "9,000",
        "compromiso_anual": "8,000",
        "compromiso_mensual": "7,000",
        "devengado": "6,000",
        "girado": "5,000",
        "avance_porcentaje": "54.5",
    }
    assert records[1]["nivel_jerarquia"] == "PLIEGO"
    assert records[1]["codigo"] == "010"
    assert records[1]["descripcion"] == "M. DE EDUCACION"
    assert records[2]["nivel_jerarquia"] == "UNIDAD EJECUTORA"
    assert records[2]["codigo"] == "117-1438"
    assert records[2]["descripcion"] == (
        "PROGRAMA NACIONAL DE BECAS Y CREDITO EDUCATIVO"
    )


def test_extract_mef_producto_breakdown_rows_from_fake_html() -> None:
    soup = soup_from_html(
        breakdown_table(
            "3000885: ENTREGA DE BECA DE EDUCACION SUPERIOR A POBLACION CON ALTO RENDIMIENTO ACADEMICO",
            "SIN CODIGO PRODUCTO",
        )
    )

    records = extract_mef_breakdown_rows(
        soup=soup,
        ano=2026,
        slice_name="producto",
        periodo_tipo="ANUAL",
        periodo_valor="2026",
    )

    assert records[0] == {
        "ano": "2026",
        "periodo_tipo": "ANUAL",
        "periodo_valor": "2026",
        "codigo_producto": "3000885",
        "producto_proyecto": (
            "ENTREGA DE BECA DE EDUCACION SUPERIOR A POBLACION CON ALTO "
            "RENDIMIENTO ACADEMICO"
        ),
        "pia": "1,000",
        "pim": "2,000",
        "certificacion": "1,900",
        "compromiso_anual": "1,500",
        "compromiso_mensual": "1,100",
        "devengado": "1,000",
        "girado": "900",
        "avance_porcentaje": "50.0",
    }
    assert records[1]["codigo_producto"] == ""
    assert records[1]["producto_proyecto"] == "SIN CODIGO PRODUCTO"


def test_extract_mef_generica_breakdown_rows_from_fake_html() -> None:
    soup = soup_from_html(
        breakdown_table(
            "2.3: BIENES Y SERVICIOS",
            "2.5: OTROS GASTOS",
        )
    )

    records = extract_mef_breakdown_rows(
        soup=soup,
        ano=2026,
        slice_name="generica",
    )

    assert len(records) == 2
    assert records[0]["codigo_generica"] == "2.3"
    assert records[0]["generica"] == "BIENES Y SERVICIOS"
    assert records[1]["codigo_generica"] == "2.5"
    assert records[1]["generica"] == "OTROS GASTOS"


def test_extract_mef_funding_and_geography_breakdown_rows_from_fake_html() -> None:
    fuente_records = extract_mef_breakdown_rows(
        soup=soup_from_html(
            breakdown_table(
                "1: RECURSOS ORDINARIOS",
                "3: RECURSOS POR OPERACIONES OFICIALES DE CREDITO",
            )
        ),
        ano=2026,
        slice_name="fuente",
    )
    rubro_records = extract_mef_breakdown_rows(
        soup=soup_from_html(
            breakdown_table(
                "00: RECURSOS ORDINARIOS",
                "15: FONDO DE COMPENSACION REGIONAL",
            )
        ),
        ano=2026,
        slice_name="rubro",
    )
    departamento_records = extract_mef_breakdown_rows(
        soup=soup_from_html(
            breakdown_table(
                "LIMA",
                "15: LIMA",
            )
        ),
        ano=2026,
        slice_name="departamento",
    )

    assert fuente_records[0]["codigo_fuente"] == "1"
    assert fuente_records[0]["fuente_financiamiento"] == "RECURSOS ORDINARIOS"
    assert fuente_records[1]["codigo_fuente"] == "3"
    assert fuente_records[1]["fuente_financiamiento"] == (
        "RECURSOS POR OPERACIONES OFICIALES DE CREDITO"
    )

    assert rubro_records[0]["codigo_rubro"] == "00"
    assert rubro_records[0]["rubro"] == "RECURSOS ORDINARIOS"
    assert rubro_records[1]["codigo_rubro"] == "15"
    assert rubro_records[1]["rubro"] == "FONDO DE COMPENSACION REGIONAL"

    assert departamento_records[0]["departamento"] == "LIMA"
    assert departamento_records[1]["departamento"] == "15: LIMA"
    assert "codigo_departamento" not in departamento_records[1]


def test_parse_mef_temporal_period_normalizes_months_and_quarters() -> None:
    assert parse_mef_temporal_period("ENERO", ano=2026) == {
        "periodo_tipo": "MENSUAL",
        "periodo_valor": "2026-01",
        "trimestre": "1",
        "mes_numero": "01",
        "mes_nombre": "ENERO",
    }
    assert parse_mef_temporal_period("ABRIL", ano=2026)["trimestre"] == "2"
    assert parse_mef_temporal_period("JULIO", ano=2026)["trimestre"] == "3"
    assert parse_mef_temporal_period("OCTUBRE", ano=2026) == {
        "periodo_tipo": "MENSUAL",
        "periodo_valor": "2026-10",
        "trimestre": "4",
        "mes_numero": "10",
        "mes_nombre": "OCTUBRE",
    }
    assert parse_mef_temporal_period("TRIMESTRE 2", ano=2026) == {
        "periodo_tipo": "TRIMESTRAL",
        "periodo_valor": "2026-T2",
        "trimestre": "2",
        "mes_numero": "",
        "mes_nombre": "",
    }
    assert parse_mef_temporal_period("TOTAL ANUAL", ano=2026) == {
        "periodo_tipo": "ANUAL",
        "periodo_valor": "2026",
        "trimestre": "",
        "mes_numero": "",
        "mes_nombre": "",
    }


def test_extract_mef_temporal_monthly_rows_from_fake_html() -> None:
    records = extract_mef_breakdown_rows(
        soup=soup_from_html(breakdown_table("ENERO", "ABRIL")),
        ano=2026,
        slice_name="temporal",
    )

    assert records[0] == {
        "ano": "2026",
        "periodo_tipo": "MENSUAL",
        "periodo_valor": "2026-01",
        "trimestre": "1",
        "mes_numero": "01",
        "mes_nombre": "ENERO",
        "pia": "1,000",
        "pim": "2,000",
        "certificacion": "1,900",
        "compromiso_anual": "1,500",
        "compromiso_mensual": "1,100",
        "devengado": "1,000",
        "girado": "900",
        "avance_porcentaje": "50.0",
    }
    assert records[1]["periodo_tipo"] == "MENSUAL"
    assert records[1]["periodo_valor"] == "2026-04"
    assert records[1]["trimestre"] == "2"
    assert records[1]["mes_numero"] == "04"
    assert records[1]["mes_nombre"] == "ABRIL"


def test_extract_mef_temporal_quarterly_rows_from_fake_html() -> None:
    records = extract_mef_breakdown_rows(
        soup=soup_from_html(breakdown_table("TRIMESTRE 1", "T4")),
        ano=2026,
        slice_name="temporal",
    )

    assert records[0]["periodo_tipo"] == "TRIMESTRAL"
    assert records[0]["periodo_valor"] == "2026-T1"
    assert records[0]["trimestre"] == "1"
    assert records[0]["mes_numero"] == ""
    assert records[0]["mes_nombre"] == ""
    assert records[1]["periodo_tipo"] == "TRIMESTRAL"
    assert records[1]["periodo_valor"] == "2026-T4"
    assert records[1]["trimestre"] == "4"


def test_extract_mef_temporal_roman_quarters_and_anual() -> None:
    records = extract_mef_breakdown_rows(
        soup=soup_from_html(breakdown_table("I TRIMESTRE", "TRIMESTRE II", "TOTAL ANUAL")),
        ano=2026,
        slice_name="temporal",
    )

    assert len(records) == 3
    assert records[0]["periodo_tipo"] == "TRIMESTRAL"
    assert records[0]["periodo_valor"] == "2026-T1"
    assert records[0]["trimestre"] == "1"
    assert records[0]["mes_numero"] == ""
    assert records[0]["mes_nombre"] == ""

    assert records[1]["periodo_tipo"] == "TRIMESTRAL"
    assert records[1]["periodo_valor"] == "2026-T2"
    assert records[1]["trimestre"] == "2"

    assert records[2]["periodo_tipo"] == "ANUAL"
    assert records[2]["periodo_valor"] == "2026"
    assert records[2]["trimestre"] == ""
    assert records[2]["mes_numero"] == ""
    assert records[2]["mes_nombre"] == ""


def test_extract_mef_temporal_negative_and_empty_values() -> None:
    html = """
    <table>
      <tr class="Data">
        <td>ENERO</td>
        <td></td>
        <td></td>
        <td>-1,900</td>
        <td>-1,500</td>
        <td>1,100</td>
        <td>1,000</td>
        <td>900</td>
        <td></td>
      </tr>
    </table>
    """
    records = extract_mef_breakdown_rows(
        soup=soup_from_html(html),
        ano=2026,
        slice_name="temporal",
    )

    assert len(records) == 1
    assert records[0]["pia"] == ""
    assert records[0]["pim"] == ""
    assert records[0]["certificacion"] == "-1,900"
    assert records[0]["compromiso_anual"] == "-1,500"
    assert records[0]["avance_porcentaje"] == ""


def test_parse_breakdown_slices_defaults_cli_and_env_values() -> None:
    assert parse_breakdown_slices(None) == ["producto", "generica"]
    assert parse_breakdown_slices("") == ["producto", "generica"]
    assert parse_breakdown_slices("producto,generica") == ["producto", "generica"]
    assert parse_breakdown_slices(" generica ") == ["generica"]
    assert parse_breakdown_slices("temporal") == ["temporal"]
    assert parse_breakdown_slices("fuente,rubro,departamento") == [
        "fuente",
        "rubro",
        "departamento",
    ]
    assert parse_breakdown_slices("producto,generica,temporal") == [
        "producto",
        "generica",
        "temporal",
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
        "ejecutora_codigo": "117-1438",
        "ejecutora_nombre": "PROGRAMA NACIONAL DE BECAS Y CREDITO EDUCATIVO",
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
            "ejecutora_codigo": "117-1438",
            "ejecutora_nombre": "PROGRAMA NACIONAL DE BECAS Y CREDITO EDUCATIVO",
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
        "bronze/mef/presupuesto/extraction_date=2026-06-14/year=2026"
    )
    assert csv_path.exists()
    assert metadata_path.exists()

    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert metadata["metadata"] == {
        "mode": "dry_run",
        "source_mode": "consulta_amigable",
        "source_url": CONSULTA_AMIGABLE_BASE_URL,
        "source_file": None,
        "fiscal_year": "2026",
    }


def test_write_mef_hierarchy_to_local_includes_metadata(tmp_path: Path) -> None:
    logger_mock = type("MockLogger", (), {"log": lambda *args, **kwargs: None})()
    records = extract_mef_hierarchy_rows(soup_from_html(hierarchy_table()), ano=2026)

    result = write_mef_hierarchy_to_local(
        records=records,
        extraction_date="2026-06-14",
        output_dir=tmp_path,
        run_id="test_run",
        records_read=len(records),
        source_url=CONSULTA_AMIGABLE_BASE_URL,
        logger=logger_mock,
        source_mode="consulta_amigable",
    )

    csv_path = Path(result["output_uri"])
    metadata_path = Path(result["metadata_path"])

    assert str(csv_path.parent).replace("\\", "/").endswith(
        "bronze/mef/presupuesto_hierarchy/extraction_date=2026-06-14/year=2026"
    )
    assert csv_path.exists()
    assert metadata_path.exists()

    with csv_path.open("r", encoding="utf-8", newline="") as file:
        header = file.readline().strip().split(",")

    assert header == MEF_HIERARCHY_FIELDNAMES

    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert metadata["source_name"] == "MEF"
    assert metadata["source_dataset"] == "presupuesto_hierarchy"
    assert metadata["records_written"] == 3
    assert metadata["metadata"]["source_system"] == "MEF"
    assert metadata["metadata"]["source_dataset"] == "presupuesto_hierarchy"
    assert metadata["metadata"]["source_mode"] == "consulta_amigable"


def test_write_mef_breakdown_to_local_includes_metadata(tmp_path: Path) -> None:
    logger_mock = type("MockLogger", (), {"log": lambda *args, **kwargs: None})()
    slice_examples = {
        "producto": (
            "3000885: ENTREGA DE BECA",
            "presupuesto_producto",
        ),
        "generica": (
            "2.3: BIENES Y SERVICIOS",
            "presupuesto_generica",
        ),
        "fuente": (
            "1: RECURSOS ORDINARIOS",
            "presupuesto_fuente",
        ),
        "rubro": (
            "00: RECURSOS ORDINARIOS",
            "presupuesto_rubro",
        ),
        "departamento": (
            "15: LIMA",
            "presupuesto_departamento",
        ),
        "temporal": (
            "ENERO",
            "presupuesto_temporal",
        ),
    }

    for slice_name, (label, dataset) in slice_examples.items():
        records = extract_mef_breakdown_rows(
            soup_from_html(breakdown_table(label)),
            ano=2026,
            slice_name=slice_name,
        )

        result = write_mef_breakdown_to_local(
            records=records,
            extraction_date="2026-06-14",
            output_dir=tmp_path,
            run_id="test_run",
            records_read=len(records),
            source_url=CONSULTA_AMIGABLE_BASE_URL,
            slice_name=slice_name,
            logger=logger_mock,
            source_mode="consulta_amigable",
        )

        csv_path = Path(result["output_uri"])
        assert str(csv_path.parent).replace("\\", "/").endswith(
            f"bronze/mef/{dataset}/extraction_date=2026-06-14/year=2026"
        )
        assert csv_path.exists()

        metadata = json.loads(
            Path(result["metadata_path"]).read_text(encoding="utf-8")
        )
        assert metadata["source_dataset"] == dataset
        assert metadata["metadata"]["breakdown_slice"] == slice_name
        assert metadata["metadata"]["source_system"] == "MEF"


def test_mef_scraper_parametrization_cli_vs_env(monkeypatch) -> None:
    monkeypatch.setenv("MEF_SOURCE_MODE", "source_url")
    monkeypatch.setenv("MEF_SOURCE_URL", "https://example.com/test.csv")
    monkeypatch.setenv("GCS_BUCKET_NAME", "dummy-bucket")

    called_modes = []

    def mock_scrape_consulta_amigable_range(start_year, end_year, timeout):
        called_modes.append("consulta_amigable")
        return [{"ano": 2026, "ejecutora_nombre": "PRONABEC", "pia": 0, "pim": 0, "certificacion": 0, "compromiso_anual": 0, "compromiso_mensual": 0, "devengado": 0, "girado": 0, "avance_porcentaje": 0}]

    def mock_fetch_mef_records(source_url, source_file, timeout, table_index):
        called_modes.append("source_url")
        return []

    monkeypatch.setattr(scrape_mef_budget, "scrape_consulta_amigable_range", mock_scrape_consulta_amigable_range)
    monkeypatch.setattr(scrape_mef_budget, "fetch_mef_records", mock_fetch_mef_records)

    monkeypatch.setattr(scrape_mef_budget, "get_pipeline_settings", lambda config: {
        "pipeline_name": "test",
        "environment": "test",
        "log_level": "INFO",
        "bucket_name": "dummy-bucket",
        "gcs_paths": {"mef_bronze": "mef_bronze"}
    })

    monkeypatch.setattr(scrape_mef_budget, "load_yaml_config", lambda config: {
        "mef": {"expected_columns": ["ano", "ejecutora_codigo", "ejecutora_nombre", "pia", "pim", "certificacion", "compromiso_anual", "compromiso_mensual", "devengado", "girado", "avance_porcentaje"]}
    })

    args = scrape_mef_budget.parse_args([
        "--consulta-amigable",
        "--start-year", "2026",
        "--end-year", "2026",
        "--dry-run"
    ])
    scrape_mef_budget.run_extraction(args)
    assert "consulta_amigable" in called_modes
    called_modes.clear()

    monkeypatch.setenv("MEF_SOURCE_MODE", "consulta_amigable")
    monkeypatch.setenv("MEF_START_YEAR", "2026")
    monkeypatch.setenv("MEF_END_YEAR", "2026")

    args_env = scrape_mef_budget.parse_args([
        "--dry-run"
    ])
    scrape_mef_budget.run_extraction(args_env)
    assert "consulta_amigable" in called_modes
    called_modes.clear()

    monkeypatch.setenv("MEF_START_YEAR", "2020")
    monkeypatch.setenv("MEF_END_YEAR", "2020")

    resolved_years = []
    def mock_scrape_consulta_amigable_range_years(start_year, end_year, timeout):
        resolved_years.append((start_year, end_year))
        return [{"ano": 2026, "ejecutora_nombre": "PRONABEC", "pia": 0, "pim": 0, "certificacion": 0, "compromiso_anual": 0, "compromiso_mensual": 0, "devengado": 0, "girado": 0, "avance_porcentaje": 0}]

    monkeypatch.setattr(scrape_mef_budget, "scrape_consulta_amigable_range", mock_scrape_consulta_amigable_range_years)

    args_years = scrape_mef_budget.parse_args([
        "--consulta-amigable",
        "--start-year", "2026",
        "--end-year", "2026",
        "--dry-run"
    ])
    scrape_mef_budget.run_extraction(args_years)
    assert resolved_years == [(2026, 2026)]
    resolved_years.clear()

    monkeypatch.setenv("MEF_TIMEOUT_SECONDS", "45")
    resolved_timeouts = []
    def mock_scrape_consulta_amigable_range_timeout(start_year, end_year, timeout):
        resolved_timeouts.append(timeout)
        return [{"ano": 2026, "ejecutora_nombre": "PRONABEC", "pia": 0, "pim": 0, "certificacion": 0, "compromiso_anual": 0, "compromiso_mensual": 0, "devengado": 0, "girado": 0, "avance_porcentaje": 0}]

    monkeypatch.setattr(scrape_mef_budget, "scrape_consulta_amigable_range", mock_scrape_consulta_amigable_range_timeout)

    args_timeout = scrape_mef_budget.parse_args([
        "--consulta-amigable",
        "--start-year", "2026",
        "--end-year", "2026",
        "--dry-run"
    ])
    scrape_mef_budget.run_extraction(args_timeout)
    assert resolved_timeouts == [45]
    resolved_timeouts.clear()


def test_run_extraction_without_include_hierarchy_skips_hierarchy_output(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("MEF_INCLUDE_HIERARCHY", raising=False)
    monkeypatch.setenv("GCS_BUCKET_NAME", "dummy-bucket")

    monkeypatch.setattr(
        scrape_mef_budget,
        "get_pipeline_settings",
        lambda config: {
            "pipeline_name": "test",
            "environment": "test",
            "log_level": "INFO",
            "bucket_name": "dummy-bucket",
            "gcs_paths": {"mef_bronze": "mef_bronze"},
        },
    )
    monkeypatch.setattr(
        scrape_mef_budget,
        "load_yaml_config",
        lambda config: {
            "mef": {
                "expected_columns": [
                    "ano",
                    "ejecutora_codigo",
                    "ejecutora_nombre",
                    "pia",
                    "pim",
                    "certificacion",
                    "compromiso_anual",
                    "compromiso_mensual",
                    "devengado",
                    "girado",
                    "avance_porcentaje",
                ]
            }
        },
    )
    monkeypatch.setattr(
        scrape_mef_budget,
        "scrape_consulta_amigable_range",
        lambda start_year, end_year, timeout: [
            {
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
        ],
    )

    args = scrape_mef_budget.parse_args(
        [
            "--consulta-amigable",
            "--extraction-date",
            "2026-06-14",
            "--start-year",
            "2026",
            "--end-year",
            "2026",
            "--dry-run",
            "--output-dir",
            str(tmp_path),
        ]
    )
    scrape_mef_budget.run_extraction(args)

    assert (
        tmp_path
        / "bronze"
        / "mef"
        / "presupuesto"
        / "extraction_date=2026-06-14"
        / "year=2026"
        / "data.csv"
    ).exists()
    assert not (
        tmp_path
        / "bronze"
        / "mef"
        / "presupuesto_hierarchy"
        / "extraction_date=2026-06-14"
        / "year=2026"
        / "data.csv"
    ).exists()


def test_run_extraction_with_include_hierarchy_writes_hierarchy_output(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("GCS_BUCKET_NAME", "dummy-bucket")

    monkeypatch.setattr(
        scrape_mef_budget,
        "get_pipeline_settings",
        lambda config: {
            "pipeline_name": "test",
            "environment": "test",
            "log_level": "INFO",
            "bucket_name": "dummy-bucket",
            "gcs_paths": {"mef_bronze": "mef_bronze"},
        },
    )
    monkeypatch.setattr(
        scrape_mef_budget,
        "load_yaml_config",
        lambda config: {
            "mef": {
                "expected_columns": [
                    "ano",
                    "ejecutora_codigo",
                    "ejecutora_nombre",
                    "pia",
                    "pim",
                    "certificacion",
                    "compromiso_anual",
                    "compromiso_mensual",
                    "devengado",
                    "girado",
                    "avance_porcentaje",
                ]
            }
        },
    )
    monkeypatch.setattr(
        scrape_mef_budget,
        "scrape_consulta_amigable_range_snapshot",
        lambda start_year, end_year, timeout, include_hierarchy, breakdown_slices: {
            "budget_records": [
                {
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
            ],
            "hierarchy_records": extract_mef_hierarchy_rows(
                soup_from_html(hierarchy_table()),
                ano=2026,
            ),
            "breakdown_records": {},
        },
    )

    args = scrape_mef_budget.parse_args(
        [
            "--consulta-amigable",
            "--extraction-date",
            "2026-06-14",
            "--start-year",
            "2026",
            "--end-year",
            "2026",
            "--include-hierarchy",
            "--dry-run",
            "--output-dir",
            str(tmp_path),
        ]
    )
    scrape_mef_budget.run_extraction(args)

    hierarchy_path = (
        tmp_path
        / "bronze"
        / "mef"
        / "presupuesto_hierarchy"
        / "extraction_date=2026-06-14"
        / "year=2026"
    )

    assert (hierarchy_path / "data.csv").exists()
    metadata = json.loads(
        (hierarchy_path / "extraction_metadata.json").read_text(encoding="utf-8")
    )
    assert metadata["source_dataset"] == "presupuesto_hierarchy"
    assert metadata["metadata"]["source_mode"] == "consulta_amigable"
    assert metadata["records_written"] == 3


def test_run_extraction_without_spending_breakdowns_skips_slice_outputs(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("MEF_INCLUDE_SPENDING_BREAKDOWNS", raising=False)
    monkeypatch.delenv("MEF_BREAKDOWN_SLICES", raising=False)
    monkeypatch.setenv("GCS_BUCKET_NAME", "dummy-bucket")

    monkeypatch.setattr(
        scrape_mef_budget,
        "get_pipeline_settings",
        lambda config: {
            "pipeline_name": "test",
            "environment": "test",
            "log_level": "INFO",
            "bucket_name": "dummy-bucket",
            "gcs_paths": {"mef_bronze": "mef_bronze"},
        },
    )
    monkeypatch.setattr(
        scrape_mef_budget,
        "load_yaml_config",
        lambda config: {
            "mef": {
                "expected_columns": [
                    "ano",
                    "ejecutora_codigo",
                    "ejecutora_nombre",
                    "pia",
                    "pim",
                    "certificacion",
                    "compromiso_anual",
                    "compromiso_mensual",
                    "devengado",
                    "girado",
                    "avance_porcentaje",
                ]
            }
        },
    )
    monkeypatch.setattr(
        scrape_mef_budget,
        "scrape_consulta_amigable_range",
        lambda start_year, end_year, timeout: [
            {
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
        ],
    )

    args = scrape_mef_budget.parse_args(
        [
            "--consulta-amigable",
            "--extraction-date",
            "2026-06-14",
            "--start-year",
            "2026",
            "--end-year",
            "2026",
            "--dry-run",
            "--output-dir",
            str(tmp_path),
        ]
    )
    scrape_mef_budget.run_extraction(args)

    assert not (
        tmp_path
        / "bronze"
        / "mef"
        / "presupuesto_producto"
        / "extraction_date=2026-06-14"
        / "year=2026"
        / "data.csv"
    ).exists()
    assert not (
        tmp_path
        / "bronze"
        / "mef"
        / "presupuesto_generica"
        / "extraction_date=2026-06-14"
        / "year=2026"
        / "data.csv"
    ).exists()


def test_run_extraction_with_spending_breakdowns_writes_selected_slices(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("GCS_BUCKET_NAME", "dummy-bucket")

    monkeypatch.setattr(
        scrape_mef_budget,
        "get_pipeline_settings",
        lambda config: {
            "pipeline_name": "test",
            "environment": "test",
            "log_level": "INFO",
            "bucket_name": "dummy-bucket",
            "gcs_paths": {"mef_bronze": "mef_bronze"},
        },
    )
    monkeypatch.setattr(
        scrape_mef_budget,
        "load_yaml_config",
        lambda config: {
            "mef": {
                "expected_columns": [
                    "ano",
                    "ejecutora_codigo",
                    "ejecutora_nombre",
                    "pia",
                    "pim",
                    "certificacion",
                    "compromiso_anual",
                    "compromiso_mensual",
                    "devengado",
                    "girado",
                    "avance_porcentaje",
                ]
            }
        },
    )

    captured_slices = []

    def fake_range_snapshot(
        start_year,
        end_year,
        timeout,
        include_hierarchy,
        breakdown_slices,
    ):
        captured_slices.extend(breakdown_slices)
        return {
            "budget_records": [
                {
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
            ],
            "hierarchy_records": [],
            "breakdown_records": {
                "producto": extract_mef_breakdown_rows(
                    soup_from_html(breakdown_table("3000885: ENTREGA DE BECA")),
                    ano=2026,
                    slice_name="producto",
                ),
                "generica": extract_mef_breakdown_rows(
                    soup_from_html(breakdown_table("2.3: BIENES Y SERVICIOS")),
                    ano=2026,
                    slice_name="generica",
                ),
            },
        }

    monkeypatch.setattr(
        scrape_mef_budget,
        "scrape_consulta_amigable_range_snapshot",
        fake_range_snapshot,
    )

    args = scrape_mef_budget.parse_args(
        [
            "--consulta-amigable",
            "--extraction-date",
            "2026-06-14",
            "--start-year",
            "2026",
            "--end-year",
            "2026",
            "--include-spending-breakdowns",
            "--breakdown-slices",
            "producto,generica",
            "--dry-run",
            "--output-dir",
            str(tmp_path),
        ]
    )
    scrape_mef_budget.run_extraction(args)

    assert captured_slices == ["producto", "generica"]
    producto_path = (
        tmp_path
        / "bronze"
        / "mef"
        / "presupuesto_producto"
        / "extraction_date=2026-06-14"
        / "year=2026"
    )
    generica_path = (
        tmp_path
        / "bronze"
        / "mef"
        / "presupuesto_generica"
        / "extraction_date=2026-06-14"
        / "year=2026"
    )

    assert (producto_path / "data.csv").exists()
    assert (generica_path / "data.csv").exists()
    producto_metadata = json.loads(
        (producto_path / "extraction_metadata.json").read_text(encoding="utf-8")
    )
    generica_metadata = json.loads(
        (generica_path / "extraction_metadata.json").read_text(encoding="utf-8")
    )
    assert producto_metadata["metadata"]["breakdown_slice"] == "producto"
    assert generica_metadata["metadata"]["breakdown_slice"] == "generica"


def test_run_extraction_with_funding_and_geography_breakdowns_writes_slices(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("GCS_BUCKET_NAME", "dummy-bucket")

    monkeypatch.setattr(
        scrape_mef_budget,
        "get_pipeline_settings",
        lambda config: {
            "pipeline_name": "test",
            "environment": "test",
            "log_level": "INFO",
            "bucket_name": "dummy-bucket",
            "gcs_paths": {"mef_bronze": "mef_bronze"},
        },
    )
    monkeypatch.setattr(
        scrape_mef_budget,
        "load_yaml_config",
        lambda config: {
            "mef": {
                "expected_columns": [
                    "ano",
                    "ejecutora_codigo",
                    "ejecutora_nombre",
                    "pia",
                    "pim",
                    "certificacion",
                    "compromiso_anual",
                    "compromiso_mensual",
                    "devengado",
                    "girado",
                    "avance_porcentaje",
                ]
            }
        },
    )

    captured_slices = []

    def fake_range_snapshot(
        start_year,
        end_year,
        timeout,
        include_hierarchy,
        breakdown_slices,
    ):
        captured_slices.extend(breakdown_slices)
        return {
            "budget_records": [
                {
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
            ],
            "hierarchy_records": [],
            "breakdown_records": {
                "fuente": extract_mef_breakdown_rows(
                    soup_from_html(breakdown_table("1: RECURSOS ORDINARIOS")),
                    ano=2026,
                    slice_name="fuente",
                ),
                "rubro": extract_mef_breakdown_rows(
                    soup_from_html(breakdown_table("00: RECURSOS ORDINARIOS")),
                    ano=2026,
                    slice_name="rubro",
                ),
                "departamento": extract_mef_breakdown_rows(
                    soup_from_html(breakdown_table("15: LIMA")),
                    ano=2026,
                    slice_name="departamento",
                ),
            },
        }

    monkeypatch.setattr(
        scrape_mef_budget,
        "scrape_consulta_amigable_range_snapshot",
        fake_range_snapshot,
    )

    args = scrape_mef_budget.parse_args(
        [
            "--consulta-amigable",
            "--extraction-date",
            "2026-06-14",
            "--start-year",
            "2026",
            "--end-year",
            "2026",
            "--include-spending-breakdowns",
            "--breakdown-slices",
            "fuente,rubro,departamento",
            "--dry-run",
            "--output-dir",
            str(tmp_path),
        ]
    )
    scrape_mef_budget.run_extraction(args)

    assert captured_slices == ["fuente", "rubro", "departamento"]

    expected_paths = {
        "fuente": "presupuesto_fuente",
        "rubro": "presupuesto_rubro",
        "departamento": "presupuesto_departamento",
    }
    for slice_name, dataset in expected_paths.items():
        slice_path = (
            tmp_path
            / "bronze"
            / "mef"
            / dataset
            / "extraction_date=2026-06-14"
            / "year=2026"
        )
        assert (slice_path / "data.csv").exists()
        metadata = json.loads(
            (slice_path / "extraction_metadata.json").read_text(encoding="utf-8")
        )
        assert metadata["source_dataset"] == dataset
        assert metadata["metadata"]["breakdown_slice"] == slice_name


def test_run_extraction_with_temporal_breakdown_writes_slice(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("GCS_BUCKET_NAME", "dummy-bucket")

    monkeypatch.setattr(
        scrape_mef_budget,
        "get_pipeline_settings",
        lambda config: {
            "pipeline_name": "test",
            "environment": "test",
            "log_level": "INFO",
            "bucket_name": "dummy-bucket",
            "gcs_paths": {"mef_bronze": "mef_bronze"},
        },
    )
    monkeypatch.setattr(
        scrape_mef_budget,
        "load_yaml_config",
        lambda config: {
            "mef": {
                "expected_columns": [
                    "ano",
                    "ejecutora_codigo",
                    "ejecutora_nombre",
                    "pia",
                    "pim",
                    "certificacion",
                    "compromiso_anual",
                    "compromiso_mensual",
                    "devengado",
                    "girado",
                    "avance_porcentaje",
                ]
            }
        },
    )

    captured_slices = []

    def fake_range_snapshot(
        start_year,
        end_year,
        timeout,
        include_hierarchy,
        breakdown_slices,
    ):
        captured_slices.extend(breakdown_slices)
        return {
            "budget_records": [
                {
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
            ],
            "hierarchy_records": [],
            "breakdown_records": {
                "temporal": extract_mef_breakdown_rows(
                    soup_from_html(breakdown_table("ENERO")),
                    ano=2026,
                    slice_name="temporal",
                ),
            },
        }

    monkeypatch.setattr(
        scrape_mef_budget,
        "scrape_consulta_amigable_range_snapshot",
        fake_range_snapshot,
    )

    args = scrape_mef_budget.parse_args(
        [
            "--consulta-amigable",
            "--extraction-date",
            "2026-06-14",
            "--start-year",
            "2026",
            "--end-year",
            "2026",
            "--include-spending-breakdowns",
            "--breakdown-slices",
            "temporal",
            "--dry-run",
            "--output-dir",
            str(tmp_path),
        ]
    )
    scrape_mef_budget.run_extraction(args)

    assert captured_slices == ["temporal"]
    temporal_path = (
        tmp_path
        / "bronze"
        / "mef"
        / "presupuesto_temporal"
        / "extraction_date=2026-06-14"
        / "year=2026"
    )
    assert (temporal_path / "data.csv").exists()

    metadata = json.loads(
        (temporal_path / "extraction_metadata.json").read_text(encoding="utf-8")
    )
    assert metadata["source_dataset"] == "presupuesto_temporal"
    assert metadata["metadata"]["breakdown_slice"] == "temporal"


def test_mef_scraper_parametrization_executora_and_base_url(monkeypatch) -> None:
    monkeypatch.setenv("MEF_PRONABEC_EXECUTORA_CODE", "999-9999")
    monkeypatch.setenv("MEF_PRONABEC_EXECUTORA_NAME", "UNIDAD PRUEBA DE BECAS Y FINANCIAMIENTO")
    monkeypatch.setenv("MEF_CONSULTA_AMIGABLE_BASE_URL", "https://custom-mef.example.com/")

    assert scrape_mef_budget.get_consulta_amigable_base_url() == "https://custom-mef.example.com/"
    assert scrape_mef_budget.get_consulta_amigable_default_url() == "https://custom-mef.example.com/default.aspx"
    assert scrape_mef_budget.get_consulta_amigable_navigate_url() == "https://custom-mef.example.com/Navegar.aspx"
    assert scrape_mef_budget.get_mef_pronabec_executora_name() == "999-9999: UNIDAD PRUEBA DE BECAS Y FINANCIAMIENTO"

    executora_code = "999-9999"
    executora_name = "UNIDAD PRUEBA DE BECAS Y FINANCIAMIENTO"
    import re
    name_parts = [p.strip() for p in re.split(r'\s+[yY]\s+', executora_name) if p.strip()]
    primary_filters = [executora_code] + name_parts
    assert primary_filters == ["999-9999", "UNIDAD PRUEBA DE BECAS", "FINANCIAMIENTO"]


class FakeNestedSession:
    def __init__(self) -> None:
        self.headers: dict[str, str] = {}
        self.post_calls: list[dict[str, object]] = []

    def post(self, url: str, data: dict[str, str], timeout: int) -> FakeResponse:
        self.post_calls.append({"url": url, "data": data, "timeout": timeout})
        button = data.get("__EVENTTARGET") or ""
        if not button:
            for k in data.keys():
                if "Btn" in k:
                    button = k
                    break
        grp1 = data.get("grp1") or ""

        if "BtnProdProy" in button:
            html = base_form(
                f"""
                <table>
                  {radio_row("P1", "3000885: ENTREGA DE BECA")}
                  {radio_row("P2", "3000001: ACCIONES COMUNES")}
                </table>
                """
            )
        elif any(b in button for b in ("BtnMes", "BtnTrimestre", "BtnPeriodo")):
            if grp1 == "P1":
                html = breakdown_table("ENERO")
            elif grp1 == "P2":
                html = breakdown_table("FEBRERO")
            else:
                html = breakdown_table("TOTAL ANUAL")
        else:
            html = breakdown_table("TOTAL ANUAL")

        return FakeResponse(html, url=url)


def test_scrape_consulta_amigable_breakdown_snapshot_nested_slices() -> None:
    session = FakeNestedSession()
    base_soup = BeautifulSoup(
        base_form("<table>" + radio_row("U1", "117-1438: PRONABEC") + "</table>"),
        "html.parser",
    )
    
    slices = ["producto_temporal"]
    res = scrape_mef_budget.scrape_consulta_amigable_breakdown_snapshot(
        session=session,
        base_soup=base_soup,
        navigate_url="https://example.test/Navegar.aspx",
        year=2026,
        timeout=10,
        breakdown_slices=slices,
    )

    # 1. Verify producto_temporal
    prod_temp = res["producto_temporal"]
    assert len(prod_temp) == 2
    assert prod_temp[0]["periodo_tipo"] == "MENSUAL"
    assert prod_temp[0]["periodo_valor"] == "2026-01"
    assert prod_temp[0]["codigo_producto"] == "3000885"
    assert prod_temp[0]["producto"] == "ENTREGA DE BECA"
    assert prod_temp[1]["periodo_tipo"] == "MENSUAL"
    assert prod_temp[1]["periodo_valor"] == "2026-02"
    assert prod_temp[1]["codigo_producto"] == "3000001"
    assert prod_temp[1]["producto"] == "ACCIONES COMUNES"

