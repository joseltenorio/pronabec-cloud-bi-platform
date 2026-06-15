"""
Extractor MEF presupuesto -> Cloud Storage Bronze.

Este script prepara la ingesta presupuestal del MEF hacia Cloud Storage Bronze.

La fuente MEF puede ser más frágil que PRONABEC porque Consulta Amigable es un
módulo web público y no siempre expone una API tabular estable para el caso de
uso específico. Por eso el extractor soporta entradas controladas:

- URL CSV.
- URL HTML con tabla presupuestal.
- Archivo CSV local controlado.

La salida Bronze del proyecto es:

gs://<bucket>/bronze/mef/presupuesto/extraction_date=YYYY-MM-DD/data.csv

La limpieza fuerte y conversión de tipos se realizará después en Silver.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

from pipelines.common.audit import create_extraction_audit_event, generate_run_id
from pipelines.common.config import ConfigError, get_env_var, get_pipeline_settings, load_yaml_config
from pipelines.common.gcs import build_mef_bronze_path, upload_csv, upload_json
from pipelines.common.logging import log_event, setup_structured_logger
from pipelines.common.validation import validate_required_columns


DEFAULT_ENDPOINTS_CONFIG = "config/endpoints.yaml"
DEFAULT_PIPELINE_CONFIG = "config/pipeline.yaml"
DEFAULT_TIMEOUT_SECONDS = 90
MEF_HIERARCHY_FIELDNAMES = [
    "ano",
    "periodo_tipo",
    "periodo_valor",
    "nivel_jerarquia",
    "codigo",
    "descripcion",
    "pia",
    "pim",
    "certificacion",
    "compromiso_anual",
    "compromiso_mensual",
    "devengado",
    "girado",
    "avance_porcentaje",
]
MEF_BREAKDOWN_CONFIG = {
    "producto": {
        "button_name": "ctl00$CPH1$BtnProdProy",
        "source_dataset": "presupuesto_producto",
        "code_field": "codigo_producto",
        "description_field": "producto_proyecto",
        "fieldnames": [
            "ano",
            "periodo_tipo",
            "periodo_valor",
            "codigo_producto",
            "producto_proyecto",
            "pia",
            "pim",
            "certificacion",
            "compromiso_anual",
            "compromiso_mensual",
            "devengado",
            "girado",
            "avance_porcentaje",
        ],
    },
    "generica": {
        "button_name": "ctl00$CPH1$BtnGenerica",
        "source_dataset": "presupuesto_generica",
        "code_field": "codigo_generica",
        "description_field": "generica",
        "fieldnames": [
            "ano",
            "periodo_tipo",
            "periodo_valor",
            "codigo_generica",
            "generica",
            "pia",
            "pim",
            "certificacion",
            "compromiso_anual",
            "compromiso_mensual",
            "devengado",
            "girado",
            "avance_porcentaje",
        ],
    },
    "fuente": {
        "button_name": "ctl00$CPH1$BtnFuenteAgregada",
        "source_dataset": "presupuesto_fuente",
        "code_field": "codigo_fuente",
        "description_field": "fuente_financiamiento",
        "fieldnames": [
            "ano",
            "periodo_tipo",
            "periodo_valor",
            "codigo_fuente",
            "fuente_financiamiento",
            "pia",
            "pim",
            "certificacion",
            "compromiso_anual",
            "compromiso_mensual",
            "devengado",
            "girado",
            "avance_porcentaje",
        ],
    },
    "rubro": {
        "button_name": "ctl00$CPH1$BtnRubro",
        "source_dataset": "presupuesto_rubro",
        "code_field": "codigo_rubro",
        "description_field": "rubro",
        "fieldnames": [
            "ano",
            "periodo_tipo",
            "periodo_valor",
            "codigo_rubro",
            "rubro",
            "pia",
            "pim",
            "certificacion",
            "compromiso_anual",
            "compromiso_mensual",
            "devengado",
            "girado",
            "avance_porcentaje",
        ],
    },
    "departamento": {
        "button_name": "ctl00$CPH1$BtnDepartamentoMeta",
        "source_dataset": "presupuesto_departamento",
        "code_field": None,
        "description_field": "departamento",
        "fieldnames": [
            "ano",
            "periodo_tipo",
            "periodo_valor",
            "departamento",
            "pia",
            "pim",
            "certificacion",
            "compromiso_anual",
            "compromiso_mensual",
            "devengado",
            "girado",
            "avance_porcentaje",
        ],
    },
    "temporal": {
        "button_name": "ctl00$CPH1$BtnMes",
        "button_names": [
            "ctl00$CPH1$BtnMes",
            "ctl00$CPH1$BtnTrimestre",
            "ctl00$CPH1$BtnPeriodo",
        ],
        "source_dataset": "presupuesto_temporal",
        "code_field": None,
        "description_field": None,
        "fieldnames": [
            "ano",
            "periodo_tipo",
            "periodo_valor",
            "trimestre",
            "mes_numero",
            "mes_nombre",
            "pia",
            "pim",
            "certificacion",
            "compromiso_anual",
            "compromiso_mensual",
            "devengado",
            "girado",
            "avance_porcentaje",
        ],
    },
    "producto_temporal": {
        "button_name": "ctl00$CPH1$BtnMes",
        "button_names": [
            "ctl00$CPH1$BtnMes",
            "ctl00$CPH1$BtnTrimestre",
            "ctl00$CPH1$BtnPeriodo",
        ],
        "source_dataset": "presupuesto_producto_temporal",
        "code_field": None,
        "description_field": None,
        "fieldnames": [
            "ano",
            "periodo_tipo",
            "periodo_valor",
            "trimestre",
            "mes_numero",
            "mes_nombre",
            "codigo_producto",
            "producto",
            "pia",
            "pim",
            "certificacion",
            "compromiso_anual",
            "compromiso_mensual",
            "devengado",
            "girado",
            "avance_porcentaje",
        ],
    },
    "actividad": {
        "button_name": "ctl00$CPH1$BtnActProyObra",
        "source_dataset": "presupuesto_actividad",
        "code_field": "codigo_actividad",
        "description_field": "actividad",
        "fieldnames": [
            "ano",
            "codigo_producto",
            "producto",
            "codigo_actividad",
            "actividad",
            "pia",
            "pim",
            "certificacion",
            "compromiso_anual",
            "compromiso_mensual",
            "devengado",
            "girado",
            "avance_porcentaje",
        ],
    },
    "actividad_temporal": {
        "button_name": "ctl00$CPH1$BtnMes",
        "button_names": [
            "ctl00$CPH1$BtnMes",
            "ctl00$CPH1$BtnTrimestre",
            "ctl00$CPH1$BtnPeriodo",
        ],
        "source_dataset": "presupuesto_actividad_temporal",
        "code_field": None,
        "description_field": None,
        "fieldnames": [
            "ano",
            "periodo_tipo",
            "periodo_valor",
            "trimestre",
            "mes_numero",
            "mes_nombre",
            "codigo_producto",
            "producto",
            "codigo_actividad",
            "actividad",
            "pia",
            "pim",
            "certificacion",
            "compromiso_anual",
            "compromiso_mensual",
            "devengado",
            "girado",
            "avance_porcentaje",
        ],
    },
    "generica_temporal": {
        "button_name": "ctl00$CPH1$BtnMes",
        "button_names": [
            "ctl00$CPH1$BtnMes",
            "ctl00$CPH1$BtnTrimestre",
            "ctl00$CPH1$BtnPeriodo",
        ],
        "source_dataset": "presupuesto_generica_temporal",
        "code_field": None,
        "description_field": None,
        "fieldnames": [
            "ano",
            "periodo_tipo",
            "periodo_valor",
            "trimestre",
            "mes_numero",
            "mes_nombre",
            "codigo_generica",
            "generica",
            "pia",
            "pim",
            "certificacion",
            "compromiso_anual",
            "compromiso_mensual",
            "devengado",
            "girado",
            "avance_porcentaje",
        ],
    },
}
DEFAULT_MEF_BREAKDOWN_SLICES = ["producto", "generica"]
MEF_MONTHS = {
    "ENERO": ("01", "1"),
    "FEBRERO": ("02", "1"),
    "MARZO": ("03", "1"),
    "ABRIL": ("04", "2"),
    "MAYO": ("05", "2"),
    "JUNIO": ("06", "2"),
    "JULIO": ("07", "3"),
    "AGOSTO": ("08", "3"),
    "SETIEMBRE": ("09", "3"),
    "SEPTIEMBRE": ("09", "3"),
    "OCTUBRE": ("10", "4"),
    "NOVIEMBRE": ("11", "4"),
    "DICIEMBRE": ("12", "4"),
}
CONSULTA_AMIGABLE_BASE_URL = "https://apps5.mineco.gob.pe/transparencia/Navegador/"
CONSULTA_AMIGABLE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-PE,es;q=0.9,en;q=0.8",
}


def get_consulta_amigable_base_url() -> str:
    """
    Obtiene la URL base de Consulta Amigable parametrizada.
    """
    base = os.getenv("MEF_CONSULTA_AMIGABLE_BASE_URL", CONSULTA_AMIGABLE_BASE_URL)
    if not base.endswith("/"):
        base += "/"
    return base


def get_consulta_amigable_default_url() -> str:
    """
    Obtiene la URL default.aspx construida a partir de la base.
    """
    return get_consulta_amigable_base_url() + "default.aspx"


def get_consulta_amigable_navigate_url() -> str:
    """
    Obtiene la URL Navegar.aspx construida a partir de la base.
    """
    return get_consulta_amigable_base_url() + "Navegar.aspx"


def get_mef_pronabec_executora_name() -> str:
    """
    Obtiene el nombre completo formateado de la Unidad Ejecutora de PRONABEC.
    """
    code = os.getenv("MEF_PRONABEC_EXECUTORA_CODE", "117-1438")
    name = os.getenv("MEF_PRONABEC_EXECUTORA_NAME", "PROGRAMA NACIONAL DE BECAS Y CREDITO EDUCATIVO")
    return f"{code}: {name}"


def parse_bool_env(value: str | None, default: bool = False) -> bool:
    """
    Convierte una variable de entorno booleana comun a bool.
    """
    if value is None or value == "":
        return default

    normalized = value.strip().lower()
    if normalized in {"1", "true", "t", "yes", "y", "si", "sí"}:
        return True
    if normalized in {"0", "false", "f", "no", "n"}:
        return False

    raise ConfigError(f"Valor booleano no valido: {value}")


def parse_breakdown_slices(value: str | None) -> list[str]:
    """
    Lee slices MEF separados por coma.
    """
    if value is None or value.strip() == "":
        return DEFAULT_MEF_BREAKDOWN_SLICES.copy()

    slices = [
        item.strip().lower()
        for item in value.split(",")
        if item.strip()
    ]
    invalid_slices = [
        item
        for item in slices
        if item not in MEF_BREAKDOWN_CONFIG
    ]
    if invalid_slices:
        raise ConfigError(
            "Slices MEF no soportados: "
            f"{invalid_slices}. Soportados: {sorted(MEF_BREAKDOWN_CONFIG)}"
        )

    return slices


class MEFExtractionError(Exception):
    """Error controlado durante extracción MEF."""


COLUMN_ALIASES = {
    "ano": {
        "ano",
        "año",
        "anio",
        "year",
        "ejercicio",
        "periodo",
        "periodo_fiscal",
    },
    "ejecutora_nombre": {
        "ejecutora_nombre",
        "unidad_ejecutora",
        "ue",
        "nombre_ejecutora",
        "ejecutora",
        "pliego",
        "entidad",
        "sector",
        "nombre",
    },
    "pia": {
        "pia",
        "presupuesto_institucional_de_apertura",
        "presupuesto_apertura",
    },
    "pim": {
        "pim",
        "presupuesto_institucional_modificado",
        "presupuesto_modificado",
    },
    "certificacion": {
        "certificacion",
        "certificación",
        "certificado",
        "monto_certificado",
    },
    "compromiso_anual": {
        "compromiso_anual",
        "compromiso_anualizado",
        "compromiso",
    },
    "compromiso_mensual": {
        "compromiso_mensual",
        "compromiso_mes",
        "compromiso_m",
    },
    "devengado": {
        "devengado",
        "monto_devengado",
    },
    "girado": {
        "girado",
        "monto_girado",
    },
    "avance_porcentaje": {
        "avance_porcentaje",
        "avance_%",
        "avance",
        "porcentaje_avance",
        "%_avance",
    },
}


def normalize_column_name(value: str) -> str:
    """
    Normaliza nombres de columnas para facilitar el mapeo.
    """
    normalized = value.strip().lower()
    normalized = normalized.replace("%", " porcentaje ")
    normalized = normalized.replace("°", "")
    normalized = normalized.replace("º", "")
    normalized = re.sub(r"[áàäâ]", "a", normalized)
    normalized = re.sub(r"[éèëê]", "e", normalized)
    normalized = re.sub(r"[íìïî]", "i", normalized)
    normalized = re.sub(r"[óòöô]", "o", normalized)
    normalized = re.sub(r"[úùüû]", "u", normalized)
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized


def resolve_column_mapping(
    actual_columns: list[str],
    expected_columns: list[str],
) -> dict[str, str]:
    """
    Construye un mapeo desde columnas reales hacia columnas esperadas.

    Returns:
        Diccionario {expected_column: actual_column}
    """
    normalized_actual = {
        normalize_column_name(column): column
        for column in actual_columns
    }

    mapping: dict[str, str] = {}

    for expected_column in expected_columns:
        aliases = COLUMN_ALIASES.get(expected_column, {expected_column})
        normalized_aliases = {normalize_column_name(alias) for alias in aliases}

        for alias in normalized_aliases:
            if alias in normalized_actual:
                mapping[expected_column] = normalized_actual[alias]
                break

    if "ejecutora_codigo" in expected_columns and "ejecutora_codigo" not in mapping:
        nombre_col = mapping.get("ejecutora_nombre")
        if nombre_col:
            mapping["ejecutora_codigo"] = nombre_col

    return mapping


def normalize_record(
    record: dict[str, Any],
    column_mapping: dict[str, str],
    expected_columns: list[str],
) -> dict[str, Any]:
    """
    Normaliza un registro al contrato Bronze esperado para MEF.
    """
    normalized: dict[str, Any] = {}

    for expected_column in expected_columns:
        source_column = column_mapping.get(expected_column)
        value = record.get(source_column) if source_column else None
        normalized[expected_column] = clean_cell_value(value)

    if "ejecutora_nombre" in normalized:
        label = normalized["ejecutora_nombre"]
        code, description = split_mef_code_description(label)
        if code:
            if not normalized.get("ejecutora_codigo") or normalized.get("ejecutora_codigo") == label:
                normalized["ejecutora_codigo"] = code
                normalized["ejecutora_nombre"] = description
        else:
            if normalized.get("ejecutora_codigo") == label:
                normalized["ejecutora_codigo"] = ""

    return normalized


def clean_cell_value(value: Any) -> str:
    """
    Limpia valores de celda sin aplicar conversión fuerte de tipos.
    """
    if value is None:
        return ""

    cleaned = str(value).replace("\xa0", " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def clean_mef_number(value: str | None) -> float:
    """
    Convierte valores numericos del portal MEF a float.
    """
    if value is None:
        return 0.0

    cleaned = clean_cell_value(value)
    if cleaned in {"", "-"}:
        return 0.0

    cleaned = cleaned.replace("%", "")
    cleaned = cleaned.replace(",", "")
    cleaned = re.sub(r"[^0-9.\-]", "", cleaned)

    if cleaned in {"", "-", "."}:
        return 0.0

    return float(cleaned)


def build_mef_form_payload(soup: BeautifulSoup, next_button_name: str) -> dict[str, str]:
    """
    Extrae campos de formulario ASP.NET y agrega el boton que dispara el POST.
    """
    payload: dict[str, str] = {}

    for field in soup.find_all(["input", "select", "textarea"]):
        name = field.get("name")
        if not name:
            continue

        tag_name = field.name
        field_type = (field.get("type") or "").lower()

        if tag_name == "input" and field_type in {"submit", "button", "image"}:
            continue

        if tag_name == "input" and field_type in {"radio", "checkbox"}:
            if field.has_attr("checked"):
                payload[name] = field.get("value", "on")
            continue

        if tag_name == "select":
            selected_option = field.find("option", selected=True) or field.find("option")
            payload[name] = selected_option.get("value", "") if selected_option else ""
            continue

        payload[name] = field.get("value", field.get_text("", strip=True))

    payload[next_button_name] = ""
    return payload


def normalize_mef_text(value: str) -> str:
    """
    Normaliza texto para busquedas tolerantes en Consulta Amigable.
    """
    normalized = clean_cell_value(value).upper()
    replacements = {
        "Á": "A",
        "É": "E",
        "Í": "I",
        "Ó": "O",
        "Ú": "U",
        "Ü": "U",
        "Ñ": "N",
    }
    for source, target in replacements.items():
        normalized = normalized.replace(source, target)
    return normalized


def text_matches_all_filters(value: str, text_filters: list[str]) -> bool:
    normalized = normalize_mef_text(value)
    return all(normalize_mef_text(text_filter) in normalized for text_filter in text_filters)


def find_mef_grp1_value(
    soup: BeautifulSoup,
    text_filters: list[str],
) -> tuple[str | None, str | None]:
    """
    Busca el valor de radio `grp1` cuyo texto asociado contiene los filtros.
    """
    for radio in soup.find_all("input", {"name": "grp1"}):
        value = radio.get("value")
        row = radio.find_parent("tr")
        label = clean_cell_value(row.get_text(" ")) if row else ""

        if label and text_matches_all_filters(label, text_filters):
            return value, label

    for label in soup.find_all("label"):
        label_text = clean_cell_value(label.get_text(" "))
        if not text_matches_all_filters(label_text, text_filters):
            continue

        radio_id = label.get("for")
        radio = soup.find("input", {"id": radio_id}) if radio_id else None
        if radio and radio.get("name") == "grp1":
            return radio.get("value"), label_text

    return None, None


def extract_mef_budget_row(
    soup: BeautifulSoup,
    text_filters: list[str],
) -> list[str] | None:
    """
    Extrae la fila presupuestal final desde tablas de Consulta Amigable.
    """
    tables = soup.find_all("table", class_="Data") or soup.find_all("table")

    for table in tables:
        for row in table.find_all("tr"):
            cells = [
                clean_cell_value(cell.get_text(" "))
                for cell in row.find_all(["td", "th"])
            ]
            if len(cells) < 9:
                continue

            row_text = " ".join(cells)
            if text_matches_all_filters(row_text, text_filters):
                return cells

    return None


def split_mef_code_description(value: str) -> tuple[str, str]:
    """
    Separa textos tipo `010: M. DE EDUCACION` en codigo y descripcion.
    """
    cleaned = clean_cell_value(value)
    match = re.match(r"^([^:]+):\s*(.+)$", cleaned)
    if not match:
        return "", cleaned

    return clean_cell_value(match.group(1)), clean_cell_value(match.group(2))


def parse_mef_hierarchy_descriptor(
    explicit_level: str,
    label: str,
) -> tuple[str, str, str]:
    """
    Obtiene nivel, codigo y descripcion desde una etiqueta de jerarquia MEF.
    """
    cleaned_level = clean_cell_value(explicit_level)
    cleaned_label = clean_cell_value(label)
    prefixed_match = re.match(
        r"^(Nivel de Gobierno|Sector|Pliego|Unidad Ejecutora|Funci[oó]n|"
        r"Divisi[oó]n Funcional|Grupo Funcional|Producto/Proyecto|"
        r"Actividad/Acci[oó]n|Categor[ií]a Presupuestal|Meta)\s+([^:]+):\s*(.+)$",
        cleaned_label,
        flags=re.IGNORECASE,
    )

    if prefixed_match:
        return (
            normalize_mef_text(prefixed_match.group(1)),
            clean_cell_value(prefixed_match.group(2)),
            clean_cell_value(prefixed_match.group(3)),
        )

    code, description = split_mef_code_description(cleaned_label)
    level = cleaned_level or infer_mef_hierarchy_level(cleaned_label)

    if not level and text_matches_all_filters(
        description,
        ["PROGRAMA NACIONAL DE BECAS", "CREDITO EDUCATIVO"],
    ):
        level = "UNIDAD EJECUTORA"

    return level, code, description


def infer_mef_hierarchy_level(description: str) -> str:
    """
    Infere un nivel de jerarquia para filas sin columna explicita de nivel.
    """
    normalized = normalize_mef_text(description)

    if normalized == "TOTAL":
        return "TOTAL"
    if "GOBIERNO" in normalized:
        return "NIVEL DE GOBIERNO"
    if normalized.startswith("10:") or normalized == "EDUCACION":
        return "SECTOR"
    if "M. DE EDUCACION" in normalized:
        return "PLIEGO"
    if "PROGRAMA NACIONAL DE BECAS" in normalized or "PRONABEC" in normalized:
        return "UNIDAD EJECUTORA"

    return ""


def build_mef_hierarchy_record(
    descriptor_cells: list[str],
    budget_values: list[str],
    ano: int | str,
    periodo_tipo: str,
    periodo_valor: str,
) -> dict[str, str] | None:
    """
    Normaliza una fila de jerarquia MEF al contrato Bronze conservador.
    """
    descriptors = [cell for cell in descriptor_cells if cell]
    if not descriptors or len(budget_values) < 8:
        return None

    explicit_level = descriptors[0] if len(descriptors) >= 2 else ""
    label = descriptors[1] if len(descriptors) >= 2 else descriptors[0]
    nivel_jerarquia, codigo, descripcion = parse_mef_hierarchy_descriptor(
        explicit_level=explicit_level,
        label=label,
    )

    if not nivel_jerarquia:
        return None

    return {
        "ano": str(ano),
        "periodo_tipo": periodo_tipo,
        "periodo_valor": periodo_valor,
        "nivel_jerarquia": nivel_jerarquia,
        "codigo": codigo,
        "descripcion": descripcion,
        "pia": budget_values[0],
        "pim": budget_values[1],
        "certificacion": budget_values[2],
        "compromiso_anual": budget_values[3],
        "compromiso_mensual": budget_values[4],
        "devengado": budget_values[5],
        "girado": budget_values[6],
        "avance_porcentaje": budget_values[7],
    }


def extract_mef_hierarchy_rows(
    soup: BeautifulSoup,
    ano: int | str,
    periodo_tipo: str = "ANUAL",
    periodo_valor: str | None = None,
) -> list[dict[str, str]]:
    """
    Extrae filas de la tabla superior de jerarquia presupuestal MEF.
    """
    resolved_periodo_valor = periodo_valor or str(ano)
    records: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()

    candidate_tables = soup.find_all("table", class_=["Hierarchy", "History", "MapTable"])
    candidate_tables.extend(soup.find_all("table"))

    for table in candidate_tables:
        for row in table.find_all("tr"):
            cells = [
                clean_cell_value(cell.get_text(" "))
                for cell in row.find_all(["td", "th"])
            ]

            if len(cells) < 9:
                continue

            budget_values = cells[-8:]
            descriptor_cells = cells[: -len(budget_values)]
            if not any(descriptor_cells):
                continue
            if not any(re.search(r"\d", value) for value in budget_values):
                continue

            record = build_mef_hierarchy_record(
                descriptor_cells=descriptor_cells,
                budget_values=budget_values,
                ano=ano,
                periodo_tipo=periodo_tipo,
                periodo_valor=resolved_periodo_valor,
            )
            if not record:
                continue

            key = (
                record["nivel_jerarquia"],
                record["codigo"],
                record["descripcion"],
            )
            if key in seen:
                continue

            seen.add(key)
            records.append(record)

    return records


def build_mef_breakdown_record(
    descriptor_cells: list[str],
    budget_values: list[str],
    ano: int | str,
    slice_name: str,
    periodo_tipo: str,
    periodo_valor: str,
) -> dict[str, str] | None:
    """
    Normaliza una fila de breakdown MEF al contrato Bronze del slice.
    """
    config = MEF_BREAKDOWN_CONFIG[slice_name]
    descriptors = [cell for cell in descriptor_cells if cell]
    if not descriptors or len(budget_values) < 8:
        return None

    label = descriptors[-1]
    code_field = config["code_field"]
    description_field = config["description_field"]

    if code_field:
        code, description = split_mef_code_description(label)
    else:
        code = ""
        description = clean_cell_value(label)

    if not description:
        return None

    record = {
        "ano": str(ano),
        "periodo_tipo": periodo_tipo,
        "periodo_valor": periodo_valor,
        description_field: description,
        "pia": budget_values[0],
        "pim": budget_values[1],
        "certificacion": budget_values[2],
        "compromiso_anual": budget_values[3],
        "compromiso_mensual": budget_values[4],
        "devengado": budget_values[5],
        "girado": budget_values[6],
        "avance_porcentaje": budget_values[7],
    }

    if code_field:
        record[code_field] = code

    # Filter keys to only keep fields present in fieldnames config
    fieldnames_set = set(config["fieldnames"])
    filtered_record = {k: v for k, v in record.items() if k in fieldnames_set}

    return filtered_record


def parse_mef_temporal_period(label: str, ano: int | str) -> dict[str, str]:
    """
    Interpreta una etiqueta temporal del portal MEF sin inferir periodos ausentes.
    """
    cleaned = clean_cell_value(label)
    normalized = normalize_mef_text(cleaned)
    year = str(ano)

    for month_name, (month_number, quarter) in MEF_MONTHS.items():
        if re.search(rf"\b{month_name}\b", normalized):
            return {
                "periodo_tipo": "MENSUAL",
                "periodo_valor": f"{year}-{month_number}",
                "trimestre": quarter,
                "mes_numero": month_number,
                "mes_nombre": month_name,
            }

    quarter_match = re.search(r"\b(?:TRIMESTRE|TRIM|T)\s*([1-4])\b", normalized)
    if not quarter_match:
        quarter_match = re.search(
            r"\b([1-4])(?:ER|RO|DO|TO)?\s*(?:TRIMESTRE|TRIM)\b",
            normalized,
        )

    roman_quarter = None
    if not quarter_match:
        roman_match = re.search(r"\b(IV|III|II|I)\s*(?:TRIMESTRE|TRIM)\b", normalized)
        if not roman_match:
            roman_match = re.search(r"\b(?:TRIMESTRE|TRIM)\s*(IV|III|II|I)\b", normalized)
        if roman_match:
            roman_map = {"I": "1", "II": "2", "III": "3", "IV": "4"}
            roman_quarter = roman_map[roman_match.group(1)]

    if quarter_match or roman_quarter:
        quarter = quarter_match.group(1) if quarter_match else roman_quarter
        return {
            "periodo_tipo": "TRIMESTRAL",
            "periodo_valor": f"{year}-T{quarter}",
            "trimestre": quarter,
            "mes_numero": "",
            "mes_nombre": "",
        }

    return {
        "periodo_tipo": "ANUAL",
        "periodo_valor": year,
        "trimestre": "",
        "mes_numero": "",
        "mes_nombre": "",
    }


def build_mef_temporal_record(
    descriptor_cells: list[str],
    budget_values: list[str],
    ano: int | str,
) -> dict[str, str] | None:
    """
    Normaliza una fila temporal MEF al contrato Bronze conservador.
    """
    descriptors = [cell for cell in descriptor_cells if cell]
    if not descriptors or len(budget_values) < 8:
        return None

    period = parse_mef_temporal_period(descriptors[-1], ano=ano)

    return {
        "ano": str(ano),
        "periodo_tipo": period["periodo_tipo"],
        "periodo_valor": period["periodo_valor"],
        "trimestre": period["trimestre"],
        "mes_numero": period["mes_numero"],
        "mes_nombre": period["mes_nombre"],
        "pia": budget_values[0],
        "pim": budget_values[1],
        "certificacion": budget_values[2],
        "compromiso_anual": budget_values[3],
        "compromiso_mensual": budget_values[4],
        "devengado": budget_values[5],
        "girado": budget_values[6],
        "avance_porcentaje": budget_values[7],
    }


def extract_mef_breakdown_rows(
    soup: BeautifulSoup,
    ano: int | str,
    slice_name: str,
    periodo_tipo: str = "ANUAL",
    periodo_valor: str | None = None,
) -> list[dict[str, str]]:
    """
    Extrae filas de un slice presupuestal MEF desde una tabla HTML.
    """
    if slice_name not in MEF_BREAKDOWN_CONFIG:
        raise ConfigError(f"Slice MEF no soportado: {slice_name}")

    resolved_periodo_valor = periodo_valor or str(ano)
    records: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    tables = soup.find_all("table", class_="Data") or soup.find_all("table")

    for table in tables:
        for row in table.find_all("tr"):
            cells = [
                clean_cell_value(cell.get_text(" "))
                for cell in row.find_all(["td", "th"])
            ]

            if len(cells) < 9:
                continue

            budget_values = cells[-8:]
            descriptor_cells = cells[: -len(budget_values)]
            if not any(descriptor_cells):
                continue
            if not any(re.search(r"\d", value) for value in budget_values):
                continue

            if slice_name == "temporal":
                record = build_mef_temporal_record(
                    descriptor_cells=descriptor_cells,
                    budget_values=budget_values,
                    ano=ano,
                )
            else:
                record = build_mef_breakdown_record(
                    descriptor_cells=descriptor_cells,
                    budget_values=budget_values,
                    ano=ano,
                    slice_name=slice_name,
                    periodo_tipo=periodo_tipo,
                    periodo_valor=resolved_periodo_valor,
                )
            if not record:
                continue

            config = MEF_BREAKDOWN_CONFIG[slice_name]
            code_field = config["code_field"]
            if slice_name == "temporal":
                key = (record["periodo_tipo"], record["periodo_valor"])
            else:
                key = (
                    record[code_field] if code_field else "",
                    record[config["description_field"]],
                )
            if key in seen:
                continue

            seen.add(key)
            records.append(record)

    return records


def post_mef_navigation(
    session: requests.Session,
    soup: BeautifulSoup,
    url: str,
    button_name: str,
    timeout: int,
    grp1_value: str | None = None,
) -> BeautifulSoup:
    payload = build_mef_form_payload(soup, button_name)
    if grp1_value is not None:
        payload["grp1"] = grp1_value

    response = session.post(url, data=payload, timeout=timeout)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


def resolve_mef_breakdown_button_name(soup: BeautifulSoup, slice_name: str) -> str:
    """
    Resuelve el boton ASP.NET disponible para un slice de breakdown.
    """
    config = MEF_BREAKDOWN_CONFIG[slice_name]
    button_names = config.get("button_names") or [config["button_name"]]

    for button_name in button_names:
        if soup.find(attrs={"name": button_name}):
            return button_name

    return config["button_name"]


def choose_mef_grp1_or_raise(
    soup: BeautifulSoup,
    text_filters: list[str],
    step_name: str,
) -> str:
    value, _ = find_mef_grp1_value(soup, text_filters)
    if not value:
        raise MEFExtractionError(
            f"No se encontro opcion MEF para {step_name}: {text_filters}"
        )
    return value


def navigate_consulta_amigable_session_to_pronabec(
    session: requests.Session,
    year: int,
    timeout: int = 60,
) -> tuple[BeautifulSoup, str]:
    """
    Navega Consulta Amigable hasta la vista de Unidad Ejecutora PRONABEC.
    """
    params = {"y": str(year), "ap": "ActProy"}

    session.headers.update(CONSULTA_AMIGABLE_HEADERS)

    default_response = session.get(
        get_consulta_amigable_default_url(),
        params=params,
        timeout=timeout,
    )
    default_response.raise_for_status()

    navigate_response = session.get(
        get_consulta_amigable_navigate_url(),
        params=params,
        timeout=timeout,
    )
    navigate_response.raise_for_status()
    navigate_url = navigate_response.url
    soup = BeautifulSoup(navigate_response.text, "html.parser")

    soup = post_mef_navigation(
        session=session,
        soup=soup,
        url=navigate_url,
        button_name="ctl00$CPH1$BtnTipoGobierno",
        timeout=timeout,
    )
    gobierno_value = choose_mef_grp1_or_raise(
        soup,
        ["GOBIERNO NACIONAL"],
        "Nivel de Gobierno",
    )

    soup = post_mef_navigation(
        session=session,
        soup=soup,
        url=navigate_url,
        button_name="ctl00$CPH1$BtnSector",
        timeout=timeout,
        grp1_value=gobierno_value,
    )
    sector_value = choose_mef_grp1_or_raise(soup, ["EDUCACION"], "Sector")

    soup = post_mef_navigation(
        session=session,
        soup=soup,
        url=navigate_url,
        button_name="ctl00$CPH1$BtnPliego",
        timeout=timeout,
        grp1_value=sector_value,
    )
    pliego_value = choose_mef_grp1_or_raise(
        soup,
        ["010", "M. DE EDUCACION"],
        "Pliego",
    )

    soup = post_mef_navigation(
        session=session,
        soup=soup,
        url=navigate_url,
        button_name="ctl00$CPH1$BtnEjecutora",
        timeout=timeout,
        grp1_value=pliego_value,
    )

    return soup, navigate_url


def navigate_consulta_amigable_to_pronabec(
    year: int,
    timeout: int = 60,
) -> BeautifulSoup:
    """
    Navega Consulta Amigable hasta la vista de Unidad Ejecutora PRONABEC.
    """
    with requests.Session() as session:
        soup, _ = navigate_consulta_amigable_session_to_pronabec(
            session=session,
            year=year,
            timeout=timeout,
        )

    return soup


def extract_mef_budget_record_from_soup(
    soup: BeautifulSoup,
    year: int,
) -> dict[str, Any] | None:
    """
    Extrae el registro anual PRONABEC desde la vista final de Consulta Amigable.
    """
    executora_code = os.getenv("MEF_PRONABEC_EXECUTORA_CODE", "117-1438")
    executora_name = os.getenv(
        "MEF_PRONABEC_EXECUTORA_NAME",
        "PROGRAMA NACIONAL DE BECAS Y CREDITO EDUCATIVO",
    )
    name_parts = [
        part.strip()
        for part in re.split(r"\s+[yY]\s+", executora_name)
        if part.strip()
    ]
    primary_filters = [executora_code] + name_parts

    row = (
        extract_mef_budget_row(soup, primary_filters)
        or extract_mef_budget_row(soup, ["BECAS", "CREDITO"])
        or extract_mef_budget_row(soup, ["PRONABEC"])
    )

    if not row:
        return None

    budget_values = row[-8:]
    descriptive_cells = row[: -len(budget_values)]
    ejecutora_label = next((cell for cell in descriptive_cells if cell), "")

    if ejecutora_label:
        code, description = split_mef_code_description(ejecutora_label)
    else:
        fallback_label = get_mef_pronabec_executora_name()
        code, description = split_mef_code_description(fallback_label)

    if not code:
        code = executora_code
    if not description:
        description = executora_name

    return {
        "ano": year,
        "ejecutora_codigo": code,
        "ejecutora_nombre": description,
        "pia": clean_mef_number(budget_values[0]),
        "pim": clean_mef_number(budget_values[1]),
        "certificacion": clean_mef_number(budget_values[2]),
        "compromiso_anual": clean_mef_number(budget_values[3]),
        "compromiso_mensual": clean_mef_number(budget_values[4]),
        "devengado": clean_mef_number(budget_values[5]),
        "girado": clean_mef_number(budget_values[6]),
        "avance_porcentaje": clean_mef_number(budget_values[7]),
    }


def find_pronabec_grp1_value(soup: BeautifulSoup) -> str:
    """
    Encuentra el valor grp1 de la Unidad Ejecutora PRONABEC.
    """
    executora_code = os.getenv("MEF_PRONABEC_EXECUTORA_CODE", "117-1438")
    executora_name = os.getenv(
        "MEF_PRONABEC_EXECUTORA_NAME",
        "PROGRAMA NACIONAL DE BECAS Y CREDITO EDUCATIVO",
    )
    name_parts = [
        part.strip()
        for part in re.split(r"\s+[yY]\s+", executora_name)
        if part.strip()
    ]
    value, _ = find_mef_grp1_value(soup, [executora_code] + name_parts)

    if not value:
        value, _ = find_mef_grp1_value(soup, ["BECAS", "CREDITO"])

    if not value:
        value, _ = find_mef_grp1_value(soup, ["PRONABEC"])

    if not value:
        raise MEFExtractionError("No se encontro grp1 para Unidad Ejecutora PRONABEC.")

    return value


def scrape_consulta_amigable_breakdown_snapshot(
    session: requests.Session,
    base_soup: BeautifulSoup,
    navigate_url: str,
    year: int,
    timeout: int,
    breakdown_slices: list[str],
) -> dict[str, list[dict[str, str]]]:
    """
    Extrae los slices de gasto desde la vista final de PRONABEC.
    """
    pronabec_grp1_value = find_pronabec_grp1_value(base_soup)
    records_by_slice: dict[str, list[dict[str, str]]] = {}

    for slice_name in breakdown_slices:
        if slice_name == "producto_temporal":
            product_soup = post_mef_navigation(
                session=session,
                soup=base_soup,
                url=navigate_url,
                button_name="ctl00$CPH1$BtnProdProy",
                timeout=timeout,
                grp1_value=pronabec_grp1_value,
            )
            product_radios = product_soup.find_all("input", {"name": "grp1"})
            product_records = []
            for radio in product_radios:
                grp1_val = radio.get("value")
                row = radio.find_parent("tr")
                if not row:
                    continue
                cells = row.find_all(["td", "th"])
                label = clean_cell_value(cells[1].get_text(" ")) if len(cells) > 1 else ""
                if not label:
                    continue
                code_prod, desc_prod = split_mef_code_description(label)
                
                temporal_btn = resolve_mef_breakdown_button_name(product_soup, "temporal")
                temporal_soup = post_mef_navigation(
                    session=session,
                    soup=product_soup,
                    url=navigate_url,
                    button_name=temporal_btn,
                    timeout=timeout,
                    grp1_value=grp1_val,
                )
                slice_records = extract_mef_breakdown_rows(
                    soup=temporal_soup,
                    ano=year,
                    slice_name="temporal",
                )
                for rec in slice_records:
                    rec["codigo_producto"] = code_prod
                    rec["producto"] = desc_prod
                    product_records.append(rec)
            records_by_slice[slice_name] = product_records

        elif slice_name == "actividad":
            product_soup = post_mef_navigation(
                session=session,
                soup=base_soup,
                url=navigate_url,
                button_name="ctl00$CPH1$BtnProdProy",
                timeout=timeout,
                grp1_value=pronabec_grp1_value,
            )
            product_radios = product_soup.find_all("input", {"name": "grp1"})
            actividad_records = []
            for radio in product_radios:
                grp1_val = radio.get("value")
                row = radio.find_parent("tr")
                if not row:
                    continue
                cells = row.find_all(["td", "th"])
                label = clean_cell_value(cells[1].get_text(" ")) if len(cells) > 1 else ""
                if not label:
                    continue
                code_prod, desc_prod = split_mef_code_description(label)
                
                activity_soup = post_mef_navigation(
                    session=session,
                    soup=product_soup,
                    url=navigate_url,
                    button_name="ctl00$CPH1$BtnActProyObra",
                    timeout=timeout,
                    grp1_value=grp1_val,
                )
                slice_records = extract_mef_breakdown_rows(
                    soup=activity_soup,
                    ano=year,
                    slice_name="actividad",
                )
                for rec in slice_records:
                    rec["codigo_producto"] = code_prod
                    rec["producto"] = desc_prod
                    actividad_records.append(rec)
            records_by_slice[slice_name] = actividad_records

        elif slice_name == "actividad_temporal":
            product_soup = post_mef_navigation(
                session=session,
                soup=base_soup,
                url=navigate_url,
                button_name="ctl00$CPH1$BtnProdProy",
                timeout=timeout,
                grp1_value=pronabec_grp1_value,
            )
            product_radios = product_soup.find_all("input", {"name": "grp1"})
            act_temp_records = []
            for radio in product_radios:
                grp1_val = radio.get("value")
                row = radio.find_parent("tr")
                if not row:
                    continue
                cells = row.find_all(["td", "th"])
                label = clean_cell_value(cells[1].get_text(" ")) if len(cells) > 1 else ""
                if not label:
                    continue
                code_prod, desc_prod = split_mef_code_description(label)
                
                activity_soup = post_mef_navigation(
                    session=session,
                    soup=product_soup,
                    url=navigate_url,
                    button_name="ctl00$CPH1$BtnActProyObra",
                    timeout=timeout,
                    grp1_value=grp1_val,
                )
                
                activity_radios = activity_soup.find_all("input", {"name": "grp1"})
                for act_radio in activity_radios:
                    act_grp1_val = act_radio.get("value")
                    act_row = act_radio.find_parent("tr")
                    if not act_row:
                        continue
                    act_cells = act_row.find_all(["td", "th"])
                    act_label = clean_cell_value(act_cells[1].get_text(" ")) if len(act_cells) > 1 else ""
                    if not act_label:
                        continue
                    code_act, desc_act = split_mef_code_description(act_label)
                    
                    temporal_btn = resolve_mef_breakdown_button_name(activity_soup, "temporal")
                    temporal_soup = post_mef_navigation(
                        session=session,
                        soup=activity_soup,
                        url=navigate_url,
                        button_name=temporal_btn,
                        timeout=timeout,
                        grp1_value=act_grp1_val,
                    )
                    slice_records = extract_mef_breakdown_rows(
                        soup=temporal_soup,
                        ano=year,
                        slice_name="temporal",
                    )
                    for rec in slice_records:
                        rec["codigo_producto"] = code_prod
                        rec["producto"] = desc_prod
                        rec["codigo_actividad"] = code_act
                        rec["actividad"] = desc_act
                        act_temp_records.append(rec)
            records_by_slice[slice_name] = act_temp_records

        elif slice_name == "generica_temporal":
            generica_soup = post_mef_navigation(
                session=session,
                soup=base_soup,
                url=navigate_url,
                button_name="ctl00$CPH1$BtnGenerica",
                timeout=timeout,
                grp1_value=pronabec_grp1_value,
            )
            generica_radios = generica_soup.find_all("input", {"name": "grp1"})
            generica_records = []
            for radio in generica_radios:
                grp1_val = radio.get("value")
                row = radio.find_parent("tr")
                if not row:
                    continue
                cells = row.find_all(["td", "th"])
                label = clean_cell_value(cells[1].get_text(" ")) if len(cells) > 1 else ""
                if not label:
                    continue
                code_gen, desc_gen = split_mef_code_description(label)
                
                temporal_btn = resolve_mef_breakdown_button_name(generica_soup, "temporal")
                temporal_soup = post_mef_navigation(
                    session=session,
                    soup=generica_soup,
                    url=navigate_url,
                    button_name=temporal_btn,
                    timeout=timeout,
                    grp1_value=grp1_val,
                )
                slice_records = extract_mef_breakdown_rows(
                    soup=temporal_soup,
                    ano=year,
                    slice_name="temporal",
                )
                for rec in slice_records:
                    rec["codigo_generica"] = code_gen
                    rec["generica"] = desc_gen
                    generica_records.append(rec)
            records_by_slice[slice_name] = generica_records

        else:
            slice_soup = post_mef_navigation(
                session=session,
                soup=base_soup,
                url=navigate_url,
                button_name=resolve_mef_breakdown_button_name(base_soup, slice_name),
                timeout=timeout,
                grp1_value=pronabec_grp1_value,
            )
            records_by_slice[slice_name] = extract_mef_breakdown_rows(
                soup=slice_soup,
                ano=year,
                slice_name=slice_name,
                periodo_tipo="ANUAL",
                periodo_valor=str(year),
            )

    return records_by_slice


def scrape_consulta_amigable_year(year: int, timeout: int = 60) -> dict[str, Any] | None:
    """
    Extrae presupuesto PRONABEC de Consulta Amigable para un anio fiscal.
    """
    soup = navigate_consulta_amigable_to_pronabec(year=year, timeout=timeout)
    return extract_mef_budget_record_from_soup(soup=soup, year=year)


def scrape_consulta_amigable_year_snapshot(
    year: int,
    timeout: int = 60,
    include_hierarchy: bool = False,
    breakdown_slices: list[str] | None = None,
) -> dict[str, Any]:
    """
    Extrae presupuesto anual y snapshots opcionales MEF.
    """
    with requests.Session() as session:
        soup, navigate_url = navigate_consulta_amigable_session_to_pronabec(
            session=session,
            year=year,
            timeout=timeout,
        )
        budget_record = extract_mef_budget_record_from_soup(soup=soup, year=year)
        hierarchy_records = (
            extract_mef_hierarchy_rows(
                soup=soup,
                ano=year,
                periodo_tipo="ANUAL",
                periodo_valor=str(year),
            )
            if include_hierarchy
            else []
        )
        breakdown_records = (
            scrape_consulta_amigable_breakdown_snapshot(
                session=session,
                base_soup=soup,
                navigate_url=navigate_url,
                year=year,
                timeout=timeout,
                breakdown_slices=breakdown_slices,
            )
            if breakdown_slices
            else {}
        )

    return {
        "budget_record": budget_record,
        "hierarchy_records": hierarchy_records,
        "breakdown_records": breakdown_records,
    }


def scrape_consulta_amigable_range(
    start_year: int,
    end_year: int,
    timeout: int = 60,
) -> list[dict[str, Any]]:
    """
    Extrae presupuesto PRONABEC para un rango inclusivo de anios fiscales.
    """
    records: list[dict[str, Any]] = []

    for year in range(start_year, end_year + 1):
        record = scrape_consulta_amigable_year(year, timeout=timeout)
        if record:
            records.append(record)

    return records


def scrape_consulta_amigable_range_snapshot(
    start_year: int,
    end_year: int,
    timeout: int = 60,
    include_hierarchy: bool = False,
    breakdown_slices: list[str] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """
    Extrae presupuesto anual y snapshots opcionales para un rango inclusivo.
    """
    budget_records: list[dict[str, Any]] = []
    hierarchy_records: list[dict[str, Any]] = []
    breakdown_records: dict[str, list[dict[str, Any]]] = {
        slice_name: []
        for slice_name in (breakdown_slices or [])
    }

    for year in range(start_year, end_year + 1):
        snapshot = scrape_consulta_amigable_year_snapshot(
            year=year,
            timeout=timeout,
            include_hierarchy=include_hierarchy,
            breakdown_slices=breakdown_slices,
        )
        budget_record = snapshot["budget_record"]
        if budget_record:
            budget_records.append(budget_record)
        hierarchy_records.extend(snapshot["hierarchy_records"])
        for slice_name, records in snapshot["breakdown_records"].items():
            breakdown_records.setdefault(slice_name, []).extend(records)

    return {
        "budget_records": budget_records,
        "hierarchy_records": hierarchy_records,
        "breakdown_records": breakdown_records,
    }


def read_csv_records_from_text(content: str) -> list[dict[str, Any]]:
    """
    Lee registros CSV desde texto.
    """
    sample = content[:4096]

    try:
        dialect = csv.Sniffer().sniff(sample)
    except csv.Error:
        dialect = csv.excel

    reader = csv.DictReader(content.splitlines(), dialect=dialect)
    return [dict(row) for row in reader]


def read_csv_records_from_file(path: str | Path) -> list[dict[str, Any]]:
    """
    Lee registros CSV desde archivo local controlado.
    """
    file_path = Path(path)

    if not file_path.exists():
        raise FileNotFoundError(f"No existe el archivo CSV local: {file_path}")

    content = file_path.read_text(encoding="utf-8-sig")
    return read_csv_records_from_text(content)


def fetch_csv_records_from_url(url: str, timeout: int) -> list[dict[str, Any]]:
    """
    Descarga y parsea registros desde una URL CSV.
    """
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()

    return read_csv_records_from_text(response.text)


def fetch_html_table_records_from_url(
    url: str,
    timeout: int,
    table_index: int = 0,
) -> list[dict[str, Any]]:
    """
    Descarga una página HTML y extrae una tabla.

    Este método es intencionalmente simple y controlado. Si el portal cambia,
    se debe ajustar esta función o reemplazarla por una integración más estable.
    """
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    tables = soup.find_all("table")

    if not tables:
        raise MEFExtractionError("No se encontraron tablas HTML en la URL MEF.")

    if table_index >= len(tables):
        raise MEFExtractionError(
            f"table_index={table_index} fuera de rango. Tablas encontradas: {len(tables)}"
        )

    table = tables[table_index]
    rows = table.find_all("tr")

    if not rows:
        raise MEFExtractionError("La tabla HTML no contiene filas.")

    headers = [
        clean_cell_value(cell.get_text())
        for cell in rows[0].find_all(["th", "td"])
    ]

    if not headers:
        raise MEFExtractionError("No se pudieron leer encabezados de la tabla HTML.")

    records: list[dict[str, Any]] = []

    for row in rows[1:]:
        cells = [
            clean_cell_value(cell.get_text())
            for cell in row.find_all(["td", "th"])
        ]

        if not any(cells):
            continue

        record = {
            headers[index]: cells[index] if index < len(cells) else ""
            for index in range(len(headers))
        }
        records.append(record)

    return records


def fetch_mef_records(
    source_url: str | None,
    source_file: str | None,
    timeout: int,
    table_index: int,
) -> list[dict[str, Any]]:
    """
    Obtiene registros MEF desde archivo local, URL CSV o tabla HTML.
    """
    if source_file:
        return read_csv_records_from_file(source_file)

    if not source_url:
        raise ConfigError(
            "No se definió fuente MEF. Usa --consulta-amigable, --source-url, "
            "--source-file o configura MEF_SOURCE_URL."
        )

    parsed_url = urlparse(source_url)
    path = parsed_url.path.lower()

    if path.endswith(".csv"):
        return fetch_csv_records_from_url(source_url, timeout=timeout)

    return fetch_html_table_records_from_url(
        url=source_url,
        timeout=timeout,
        table_index=table_index,
    )


def filter_records_by_year(
    records: list[dict[str, Any]],
    start_year: int | None,
    end_year: int | None,
) -> list[dict[str, Any]]:
    """
    Filtra registros por año si la columna ano está disponible.
    """
    if start_year is None and end_year is None:
        return records

    filtered: list[dict[str, Any]] = []

    for record in records:
        raw_year = record.get("ano", "")
        match = re.search(r"\d{4}", str(raw_year))

        if not match:
            continue

        year = int(match.group(0))

        if start_year is not None and year < start_year:
            continue

        if end_year is not None and year > end_year:
            continue

        filtered.append(record)

    return filtered


def filter_records_by_text(
    records: list[dict[str, Any]],
    text_filter: str | None,
) -> list[dict[str, Any]]:
    """
    Filtra registros que contengan cierto texto en alguna columna.
    """
    if not text_filter:
        return records

    normalized_filter = text_filter.lower().strip()

    return [
        record
        for record in records
        if normalized_filter in " ".join(str(value).lower() for value in record.values())
    ]


def normalize_mef_records(
    records: list[dict[str, Any]],
    expected_columns: list[str],
) -> list[dict[str, Any]]:
    """
    Normaliza registros MEF al contrato Bronze esperado.
    """
    if not records:
        return []

    actual_columns = list(records[0].keys())
    column_mapping = resolve_column_mapping(actual_columns, expected_columns)

    missing_columns = validate_required_columns(
        actual_columns=column_mapping.keys(),
        expected_columns=expected_columns,
    )

    if missing_columns:
        raise MEFExtractionError(
            "No se pudieron mapear todas las columnas esperadas de MEF. "
            f"Faltantes: {missing_columns}. "
            f"Columnas disponibles: {actual_columns}"
        )

    return [
        normalize_record(record, column_mapping, expected_columns)
        for record in records
    ]


def write_json_file(path: Path, payload: Any) -> None:
    """
    Escribe un archivo JSON local.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def write_csv_file(
    path: Path,
    records: list[dict[str, Any]],
    fieldnames: list[str],
) -> None:
    """
    Escribe registros CSV localmente.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)


def resolve_mef_record_year(record: dict[str, Any]) -> str:
    """
    Resuelve el año fiscal de un registro MEF.
    """
    raw_year = record.get("ano")
    if raw_year is None or raw_year == "":
        raise MEFExtractionError(f"El registro MEF no contiene el campo 'ano': {record}")
    match = re.search(r"\d{4}", str(raw_year))
    if not match:
        raise MEFExtractionError(f"No se pudo resolver el año fiscal del registro MEF: {record}")
    return match.group(0)


def group_records_by_fiscal_year(records: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """
    Agrupa registros MEF por año fiscal.
    """
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        year = resolve_mef_record_year(record)
        grouped.setdefault(year, []).append(record)
    return grouped


def build_local_mef_output_paths(
    output_dir: str | Path,
    extraction_date: str,
    fiscal_year: str | int,
) -> dict[str, Path]:
    """
    Construye rutas locales equivalentes a Bronze para dry-run MEF.
    """
    base_path = (
        Path(output_dir)
        / "bronze"
        / "mef"
        / "presupuesto"
        / f"extraction_date={extraction_date}"
        / f"year={fiscal_year}"
    )

    return {
        "csv_path": base_path / "data.csv",
        "metadata_path": base_path / "extraction_metadata.json",
    }


def build_local_mef_hierarchy_output_paths(
    output_dir: str | Path,
    extraction_date: str,
    fiscal_year: str | int,
) -> dict[str, Path]:
    """
    Construye rutas locales equivalentes a Bronze para hierarchy MEF.
    """
    base_path = (
        Path(output_dir)
        / "bronze"
        / "mef"
        / "presupuesto_hierarchy"
        / f"extraction_date={extraction_date}"
        / f"year={fiscal_year}"
    )

    return {
        "csv_path": base_path / "data.csv",
        "metadata_path": base_path / "extraction_metadata.json",
    }


def build_local_mef_breakdown_output_paths(
    output_dir: str | Path,
    extraction_date: str,
    slice_name: str,
    fiscal_year: str | int,
) -> dict[str, Path]:
    """
    Construye rutas locales Bronze para un slice de breakdown MEF.
    """
    source_dataset = MEF_BREAKDOWN_CONFIG[slice_name]["source_dataset"]
    base_path = (
        Path(output_dir)
        / "bronze"
        / "mef"
        / source_dataset
        / f"extraction_date={extraction_date}"
        / f"year={fiscal_year}"
    )

    return {
        "csv_path": base_path / "data.csv",
        "metadata_path": base_path / "extraction_metadata.json",
    }


def build_extraction_metadata(
    source_name: str,
    source_dataset: str,
    extraction_date: str,
    run_id: str,
    records_read: int,
    records_written: int,
    output_paths: dict[str, str],
    status: str = "SUCCESS",
    extra_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Construye metadata técnica de extracción.
    """
    return {
        "source_name": source_name,
        "source_dataset": source_dataset,
        "extraction_date": extraction_date,
        "run_id": run_id,
        "records_read": records_read,
        "records_written": records_written,
        "status": status,
        "output_paths": output_paths,
        "metadata": extra_metadata or {},
        "created_at": datetime.utcnow().isoformat(),
    }


def write_mef_to_local(
    records: list[dict[str, Any]],
    fieldnames: list[str],
    extraction_date: str,
    output_dir: str | Path,
    run_id: str,
    records_read: int,
    source_url: str | None,
    source_file: str | None,
    logger,
    source_mode: str = "external",
    fiscal_year: str | int | None = None,
) -> dict[str, str]:
    """
    Escribe data.csv y extraction_metadata.json localmente.
    """
    if fiscal_year is None:
        fiscal_year = resolve_mef_record_year(records[0]) if records else "9999"
    paths = build_local_mef_output_paths(
        output_dir=output_dir,
        extraction_date=extraction_date,
        fiscal_year=fiscal_year,
    )

    write_csv_file(
        path=paths["csv_path"],
        records=records,
        fieldnames=fieldnames,
    )

    metadata = build_extraction_metadata(
        source_name="MEF Consulta Amigable",
        source_dataset="presupuesto",
        extraction_date=extraction_date,
        run_id=run_id,
        records_read=records_read,
        records_written=len(records),
        output_paths={
            "csv_path": str(paths["csv_path"]),
        },
        extra_metadata={
            "mode": "dry_run",
            "source_mode": source_mode,
            "source_url": source_url,
            "source_file": source_file,
            "fiscal_year": str(fiscal_year),
        },
    )

    write_json_file(paths["metadata_path"], metadata)

    log_event(
        logger,
        "INFO",
        "Archivo MEF escrito en modo dry-run",
        csv_path=str(paths["csv_path"]),
        metadata_path=str(paths["metadata_path"]),
        records_written=len(records),
    )

    return {
        "output_uri": str(paths["csv_path"]),
        "metadata_path": str(paths["metadata_path"]),
    }


def write_mef_hierarchy_to_local(
    records: list[dict[str, Any]],
    extraction_date: str,
    output_dir: str | Path,
    run_id: str,
    records_read: int,
    source_url: str | None,
    logger,
    source_mode: str = "consulta_amigable",
    fiscal_year: str | int | None = None,
) -> dict[str, str]:
    """
    Escribe data.csv y metadata local para presupuesto_hierarchy.
    """
    if fiscal_year is None:
        fiscal_year = resolve_mef_record_year(records[0]) if records else "9999"
    paths = build_local_mef_hierarchy_output_paths(
        output_dir=output_dir,
        extraction_date=extraction_date,
        fiscal_year=fiscal_year,
    )

    write_csv_file(
        path=paths["csv_path"],
        records=records,
        fieldnames=MEF_HIERARCHY_FIELDNAMES,
    )

    metadata = build_extraction_metadata(
        source_name="MEF",
        source_dataset="presupuesto_hierarchy",
        extraction_date=extraction_date,
        run_id=run_id,
        records_read=records_read,
        records_written=len(records),
        output_paths={
            "csv_path": str(paths["csv_path"]),
        },
        extra_metadata={
            "mode": "dry_run",
            "source_system": "MEF",
            "source_dataset": "presupuesto_hierarchy",
            "source_mode": source_mode,
            "source_url": source_url,
            "fiscal_year": str(fiscal_year),
        },
    )

    write_json_file(paths["metadata_path"], metadata)

    log_event(
        logger,
        "INFO",
        "Archivo MEF hierarchy escrito en modo dry-run",
        csv_path=str(paths["csv_path"]),
        metadata_path=str(paths["metadata_path"]),
        records_written=len(records),
    )

    return {
        "output_uri": str(paths["csv_path"]),
        "metadata_path": str(paths["metadata_path"]),
    }


def build_mef_hierarchy_bronze_path(
    extraction_date: str,
    fiscal_year: str | int | None = None,
) -> str:
    """
    Construye ruta GCS Bronze para presupuesto_hierarchy.
    """
    base = f"bronze/mef/presupuesto_hierarchy/extraction_date={extraction_date}"
    if fiscal_year is not None:
        base += f"/year={fiscal_year}"
    return f"{base}/data.csv"


def write_mef_breakdown_to_local(
    records: list[dict[str, Any]],
    extraction_date: str,
    output_dir: str | Path,
    run_id: str,
    records_read: int,
    source_url: str | None,
    slice_name: str,
    logger,
    source_mode: str = "consulta_amigable",
    fiscal_year: str | int | None = None,
) -> dict[str, str]:
    """
    Escribe data.csv y metadata local para un slice de gasto MEF.
    """
    if fiscal_year is None:
        fiscal_year = resolve_mef_record_year(records[0]) if records else "9999"
    config = MEF_BREAKDOWN_CONFIG[slice_name]
    paths = build_local_mef_breakdown_output_paths(
        output_dir=output_dir,
        extraction_date=extraction_date,
        slice_name=slice_name,
        fiscal_year=fiscal_year,
    )

    write_csv_file(
        path=paths["csv_path"],
        records=records,
        fieldnames=config["fieldnames"],
    )

    metadata = build_extraction_metadata(
        source_name="MEF",
        source_dataset=config["source_dataset"],
        extraction_date=extraction_date,
        run_id=run_id,
        records_read=records_read,
        records_written=len(records),
        output_paths={
            "csv_path": str(paths["csv_path"]),
        },
        extra_metadata={
            "mode": "dry_run",
            "source_system": "MEF",
            "source_dataset": config["source_dataset"],
            "source_mode": source_mode,
            "source_url": source_url,
            "breakdown_slice": slice_name,
            "fiscal_year": str(fiscal_year),
        },
    )

    write_json_file(paths["metadata_path"], metadata)

    log_event(
        logger,
        "INFO",
        "Archivo MEF breakdown escrito en modo dry-run",
        csv_path=str(paths["csv_path"]),
        metadata_path=str(paths["metadata_path"]),
        records_written=len(records),
        breakdown_slice=slice_name,
    )

    return {
        "output_uri": str(paths["csv_path"]),
        "metadata_path": str(paths["metadata_path"]),
    }


def build_mef_breakdown_bronze_path(
    extraction_date: str,
    slice_name: str,
    fiscal_year: str | int | None = None,
) -> str:
    """
    Construye ruta GCS Bronze para un slice de breakdown MEF.
    """
    source_dataset = MEF_BREAKDOWN_CONFIG[slice_name]["source_dataset"]
    base = f"bronze/mef/{source_dataset}/extraction_date={extraction_date}"
    if fiscal_year is not None:
        base += f"/year={fiscal_year}"
    return f"{base}/data.csv"


def get_mef_expected_columns(endpoints_config: dict[str, Any]) -> list[str]:
    """
    Obtiene columnas esperadas para MEF desde config/endpoints.yaml.
    """
    return endpoints_config["mef"]["expected_columns"]


def parse_optional_int(value: str | None) -> int | None:
    """
    Convierte un valor opcional a entero.
    """
    if value is None or value == "":
        return None

    return int(value)


def run_extraction(args: argparse.Namespace) -> None:
    """
    Orquesta extracción MEF.
    """
    pipeline_settings = get_pipeline_settings(args.pipeline_config)
    endpoints_config = load_yaml_config(args.endpoints_config)

    logger = setup_structured_logger(
        name="scrape_mef_budget",
        level=pipeline_settings["log_level"],
        structured=True,
    )

    bucket_name = args.bucket or pipeline_settings["bucket_name"]

    if not args.dry_run and not bucket_name:
        raise ConfigError(
            "No se definió bucket. Configura GCS_BUCKET_NAME o usa --bucket."
        )

    mef_config = endpoints_config["mef"]
    expected_columns = get_mef_expected_columns(endpoints_config)

    # 1. Determine source mode and paths following CLI > env > defaults priority
    if args.consulta_amigable:
        source_mode = "consulta_amigable"
        source_url = get_consulta_amigable_base_url()
        source_file = None
    elif args.source_file:
        source_mode = "source_file"
        source_url = args.source_url or get_env_var("MEF_SOURCE_URL")
        source_file = args.source_file
    elif args.source_url:
        source_mode = "source_url"
        source_url = args.source_url
        source_file = None
    else:
        # Fallback to env configurations
        env_source_mode = os.getenv("MEF_SOURCE_MODE")
        if env_source_mode == "consulta_amigable":
            source_mode = "consulta_amigable"
            source_url = get_consulta_amigable_base_url()
            source_file = None
        elif env_source_mode == "source_file":
            env_source_file = os.getenv("MEF_SOURCE_FILE")
            if not env_source_file:
                raise ConfigError("MEF_SOURCE_MODE=source_file requiere configurar MEF_SOURCE_FILE en el entorno.")
            source_mode = "source_file"
            source_url = args.source_url or get_env_var("MEF_SOURCE_URL")
            source_file = env_source_file
        elif env_source_mode == "source_url":
            env_source_url = os.getenv("MEF_SOURCE_URL")
            if not env_source_url:
                raise ConfigError("MEF_SOURCE_MODE=source_url requiere configurar MEF_SOURCE_URL en el entorno.")
            source_mode = "source_url"
            source_url = env_source_url
            source_file = None
        elif env_source_mode:
            raise ConfigError(
                f"Valor no válido para MEF_SOURCE_MODE: '{env_source_mode}'. "
                "Debe ser: consulta_amigable, source_file o source_url."
            )
        else:
            # If no CLI and no MEF_SOURCE_MODE, check if MEF_SOURCE_URL exists to default to source_url
            env_source_url = os.getenv("MEF_SOURCE_URL")
            if env_source_url:
                source_mode = "source_url"
                source_url = env_source_url
                source_file = None
            else:
                raise ConfigError(
                    "No se especificó fuente para MEF. "
                    "Usa --consulta-amigable, --source-file, --source-url, "
                    "o configura MEF_SOURCE_MODE en el entorno."
                )

    extraction_date = args.extraction_date or date.today().isoformat()
    run_id = args.run_id or generate_run_id("mef_extraction")

    # 2. Determine range of years: CLI > env > defaults
    start_year = args.start_year
    if start_year is None:
        start_year = parse_optional_int(
            get_env_var(mef_config.get("start_year_env_var", "MEF_START_YEAR"))
        )

    end_year = args.end_year
    if end_year is None:
        end_year = parse_optional_int(
            get_env_var(mef_config.get("end_year_env_var", "MEF_END_YEAR"))
        )

    # 3. Determine text filter: CLI > env > default
    text_filter = args.text_filter
    if text_filter is None:
        text_filter = os.getenv("MEF_TEXT_FILTER", "PRONABEC")

    # 4. Determine timeout: CLI > env > default
    timeout = args.timeout
    if timeout is None:
        env_timeout = os.getenv("MEF_TIMEOUT_SECONDS")
        if env_timeout:
            timeout = int(env_timeout)
        else:
            timeout = DEFAULT_TIMEOUT_SECONDS

    include_hierarchy = args.include_hierarchy or parse_bool_env(
        os.getenv("MEF_INCLUDE_HIERARCHY"),
        default=False,
    )
    if include_hierarchy and source_mode != "consulta_amigable":
        raise ConfigError(
            "--include-hierarchy requiere source_mode=consulta_amigable."
        )

    include_spending_breakdowns = (
        args.include_spending_breakdowns
        or parse_bool_env(
            os.getenv("MEF_INCLUDE_SPENDING_BREAKDOWNS"),
            default=False,
        )
    )
    breakdown_slices = parse_breakdown_slices(
        args.breakdown_slices or os.getenv("MEF_BREAKDOWN_SLICES")
    )
    if include_spending_breakdowns and source_mode != "consulta_amigable":
        raise ConfigError(
            "--include-spending-breakdowns requiere source_mode=consulta_amigable."
        )

    log_event(
        logger,
        "INFO",
        "Iniciando extracción MEF",
        pipeline_name=pipeline_settings["pipeline_name"],
        run_id=run_id,
        extraction_date=extraction_date,
        source_url=source_url,
        source_file=source_file,
        start_year=start_year,
        end_year=end_year,
        text_filter=text_filter,
        source_mode=source_mode,
        include_hierarchy=include_hierarchy,
        include_spending_breakdowns=include_spending_breakdowns,
        breakdown_slices=breakdown_slices if include_spending_breakdowns else [],
    )

    started_at = datetime.utcnow()

    try:
        hierarchy_records: list[dict[str, Any]] = []
        breakdown_records: dict[str, list[dict[str, Any]]] = {}

        if source_mode == "consulta_amigable":
            if start_year is None or end_year is None:
                raise ConfigError(
                    "El modo consulta_amigable requiere --start-year y --end-year "
                    "o MEF_START_YEAR/MEF_END_YEAR."
                )

            if include_hierarchy or include_spending_breakdowns:
                snapshot = scrape_consulta_amigable_range_snapshot(
                    start_year=start_year,
                    end_year=end_year,
                    timeout=timeout,
                    include_hierarchy=include_hierarchy,
                    breakdown_slices=breakdown_slices
                    if include_spending_breakdowns
                    else None,
                )
                raw_records = snapshot["budget_records"]
                hierarchy_records = snapshot["hierarchy_records"]
                breakdown_records = snapshot["breakdown_records"]
            else:
                raw_records = scrape_consulta_amigable_range(
                    start_year=start_year,
                    end_year=end_year,
                    timeout=timeout,
                )
        else:
            raw_records = fetch_mef_records(
                source_url=source_url,
                source_file=source_file,
                timeout=timeout,
                table_index=args.table_index,
            )

        if source_mode == "consulta_amigable":
            filtered_records = raw_records
        else:
            filtered_records = filter_records_by_text(raw_records, text_filter)
        normalized_records = normalize_mef_records(filtered_records, expected_columns)
        normalized_records = filter_records_by_year(
            normalized_records,
            start_year=start_year,
            end_year=end_year,
        )

        if not normalized_records:
            raise MEFExtractionError(
                "La extracción MEF no produjo registros después de filtros y normalización."
            )
        # Group records by fiscal year
        normalized_by_year = group_records_by_fiscal_year(normalized_records)
        hierarchy_by_year = group_records_by_fiscal_year(hierarchy_records)
        
        breakdown_by_year: dict[str, dict[str, list[dict[str, Any]]]] = {}
        for slice_name, records in breakdown_records.items():
            breakdown_by_year[slice_name] = group_records_by_fiscal_year(records)

        # Resolve raw_records by year for records_read counts
        raw_by_year = {}
        try:
            raw_by_year = group_records_by_fiscal_year(raw_records)
        except Exception:
            if raw_records:
                actual_columns = list(raw_records[0].keys())
                column_mapping = resolve_column_mapping(actual_columns, expected_columns)
                ano_col = column_mapping.get("ano")
                if ano_col:
                    for r in raw_records:
                        val = r.get(ano_col)
                        if val:
                            match = re.search(r"\d{4}", str(val))
                            if match:
                                raw_by_year.setdefault(match.group(0), []).append(r)

        all_years = set(normalized_by_year.keys())
        if include_hierarchy:
            all_years.update(hierarchy_by_year.keys())
        if include_spending_breakdowns:
            for slice_name in breakdown_by_year:
                all_years.update(breakdown_by_year[slice_name].keys())
        
        all_years_sorted = sorted(list(all_years))

        output_uris = {}
        hierarchy_output_uris = {}
        breakdown_output_uris = {}

        for year in all_years_sorted:
            # 1. Budget records for this year
            year_budget_records = normalized_by_year.get(year, [])
            year_raw_count = len(raw_by_year.get(year, []))
            
            if year_budget_records:
                if args.dry_run:
                    res = write_mef_to_local(
                        records=year_budget_records,
                        fieldnames=expected_columns,
                        extraction_date=extraction_date,
                        output_dir=args.output_dir,
                        run_id=run_id,
                        records_read=year_raw_count,
                        source_url=source_url,
                        source_file=source_file,
                        source_mode=source_mode,
                        logger=logger,
                        fiscal_year=year,
                    )
                    output_uris[year] = res["output_uri"]
                else:
                    object_path = build_mef_bronze_path(
                        pipeline_settings["gcs_paths"]["mef_bronze"],
                        extraction_date=extraction_date,
                        fiscal_year=year,
                    )
                    csv_uri = upload_csv(
                        bucket_name=bucket_name,
                        object_path=object_path,
                        records=year_budget_records,
                        fieldnames=expected_columns,
                    )
                    output_uris[year] = csv_uri
                    
                    # Upload extraction_metadata.json
                    meta_path = object_path.replace("data.csv", "extraction_metadata.json")
                    meta_payload = build_extraction_metadata(
                        source_name="MEF Consulta Amigable",
                        source_dataset="presupuesto",
                        extraction_date=extraction_date,
                        run_id=run_id,
                        records_read=year_raw_count,
                        records_written=len(year_budget_records),
                        output_paths={"csv_path": csv_uri},
                        extra_metadata={
                            "mode": "production",
                            "source_mode": source_mode,
                            "source_url": source_url,
                            "source_file": source_file,
                            "fiscal_year": str(year),
                        },
                    )
                    upload_json(
                        bucket_name=bucket_name,
                        object_path=meta_path,
                        payload=meta_payload,
                    )

            # 2. Hierarchy records for this year
            if include_hierarchy:
                year_hierarchy_records = hierarchy_by_year.get(year, [])
                if year_hierarchy_records:
                    if args.dry_run:
                        res = write_mef_hierarchy_to_local(
                            records=year_hierarchy_records,
                            extraction_date=extraction_date,
                            output_dir=args.output_dir,
                            run_id=run_id,
                            records_read=len(year_hierarchy_records),
                            source_url=source_url,
                            source_mode=source_mode,
                            logger=logger,
                            fiscal_year=year,
                        )
                        hierarchy_output_uris[year] = res["output_uri"]
                    else:
                        object_path = build_mef_hierarchy_bronze_path(
                            extraction_date=extraction_date,
                            fiscal_year=year,
                        )
                        csv_uri = upload_csv(
                            bucket_name=bucket_name,
                            object_path=object_path,
                            records=year_hierarchy_records,
                            fieldnames=MEF_HIERARCHY_FIELDNAMES,
                        )
                        hierarchy_output_uris[year] = csv_uri
                        
                        # Upload extraction_metadata.json
                        meta_path = object_path.replace("data.csv", "extraction_metadata.json")
                        meta_payload = build_extraction_metadata(
                            source_name="MEF",
                            source_dataset="presupuesto_hierarchy",
                            extraction_date=extraction_date,
                            run_id=run_id,
                            records_read=len(year_hierarchy_records),
                            records_written=len(year_hierarchy_records),
                            output_paths={"csv_path": csv_uri},
                            extra_metadata={
                                "mode": "production",
                                "source_system": "MEF",
                                "source_dataset": "presupuesto_hierarchy",
                                "source_mode": source_mode,
                                "source_url": source_url,
                                "fiscal_year": str(year),
                            },
                        )
                        upload_json(
                            bucket_name=bucket_name,
                            object_path=meta_path,
                            payload=meta_payload,
                        )

            # 3. Breakdown slices for this year
            if include_spending_breakdowns:
                for slice_name in breakdown_slices:
                    slice_records = breakdown_by_year[slice_name].get(year, [])
                    if slice_records:
                        breakdown_output_uris.setdefault(slice_name, {})
                        if args.dry_run:
                            res = write_mef_breakdown_to_local(
                                records=slice_records,
                                extraction_date=extraction_date,
                                output_dir=args.output_dir,
                                run_id=run_id,
                                records_read=len(slice_records),
                                source_url=source_url,
                                source_mode=source_mode,
                                slice_name=slice_name,
                                logger=logger,
                                fiscal_year=year,
                            )
                            breakdown_output_uris[slice_name][year] = res["output_uri"]
                        else:
                            object_path = build_mef_breakdown_bronze_path(
                                extraction_date=extraction_date,
                                slice_name=slice_name,
                                fiscal_year=year,
                            )
                            csv_uri = upload_csv(
                                bucket_name=bucket_name,
                                object_path=object_path,
                                records=slice_records,
                                fieldnames=MEF_BREAKDOWN_CONFIG[slice_name]["fieldnames"],
                            )
                            breakdown_output_uris[slice_name][year] = csv_uri
                            
                            # Upload extraction_metadata.json
                            meta_path = object_path.replace("data.csv", "extraction_metadata.json")
                            meta_payload = build_extraction_metadata(
                                source_name="MEF",
                                source_dataset=MEF_BREAKDOWN_CONFIG[slice_name]["source_dataset"],
                                extraction_date=extraction_date,
                                run_id=run_id,
                                records_read=len(slice_records),
                                records_written=len(slice_records),
                                output_paths={"csv_path": csv_uri},
                                extra_metadata={
                                    "mode": "production",
                                    "source_system": "MEF",
                                    "source_dataset": MEF_BREAKDOWN_CONFIG[slice_name]["source_dataset"],
                                    "source_mode": source_mode,
                                    "source_url": source_url,
                                    "breakdown_slice": slice_name,
                                    "fiscal_year": str(year),
                                },
                            )
                            upload_json(
                                bucket_name=bucket_name,
                                object_path=meta_path,
                                payload=meta_payload,
                            )

        audit_event = create_extraction_audit_event(
            pipeline_name=pipeline_settings["pipeline_name"],
            source_name=mef_config.get("source_name", "MEF Consulta Amigable"),
            source_dataset="presupuesto",
            status="SUCCESS",
            run_id=run_id,
            environment=pipeline_settings["environment"],
            execution_date=date.fromisoformat(extraction_date),
            records_read=len(raw_records),
            records_written=len(normalized_records),
            metadata={
                "output_uris": output_uris,
                "hierarchy_output_uris": hierarchy_output_uris,
                "hierarchy_records_written": len(hierarchy_records),
                "include_hierarchy": include_hierarchy,
                "breakdown_output_uris": breakdown_output_uris,
                "breakdown_records_written": {
                    slice_name: len(records)
                    for slice_name, records in breakdown_records.items()
                },
                "include_spending_breakdowns": include_spending_breakdowns,
                "breakdown_slices": breakdown_slices if include_spending_breakdowns else [],
                "source_mode": source_mode,
                "source_url": source_url,
                "source_file": source_file,
                "records_after_text_filter": len(filtered_records),
                "start_year": start_year,
                "end_year": end_year,
                "started_at": started_at.isoformat(),
                "finished_at": datetime.utcnow().isoformat(),
            },
        )

        log_event(
            logger,
            "INFO",
            "Extracción MEF completada",
            output_uris=output_uris,
            hierarchy_output_uris=hierarchy_output_uris,
            breakdown_output_uris=breakdown_output_uris,
            records_read=len(raw_records),
            records_written=len(normalized_records),
            hierarchy_records_written=len(hierarchy_records),
            breakdown_records_written={
                slice_name: len(records)
                for slice_name, records in breakdown_records.items()
            },
            audit_event=audit_event.to_dict(),
        )

    except Exception as exc:
        audit_event = create_extraction_audit_event(
            pipeline_name=pipeline_settings["pipeline_name"],
            source_name=mef_config.get("source_name", "MEF Consulta Amigable"),
            source_dataset="presupuesto",
            status="FAILED",
            run_id=run_id,
            environment=pipeline_settings["environment"],
            execution_date=date.fromisoformat(extraction_date),
            error_message=str(exc),
            metadata={
                "source_url": source_url,
                "source_file": source_file,
                "source_mode": source_mode,
                "include_hierarchy": include_hierarchy,
                "include_spending_breakdowns": include_spending_breakdowns,
                "breakdown_slices": breakdown_slices if include_spending_breakdowns else [],
                "started_at": started_at.isoformat(),
                "finished_at": datetime.utcnow().isoformat(),
            },
        )

        log_event(
            logger,
            "ERROR",
            "Error extrayendo MEF",
            error_message=str(exc),
            audit_event=audit_event.to_dict(),
        )

        raise


def parse_args(args_list: list[str] | None = None) -> argparse.Namespace:
    """
    Lee argumentos CLI.
    """
    parser = argparse.ArgumentParser(
        description="Extrae presupuesto MEF hacia Cloud Storage Bronze."
    )

    parser.add_argument(
        "--pipeline-config",
        default=DEFAULT_PIPELINE_CONFIG,
        help="Ruta a config/pipeline.yaml.",
    )
    parser.add_argument(
        "--endpoints-config",
        default=DEFAULT_ENDPOINTS_CONFIG,
        help="Ruta a config/endpoints.yaml.",
    )
    parser.add_argument(
        "--source-url",
        help="URL CSV o URL HTML con tabla presupuestal MEF.",
    )
    parser.add_argument(
        "--source-file",
        help="Archivo CSV local controlado para carga MEF.",
    )
    parser.add_argument(
        "--consulta-amigable",
        action="store_true",
        help="Scrapea directamente el Navegador MEF de Consulta Amigable.",
    )
    parser.add_argument(
        "--include-hierarchy",
        action="store_true",
        help="Incluye snapshot Bronze de jerarquia presupuestal MEF.",
    )
    parser.add_argument(
        "--include-spending-breakdowns",
        action="store_true",
        help="Incluye slices Bronze de gasto MEF.",
    )
    parser.add_argument(
        "--breakdown-slices",
        help="Slices MEF separados por coma. Default: producto,generica.",
    )
    parser.add_argument(
        "--bucket",
        help="Bucket destino. Si se omite, usa GCS_BUCKET_NAME.",
    )
    parser.add_argument(
        "--extraction-date",
        help="Fecha lógica de extracción en formato YYYY-MM-DD.",
    )
    parser.add_argument(
        "--run-id",
        help="Identificador de ejecución. Si se omite, se genera automáticamente.",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        help="Año inicial a conservar.",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        help="Año final a conservar.",
    )
    parser.add_argument(
        "--text-filter",
        default=None,
        help="Texto usado para filtrar registros relacionados con PRONABEC. Si se omite, se usa MEF_TEXT_FILTER o 'PRONABEC'.",
    )
    parser.add_argument(
        "--table-index",
        type=int,
        default=0,
        help="Índice de tabla HTML a leer si la fuente no es CSV.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=None,
        help="Timeout HTTP por request en segundos. Si se omite, se usa MEF_TIMEOUT_SECONDS o 90.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Ejecuta la extracción y escribe salida local en tmp/ sin usar GCS.",
    )
    parser.add_argument(
        "--output-dir",
        default="tmp",
        help="Directorio local para salidas dry-run.",
    )

    return parser.parse_args(args_list)


def main() -> None:
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    args = parse_args()
    run_extraction(args)


if __name__ == "__main__":
    main()
