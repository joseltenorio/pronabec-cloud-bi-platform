"""Scraper MINEDU ESCALE -> Bronze CSV en GCS."""

from __future__ import annotations

import argparse
import csv
import io
import os
import re
import uuid
from collections import Counter
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup

from pipelines.common.config import (
    ConfigError,
    build_gcs_path,
    get_pipeline_settings,
    load_yaml_config,
)
from pipelines.common.gcs import upload_csv
from pipelines.common.logging import log_event, setup_structured_logger

try:
    import pandas as pd
except ImportError:  # pragma: no cover - runtime fallback without pandas
    pd = None


DEFAULT_ENDPOINTS_CONFIG = "config/endpoints.yaml"
DEFAULT_PIPELINE_CONFIG = "config/pipeline.yaml"
DEFAULT_TIMEOUT_SECONDS = 60
DATASET_NAME = "minedu_matricula_secundaria_departamental"
FIELDNAMES = [
    "anio",
    "codigo_departamento",
    "region",
    "nivel_educativo",
    "grado",
    "matricula_total",
    "matricula_publica",
    "matricula_privada",
    "matricula_urbana",
    "matricula_rural",
    "matricula_masculino",
    "matricula_femenino",
    "source_url",
    "extraction_date",
    "ingestion_timestamp",
    "pipeline_run_id",
]
GRADE_MAP = {
    "PRIMER GRADO": "PRIMER_GRADO",
    "SEGUNDO GRADO": "SEGUNDO_GRADO",
    "TERCER GRADO": "TERCER_GRADO",
    "CUARTO GRADO": "CUARTO_GRADO",
    "QUINTO GRADO": "QUINTO_GRADO",
}
SECTION_END_MARKERS = {"PRESENCIAL", "A DISTANCIA", "EN ALTERNANCIA"}
HEADER_ALIASES = {
    "matricula_total": {"TOTAL", "MATRICULA TOTAL", "MATRICULA", "ALUMNOS TOTAL"},
    "matricula_publica": {"PUBLICA", "GESTION PUBLICA"},
    "matricula_privada": {"PRIVADA", "GESTION PRIVADA"},
    "matricula_urbana": {"URBANA"},
    "matricula_rural": {"RURAL"},
    "matricula_masculino": {"MASCULINO", "HOMBRES", "HOMBRE"},
    "matricula_femenino": {"FEMENINO", "MUJERES", "MUJER"},
}
DEFAULT_MINEDU_COLUMN_INDEXES = {
    "matricula_total": 1,
    "matricula_publica": 2,
    "matricula_privada": 3,
    "matricula_urbana": 4,
    "matricula_rural": 5,
    "matricula_masculino": 6,
    "matricula_femenino": 7,
}

logger = setup_structured_logger("scrape_minedu_escale", level="INFO")


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("\xa0", " ")
    text = " ".join(text.split()).strip().upper()
    replacements = str.maketrans("ÁÉÍÓÚÜ", "AEIOUU")
    return text.translate(replacements)


def build_minedu_escale_url(
    base_url: str,
    anio_param: int,
    cuadro: int,
    codigo_departamento: str,
) -> str:
    query = urlencode(
        {
            "anio": anio_param,
            "cuadro": cuadro,
            "forma": "U",
            "dpto": codigo_departamento,
            "prov": "",
            "dre": "",
            "tipo_ambito": "ambito-ubigeo",
        }
    )
    return f"{base_url}?{query}"


def clean_number(value: Any) -> int:
    text = str(value or "").replace("\xa0", " ").strip()
    if text in {"", "-", "—"}:
        return 0
    cleaned = re.sub(r"[^0-9-]", "", text)
    if cleaned in {"", "-"}:
        return 0
    return int(cleaned)


def normalize_grade(value: str) -> str:
    normalized = normalize_text(value)
    if normalized not in GRADE_MAP:
        raise ValueError(f"Grado no soportado: {value}")
    return GRADE_MAP[normalized]


def _try_parse_with_pandas(html: str) -> None:
    if pd is None:
        return
    try:
        pd.read_html(io.StringIO(html))
    except Exception:
        return


def _table_rows_from_html(html: str) -> list[list[str]]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if table is None:
        raise ValueError("MINEDU ESCALE no contiene ninguna tabla HTML parseable.")

    rows: list[list[str]] = []
    pending_rowspans: dict[int, tuple[str, int]] = {}

    for tr in table.find_all("tr"):
        cells = tr.find_all(["th", "td"])
        if not cells:
            continue
        row: list[str] = []
        column_index = 0

        def consume_pending() -> None:
            nonlocal column_index
            while column_index in pending_rowspans:
                value, remaining = pending_rowspans[column_index]
                row.append(value)
                if remaining <= 1:
                    del pending_rowspans[column_index]
                else:
                    pending_rowspans[column_index] = (value, remaining - 1)
                column_index += 1

        consume_pending()
        for cell in cells:
            consume_pending()
            cell_value = " ".join(cell.get_text(" ", strip=True).split())
            rowspan = int(cell.get("rowspan", 1))
            colspan = int(cell.get("colspan", 1))
            for colspan_offset in range(colspan):
                row.append(cell_value)
                if rowspan > 1:
                    pending_rowspans[column_index + colspan_offset] = (
                        cell_value,
                        rowspan - 1,
                    )
            column_index += colspan
        consume_pending()
        rows.append(row)

    if not rows:
        raise ValueError("MINEDU ESCALE no contiene filas tabulares parseables.")
    return rows


def _looks_like_grade_data_row(row: list[str]) -> bool:
    if not row:
        return False
    label = normalize_text(row[0])
    return label in GRADE_MAP and len(row) >= 8


def _preview_rows(rows: list[list[str]], limit: int = 5) -> list[list[str]]:
    return [
        [normalize_text(value) for value in row]
        for row in rows[:limit]
    ]


def _resolve_header_indexes(rows: list[list[str]]) -> dict[str, int]:
    indexes: dict[str, int] = {}
    header_candidate_rows: list[list[str]] = []

    for row in rows:
        if _looks_like_grade_data_row(row):
            break
        header_candidate_rows.append(row)

    for header_row in header_candidate_rows[:5]:
        normalized_headers = [normalize_text(value) for value in header_row]
        for idx, header in enumerate(normalized_headers):
            for field_name, aliases in HEADER_ALIASES.items():
                if header in aliases and field_name not in indexes:
                    indexes[field_name] = idx

    missing = [field for field in HEADER_ALIASES if field not in indexes]
    if not missing:
        return indexes

    fallback_grade_row = next((row for row in rows if _looks_like_grade_data_row(row)), None)
    if fallback_grade_row is not None:
        log_event(
            logger,
            "WARNING",
            "minedu_escale_header_resolution_fallback",
            missing_fields=missing,
            fallback_used=True,
            detected_column_count=len(fallback_grade_row),
            preview_rows=_preview_rows(rows),
        )
        return dict(DEFAULT_MINEDU_COLUMN_INDEXES)

    preview_rows = _preview_rows(rows)
    log_event(
        logger,
        "ERROR",
        "minedu_escale_header_resolution_failed",
        missing_fields=missing,
        fallback_used=False,
        detected_column_count=max((len(row) for row in rows), default=0),
        preview_rows=preview_rows,
    )
    raise ValueError(
        "No se pudieron resolver columnas MINEDU ESCALE: "
        f"{missing}. Muestra de filas iniciales: {preview_rows}"
    )


def _get_row_value(row: list[str], field_name: str, header_indexes: dict[str, int]) -> int:
    column_index = header_indexes[field_name]
    if len(row) <= column_index:
        raise ValueError(
            "Fila MINEDU ESCALE incompleta para la estructura esperada. "
            f"field={field_name} column_index={column_index} row={row}"
        )
    return clean_number(row[column_index])


def _validate_fallback_row_shape(rows: list[list[str]], header_indexes: dict[str, int]) -> None:
    max_required_index = max(header_indexes.values())
    fallback_grade_row = next((row for row in rows if normalize_text(row[0]) in GRADE_MAP), None)
    if fallback_grade_row is not None and len(fallback_grade_row) <= max_required_index:
        preview_rows = _preview_rows(rows)
        log_event(
            logger,
            "ERROR",
            "minedu_escale_header_resolution_failed",
            fallback_used=True,
            detected_column_count=len(fallback_grade_row),
            preview_rows=preview_rows,
        )
        raise ValueError(
            "No se pudo aplicar el fallback posicional MINEDU ESCALE porque la fila de grado "
            f"tiene menos de {max_required_index + 1} columnas. "
            f"Fila detectada: {fallback_grade_row}. Muestra: {preview_rows}"
        )


def extract_total_secundaria_rows(html: str) -> list[dict[str, int | str]]:
    _try_parse_with_pandas(html)
    rows = _table_rows_from_html(html)
    header_indexes = _resolve_header_indexes(rows)
    _validate_fallback_row_shape(rows, header_indexes)
    in_total_secundaria = False
    extracted: list[dict[str, int | str]] = []

    for row in rows[1:]:
        if not row:
            continue
        label = normalize_text(row[0])
        if label == "TOTAL SECUNDARIA":
            in_total_secundaria = True
            continue
        if in_total_secundaria and label in SECTION_END_MARKERS:
            break
        if not in_total_secundaria or label == "":
            continue
        if label not in GRADE_MAP:
            continue

        extracted.append(
            {
                "grado": normalize_grade(row[0]),
                "matricula_total": _get_row_value(row, "matricula_total", header_indexes),
                "matricula_publica": _get_row_value(row, "matricula_publica", header_indexes),
                "matricula_privada": _get_row_value(row, "matricula_privada", header_indexes),
                "matricula_urbana": _get_row_value(row, "matricula_urbana", header_indexes),
                "matricula_rural": _get_row_value(row, "matricula_rural", header_indexes),
                "matricula_masculino": _get_row_value(row, "matricula_masculino", header_indexes),
                "matricula_femenino": _get_row_value(row, "matricula_femenino", header_indexes),
            }
        )

    if len(extracted) != 5:
        raise ValueError(
            f"No se pudo extraer el bloque Total Secundaria completo. Filas encontradas: {len(extracted)}"
        )
    return extracted


def validate_enrollment_row(row: dict[str, Any], tolerance: int = 1) -> None:
    total = int(row["matricula_total"])
    if total < 0:
        raise ValueError("matricula_total no puede ser negativa.")
    checks = {
        "publica_privada": int(row["matricula_publica"]) + int(row["matricula_privada"]),
        "urbana_rural": int(row["matricula_urbana"]) + int(row["matricula_rural"]),
        "masculino_femenino": int(row["matricula_masculino"]) + int(row["matricula_femenino"]),
    }
    for check_name, computed in checks.items():
        if abs(computed - total) > tolerance:
            raise ValueError(
                f"Inconsistencia de matrícula en {check_name}: total={total} computed={computed}"
            )


def scrape_year_department(
    *,
    session: requests.Session,
    base_url: str,
    year: int,
    year_config: dict[str, int],
    codigo_departamento: str,
    region: str,
    extraction_date: str,
    pipeline_run_id: str,
    timeout: int,
    debug_html_output_dir: str | Path | None = None,
) -> list[dict[str, str]]:
    source_url = build_minedu_escale_url(
        base_url=base_url,
        anio_param=year_config["anio_param"],
        cuadro=year_config["cuadro"],
        codigo_departamento=codigo_departamento,
    )
    response = session.get(source_url, timeout=timeout)
    response.raise_for_status()
    if debug_html_output_dir:
        debug_path = (
            Path(debug_html_output_dir)
            / f"year={year}_dpto={codigo_departamento}.html"
        )
        debug_path.parent.mkdir(parents=True, exist_ok=True)
        debug_path.write_text(response.text, encoding="utf-8")
    rows = extract_total_secundaria_rows(response.text)
    bronze_rows: list[dict[str, str]] = []
    ingestion_timestamp = datetime.now(UTC).isoformat()

    for row in rows:
        payload = {
            "anio": str(year),
            "codigo_departamento": codigo_departamento,
            "region": region,
            "nivel_educativo": "SECUNDARIA",
            "grado": str(row["grado"]),
            "matricula_total": str(row["matricula_total"]),
            "matricula_publica": str(row["matricula_publica"]),
            "matricula_privada": str(row["matricula_privada"]),
            "matricula_urbana": str(row["matricula_urbana"]),
            "matricula_rural": str(row["matricula_rural"]),
            "matricula_masculino": str(row["matricula_masculino"]),
            "matricula_femenino": str(row["matricula_femenino"]),
            "source_url": source_url,
            "extraction_date": extraction_date,
            "ingestion_timestamp": ingestion_timestamp,
            "pipeline_run_id": pipeline_run_id,
        }
        validate_enrollment_row(payload)
        bronze_rows.append(payload)

    return bronze_rows


def scrape_minedu_escale(
    *,
    base_url: str,
    departments: dict[str, str],
    yearly_tables: dict[int, dict[str, int]],
    extraction_date: str,
    pipeline_run_id: str,
    start_year: int,
    end_year: int,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    session: requests.Session | None = None,
    department_code: str | None = None,
    debug_html_output_dir: str | Path | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    selected_departments = departments
    if department_code is not None:
        if department_code not in departments:
            raise ValueError(
                f"department_code no existe en la configuración MINEDU ESCALE: {department_code}"
            )
        selected_departments = {department_code: departments[department_code]}

    records: list[dict[str, str]] = []
    for year in sorted(yearly_tables):
        if year < start_year or year > end_year:
            continue
        for codigo_departamento, region in selected_departments.items():
            records.extend(
                scrape_year_department(
                    session=session,
                    base_url=base_url,
                    year=year,
                    year_config=yearly_tables[year],
                    codigo_departamento=codigo_departamento,
                    region=region,
                    extraction_date=extraction_date,
                    pipeline_run_id=pipeline_run_id,
                    timeout=timeout,
                    debug_html_output_dir=debug_html_output_dir,
                )
            )

    combinations = Counter(
        (row["anio"], row["codigo_departamento"], row["grado"])
        for row in records
    )
    duplicates = [key for key, count in combinations.items() if count > 1]
    if duplicates:
        raise ValueError(f"Se detectaron duplicados MINEDU ESCALE: {duplicates[:5]}")

    expected_years = [year for year in yearly_tables if start_year <= year <= end_year]
    expected_rows = len(expected_years) * len(selected_departments) * 5
    if len(records) != expected_rows:
        raise ValueError(
            f"MINEDU ESCALE produjo {len(records)} filas y se esperaban {expected_rows}."
        )

    return records


def write_bronze_csv(
    *,
    bucket_name: str,
    object_path: str,
    records: list[dict[str, str]],
) -> str:
    return upload_csv(
        bucket_name=bucket_name,
        object_path=object_path,
        records=records,
        fieldnames=FIELDNAMES,
    )


def write_bronze_csv_to_local(
    *,
    output_dir: str | Path,
    extraction_date: str,
    records: list[dict[str, str]],
) -> str:
    local_path = (
        Path(output_dir)
        / "bronze"
        / "minedu"
        / "escale_matricula_secundaria"
        / f"extraction_date={extraction_date}"
        / "data.csv"
    )
    local_path.parent.mkdir(parents=True, exist_ok=True)
    with local_path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(records)
    return str(local_path)


def _format_years_range(start_year: int, end_year: int) -> str:
    if start_year == end_year:
        return str(start_year)
    return f"{start_year}-{end_year}"


def _write_local_output_metadata(
    *,
    output_path: str | Path,
    records_written: int,
    start_year: int,
    end_year: int,
    department_count: int,
    dry_run: bool,
) -> None:
    log_event(
        logger,
        "INFO",
        "Extracción MINEDU ESCALE completada",
        source_system="MINEDU_ESCALE",
        source_dataset=DATASET_NAME,
        records_written=records_written,
        years=_format_years_range(start_year, end_year),
        departments=department_count,
        output_uri=str(output_path),
        dry_run=dry_run,
    )


def resolve_extraction_date(value: str | None) -> str:
    resolved = value or str(date.today())
    try:
        date.fromisoformat(resolved)
    except ValueError as exc:
        raise ConfigError(f"extraction_date inválida: {resolved}") from exc
    return resolved


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrapea MINEDU ESCALE hacia Bronze.")
    parser.add_argument("--pipeline-config", default=DEFAULT_PIPELINE_CONFIG)
    parser.add_argument("--endpoints-config", default=DEFAULT_ENDPOINTS_CONFIG)
    parser.add_argument("--bucket", default=None)
    parser.add_argument("--extraction-date", default=None)
    parser.add_argument("--pipeline-run-id", default=None)
    parser.add_argument("--start-year", type=int, default=None)
    parser.add_argument("--end-year", type=int, default=None)
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Ejecuta la extracción y escribe salidas locales en tmp/ sin usar GCS.",
    )
    parser.add_argument(
        "--output-dir",
        default="tmp",
        help="Directorio local base para salidas dry-run.",
    )
    parser.add_argument(
        "--department-code",
        help="Código de departamento ESCALE opcional para probar un solo departamento, por ejemplo 01.",
    )
    parser.add_argument(
        "--debug-html-output-dir",
        help="Directorio opcional para guardar HTML real descargado por año/departamento.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    pipeline_settings = get_pipeline_settings(args.pipeline_config)
    endpoints = load_yaml_config(args.endpoints_config)
    config = endpoints["minedu_escale"]

    bucket_name = args.bucket or pipeline_settings["bucket_name"]
    if not args.dry_run and not bucket_name:
        raise ConfigError("bucket es requerido via --bucket o GCS_BUCKET_NAME.")

    extraction_date = resolve_extraction_date(args.extraction_date)
    pipeline_run_id = args.pipeline_run_id or f"minedu-{uuid.uuid4().hex[:12]}"
    start_year = args.start_year or int(config["default_start_year"])
    end_year = args.end_year or int(config["default_end_year"])

    if config.get("start_year_env_var") and not args.start_year:
        start_year = int(os.getenv(config["start_year_env_var"], start_year))
    if config.get("end_year_env_var") and not args.end_year:
        end_year = int(os.getenv(config["end_year_env_var"], end_year))

    if start_year > end_year:
        raise ConfigError("start_year no puede ser mayor que end_year.")

    departments = config["departments"]
    if args.department_code is not None and args.department_code not in departments:
        raise ValueError(
            f"department_code no existe en la configuración MINEDU ESCALE: {args.department_code}"
        )

    records = scrape_minedu_escale(
        base_url=config["base_url"],
        departments=departments,
        yearly_tables={
            int(year): value for year, value in config["yearly_tables"].items()
        },
        extraction_date=extraction_date,
        pipeline_run_id=pipeline_run_id,
        start_year=start_year,
        end_year=end_year,
        timeout=args.timeout,
        department_code=args.department_code,
        debug_html_output_dir=args.debug_html_output_dir,
    )
    if args.dry_run:
        output_uri = write_bronze_csv_to_local(
            output_dir=args.output_dir,
            extraction_date=extraction_date,
            records=records,
        )
    else:
        object_path = build_gcs_path(
            pipeline_settings["gcs_paths"]["minedu_escale_bronze"],
            extraction_date=extraction_date,
        )
        output_uri = write_bronze_csv(
            bucket_name=bucket_name,
            object_path=object_path,
            records=records,
        )

    _write_local_output_metadata(
        output_path=output_uri,
        records_written=len(records),
        start_year=start_year,
        end_year=end_year,
        department_count=len({row["codigo_departamento"] for row in records}),
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
