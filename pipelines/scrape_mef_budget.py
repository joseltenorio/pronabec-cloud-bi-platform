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
from pipelines.common.gcs import build_mef_bronze_path, upload_csv
from pipelines.common.logging import log_event, setup_structured_logger
from pipelines.common.validation import validate_required_columns


DEFAULT_ENDPOINTS_CONFIG = "config/endpoints.yaml"
DEFAULT_PIPELINE_CONFIG = "config/pipeline.yaml"
DEFAULT_TIMEOUT_SECONDS = 90


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
            "No se definió fuente MEF. Usa --source-url, --source-file "
            "o configura MEF_SOURCE_URL."
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


def build_local_mef_output_paths(
    output_dir: str | Path,
    extraction_date: str,
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
) -> dict[str, str]:
    """
    Escribe data.csv y extraction_metadata.json localmente.
    """
    paths = build_local_mef_output_paths(
        output_dir=output_dir,
        extraction_date=extraction_date,
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
            "source_url": source_url,
            "source_file": source_file,
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

    source_url = args.source_url or get_env_var("MEF_SOURCE_URL")
    source_file = args.source_file
    extraction_date = args.extraction_date or date.today().isoformat()
    run_id = args.run_id or generate_run_id("mef_extraction")

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

    text_filter = args.text_filter or "PRONABEC"

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
    )

    started_at = datetime.utcnow()

    try:
        raw_records = fetch_mef_records(
            source_url=source_url,
            source_file=source_file,
            timeout=args.timeout,
            table_index=args.table_index,
        )

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

        if args.dry_run:
            output_result = write_mef_to_local(
                records=normalized_records,
                fieldnames=expected_columns,
                extraction_date=extraction_date,
                output_dir=args.output_dir,
                run_id=run_id,
                records_read=len(raw_records),
                source_url=source_url,
                source_file=source_file,
                logger=logger,
            )
            output_uri = output_result["output_uri"]
        else:
            object_path = build_mef_bronze_path(
                pipeline_settings["gcs_paths"]["mef_bronze"],
                extraction_date=extraction_date,
            )

            output_uri = upload_csv(
                bucket_name=bucket_name,
                object_path=object_path,
                records=normalized_records,
                fieldnames=expected_columns,
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
                "output_uri": output_uri,
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
            output_uri=output_uri,
            records_read=len(raw_records),
            records_written=len(normalized_records),
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


def parse_args() -> argparse.Namespace:
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
        default=os.getenv("MEF_TEXT_FILTER", "PRONABEC"),
        help="Texto usado para filtrar registros relacionados con PRONABEC.",
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
        default=DEFAULT_TIMEOUT_SECONDS,
        help="Timeout HTTP por request en segundos.",
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

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_extraction(args)


if __name__ == "__main__":
    main()