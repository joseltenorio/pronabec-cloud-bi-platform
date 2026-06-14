"""
Extractor PRONABEC API -> Cloud Storage Bronze.

Este script descarga datasets públicos de PRONABEC desde endpoints JSON paginados
con estructura jqGrid y guarda dos salidas por dataset:

- data_raw.json: respuesta cruda consolidada para trazabilidad.
- data.jsonl: registros normalizados estructuralmente desde rows[].cell.

La limpieza de negocio y conversión fuerte de tipos se realiza después en Silver.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import requests

from pipelines.common.audit import create_extraction_audit_event, generate_run_id
from pipelines.common.config import ConfigError, get_pipeline_settings, load_yaml_config
from pipelines.common.gcs import (
    build_pronabec_normalized_path,
    build_pronabec_raw_path,
    upload_json,
    upload_jsonl,
)
from pipelines.common.logging import log_event, setup_structured_logger
from pipelines.common.validation import validate_required_columns


DEFAULT_ENDPOINTS_CONFIG = "config/endpoints.yaml"
DEFAULT_PIPELINE_CONFIG = "config/pipeline.yaml"
DEFAULT_ROWS_PER_PAGE = 100
DEFAULT_TIMEOUT_SECONDS = 60


class PronabecExtractionError(Exception):
    """Error controlado durante extracción PRONABEC."""


def build_pronabec_data_url(base_url: str, endpoint_path: str) -> str:
    """
    Construye la URL del endpoint de datos PRONABEC.

    Ejemplo:
        base_url=https://datosabiertos.pronabec.gob.pe/Dataset
        endpoint_path=NotasDeBecarios
        -> https://datosabiertos.pronabec.gob.pe/Dataset/ListarNotasDeBecarios
    """
    return f"{base_url.rstrip('/')}/Listar{endpoint_path.strip('/')}"


def build_jqgrid_params(
    page: int,
    rows: int,
    sort_column: str = "NRO_FILA",
    sort_order: str = "asc",
) -> dict[str, Any]:
    """
    Construye parámetros esperados por endpoints jqGrid de PRONABEC.
    """
    return {
        "_search": "false",
        "nd": int(time.time() * 1000),
        "rows": rows,
        "page": page,
        "sidx": sort_column,
        "sord": sort_order,
    }


def fetch_pronabec_page(
    session: requests.Session,
    url: str,
    page: int,
    rows: int,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """
    Descarga una página del endpoint PRONABEC.
    """
    response = session.get(
        url,
        params=build_jqgrid_params(page=page, rows=rows),
        timeout=timeout,
    )
    response.raise_for_status()

    payload = response.json()

    if not isinstance(payload, dict):
        raise PronabecExtractionError("La respuesta PRONABEC no es un objeto JSON.")

    if "rows" not in payload:
        raise PronabecExtractionError("La respuesta PRONABEC no contiene la clave rows.")

    if not isinstance(payload["rows"], list):
        raise PronabecExtractionError("La clave rows no contiene una lista.")

    return payload


def normalize_jqgrid_row(
    row: dict[str, Any],
    expected_columns: list[str],
) -> dict[str, Any]:
    """
    Convierte una fila jqGrid {"id": ..., "cell": [...]} a un diccionario tabular.
    """
    if "cell" not in row or not isinstance(row["cell"], list):
        raise PronabecExtractionError("Fila jqGrid inválida: no contiene cell como lista.")

    normalized = {
        "source_row_id": row.get("id"),
    }

    for index, value in enumerate(row["cell"]):
        if index < len(expected_columns):
            column_name = expected_columns[index]
        else:
            column_name = f"cell_{index + 1}"

        normalized[column_name] = value

    return normalized


def normalize_rows(
    rows: list[dict[str, Any]],
    expected_columns: list[str],
) -> list[dict[str, Any]]:
    """
    Normaliza múltiples filas jqGrid.
    """
    return [
        normalize_jqgrid_row(row, expected_columns)
        for row in rows
    ]


def consolidate_raw_payload(
    dataset_name: str,
    url: str,
    pages: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Consolida la respuesta cruda en un JSON trazable.
    """
    first_page = pages[0] if pages else {}

    return {
        "dataset": dataset_name,
        "url": url,
        "extracted_at": datetime.now(UTC).isoformat(),
        "total_pages": first_page.get("total"),
        "reported_records": first_page.get("records"),
        "pages_read": len(pages),
        "pages": pages,
    }


def get_enabled_pronabec_endpoints(
    endpoints_config: dict[str, Any],
    dataset_filter: str | None = None,
) -> list[dict[str, Any]]:
    """
    Devuelve endpoints PRONABEC habilitados desde config/endpoints.yaml.
    """
    endpoints = endpoints_config["pronabec"]["endpoints"]

    enabled = [
        endpoint
        for endpoint in endpoints
        if endpoint.get("enabled", True)
    ]

    if dataset_filter:
        enabled = [
            endpoint
            for endpoint in enabled
            if endpoint["name"] == dataset_filter
        ]

        if not enabled:
            available = ", ".join(endpoint["name"] for endpoint in endpoints)
            raise ConfigError(
                f"Dataset PRONABEC no encontrado o no habilitado: {dataset_filter}. "
                f"Disponibles: {available}"
            )

    return enabled


def extract_dataset(
    endpoint: dict[str, Any],
    base_url: str,
    rows_per_page: int,
    max_pages: int | None,
    timeout: int,
    sleep_seconds: float,
    logger,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """
    Extrae un dataset PRONABEC completo o limitado por max_pages.
    """
    dataset_name = endpoint["name"]
    endpoint_path = endpoint["path"]
    expected_columns = endpoint.get("expected_columns", [])

    url = build_pronabec_data_url(base_url, endpoint_path)

    session = requests.Session()
    pages: list[dict[str, Any]] = []
    normalized_records: list[dict[str, Any]] = []

    log_event(
        logger,
        "INFO",
        "Iniciando extracción PRONABEC",
        dataset=dataset_name,
        url=url,
    )

    first_page = fetch_pronabec_page(
        session=session,
        url=url,
        page=1,
        rows=rows_per_page,
        timeout=timeout,
    )

    total_pages = int(first_page.get("total") or 1)
    pages_to_read = min(total_pages, max_pages) if max_pages else total_pages

    pages.append(first_page)
    normalized_records.extend(
        normalize_rows(first_page.get("rows", []), expected_columns)
    )

    log_event(
        logger,
        "INFO",
        "Primera página PRONABEC descargada",
        dataset=dataset_name,
        total_pages=total_pages,
        pages_to_read=pages_to_read,
        reported_records=first_page.get("records"),
        first_page_records=len(first_page.get("rows", [])),
    )

    for page_number in range(2, pages_to_read + 1):
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

        page_payload = fetch_pronabec_page(
            session=session,
            url=url,
            page=page_number,
            rows=rows_per_page,
            timeout=timeout,
        )

        pages.append(page_payload)
        normalized_records.extend(
            normalize_rows(page_payload.get("rows", []), expected_columns)
        )

        log_event(
            logger,
            "INFO",
            "Página PRONABEC descargada",
            dataset=dataset_name,
            page=page_number,
            page_records=len(page_payload.get("rows", [])),
        )

    raw_payload = consolidate_raw_payload(
        dataset_name=dataset_name,
        url=url,
        pages=pages,
    )

    if normalized_records:
        missing_columns = validate_required_columns(
            actual_columns=normalized_records[0].keys(),
            expected_columns=["source_row_id", *expected_columns],
        )

        if missing_columns:
            raise PronabecExtractionError(
                f"Columnas esperadas ausentes en {dataset_name}: {missing_columns}"
            )

    log_event(
        logger,
        "INFO",
        "Extracción PRONABEC completada",
        dataset=dataset_name,
        pages_read=len(pages),
        records_normalized=len(normalized_records),
    )

    return raw_payload, normalized_records


def write_dataset_to_gcs(
    dataset_name: str,
    raw_payload: dict[str, Any],
    normalized_records: list[dict[str, Any]],
    bucket_name: str,
    extraction_date: str,
    gcs_paths: dict[str, str],
    logger,
) -> dict[str, str]:
    """
    Escribe data_raw.json y data.jsonl en Cloud Storage Bronze.
    """
    raw_path = build_pronabec_raw_path(
        gcs_paths["pronabec_bronze_raw"],
        dataset=dataset_name,
        extraction_date=extraction_date,
    )

    normalized_path = build_pronabec_normalized_path(
        gcs_paths["pronabec_bronze_normalized"],
        dataset=dataset_name,
        extraction_date=extraction_date,
    )

    raw_uri = upload_json(
        bucket_name=bucket_name,
        object_path=raw_path,
        payload=raw_payload,
    )

    normalized_uri = upload_jsonl(
        bucket_name=bucket_name,
        object_path=normalized_path,
        records=normalized_records,
    )

    log_event(
        logger,
        "INFO",
        "Archivos PRONABEC escritos en GCS Bronze",
        dataset=dataset_name,
        raw_uri=raw_uri,
        normalized_uri=normalized_uri,
        records_written=len(normalized_records),
    )

    return {
        "raw_uri": raw_uri,
        "normalized_uri": normalized_uri,
    }


def write_json_file(path: Path, payload: Any) -> None:
    """
    Escribe un archivo JSON local.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def write_jsonl_file(path: Path, records: list[dict[str, Any]]) -> None:
    """
    Escribe registros JSONL localmente.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    content = "".join(
        json.dumps(record, ensure_ascii=False) + "\n"
        for record in records
    )

    path.write_text(content, encoding="utf-8")


def build_local_pronabec_output_paths(
    output_dir: str | Path,
    dataset_name: str,
    extraction_date: str,
) -> dict[str, Path]:
    """
    Construye rutas locales equivalentes a Bronze para dry-run.
    """
    base_path = (
        Path(output_dir)
        / "bronze"
        / "pronabec"
        / dataset_name
        / f"extraction_date={extraction_date}"
    )

    return {
        "raw_path": base_path / "data_raw.json",
        "normalized_path": base_path / "data.jsonl",
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
        "created_at": datetime.now(UTC).isoformat(),
    }


def write_dataset_to_local(
    dataset_name: str,
    raw_payload: dict[str, Any],
    normalized_records: list[dict[str, Any]],
    extraction_date: str,
    output_dir: str | Path,
    run_id: str,
    logger,
) -> dict[str, str]:
    """
    Escribe data_raw.json, data.jsonl y extraction_metadata.json localmente.
    """
    paths = build_local_pronabec_output_paths(
        output_dir=output_dir,
        dataset_name=dataset_name,
        extraction_date=extraction_date,
    )

    write_json_file(paths["raw_path"], raw_payload)
    write_jsonl_file(paths["normalized_path"], normalized_records)

    metadata = build_extraction_metadata(
        source_name="PRONABEC Datos Abiertos",
        source_dataset=dataset_name,
        extraction_date=extraction_date,
        run_id=run_id,
        records_read=len(normalized_records),
        records_written=len(normalized_records),
        output_paths={
            "raw_path": str(paths["raw_path"]),
            "normalized_path": str(paths["normalized_path"]),
        },
        extra_metadata={
            "mode": "dry_run",
            "pages_read": raw_payload.get("pages_read"),
            "reported_records": raw_payload.get("reported_records"),
        },
    )

    write_json_file(paths["metadata_path"], metadata)

    log_event(
        logger,
        "INFO",
        "Archivos PRONABEC escritos en modo dry-run",
        dataset=dataset_name,
        raw_path=str(paths["raw_path"]),
        normalized_path=str(paths["normalized_path"]),
        metadata_path=str(paths["metadata_path"]),
        records_written=len(normalized_records),
    )

    return {
        "raw_uri": str(paths["raw_path"]),
        "normalized_uri": str(paths["normalized_path"]),
        "metadata_path": str(paths["metadata_path"]),
    }


def run_extraction(args: argparse.Namespace) -> None:
    """
    Orquesta la extracción PRONABEC.
    """
    pipeline_settings = get_pipeline_settings(args.pipeline_config)
    endpoints_config = load_yaml_config(args.endpoints_config)

    logger = setup_structured_logger(
        name="extract_pronabec_api",
        level=pipeline_settings["log_level"],
        structured=True,
    )

    bucket_name = args.bucket or pipeline_settings["bucket_name"]

    if not args.dry_run and not bucket_name:
        raise ConfigError(
            "No se definió bucket. Configura GCS_BUCKET_NAME o usa --bucket."
        )

    extraction_date = args.extraction_date or date.today().isoformat()
    run_id = args.run_id or generate_run_id("pronabec_extraction")

    pronabec_config = endpoints_config["pronabec"]
    base_url = pronabec_config["base_url"]

    endpoints = get_enabled_pronabec_endpoints(
        endpoints_config=endpoints_config,
        dataset_filter=args.dataset,
    )

    log_event(
        logger,
        "INFO",
        "Iniciando job de extracción PRONABEC",
        pipeline_name=pipeline_settings["pipeline_name"],
        run_id=run_id,
        extraction_date=extraction_date,
        datasets=[endpoint["name"] for endpoint in endpoints],
    )

    for endpoint in endpoints:
        dataset_name = endpoint["name"]
        started_at = datetime.now(UTC)

        try:
            raw_payload, normalized_records = extract_dataset(
                endpoint=endpoint,
                base_url=base_url,
                rows_per_page=args.rows_per_page,
                max_pages=args.max_pages,
                timeout=args.timeout,
                sleep_seconds=args.sleep_seconds,
                logger=logger,
            )

            if args.dry_run:
                uris = write_dataset_to_local(
                    dataset_name=dataset_name,
                    raw_payload=raw_payload,
                    normalized_records=normalized_records,
                    extraction_date=extraction_date,
                    output_dir=args.output_dir,
                    run_id=run_id,
                    logger=logger,
                )
            else:
                uris = write_dataset_to_gcs(
                    dataset_name=dataset_name,
                    raw_payload=raw_payload,
                    normalized_records=normalized_records,
                    bucket_name=bucket_name,
                    extraction_date=extraction_date,
                    gcs_paths=pipeline_settings["gcs_paths"],
                    logger=logger,
                )

            finished_at = datetime.now(UTC)

            audit_event = create_extraction_audit_event(
                pipeline_name=pipeline_settings["pipeline_name"],
                source_name="PRONABEC Datos Abiertos",
                source_dataset=dataset_name,
                status="SUCCESS",
                run_id=run_id,
                environment=pipeline_settings["environment"],
                execution_date=date.fromisoformat(extraction_date),
                records_read=len(normalized_records),
                records_written=len(normalized_records),
                records_rejected=0,
                started_at=started_at,
                finished_at=finished_at,
                metadata={
                    "raw_uri": uris["raw_uri"],
                    "normalized_uri": uris["normalized_uri"],
                },
            )

            log_event(
                logger,
                "INFO",
                "Evento de auditoría generado",
                **audit_event.to_dict(),
            )

        except Exception as exc:
            finished_at = datetime.now(UTC)

            audit_event = create_extraction_audit_event(
                pipeline_name=pipeline_settings["pipeline_name"],
                source_name="PRONABEC Datos Abiertos",
                source_dataset=dataset_name,
                status="FAILED",
                run_id=run_id,
                environment=pipeline_settings["environment"],
                execution_date=date.fromisoformat(extraction_date),
                records_rejected=None,
                error_message=str(exc),
                started_at=started_at,
                finished_at=finished_at,
                metadata={},
            )

            log_event(
                logger,
                "ERROR",
                "Error extrayendo dataset PRONABEC",
                dataset=dataset_name,
                error_message=str(exc),
                audit_event=audit_event.to_dict(),
            )

            raise


def parse_args() -> argparse.Namespace:
    """
    Lee argumentos CLI.
    """
    parser = argparse.ArgumentParser(
        description="Extrae datos PRONABEC hacia Cloud Storage Bronze."
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
        "--dataset",
        help="Dataset PRONABEC específico. Si se omite, extrae todos los habilitados.",
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
        "--rows-per-page",
        type=int,
        default=DEFAULT_ROWS_PER_PAGE,
        help="Cantidad de filas por página.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        help="Límite de páginas para pruebas controladas.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="Timeout HTTP por request en segundos.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.2,
        help="Pausa entre requests para evitar presión sobre la fuente.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Ejecuta la extracción y escribe salidas locales en tmp/ sin usar GCS.",
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