"""Ejecuta los chunks definidos en plan.json para PRONABEC."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pipelines.common.config import ConfigError, get_pipeline_settings, load_yaml_config
from pipelines.common.gcs import read_gcs_bytes
from pipelines.common.logging import log_event, setup_structured_logger
from pipelines.extract_pronabec import (
    build_pronabec_chunk_base_path,
    extract_dataset,
    resolve_extraction_date,
    resolve_pipeline_run_id,
    resolve_retry_settings,
    resolve_source_dataset,
    write_chunk_dataset_to_gcs,
    write_chunk_dataset_to_local,
)
from pipelines.finalize_pronabec_dataset import read_plan_json


def _build_endpoint_index(endpoints_config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    pronabec = endpoints_config.get("pronabec", {})
    endpoints = pronabec.get("endpoints", [])
    if not isinstance(endpoints, list):
        raise ConfigError("config/endpoints.yaml: pronabec.endpoints debe ser una lista")

    endpoint_index: dict[str, dict[str, Any]] = {}
    for endpoint in endpoints:
        if not isinstance(endpoint, dict):
            continue
        name = endpoint.get("name")
        if isinstance(name, str) and name.strip():
            endpoint_index[name.strip()] = endpoint
    return endpoint_index


def _read_chunk_manifest(
    dry_run: bool,
    output_dir: str,
    bucket_name: str,
    dataset_name: str,
    extraction_date: str,
    pipeline_run_id: str,
    page_start: int,
    page_end: int,
) -> dict[str, Any] | None:
    chunk_base = build_pronabec_chunk_base_path(
        dataset_name=dataset_name,
        extraction_date=extraction_date,
        pipeline_run_id=pipeline_run_id,
        page_start=page_start,
        page_end=page_end,
    )
    manifest_rel_path = f"{chunk_base}/chunk_manifest.json"

    if dry_run:
        manifest_path = Path(output_dir) / manifest_rel_path
        if not manifest_path.exists():
            return None
        with manifest_path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    manifest_uri = f"gs://{bucket_name}/{manifest_rel_path}"
    try:
        manifest_bytes = read_gcs_bytes(manifest_uri)
    except Exception:
        return None

    return json.loads(manifest_bytes.decode("utf-8"))


def _execute_chunk(
    *,
    endpoint: dict[str, Any],
    base_url: str,
    chunk: dict[str, Any],
    extraction_date: str,
    pipeline_run_id: str,
    bucket_name: str,
    output_dir: str,
    dry_run: bool,
    retry_settings: dict[str, int | float],
    logger,
) -> dict[str, Any]:
    dataset_name = chunk["source_dataset"]
    page_start = int(chunk["page_start"])
    page_end = int(chunk["page_end"])
    effective_page_size = int(chunk["effective_page_size"])
    requested_page_size = int(chunk.get("requested_page_size") or effective_page_size)

    started_at = datetime.now(UTC)
    existing_manifest = _read_chunk_manifest(
        dry_run=dry_run,
        output_dir=output_dir,
        bucket_name=bucket_name,
        dataset_name=dataset_name,
        extraction_date=extraction_date,
        pipeline_run_id=pipeline_run_id,
        page_start=page_start,
        page_end=page_end,
    )
    if existing_manifest and existing_manifest.get("status") == "SUCCESS":
        log_event(
            logger,
            "INFO",
            "chunk_already_completed",
            source_dataset=dataset_name,
            page_start=page_start,
            page_end=page_end,
            extraction_date=extraction_date,
            pipeline_run_id=pipeline_run_id,
        )
        return {
            "source_dataset": dataset_name,
            "page_start": page_start,
            "page_end": page_end,
            "status": "SKIPPED",
            "effective_page_size": effective_page_size,
        }

    raw_payload, normalized_records = extract_dataset(
        endpoint=endpoint,
        base_url=base_url,
        rows_per_page=effective_page_size,
        requested_page_size=requested_page_size,
        max_pages=None,
        page_start=page_start,
        page_end=page_end,
        timeout=int(retry_settings["timeout"]),
        max_retries=int(retry_settings["max_retries"]),
        backoff_base_seconds=float(retry_settings["backoff_base_seconds"]),
        backoff_max_seconds=float(retry_settings["backoff_max_seconds"]),
        sleep_seconds=0.0,
        extraction_date=extraction_date,
        run_id=pipeline_run_id,
        logger=logger,
    )

    if dry_run:
        write_chunk_dataset_to_local(
            dataset_name=dataset_name,
            raw_payload=raw_payload,
            normalized_records=normalized_records,
            extraction_date=extraction_date,
            output_dir=output_dir,
            pipeline_run_id=pipeline_run_id,
            page_start=page_start,
            page_end=page_end,
            started_at=started_at,
            logger=logger,
        )
    else:
        write_chunk_dataset_to_gcs(
            dataset_name=dataset_name,
            raw_payload=raw_payload,
            normalized_records=normalized_records,
            bucket_name=bucket_name,
            extraction_date=extraction_date,
            pipeline_run_id=pipeline_run_id,
            page_start=page_start,
            page_end=page_end,
            started_at=started_at,
            logger=logger,
        )

    log_event(
        logger,
        "INFO",
        "chunk_completed",
        source_dataset=dataset_name,
        page_start=page_start,
        page_end=page_end,
        extraction_date=extraction_date,
        pipeline_run_id=pipeline_run_id,
        effective_page_size=effective_page_size,
        records_written=len(normalized_records),
    )

    return {
        "source_dataset": dataset_name,
        "page_start": page_start,
        "page_end": page_end,
        "status": "SUCCESS",
        "effective_page_size": effective_page_size,
        "records_written": len(normalized_records),
    }


def run_extraction_plan(args: argparse.Namespace) -> dict[str, Any]:
    pipeline_settings = get_pipeline_settings(args.pipeline_config)
    endpoints_config = load_yaml_config(args.endpoints_config)

    retry_settings = resolve_retry_settings(
        timeout=args.timeout,
        max_retries=args.max_retries,
        backoff_base_seconds=args.backoff_base_seconds,
        backoff_max_seconds=args.backoff_max_seconds,
    )

    logger = setup_structured_logger(
        name="run_pronabec_extraction_plan",
        level=pipeline_settings["log_level"],
        structured=True,
    )

    bucket_name = args.bucket or pipeline_settings["bucket_name"]
    if not args.dry_run and not bucket_name:
        raise ConfigError("No se definio bucket. Configura GCS_BUCKET_NAME o usa --bucket.")

    extraction_date = resolve_extraction_date(
        cli_value=args.extraction_date,
        dry_run=args.dry_run,
        allow_default_date=args.allow_default_date,
    )
    pipeline_run_id = resolve_pipeline_run_id(args.pipeline_run_id, args.run_id)
    if not pipeline_run_id:
        raise ConfigError("Se requiere --pipeline-run-id o PIPELINE_RUN_ID para ejecutar plan.json.")

    source_dataset = resolve_source_dataset(cli_value=args.source_dataset, legacy_value=args.dataset)

    log_event(
        logger,
        "INFO",
        "Iniciando ejecucion de plan PRONABEC",
        extraction_date=extraction_date,
        pipeline_run_id=pipeline_run_id,
        source_dataset=source_dataset,
        dry_run=args.dry_run,
    )

    plan_dict = read_plan_json(
        dry_run=args.dry_run,
        output_dir=args.output_dir,
        bucket_name=bucket_name,
        extraction_date=extraction_date,
        run_id=pipeline_run_id,
    )

    if plan_dict.get("status") != "READY":
        raise ConfigError("plan.json debe tener status READY antes de ejecutar chunks.")

    endpoint_index = _build_endpoint_index(endpoints_config)
    base_url = endpoints_config.get("pronabec", {}).get("base_url")
    if not isinstance(base_url, str) or not base_url.strip():
        raise ConfigError("config/endpoints.yaml: pronabec.base_url es requerido")

    chunks = plan_dict.get("chunks", [])
    if not isinstance(chunks, list):
        raise ConfigError("plan.json invalido: chunks debe ser una lista")

    if source_dataset:
        chunks = [chunk for chunk in chunks if chunk.get("source_dataset") == source_dataset]

    if not chunks:
        raise ConfigError("No hay chunks para ejecutar en plan.json.")

    chunks = sorted(
        chunks,
        key=lambda chunk: (
            str(chunk.get("source_dataset")),
            int(chunk.get("page_start", 0)),
            int(chunk.get("page_end", 0)),
        ),
    )

    completed_chunks = 0
    skipped_chunks = 0
    failed_chunks = 0
    datasets_processed: set[str] = set()
    results: list[dict[str, Any]] = []

    for chunk in chunks:
        dataset_name = chunk.get("source_dataset")
        if dataset_name not in endpoint_index:
            raise ConfigError(f"Dataset PRONABEC no encontrado en config/endpoints.yaml: {dataset_name}")

        endpoint = endpoint_index[dataset_name]
        datasets_processed.add(dataset_name)

        try:
            result = _execute_chunk(
                endpoint=endpoint,
                base_url=base_url,
                chunk=chunk,
                extraction_date=extraction_date,
                pipeline_run_id=pipeline_run_id,
                bucket_name=bucket_name,
                output_dir=args.output_dir,
                dry_run=args.dry_run,
                retry_settings=retry_settings,
                logger=logger,
            )
            results.append(result)
            if result["status"] == "SKIPPED":
                skipped_chunks += 1
            else:
                completed_chunks += 1
        except Exception as exc:
            failed_chunks += 1
            log_event(
                logger,
                "ERROR",
                "chunk_failed",
                source_dataset=dataset_name,
                page_start=chunk.get("page_start"),
                page_end=chunk.get("page_end"),
                extraction_date=extraction_date,
                pipeline_run_id=pipeline_run_id,
                error_message=str(exc),
                required_for_e2e=bool(chunk.get("required_for_e2e", False)),
            )
            raise

    summary = {
        "source_system": "pronabec",
        "status": "SUCCESS",
        "extraction_date": extraction_date,
        "pipeline_run_id": pipeline_run_id,
        "total_chunks": len(chunks),
        "completed_chunks": completed_chunks,
        "skipped_chunks": skipped_chunks,
        "failed_chunks": failed_chunks,
        "datasets_processed": sorted(datasets_processed),
        "results": results,
    }

    log_event(
        logger,
        "INFO",
        "Resumen de ejecucion del plan PRONABEC",
        **summary,
    )
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ejecuta los chunks definidos en plan.json para PRONABEC.",
    )
    parser.add_argument(
        "--pipeline-config",
        default="config/pipeline.yaml",
        help="Ruta a config/pipeline.yaml.",
    )
    parser.add_argument(
        "--endpoints-config",
        default="config/endpoints.yaml",
        help="Ruta a config/endpoints.yaml.",
    )
    parser.add_argument(
        "--orchestration-config",
        default="config/orchestration.yaml",
        help="Ruta a config/orchestration.yaml.",
    )
    parser.add_argument(
        "--source-dataset",
        help="Filtra la ejecucion a un unico dataset PRONABEC.",
    )
    parser.add_argument(
        "--dataset",
        help="Alias legacy de --source-dataset.",
    )
    parser.add_argument(
        "--bucket",
        help="Bucket de GCS. Si se omite, usa GCS_BUCKET_NAME.",
    )
    parser.add_argument(
        "--extraction-date",
        help="Fecha logica de extraccion en formato YYYY-MM-DD.",
    )
    parser.add_argument(
        "--allow-default-date",
        action="store_true",
        help="Permite fecha actual solo en dry-run local.",
    )
    parser.add_argument(
        "--run-id",
        help="Alias legacy de --pipeline-run-id.",
    )
    parser.add_argument(
        "--pipeline-run-id",
        help="Identificador de corrida usado para ubicar plan.json y chunks.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=None,
        help="Timeout HTTP por request en segundos.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=None,
        help="Numero maximo de reintentos por pagina.",
    )
    parser.add_argument(
        "--backoff-base-seconds",
        type=float,
        default=None,
        help="Backoff base para reintentos HTTP.",
    )
    parser.add_argument(
        "--backoff-max-seconds",
        type=float,
        default=None,
        help="Backoff maximo para reintentos HTTP.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Lee y escribe salidas locales en vez de GCS.",
    )
    parser.add_argument(
        "--output-dir",
        default="tmp",
        help="Directorio local para dry-run.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=1,
        help="Reservado para ejecucion futura paralela; esta version usa 1.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_extraction_plan(args)


if __name__ == "__main__":
    main()
