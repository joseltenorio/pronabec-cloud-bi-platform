# -*- coding: utf-8 -*-
"""Módulo de Discovery para datasets PRONABEC API.

Determina el estado inicial, tamaño de página efectivo, cantidad de registros
y páginas para los datasets habilitados antes de la extracción.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

from pipelines.common.audit import generate_run_id
from pipelines.common.config import ConfigError, get_pipeline_settings, load_yaml_config
from pipelines.common.gcs import upload_json
from pipelines.common.logging import log_event, setup_structured_logger
from pipelines.common.orchestration_config import (
    get_pronabec_dataset_policies,
    load_orchestration_config,
)
from pipelines.extract_pronabec import (
    build_pronabec_data_url,
    fetch_pronabec_page,
    is_http_500_pronabec_error,
    resolve_extraction_date,
    resolve_pipeline_run_id,
    resolve_retry_settings,
    resolve_source_dataset,
    select_pronabec_endpoints,
)


def discover_dataset(
    endpoint: dict[str, Any],
    policy: Any,
    base_url: str,
    retry_settings: dict[str, Any],
    logger,
) -> dict[str, Any]:
    """Realiza el descubrimiento de un dataset PRONABEC determinando effective_page_size,

    total_records, total_pages y actual_records_returned.
    """
    dataset_name = endpoint["name"]
    endpoint_path = endpoint["path"]
    url = build_pronabec_data_url(base_url, endpoint_path)

    recommended_page_size = policy.recommended_page_size
    fallback_page_sizes = policy.fallback_page_sizes
    page_sizes = [recommended_page_size]
    for size in fallback_page_sizes:
        if size not in page_sizes:
            page_sizes.append(size)

    session = requests.Session()
    failures: list[str] = []

    for index, page_size in enumerate(page_sizes):
        start_time = time.time()
        try:
            payload = fetch_pronabec_page(
                session=session,
                url=url,
                page=1,
                rows=page_size,
                timeout=retry_settings["timeout"],
                max_retries=retry_settings["max_retries"],
                backoff_base_seconds=retry_settings["backoff_base_seconds"],
                backoff_max_seconds=retry_settings["backoff_max_seconds"],
                dataset_name=dataset_name,
                logger=logger,
            )
            elapsed = time.time() - start_time

            if index > 0:
                log_event(
                    logger,
                    "WARNING",
                    "Usando fallback page_size en Discovery PRONABEC",
                    dataset=dataset_name,
                    requested_page_size=recommended_page_size,
                    effective_page_size=page_size,
                )

            total_records = int(payload.get("records") or 0)
            total_pages = int(payload.get("total") or 1)
            actual_records_returned = len(payload.get("rows", []))

            # Asegurar consistencia de páginas si total viene vacío
            if total_records > 0 and total_pages <= 0:
                total_pages = math.ceil(total_records / page_size)

            return {
                "source_dataset": dataset_name,
                "extraction_enabled": policy.extraction_enabled,
                "silver_enabled": policy.silver_enabled,
                "required_for_e2e": policy.required_for_e2e,
                "extraction_mode": policy.extraction_mode,
                "recommended_page_size": recommended_page_size,
                "fallback_page_sizes": fallback_page_sizes,
                "effective_page_size": page_size,
                "total_records": total_records,
                "total_pages": total_pages,
                "actual_records_returned": actual_records_returned,
                "status": "SUCCESS",
                "elapsed_seconds": round(elapsed, 2),
            }

        except Exception as exc:
            elapsed = time.time() - start_time
            is_500 = False
            if isinstance(exc, requests.exceptions.HTTPError):
                is_500 = exc.response is not None and exc.response.status_code == 500
            elif is_http_500_pronabec_error(exc):
                is_500 = True

            if not is_500:
                # Errores que no son HTTP 500 no pasan por fallback de page size
                raise exc
            failures.append(f"page_size={page_size}: {exc}")
            log_event(
                logger,
                "WARNING",
                "Fallo HTTP 500 probando page_size en Discovery PRONABEC",
                dataset=dataset_name,
                page_size=page_size,
                error_message=str(exc),
            )

    raise ConfigError(
        f"Todos los page_size fallaron con HTTP 500 para {dataset_name}: {'; '.join(failures)}"
    )


def run_discovery(args: argparse.Namespace) -> None:
    """Orquesta el proceso de discovery para PRONABEC."""
    pipeline_settings = get_pipeline_settings(args.pipeline_config)
    endpoints_config = load_yaml_config(args.endpoints_config)
    orchestration_config = load_orchestration_config(args.orchestration_config)

    retry_settings = resolve_retry_settings(
        timeout=args.timeout,
        max_retries=args.max_retries,
        backoff_base_seconds=args.backoff_base_seconds,
        backoff_max_seconds=args.backoff_max_seconds,
    )

    logger = setup_structured_logger(
        name="discover_pronabec",
        level=pipeline_settings["log_level"],
        structured=True,
    )

    bucket_name = args.bucket or pipeline_settings["bucket_name"]

    if not args.dry_run and not bucket_name:
        raise ConfigError(
            "No se definió bucket. Configura GCS_BUCKET_NAME o usa --bucket."
        )

    extraction_date = resolve_extraction_date(
        cli_value=args.extraction_date,
        dry_run=args.dry_run,
        allow_default_date=args.allow_default_date,
    )

    pipeline_run_id = resolve_pipeline_run_id(args.pipeline_run_id, args.run_id)
    run_id = pipeline_run_id or generate_run_id("pronabec_discovery")

    source_dataset = resolve_source_dataset(
        cli_value=args.source_dataset,
        legacy_value=args.dataset,
    )

    log_event(
        logger,
        "INFO",
        "Iniciando discovery de datasets PRONABEC",
        source_dataset=source_dataset,
        extraction_date=extraction_date,
        run_id=run_id,
        dry_run=args.dry_run,
    )

    # Si no hay source_dataset específico, descubrimos todos los habilitados
    if not source_dataset:
        policies = get_pronabec_dataset_policies(orchestration_config)
        enabled_datasets = [p.source_dataset for p in policies if p.extraction_enabled]
        endpoints = [
            ep for ep in endpoints_config["pronabec"]["endpoints"]
            if ep["name"] in enabled_datasets
        ]
    else:
        # select_pronabec_endpoints ya valida existencia, politicas y deshabilitados
        endpoints = select_pronabec_endpoints(
            endpoints_config=endpoints_config,
            orchestration_config=orchestration_config,
            source_dataset=source_dataset,
            allow_disabled_dataset=args.allow_disabled_dataset,
        )

    policies_by_dataset = {
        p.source_dataset: p
        for p in get_pronabec_dataset_policies(orchestration_config)
    }

    base_url = endpoints_config["pronabec"]["base_url"]
    dataset_results: list[dict[str, Any]] = []
    discovery_status = "SUCCESS"

    for endpoint in endpoints:
        dataset_name = endpoint["name"]
        policy = policies_by_dataset.get(dataset_name)

        if not policy:
            log_event(
                logger,
                "ERROR",
                "No se encontró política de extracción para dataset",
                dataset=dataset_name,
            )
            continue

        log_event(
            logger,
            "INFO",
            "Descubriendo dataset",
            dataset=dataset_name,
        )

        # Resolver fail_on_error desde el raw dict o requerido
        # Si no existe en policy, usamos policy.required_for_e2e
        fail_on_error = getattr(policy, "fail_on_error", None)
        if fail_on_error is None:
            fail_on_error = policy.required_for_e2e

        try:
            res = discover_dataset(
                endpoint=endpoint,
                policy=policy,
                base_url=base_url,
                retry_settings=retry_settings,
                logger=logger,
            )
            dataset_results.append(res)
        except Exception as exc:
            log_event(
                logger,
                "ERROR",
                "Fallo en discovery de dataset",
                dataset=dataset_name,
                error_message=str(exc),
            )

            res_failed = {
                "source_dataset": dataset_name,
                "extraction_enabled": policy.extraction_enabled,
                "silver_enabled": policy.silver_enabled,
                "required_for_e2e": policy.required_for_e2e,
                "extraction_mode": policy.extraction_mode,
                "recommended_page_size": policy.recommended_page_size,
                "fallback_page_sizes": policy.fallback_page_sizes,
                "status": "FAILED",
                "error": str(exc),
            }
            dataset_results.append(res_failed)

            if fail_on_error:
                discovery_status = "FAILED"
                # Si es crítico, abortamos inmediatamente después de escribir el discovery.json fallido
                break

    # Guardar metadata de discovery
    discovery_manifest = {
        "source_system": "pronabec",
        "extraction_date": extraction_date,
        "pipeline_run_id": run_id,
        "source_snapshot_observed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "status": discovery_status,
        "datasets": dataset_results,
    }

    # Escribir discovery.json
    if args.dry_run:
        # En dry-run, escribimos localmente bajo tmp/
        base_dir = (
            Path(args.output_dir)
            / "bronze_work"
            / "pronabec"
            / "_plans"
            / f"extraction_date={extraction_date}"
            / f"run_id={run_id}"
        )
        base_dir.mkdir(parents=True, exist_ok=True)
        out_path = base_dir / "discovery.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(discovery_manifest, f, ensure_ascii=False, indent=2)
        log_event(
            logger,
            "INFO",
            "Discovery completado localmente (dry-run)",
            path=str(out_path),
            status=discovery_status,
        )
    else:
        # En cloud, subimos a GCS
        object_path = (
            f"bronze_work/pronabec/_plans/extraction_date={extraction_date}"
            f"/run_id={run_id}/discovery.json"
        )
        upload_json(
            bucket_name=bucket_name,
            object_path=object_path,
            payload=discovery_manifest,
        )
        log_event(
            logger,
            "INFO",
            "Discovery completado en GCS",
            bucket=bucket_name,
            object_path=object_path,
            status=discovery_status,
        )

    if discovery_status == "FAILED":
        sys.exit("Discovery falló por error crítico en dataset requerido o configurado para fallar.")


def parse_args() -> argparse.Namespace:
    """Parsea argumentos CLI."""
    parser = argparse.ArgumentParser(
        description="Descubre metadatos de datasets de PRONABEC."
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
        help="Dataset específico a descubrir.",
    )
    parser.add_argument(
        "--dataset",
        help="Alias legacy de --source-dataset.",
    )
    parser.add_argument(
        "--allow-disabled-dataset",
        action="store_true",
        help="Permite descubrir datasets deshabilitados.",
    )
    parser.add_argument(
        "--bucket",
        help="Bucket GCS.",
    )
    parser.add_argument(
        "--extraction-date",
        help="Fecha lógica YYYY-MM-DD.",
    )
    parser.add_argument(
        "--allow-default-date",
        action="store_true",
        help="Permite usar la fecha actual para dry-run local.",
    )
    parser.add_argument(
        "--run-id",
        help="Identificador de ejecución legacy.",
    )
    parser.add_argument(
        "--pipeline-run-id",
        help="Identificador de ejecución.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Escribe outputs locales bajo tmp/ sin usar GCS.",
    )
    parser.add_argument(
        "--output-dir",
        default="tmp",
        help="Directorio local para salidas dry-run.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        help="Timeout HTTP.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        help="Intentos máximos.",
    )
    parser.add_argument(
        "--backoff-base-seconds",
        type=float,
        help="Backoff base.",
    )
    parser.add_argument(
        "--backoff-max-seconds",
        type=float,
        help="Backoff max.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.2,
        help="Pausa entre requests.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        run_discovery(args)
    except Exception as exc:
        sys.exit(f"Error fatal durante discovery: {exc}")


if __name__ == "__main__":
    main()
