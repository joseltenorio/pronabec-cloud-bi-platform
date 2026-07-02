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
    get_bronze_enabled_pronabec_datasets,
    get_pronabec_discovery_config,
    get_pronabec_dataset_policies,
    load_orchestration_config,
    resolve_pronabec_discovery_validation_mode,
    resolve_pronabec_page_size_candidates,
)
from pipelines.extract_pronabec import (
    build_pronabec_data_url,
    fetch_pronabec_page,
    resolve_extraction_date,
    resolve_pipeline_run_id,
    resolve_retry_settings,
    resolve_source_dataset,
    select_pronabec_endpoints,
)


DISCOVERY_PROGRESS_LOG_INTERVAL_PAGES = 10


def _extract_pronabec_pagination(payload: dict[str, Any]) -> tuple[int, int]:
    if "rows" not in payload or not isinstance(payload.get("rows"), list):
        raise ConfigError("La respuesta PRONABEC no contiene rows validos")

    records_value = payload.get("records", payload.get("total_records"))
    pages_value = payload.get("total", payload.get("total_pages"))
    if records_value is None:
        raise ConfigError("La respuesta PRONABEC no contiene records o total_records")
    if pages_value is None:
        raise ConfigError("La respuesta PRONABEC no contiene total o total_pages")

    return int(records_value), int(pages_value)


def _extract_status_code(exc: Exception) -> int | None:
    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None)
    return status_code if isinstance(status_code, int) else None


def _build_rejected_page_size(
    *,
    page_size: int,
    failed_page: int,
    exc: Exception,
) -> dict[str, Any]:
    return {
        "page_size": page_size,
        "failed_page": failed_page,
        "status_code": _extract_status_code(exc),
        "error_type": type(exc).__name__,
        "error_message": str(exc)[:500],
    }


class DiscoveryPageSizeCalibrationError(ConfigError):
    def __init__(self, dataset_name: str, rejected_page_sizes: list[dict[str, Any]]):
        super().__init__(
            f"No se encontro page_size estable para {dataset_name}: {rejected_page_sizes}"
        )
        self.rejected_page_sizes = rejected_page_sizes


def _extract_rejected_page_sizes(exc: Exception) -> list[dict[str, Any]]:
    if isinstance(exc, DiscoveryPageSizeCalibrationError):
        return exc.rejected_page_sizes
    return []


def _log_discovery_validation_progress(
    logger,
    *,
    dataset_name: str,
    candidate_page_size: int,
    current_page: int,
    total_pages: int,
    validated_pages: int,
    start_time: float,
) -> None:
    if total_pages <= 0:
        progress_percent = 100.0
    else:
        progress_percent = round((validated_pages / total_pages) * 100, 2)

    log_event(
        logger,
        "INFO",
        "discovery_page_validation_progress",
        dataset=dataset_name,
        candidate_page_size=candidate_page_size,
        current_page=current_page,
        total_pages=total_pages,
        validated_pages=validated_pages,
        progress_percent=progress_percent,
        elapsed_seconds=round(time.time() - start_time, 2),
    )


def discover_dataset(
    endpoint: dict[str, Any],
    policy: Any,
    orchestration_config: dict[str, Any],
    base_url: str,
    retry_settings: dict[str, Any],
    logger,
) -> dict[str, Any]:
    """Discover stable page size metadata for one PRONABEC dataset."""
    dataset_name = endpoint["name"]
    endpoint_path = endpoint["path"]
    url = build_pronabec_data_url(base_url, endpoint_path)

    recommended_page_size = policy.recommended_page_size
    fallback_page_sizes = policy.fallback_page_sizes
    page_sizes = resolve_pronabec_page_size_candidates(orchestration_config, policy)
    validation_mode = resolve_pronabec_discovery_validation_mode(orchestration_config, policy)
    discovery_config = get_pronabec_discovery_config(orchestration_config)

    session = requests.Session()
    rejected_page_sizes: list[dict[str, Any]] = []

    for page_size in page_sizes:
        start_time = time.time()
        log_event(
            logger,
            "INFO",
            "discovery_page_size_candidate_started",
            dataset=dataset_name,
            candidate_page_size=page_size,
        )
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
            total_records, total_pages = _extract_pronabec_pagination(payload)
            actual_records_returned = len(payload.get("rows", []))

            if total_records > 0 and total_pages <= 0:
                total_pages = math.ceil(total_records / page_size)
            if total_pages > discovery_config.max_validation_pages:
                raise ConfigError(
                    f"total_pages={total_pages} excede max_validation_pages={discovery_config.max_validation_pages}"
                )

            validated_pages = 1 if total_pages > 0 else 0
            if total_pages <= 1:
                _log_discovery_validation_progress(
                    logger,
                    dataset_name=dataset_name,
                    candidate_page_size=page_size,
                    current_page=1 if total_pages == 1 else 0,
                    total_pages=total_pages,
                    validated_pages=validated_pages,
                    start_time=start_time,
                )
            for page in range(2, total_pages + 1):
                try:
                    page_payload = fetch_pronabec_page(
                        session=session,
                        url=url,
                        page=page,
                        rows=page_size,
                        timeout=retry_settings["timeout"],
                        max_retries=retry_settings["max_retries"],
                        backoff_base_seconds=retry_settings["backoff_base_seconds"],
                        backoff_max_seconds=retry_settings["backoff_max_seconds"],
                        dataset_name=dataset_name,
                        logger=logger,
                    )
                    _extract_pronabec_pagination(page_payload)
                    validated_pages += 1
                    if (
                        page % DISCOVERY_PROGRESS_LOG_INTERVAL_PAGES == 0
                        or page == total_pages
                    ):
                        _log_discovery_validation_progress(
                            logger,
                            dataset_name=dataset_name,
                            candidate_page_size=page_size,
                            current_page=page,
                            total_pages=total_pages,
                            validated_pages=validated_pages,
                            start_time=start_time,
                        )
                except Exception as exc:
                    rejected = _build_rejected_page_size(
                        page_size=page_size,
                        failed_page=page,
                        exc=exc,
                    )
                    rejected_page_sizes.append(rejected)
                    log_event(
                        logger,
                        "WARNING",
                        "discovery_page_validation_failed",
                        dataset=dataset_name,
                        candidate_page_size=page_size,
                        page=page,
                        status_code=rejected["status_code"],
                        error_type=rejected["error_type"],
                        error_message=rejected["error_message"],
                    )
                    raise

            elapsed = time.time() - start_time
            log_event(
                logger,
                "INFO",
                "discovery_page_size_accepted",
                dataset=dataset_name,
                effective_page_size=page_size,
                total_pages=total_pages,
                total_records=total_records,
                validated_pages=validated_pages,
            )
            return {
                "source_dataset": dataset_name,
                "extraction_enabled": policy.extraction_enabled,
                "bronze_enabled": policy.bronze_enabled,
                "silver_enabled": policy.silver_enabled,
                "required_for_e2e": policy.required_for_e2e,
                "extraction_mode": policy.extraction_mode,
                "recommended_page_size": recommended_page_size,
                "fallback_page_sizes": fallback_page_sizes,
                "effective_page_size": page_size,
                "total_records": total_records,
                "total_pages": total_pages,
                "actual_records_returned": actual_records_returned,
                "page_size_validation_mode": validation_mode,
                "validation_status": "SUCCESS",
                "validated_pages": validated_pages,
                "rejected_page_sizes": rejected_page_sizes,
                "status": "SUCCESS",
                "elapsed_seconds": round(elapsed, 2),
            }

        except Exception as exc:
            if not rejected_page_sizes or rejected_page_sizes[-1].get("page_size") != page_size:
                rejected_page_sizes.append(
                    _build_rejected_page_size(
                        page_size=page_size,
                        failed_page=1,
                        exc=exc,
                    )
                )
            rejected = rejected_page_sizes[-1]
            log_event(
                logger,
                "WARNING",
                "discovery_page_size_rejected",
                dataset=dataset_name,
                candidate_page_size=page_size,
                failed_page=rejected["failed_page"],
            )

    log_event(
        logger,
        "ERROR",
        "discovery_dataset_failed",
        dataset=dataset_name,
        rejected_page_sizes_count=len(rejected_page_sizes),
    )
    raise DiscoveryPageSizeCalibrationError(dataset_name, rejected_page_sizes)


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
        enabled_datasets = get_bronze_enabled_pronabec_datasets(orchestration_config)
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

        # Resolver fail_on_error desde el raw dict o, por defecto, exigir
        # que todo dataset Bronze habilitado aborte ante un fallo.
        fail_on_error = getattr(policy, "fail_on_error", None)
        if fail_on_error is None:
            fail_on_error = policy.bronze_enabled

        try:
            res = discover_dataset(
                endpoint=endpoint,
                policy=policy,
                orchestration_config=orchestration_config,
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
                "bronze_enabled": policy.bronze_enabled,
                "silver_enabled": policy.silver_enabled,
                "required_for_e2e": policy.required_for_e2e,
                "extraction_mode": policy.extraction_mode,
                "recommended_page_size": policy.recommended_page_size,
                "fallback_page_sizes": policy.fallback_page_sizes,
                "validation_status": "FAILED",
                "page_size_validation_mode": resolve_pronabec_discovery_validation_mode(
                    orchestration_config,
                    policy,
                ),
                "rejected_page_sizes": _extract_rejected_page_sizes(exc),
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

    success_count = sum(1 for result in dataset_results if result.get("status") == "SUCCESS")
    failed_count = sum(1 for result in dataset_results if result.get("status") == "FAILED")
    log_event(
        logger,
        "INFO",
        "discovery_completed",
        total_datasets=len(dataset_results),
        success_count=success_count,
        failed_count=failed_count,
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
