# -*- coding: utf-8 -*-
"""Módulo para construir el plan de extracción de PRONABEC API.

Lee discovery.json y genera plan.json con los chunks a extraer.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pipelines.common.audit import generate_run_id
from pipelines.common.config import ConfigError, get_pipeline_settings, load_yaml_config
from pipelines.common.gcs import read_gcs_bytes, upload_json
from pipelines.common.logging import log_event, setup_structured_logger
from pipelines.common.orchestration_config import (
    get_pronabec_dataset_policies,
    load_orchestration_config,
)
from pipelines.extract_pronabec import (
    resolve_extraction_date,
    resolve_pipeline_run_id,
    resolve_source_dataset,
)


def read_discovery_json(
    dry_run: bool,
    output_dir: str,
    bucket_name: str,
    extraction_date: str,
    run_id: str,
) -> dict[str, Any]:
    """Lee el archivo discovery.json de la ubicación correspondiente."""
    if dry_run:
        path = (
            Path(output_dir)
            / "bronze_work"
            / "pronabec"
            / "_plans"
            / f"extraction_date={extraction_date}"
            / f"run_id={run_id}"
            / "discovery.json"
        )
        if not path.exists():
            raise FileNotFoundError(f"No se encontró discovery.json local en: {path}")
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    else:
        object_path = (
            f"bronze_work/pronabec/_plans/extraction_date={extraction_date}"
            f"/run_id={run_id}/discovery.json"
        )
        uri = f"gs://{bucket_name}/{object_path}"
        try:
            data_bytes = read_gcs_bytes(uri)
            return json.loads(data_bytes.decode("utf-8"))
        except Exception as exc:
            raise FileNotFoundError(f"No se pudo leer discovery.json de GCS en {uri}: {exc}")


def build_plan(
    discovery_data: dict[str, Any],
    orchestration_config: dict[str, Any],
    source_dataset_filter: str | None,
) -> dict[str, Any]:
    """Genera el plan de extracción basándose en discovery.json y orchestration policies."""
    policies = {
        p.source_dataset: p
        for p in get_pronabec_dataset_policies(orchestration_config)
    }

    datasets_plans: list[dict[str, Any]] = []
    chunks_plans: list[dict[str, Any]] = []

    # Filtrar datasets del discovery
    for ds_discovered in discovery_data.get("datasets", []):
        dataset_name = ds_discovered["source_dataset"]

        # Si se filtró por source_dataset, saltarse los demás
        if source_dataset_filter and dataset_name != source_dataset_filter:
            continue

        # Si el dataset fue solicitado explícitamente, permitimos planificarlo aunque esté deshabilitado.
        if ds_discovered.get("status") != "SUCCESS":
            continue
        if not ds_discovered.get("extraction_enabled") and not source_dataset_filter:
            continue

        policy = policies.get(dataset_name)
        if not policy:
            continue

        extraction_mode = ds_discovered["extraction_mode"]
        effective_page_size = ds_discovered["effective_page_size"]
        total_records = ds_discovered["total_records"]
        total_pages = ds_discovered["total_pages"]
        required_for_e2e = ds_discovered["required_for_e2e"]

        chunk_size_pages = policy.chunk_size_pages
        max_parallel_chunks = policy.max_parallel_chunks

        chunks: list[dict[str, Any]] = []

        if total_pages == 0:
            # Caso dataset vacío
            chunk_id = f"{dataset_name}_0001"
            chunks.append({
                "chunk_id": chunk_id,
                "source_dataset": dataset_name,
                "page_start": 1,
                "page_end": 0,
                "effective_page_size": effective_page_size,
                "required_for_e2e": required_for_e2e,
                "output_mode": "chunk",
            })
        elif extraction_mode == "single":
            chunk_id = f"{dataset_name}_0001"
            chunks.append({
                "chunk_id": chunk_id,
                "source_dataset": dataset_name,
                "page_start": 1,
                "page_end": total_pages,
                "effective_page_size": effective_page_size,
                "required_for_e2e": required_for_e2e,
                "output_mode": "chunk", # Escribe en bronze_work intermedio
            })
        elif extraction_mode == "chunked":
            # Si chunk_size_pages es None o <= 0, tratamos como single
            if not chunk_size_pages or chunk_size_pages <= 0:
                chunk_size_pages = total_pages

            num_chunks = math.ceil(total_pages / chunk_size_pages)
            for i in range(num_chunks):
                chunk_index = i + 1
                page_start = i * chunk_size_pages + 1
                page_end = min((i + 1) * chunk_size_pages, total_pages)
                chunk_id = f"{dataset_name}_{chunk_index:04d}"

                chunks.append({
                    "chunk_id": chunk_id,
                    "source_dataset": dataset_name,
                    "page_start": page_start,
                    "page_end": page_end,
                    "effective_page_size": effective_page_size,
                    "required_for_e2e": required_for_e2e,
                    "output_mode": "chunk",
                })

        expected_chunks = len(chunks)

        datasets_plans.append({
            "source_dataset": dataset_name,
            "extraction_mode": extraction_mode,
            "effective_page_size": effective_page_size,
            "total_records": total_records,
            "total_pages": total_pages,
            "chunk_size_pages": chunk_size_pages,
            "max_parallel_chunks": max_parallel_chunks,
            "expected_chunks": expected_chunks,
        })

        chunks_plans.extend(chunks)

    return {
        "source_system": "pronabec",
        "extraction_date": discovery_data["extraction_date"],
        "pipeline_run_id": discovery_data["pipeline_run_id"],
        "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "source_snapshot_observed_at": discovery_data["source_snapshot_observed_at"],
        "status": "READY",
        "datasets": datasets_plans,
        "chunks": chunks_plans,
    }


def run_build_plan(args: argparse.Namespace) -> None:
    """Orquesta la construcción del plan de extracción."""
    pipeline_settings = get_pipeline_settings(args.pipeline_config)
    orchestration_config = load_orchestration_config(args.orchestration_config)

    logger = setup_structured_logger(
        name="build_pronabec_extraction_plan",
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
    run_id = pipeline_run_id or generate_run_id("pronabec_build_plan")

    source_dataset = resolve_source_dataset(
        cli_value=args.source_dataset,
        legacy_value=args.dataset,
    )

    log_event(
        logger,
        "INFO",
        "Construyendo plan de extracción PRONABEC",
        source_dataset=source_dataset,
        extraction_date=extraction_date,
        run_id=run_id,
        dry_run=args.dry_run,
    )

    # 1. Leer discovery.json
    discovery_data = read_discovery_json(
        dry_run=args.dry_run,
        output_dir=args.output_dir,
        bucket_name=bucket_name,
        extraction_date=extraction_date,
        run_id=run_id,
    )

    # 2. Generar plan dict
    plan_dict = build_plan(
        discovery_data=discovery_data,
        orchestration_config=orchestration_config,
        source_dataset_filter=source_dataset,
    )

    # 3. Guardar plan.json
    if args.dry_run:
        base_dir = (
            Path(args.output_dir)
            / "bronze_work"
            / "pronabec"
            / "_plans"
            / f"extraction_date={extraction_date}"
            / f"run_id={run_id}"
        )
        base_dir.mkdir(parents=True, exist_ok=True)
        out_path = base_dir / "plan.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(plan_dict, f, ensure_ascii=False, indent=2)
        log_event(
            logger,
            "INFO",
            "Plan de extracción escrito localmente (dry-run)",
            path=str(out_path),
        )
    else:
        object_path = (
            f"bronze_work/pronabec/_plans/extraction_date={extraction_date}"
            f"/run_id={run_id}/plan.json"
        )
        upload_json(
            bucket_name=bucket_name,
            object_path=object_path,
            payload=plan_dict,
        )
        log_event(
            logger,
            "INFO",
            "Plan de extracción escrito en GCS",
            bucket=bucket_name,
            object_path=object_path,
        )


def parse_args() -> argparse.Namespace:
    """Parsea argumentos CLI."""
    parser = argparse.ArgumentParser(
        description="Construye plan.json para la extracción de PRONABEC."
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
        help="Dataset específico a planificar.",
    )
    parser.add_argument(
        "--dataset",
        help="Alias legacy de --source-dataset.",
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

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        run_build_plan(args)
    except Exception as exc:
        sys.exit(f"Error fatal durante construcción del plan: {exc}")


if __name__ == "__main__":
    main()
