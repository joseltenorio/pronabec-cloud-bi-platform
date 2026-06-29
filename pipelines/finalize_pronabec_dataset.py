# -*- coding: utf-8 -*-
"""Módulo para finalizar la extracción de un dataset PRONABEC.

Consolida los chunks intermedios desde bronze_work hacia Bronze final.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pipelines.common.config import ConfigError, get_pipeline_settings, load_yaml_config
from pipelines.common.gcs import (
    read_gcs_bytes,
    upload_json,
    upload_text,
)
from pipelines.common.logging import log_event, setup_structured_logger
from pipelines.common.orchestration_config import (
    get_pronabec_dataset_policies,
    load_orchestration_config,
)
from pipelines.extract_pronabec import (
    build_pronabec_chunk_base_path,
    resolve_extraction_date,
    resolve_pipeline_run_id,
    resolve_source_dataset,
)


def read_plan_json(
    dry_run: bool,
    output_dir: str,
    bucket_name: str,
    extraction_date: str,
    run_id: str,
) -> dict[str, Any]:
    """Lee el archivo plan.json de la ubicación correspondiente."""
    if dry_run:
        path = (
            Path(output_dir)
            / "bronze_work"
            / "pronabec"
            / "_plans"
            / f"extraction_date={extraction_date}"
            / f"run_id={run_id}"
            / "plan.json"
        )
        if not path.exists():
            raise FileNotFoundError(f"No se encontró plan.json local en: {path}")
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    else:
        object_path = (
            f"bronze_work/pronabec/_plans/extraction_date={extraction_date}"
            f"/run_id={run_id}/plan.json"
        )
        uri = f"gs://{bucket_name}/{object_path}"
        try:
            data_bytes = read_gcs_bytes(uri)
            return json.loads(data_bytes.decode("utf-8"))
        except Exception as exc:
            raise FileNotFoundError(f"No se pudo leer plan.json de GCS en {uri}: {exc}")


def check_covers_full_dataset(chunks: list[dict[str, Any]], total_pages: int) -> bool:
    """Verifica si la lista de chunks cubre la extracción completa del dataset."""
    if not chunks:
        return False
    if total_pages == 0:
        return len(chunks) == 1 and chunks[0]["page_start"] == 1 and chunks[0]["page_end"] == 0

    sorted_chunks = sorted(chunks, key=lambda c: c["page_start"])
    if sorted_chunks[0]["page_start"] != 1:
        return False
    for i in range(len(sorted_chunks) - 1):
        if sorted_chunks[i + 1]["page_start"] != sorted_chunks[i]["page_end"] + 1:
            return False
    return sorted_chunks[-1]["page_end"] == total_pages


def run_finalize(args: argparse.Namespace) -> None:
    """Consolida y finaliza la extracción de un dataset específico."""
    pipeline_settings = get_pipeline_settings(args.pipeline_config)
    orchestration_config = load_orchestration_config(args.orchestration_config)

    logger = setup_structured_logger(
        name="finalize_pronabec_dataset",
        level=pipeline_settings["log_level"],
        structured=True,
    )

    source_dataset = resolve_source_dataset(
        cli_value=args.source_dataset,
        legacy_value=None,
    )

    if not source_dataset:
        raise ConfigError("El parámetro --source-dataset es obligatorio para el finalizer.")

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
    run_id = pipeline_run_id or generate_run_id("pronabec_finalize")

    log_event(
        logger,
        "INFO",
        "Finalizando extracción del dataset PRONABEC",
        source_dataset=source_dataset,
        extraction_date=extraction_date,
        run_id=run_id,
        dry_run=args.dry_run,
    )

    # 1. Leer plan.json
    plan_dict = read_plan_json(
        dry_run=args.dry_run,
        output_dir=args.output_dir,
        bucket_name=bucket_name,
        extraction_date=extraction_date,
        run_id=run_id,
    )

    # 2. Validar que el dataset exista en el plan
    dataset_plan = None
    for ds in plan_dict.get("datasets", []):
        if ds["source_dataset"] == source_dataset:
            dataset_plan = ds
            break

    if not dataset_plan:
        raise ConfigError(f"El dataset '{source_dataset}' no se encuentra en plan.json.")

    total_records_observed = dataset_plan["total_records"]
    total_pages_observed = dataset_plan["total_pages"]
    expected_chunks = dataset_plan["expected_chunks"]

    # 3. Filtrar chunks esperados para el dataset en plan.json
    plan_chunks = [
        c for c in plan_dict.get("chunks", [])
        if c["source_dataset"] == source_dataset
    ]

    if len(plan_chunks) != expected_chunks:
        raise ConfigError(
            f"La cantidad de chunks en plan.json ({len(plan_chunks)}) difiere de expected_chunks ({expected_chunks})"
        )

    # 4. Validar chunks existentes en bronze_work y recopilar manifests
    chunk_manifests: list[dict[str, Any]] = []
    chunk_contents: list[str] = []
    effective_page_sizes: set[int] = set()

    # Validar duplicados de chunks en el plan
    seen_ranges: set[tuple[int, int]] = set()
    for chunk in plan_chunks:
        r = (chunk["page_start"], chunk["page_end"])
        if r in seen_ranges:
            raise ConfigError(f"Rango de páginas duplicado en el plan: {r}")
        seen_ranges.add(r)

    # Ordenar chunks del plan por page_start para garantizar orden de consolidación
    sorted_plan_chunks = sorted(plan_chunks, key=lambda c: c["page_start"])

    # Validar gaps
    for idx in range(len(sorted_plan_chunks) - 1):
        if sorted_plan_chunks[idx + 1]["page_start"] != sorted_plan_chunks[idx]["page_end"] + 1:
            raise ConfigError(
                f"Detectado gap de páginas entre chunks: "
                f"fin={sorted_plan_chunks[idx]['page_end']}, inicio={sorted_plan_chunks[idx + 1]['page_start']}"
            )

    for chunk in sorted_plan_chunks:
        page_start = chunk["page_start"]
        page_end = chunk["page_end"]

        # Construir rutas del chunk intermedio
        chunk_base = build_pronabec_chunk_base_path(
            dataset_name=source_dataset,
            extraction_date=extraction_date,
            pipeline_run_id=run_id,
            page_start=page_start,
            page_end=page_end,
        )

        manifest_rel_path = f"{chunk_base}/chunk_manifest.json"
        data_rel_path = f"{chunk_base}/data.jsonl"

        # Leer manifest del chunk
        if args.dry_run:
            manifest_p = Path(args.output_dir) / manifest_rel_path
            data_p = Path(args.output_dir) / data_rel_path

            if not manifest_p.exists():
                raise FileNotFoundError(f"Falta manifest de chunk en: {manifest_p}")
            with open(manifest_p, "r", encoding="utf-8") as f:
                c_manifest = json.load(f)

            # Para dataset vacío total_pages=0, data.jsonl podría no existir o estar vacío
            if total_pages_observed > 0:
                if not data_p.exists():
                    raise FileNotFoundError(f"Falta archivo de datos de chunk en: {data_p}")
                with open(data_p, "r", encoding="utf-8") as f:
                    c_content = f.read()
            else:
                c_content = ""
        else:
            manifest_uri = f"gs://{bucket_name}/{manifest_rel_path}"
            data_uri = f"gs://{bucket_name}/{data_rel_path}"

            try:
                m_bytes = read_gcs_bytes(manifest_uri)
                c_manifest = json.loads(m_bytes.decode("utf-8"))
            except Exception as exc:
                raise FileNotFoundError(f"Falta o no se puede leer manifest de GCS en {manifest_uri}: {exc}")

            if total_pages_observed > 0:
                try:
                    d_bytes = read_gcs_bytes(data_uri)
                    c_content = d_bytes.decode("utf-8")
                except Exception as exc:
                    raise FileNotFoundError(f"Falta o no se puede leer datos de GCS en {data_uri}: {exc}")
            else:
                c_content = ""

        # Validaciones de consistencia del manifest del chunk
        if c_manifest.get("status") != "SUCCESS":
            raise ConfigError(f"El chunk {chunk['chunk_id']} falló durante la extracción.")

        if c_manifest.get("extraction_date") != extraction_date:
            raise ConfigError(
                f"La fecha de extracción del chunk ({c_manifest.get('extraction_date')}) "
                f"no coincide con la esperada ({extraction_date})"
            )

        if c_manifest.get("pipeline_run_id") != run_id:
            raise ConfigError(
                f"El pipeline_run_id del chunk ({c_manifest.get('pipeline_run_id')}) "
                f"no coincide con el esperado ({run_id})"
            )

        effective_page_sizes.add(c_manifest["effective_page_size"])
        chunk_manifests.append(c_manifest)
        chunk_contents.append(c_content)

    # 5. Validar consistencia de effective_page_size entre todos los chunks
    if len(effective_page_sizes) > 1:
        raise ConfigError(
            f"Diferentes effective_page_size detectados para el mismo dataset: {effective_page_sizes}"
        )

    effective_page_size = list(effective_page_sizes)[0]

    # 6. Sumar registros escritos de los manifests
    records_written = sum(m["records_written"] for m in chunk_manifests)

    # 7. Validar contra el total de registros observados si el plan cubre el dataset completo
    covers_full_dataset = check_covers_full_dataset(plan_chunks, total_pages_observed)

    if covers_full_dataset:
        policies = {
            p.source_dataset: p
            for p in get_pronabec_dataset_policies(orchestration_config)
        }
        policy = policies.get(source_dataset)
        allow_mismatch = False
        if policy:
            allow_mismatch = getattr(policy, "allow_record_count_mismatch", False)

        if records_written != total_records_observed and not allow_mismatch:
            raise ConfigError(
                f"Discrepancia en cantidad de registros: records_written={records_written} "
                f"difiere de total_records_observed={total_records_observed} en {source_dataset}."
            )

    # 8. Concatenar y consolidar data.jsonl
    final_content = "".join(chunk_contents)
    # Asegurar salto de línea final si hay contenido y no termina en salto
    if final_content and not final_content.endswith("\n"):
        final_content += "\n"

    # Determinar rutas finales
    final_dir_rel = f"bronze/pronabec/{source_dataset}/extraction_date={extraction_date}"
    final_data_rel = f"{final_dir_rel}/data.jsonl"
    final_manifest_rel = f"{final_dir_rel}/manifest.json"
    final_success_rel = f"{final_dir_rel}/_SUCCESS"

    # 9. Generar timestamps de manifest analizando chunks
    started_at_str = min(m["started_at"] for m in chunk_manifests)
    finished_at_str = max(m["finished_at"] for m in chunk_manifests)

    # Crear lista de sumario de manifests
    chunk_summaries = [
        {
            "chunk_id": chunk["chunk_id"],
            "page_start": chunk["page_start"],
            "page_end": chunk["page_end"],
            "records_written": m["records_written"],
        }
        for chunk, m in zip(sorted_plan_chunks, chunk_manifests)
    ]

    final_manifest = {
        "source_system": "pronabec",
        "source_dataset": source_dataset,
        "extraction_date": extraction_date,
        "pipeline_run_id": run_id,
        "source_snapshot_observed_at": plan_dict["source_snapshot_observed_at"],
        "status": "SUCCESS",
        "expected_chunks": expected_chunks,
        "completed_chunks": len(chunk_manifests),
        "effective_page_size": effective_page_size,
        "total_records_observed": total_records_observed,
        "total_pages_observed": total_pages_observed,
        "records_written": records_written,
        "started_at": started_at_str,
        "finished_at": finished_at_str,
        "chunk_manifests": chunk_summaries,
    }

    final_success_marker = {
        "source_system": "pronabec",
        "source_dataset": source_dataset,
        "extraction_date": extraction_date,
        "pipeline_run_id": run_id,
        "status": "SUCCESS",
        "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }

    # Escribir outputs finales
    if args.dry_run:
        final_dir = Path(args.output_dir) / final_dir_rel
        final_dir.mkdir(parents=True, exist_ok=True)

        final_data_p = final_dir / "data.jsonl"
        final_manifest_p = final_dir / "manifest.json"
        final_success_p = final_dir / "_SUCCESS"

        with open(final_data_p, "w", encoding="utf-8") as f:
            f.write(final_content)

        with open(final_manifest_p, "w", encoding="utf-8") as f:
            json.dump(final_manifest, f, ensure_ascii=False, indent=2)

        with open(final_success_p, "w", encoding="utf-8") as f:
            json.dump(final_success_marker, f, ensure_ascii=False, indent=2)

        log_event(
            logger,
            "INFO",
            "Dataset finalizado y consolidado localmente (dry-run)",
            data_path=str(final_data_p),
            manifest_path=str(final_manifest_p),
            success_path=str(final_success_p),
        )
    else:
        # GCS writing
        upload_text(
            bucket_name=bucket_name,
            object_path=final_data_rel,
            content=final_content,
            content_type="application/x-ndjson",
        )
        upload_json(
            bucket_name=bucket_name,
            object_path=final_manifest_rel,
            payload=final_manifest,
        )
        upload_json(
            bucket_name=bucket_name,
            object_path=final_success_rel,
            payload=final_success_marker,
        )
        log_event(
            logger,
            "INFO",
            "Dataset finalizado y consolidado en GCS",
            bucket=bucket_name,
            data_path=final_data_rel,
            manifest_path=final_manifest_rel,
            success_path=final_success_rel,
        )


def parse_args() -> argparse.Namespace:
    """Parsea argumentos CLI."""
    parser = argparse.ArgumentParser(
        description="Finaliza la extracción de un dataset consolidando los chunks."
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
        required=True,
        help="Dataset específico a finalizar (obligatorio).",
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
        run_finalize(args)
    except Exception as exc:
        sys.exit(f"Error fatal durante finalización del dataset: {exc}")


if __name__ == "__main__":
    main()
