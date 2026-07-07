"""Stage INEI regional context CSV files from Landing to partitioned Bronze."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from pipelines.common.config import get_pipeline_settings
from pipelines.common.gcs import (
    join_gcs_uri,
    list_gcs_objects,
    read_gcs_bytes,
    write_gcs_bytes,
    write_gcs_text,
)

DEFAULT_LANDING_PREFIX = "landing/inei_reports"
DEFAULT_BRONZE_PREFIX = "bronze/inei_reports"
SOURCE_SYSTEM = "INEI"


@dataclass(frozen=True)
class IneiReportDataset:
    name: str
    file_name: str


DEFAULT_DATASETS = (
    IneiReportDataset("inei_population_youth_region", "inei_population_youth_region.csv"),
    IneiReportDataset("inei_demographic_indicators_region", "inei_demographic_indicators_region.csv"),
    IneiReportDataset("inei_pobreza_departamental", "inei_pobreza_departamental_2012_2025.csv"),
    IneiReportDataset("inei_internet_acceso_region", "inei_internet_acceso_region_2012_2025_final.csv"),
)


def validate_date(value: str) -> None:
    try:
        date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Formato de fecha inválido: {value}. Debe ser YYYY-MM-DD.") from exc


def get_env_value(name: str) -> str | None:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return None
    return value.strip()


def load_inei_datasets(config_path: str = "config/pipeline.yaml") -> list[IneiReportDataset]:
    settings = get_pipeline_settings(config_path)
    configured = settings.get("inei_reports", {}).get("datasets") or []
    if not configured:
        return list(DEFAULT_DATASETS)
    return [
        IneiReportDataset(name=item["name"], file_name=item["file_name"])
        for item in configured
    ]


def expected_dataset_names(config_path: str = "config/pipeline.yaml") -> list[str]:
    return [dataset.name for dataset in load_inei_datasets(config_path)]


def select_datasets(
    datasets: list[IneiReportDataset],
    dataset_name: str | None = None,
) -> list[IneiReportDataset]:
    if dataset_name is None:
        return datasets
    matches = [dataset for dataset in datasets if dataset.name == dataset_name]
    if not matches:
        valid = ", ".join(dataset.name for dataset in datasets)
        raise ValueError(f"Dataset INEI desconocido: {dataset_name}. Valores válidos: {valid}")
    return matches


def build_landing_uri(bucket: str, landing_prefix: str, file_name: str) -> str:
    return f"gs://{bucket.strip('/')}/{landing_prefix.strip('/')}/{file_name}"


def build_bronze_dataset_uri(
    bucket: str,
    bronze_prefix: str,
    dataset_name: str,
    extraction_date: str,
) -> str:
    return (
        f"gs://{bucket.strip('/')}/{bronze_prefix.strip('/')}/{dataset_name}/"
        f"extraction_date={extraction_date}"
    )


def build_bronze_data_uri(
    bucket: str,
    bronze_prefix: str,
    dataset_name: str,
    extraction_date: str,
) -> str:
    return f"{build_bronze_dataset_uri(bucket, bronze_prefix, dataset_name, extraction_date)}/data.csv"


def build_manifest(
    *,
    dataset_name: str,
    source_uri: str,
    bronze_uri: str,
    extraction_date: str,
    pipeline_run_id: str,
) -> dict[str, Any]:
    now = datetime.now(UTC).isoformat()
    return {
        "source_system": SOURCE_SYSTEM,
        "source_dataset": dataset_name,
        "dataset": dataset_name,
        "source_uri": source_uri,
        "bronze_uri": bronze_uri,
        "extraction_date": extraction_date,
        "pipeline_run_id": pipeline_run_id,
        "status": "SUCCESS",
        "staged_at": now,
        "ingestion_timestamp": now,
    }


def resolve_runtime_options(
    *,
    bucket: str | None,
    landing_prefix: str | None,
    bronze_prefix: str | None,
    extraction_date: str | None,
) -> tuple[str, str, str, str]:
    resolved_bucket = bucket or get_env_value("GCS_BUCKET_NAME") or get_env_value("GCS_BUCKET")
    resolved_landing = (
        landing_prefix
        or get_env_value("INEI_REPORTS_LANDING_PREFIX")
        or DEFAULT_LANDING_PREFIX
    ).strip("/")
    resolved_bronze = (
        bronze_prefix
        or get_env_value("INEI_REPORTS_BRONZE_PREFIX")
        or DEFAULT_BRONZE_PREFIX
    ).strip("/")
    resolved_date = extraction_date or get_env_value("BRONZE_EXTRACTION_DATE")

    if not resolved_bucket:
        raise ValueError("Se requiere --bucket o GCS_BUCKET_NAME.")
    if not resolved_date:
        raise ValueError("Se requiere --extraction-date o BRONZE_EXTRACTION_DATE.")
    validate_date(resolved_date)
    return resolved_bucket, resolved_landing, resolved_bronze, resolved_date


def stage_inei_reports_gcs(
    *,
    bucket: str,
    landing_prefix: str,
    bronze_prefix: str,
    extraction_date: str,
    dataset_name: str | None = None,
    strict: bool = False,
    overwrite: bool = False,
    pipeline_run_id: str | None = None,
    config_path: str = "config/pipeline.yaml",
) -> tuple[int, int, int]:
    validate_date(extraction_date)
    run_id = pipeline_run_id or get_env_value("PIPELINE_RUN_ID") or f"inei_stage_{int(datetime.now(UTC).timestamp())}"
    datasets = select_datasets(load_inei_datasets(config_path), dataset_name)
    staged = 0
    skipped = 0
    missing = 0

    for dataset in datasets:
        source_uri = build_landing_uri(bucket, landing_prefix, dataset.file_name)
        target_dataset_uri = build_bronze_dataset_uri(
            bucket,
            bronze_prefix,
            dataset.name,
            extraction_date,
        )
        target_data_uri = join_gcs_uri(target_dataset_uri, "data.csv")
        target_success_uri = join_gcs_uri(target_dataset_uri, "_SUCCESS")
        target_manifest_uri = join_gcs_uri(target_dataset_uri, "manifest.json")

        try:
            csv_bytes = read_gcs_bytes(source_uri)
        except Exception as exc:
            if strict or dataset_name:
                raise FileNotFoundError(
                    f"No se encontró archivo INEI esperado para {dataset.name}: {source_uri}"
                ) from exc
            print(f"Skipped {dataset.name}: archivo fuente no encontrado en {source_uri}")
            missing += 1
            continue

        if not overwrite:
            existing = set(list_gcs_objects(target_dataset_uri))
            existing_outputs = {target_data_uri, target_success_uri, target_manifest_uri}
            if existing_outputs & existing:
                if strict or dataset_name:
                    raise FileExistsError(
                        f"Destino Bronze ya existe y no se especificó --overwrite: {target_dataset_uri}"
                    )
                print(f"Skipped {dataset.name}: destino existente sin --overwrite.")
                skipped += 1
                continue

        manifest = build_manifest(
            dataset_name=dataset.name,
            source_uri=source_uri,
            bronze_uri=target_data_uri,
            extraction_date=extraction_date,
            pipeline_run_id=run_id,
        )
        manifest_json = json.dumps(manifest, indent=2, ensure_ascii=False)

        write_gcs_bytes(target_data_uri, csv_bytes, content_type="text/csv")
        write_gcs_text(target_success_uri, manifest_json)
        write_gcs_text(target_manifest_uri, manifest_json)

        print(f"Staged {dataset.name}")
        print(f"  source: {source_uri}")
        print(f"  target: {target_data_uri}")
        staged += 1

    return staged, skipped, missing


def stage_inei_reports_local(
    *,
    input_dir: str,
    output_dir: str,
    extraction_date: str,
    dataset_name: str | None = None,
    strict: bool = False,
    overwrite: bool = False,
    pipeline_run_id: str | None = None,
    config_path: str = "config/pipeline.yaml",
) -> tuple[int, int, int]:
    validate_date(extraction_date)
    run_id = pipeline_run_id or get_env_value("PIPELINE_RUN_ID") or f"inei_stage_{int(datetime.now(UTC).timestamp())}"
    datasets = select_datasets(load_inei_datasets(config_path), dataset_name)
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    if not input_path.exists():
        raise FileNotFoundError(f"El directorio de entrada no existe: {input_dir}")

    staged = 0
    skipped = 0
    missing = 0
    for dataset in datasets:
        source_file = input_path / dataset.file_name
        target_dir = output_path / dataset.name / f"extraction_date={extraction_date}"
        target_data = target_dir / "data.csv"
        target_success = target_dir / "_SUCCESS"
        target_manifest = target_dir / "manifest.json"

        if not source_file.exists():
            if strict or dataset_name:
                raise FileNotFoundError(
                    f"No se encontró archivo INEI esperado para {dataset.name}: {source_file}"
                )
            print(f"Skipped {dataset.name}: archivo fuente no encontrado en {source_file}")
            missing += 1
            continue
        if target_data.exists() and not overwrite:
            if strict or dataset_name:
                raise FileExistsError(
                    f"Destino Bronze ya existe y no se especificó --overwrite: {target_data}"
                )
            print(f"Skipped {dataset.name}: destino existente sin --overwrite.")
            skipped += 1
            continue

        target_dir.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source_file, target_data)
        manifest = build_manifest(
            dataset_name=dataset.name,
            source_uri=str(source_file.resolve()),
            bronze_uri=str(target_data.resolve()),
            extraction_date=extraction_date,
            pipeline_run_id=run_id,
        )
        manifest_json = json.dumps(manifest, indent=2, ensure_ascii=False)
        target_success.write_text(manifest_json, encoding="utf-8")
        target_manifest.write_text(manifest_json, encoding="utf-8")
        print(f"Staged {dataset.name}")
        print(f"  source: {source_file}")
        print(f"  target: {target_data}")
        staged += 1

    return staged, skipped, missing


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Stage INEI regional context CSV files from GCS Landing to Bronze."
    )
    parser.add_argument("--bucket", default=None)
    parser.add_argument("--landing-prefix", default=None)
    parser.add_argument("--bronze-prefix", default=None)
    parser.add_argument("--extraction-date", default=None)
    parser.add_argument("--dataset", default=None)
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--input-dir", default=None, help="Modo local para pruebas.")
    parser.add_argument("--output-dir", default=None, help="Modo local para pruebas.")
    parser.add_argument("--config-path", default="config/pipeline.yaml")
    args = parser.parse_args()

    try:
        if args.input_dir or args.output_dir:
            if not args.input_dir or not args.output_dir:
                raise ValueError("Modo local requiere --input-dir y --output-dir.")
            _, _, _, extraction_date = resolve_runtime_options(
                bucket=args.bucket or "local",
                landing_prefix=args.landing_prefix,
                bronze_prefix=args.bronze_prefix,
                extraction_date=args.extraction_date,
            )
            staged, skipped, missing = stage_inei_reports_local(
                input_dir=args.input_dir,
                output_dir=args.output_dir,
                extraction_date=extraction_date,
                dataset_name=args.dataset,
                strict=args.strict,
                overwrite=args.overwrite,
                config_path=args.config_path,
            )
        else:
            bucket, landing_prefix, bronze_prefix, extraction_date = resolve_runtime_options(
                bucket=args.bucket,
                landing_prefix=args.landing_prefix,
                bronze_prefix=args.bronze_prefix,
                extraction_date=args.extraction_date,
            )
            staged, skipped, missing = stage_inei_reports_gcs(
                bucket=bucket,
                landing_prefix=landing_prefix,
                bronze_prefix=bronze_prefix,
                extraction_date=extraction_date,
                dataset_name=args.dataset,
                strict=args.strict,
                overwrite=args.overwrite,
                config_path=args.config_path,
            )
        print("\nStaging INEI finalizado:")
        print(f"Reports staged: {staged}")
        print(f"Reports skipped: {skipped}")
        print(f"Reports missing: {missing}")
    except Exception as exc:
        print(f"Error ejecutando staging INEI: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
