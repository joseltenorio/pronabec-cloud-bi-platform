from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import date
from typing import Any

from pipelines.common.config import ConfigError, get_pipeline_settings, load_yaml_config
from pipelines.common.gcs import build_gs_uri, parse_gs_uri, read_gcs_bytes
from pipelines.common.logging import log_event, setup_structured_logger
from pipelines.common.orchestration_config import (
    get_bronze_enabled_pronabec_datasets,
    load_orchestration_config,
)


DEFAULT_ENDPOINTS_CONFIG = "config/endpoints.yaml"
DEFAULT_ORCHESTRATION_CONFIG = "config/orchestration.yaml"
DEFAULT_PIPELINE_CONFIG = "config/pipeline.yaml"


@dataclass(frozen=True)
class BronzeManifestCheck:
    source_system: str
    source_dataset: str
    manifest_uri: str
    success_uri: str


class BronzeManifestValidationError(Exception):
    """Raised when one or more Bronze manifests are missing or invalid."""


def resolve_extraction_date(cli_value: str | None) -> str:
    """Resolve the logical extraction date from CLI first, then environment."""
    value = cli_value or os.getenv("BRONZE_EXTRACTION_DATE")

    if not value:
        raise BronzeManifestValidationError(
            "No extraction date provided. Use --extraction-date or BRONZE_EXTRACTION_DATE."
        )

    try:
        date.fromisoformat(value)
    except ValueError as exc:
        raise BronzeManifestValidationError(
            f"Invalid extraction date, expected YYYY-MM-DD: {value}"
        ) from exc

    return value


def build_pronabec_manifest_check(
    bucket_name: str,
    bronze_normalized_template: str,
    dataset_name: str,
    extraction_date: str,
) -> BronzeManifestCheck:
    normalized_path = bronze_normalized_template.format(
        dataset=dataset_name,
        extraction_date=extraction_date,
    )
    base_path = normalized_path.rsplit("/", 1)[0]

    return BronzeManifestCheck(
        source_system="pronabec",
        source_dataset=dataset_name,
        manifest_uri=build_gs_uri(bucket_name, f"{base_path}/manifest.json"),
        success_uri=build_gs_uri(bucket_name, f"{base_path}/_SUCCESS"),
    )


def read_json_from_gcs(uri: str) -> dict[str, Any]:
    content = read_gcs_bytes(uri)
    payload = json.loads(content.decode("utf-8"))

    if not isinstance(payload, dict):
        raise BronzeManifestValidationError(f"Manifest no es JSON object: {uri}")

    return payload


def validate_manifest_payload(
    check: BronzeManifestCheck,
    payload: dict[str, Any],
    extraction_date: str,
) -> None:
    expected = {
        "source_system": check.source_system,
        "source_dataset": check.source_dataset,
        "extraction_date": extraction_date,
        "status": "SUCCESS",
    }

    errors: list[str] = []

    for key, expected_value in expected.items():
        actual_value = payload.get(key)
        if actual_value != expected_value:
            errors.append(
                f"{key}: esperado={expected_value}, actual={actual_value}"
            )

    if errors:
        raise BronzeManifestValidationError(
            f"Manifest inválido para {check.source_system}/{check.source_dataset}: "
            + "; ".join(errors)
        )


def validate_bronze_manifests(
    checks: list[BronzeManifestCheck],
    extraction_date: str,
    logger,
) -> None:
    failures: list[str] = []

    for check in checks:
        try:
            manifest_payload = read_json_from_gcs(check.manifest_uri)
            success_payload = read_json_from_gcs(check.success_uri)

            validate_manifest_payload(
                check=check,
                payload=manifest_payload,
                extraction_date=extraction_date,
            )
            validate_manifest_payload(
                check=check,
                payload=success_payload,
                extraction_date=extraction_date,
            )

            log_event(
                logger,
                "INFO",
                "Bronze manifest validado",
                source_system=check.source_system,
                source_dataset=check.source_dataset,
                manifest_uri=check.manifest_uri,
                success_uri=check.success_uri,
            )

        except Exception as exc:
            failures.append(
                f"{check.source_system}/{check.source_dataset}: {exc}"
            )
            log_event(
                logger,
                "ERROR",
                "Bronze manifest inválido o ausente",
                source_system=check.source_system,
                source_dataset=check.source_dataset,
                manifest_uri=check.manifest_uri,
                success_uri=check.success_uri,
                error_message=str(exc),
            )

    if failures:
        raise BronzeManifestValidationError(
            "Falló validación de Bronze manifests:\n" + "\n".join(failures)
        )


def resolve_pronabec_checks(
    endpoints_config: dict[str, Any],
    bucket_name: str,
    bronze_normalized_template: str,
    extraction_date: str,
    orchestration_config: dict[str, Any] | None = None,
) -> list[BronzeManifestCheck]:
    checks: list[BronzeManifestCheck] = []
    if orchestration_config is None:
        selected_dataset_names = {
            endpoint["name"]
            for endpoint in endpoints_config["pronabec"]["endpoints"]
            if endpoint.get("enabled", True)
        }
    else:
        selected_dataset_names = set(get_bronze_enabled_pronabec_datasets(orchestration_config))

    for endpoint in endpoints_config["pronabec"]["endpoints"]:
        if not endpoint.get("enabled", True):
            continue
        if endpoint["name"] not in selected_dataset_names:
            continue

        checks.append(
            build_pronabec_manifest_check(
                bucket_name=bucket_name,
                bronze_normalized_template=bronze_normalized_template,
                dataset_name=endpoint["name"],
                extraction_date=extraction_date,
            )
        )

    return checks


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Valida manifests Bronze antes de ejecutar Silver."
    )
    parser.add_argument(
        "--pipeline-config",
        default=DEFAULT_PIPELINE_CONFIG,
    )
    parser.add_argument(
        "--endpoints-config",
        default=DEFAULT_ENDPOINTS_CONFIG,
    )
    parser.add_argument(
        "--orchestration-config",
        default=DEFAULT_ORCHESTRATION_CONFIG,
    )
    parser.add_argument(
        "--bucket",
        help="Bucket GCS. Si se omite, usa GCS_BUCKET_NAME.",
    )
    parser.add_argument(
        "--extraction-date",
        help="Fecha lógica de extracción YYYY-MM-DD.",
    )
    parser.add_argument(
        "--source-system",
        choices=["pronabec"],
        default="pronabec",
        help="Sistema fuente a validar.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    extraction_date = resolve_extraction_date(args.extraction_date)

    pipeline_settings = get_pipeline_settings(args.pipeline_config)
    endpoints_config = load_yaml_config(args.endpoints_config)
    orchestration_config = load_orchestration_config(args.orchestration_config)

    logger = setup_structured_logger(
        name="validate_bronze_manifests",
        level=pipeline_settings["log_level"],
        structured=True,
    )

    bucket_name = args.bucket or pipeline_settings["bucket_name"]

    if not bucket_name:
        raise ConfigError(
            "No se definió bucket. Configura GCS_BUCKET_NAME o usa --bucket."
        )

    gcs_paths = pipeline_settings["gcs_paths"]

    if args.source_system == "pronabec":
        checks = resolve_pronabec_checks(
            endpoints_config=endpoints_config,
            bucket_name=bucket_name,
            bronze_normalized_template=gcs_paths["pronabec_bronze_normalized"],
            extraction_date=extraction_date,
            orchestration_config=orchestration_config,
        )
    else:
        checks = []

    validate_bronze_manifests(
        checks=checks,
        extraction_date=extraction_date,
        logger=logger,
    )

    log_event(
        logger,
        "INFO",
        "Validación Bronze manifests completada",
        source_system=args.source_system,
        extraction_date=extraction_date,
        checks=len(checks),
    )


if __name__ == "__main__":
    try:
        main()
    except BronzeManifestValidationError as exc:
        raise SystemExit(str(exc)) from None
