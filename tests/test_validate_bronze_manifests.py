from __future__ import annotations

import pytest

from pipelines.validate_bronze_manifests import (
    BronzeManifestCheck,
    BronzeManifestValidationError,
    build_pronabec_manifest_check,
    resolve_extraction_date,
    resolve_pronabec_checks,
    validate_manifest_payload,
)


def test_build_pronabec_manifest_check():
    check = build_pronabec_manifest_check(
        bucket_name="lake-bucket",
        bronze_normalized_template=(
            "bronze/pronabec/{dataset}/extraction_date={extraction_date}/data.jsonl"
        ),
        dataset_name="perdida_becas",
        extraction_date="2026-06-28",
    )

    assert check.source_system == "pronabec"
    assert check.source_dataset == "perdida_becas"
    assert check.manifest_uri == (
        "gs://lake-bucket/bronze/pronabec/perdida_becas/"
        "extraction_date=2026-06-28/manifest.json"
    )
    assert check.success_uri == (
        "gs://lake-bucket/bronze/pronabec/perdida_becas/"
        "extraction_date=2026-06-28/_SUCCESS"
    )


def test_validate_manifest_payload_accepts_success():
    check = BronzeManifestCheck(
        source_system="pronabec",
        source_dataset="perdida_becas",
        manifest_uri="gs://bucket/path/manifest.json",
        success_uri="gs://bucket/path/_SUCCESS",
    )

    validate_manifest_payload(
        check=check,
        payload={
            "source_system": "pronabec",
            "source_dataset": "perdida_becas",
            "extraction_date": "2026-06-28",
            "status": "SUCCESS",
        },
        extraction_date="2026-06-28",
    )


def test_validate_manifest_payload_rejects_wrong_date():
    check = BronzeManifestCheck(
        source_system="pronabec",
        source_dataset="perdida_becas",
        manifest_uri="gs://bucket/path/manifest.json",
        success_uri="gs://bucket/path/_SUCCESS",
    )

    with pytest.raises(BronzeManifestValidationError):
        validate_manifest_payload(
            check=check,
            payload={
                "source_system": "pronabec",
                "source_dataset": "perdida_becas",
                "extraction_date": "2026-06-29",
                "status": "SUCCESS",
            },
            extraction_date="2026-06-28",
        )


def test_resolve_extraction_date_prefers_cli_value(monkeypatch):
    monkeypatch.setenv("BRONZE_EXTRACTION_DATE", "2026-06-29")

    assert resolve_extraction_date("2026-06-28") == "2026-06-28"


def test_resolve_extraction_date_uses_environment(monkeypatch):
    monkeypatch.setenv("BRONZE_EXTRACTION_DATE", "2026-06-28")

    assert resolve_extraction_date(None) == "2026-06-28"


def test_resolve_extraction_date_fails_without_date(monkeypatch):
    monkeypatch.delenv("BRONZE_EXTRACTION_DATE", raising=False)

    with pytest.raises(BronzeManifestValidationError) as excinfo:
        resolve_extraction_date(None)

    assert (
        "No extraction date provided. Use --extraction-date or BRONZE_EXTRACTION_DATE."
        in str(excinfo.value)
    )


def test_resolve_extraction_date_rejects_invalid_date(monkeypatch):
    monkeypatch.delenv("BRONZE_EXTRACTION_DATE", raising=False)

    with pytest.raises(BronzeManifestValidationError) as excinfo:
        resolve_extraction_date("2026/06/28")

    assert "Invalid extraction date, expected YYYY-MM-DD: 2026/06/28" in str(excinfo.value)


def test_resolve_pronabec_checks_respects_e2e_scope():
    endpoints_config = {
        "pronabec": {
            "endpoints": [
                {"name": "convocatorias", "enabled": True},
                {"name": "notas_becarios", "enabled": True},
            ]
        }
    }
    orchestration_config = {
        "datasets": {
            "pronabec_api": {
                "extraction_policies": [
                    {
                        "source_dataset": "convocatorias",
                        "extraction_enabled": True,
                        "bronze_enabled": True,
                        "silver_enabled": True,
                        "required_for_e2e": True,
                        "extraction_mode": "single",
                        "chunk_size_pages": None,
                        "max_parallel_chunks": 1,
                        "recommended_page_size": 1000,
                        "fallback_page_sizes": [500, 100],
                        "max_page_size_tested_ok": 1000,
                        "page_size_policy": "dataset_safe_default",
                    },
                    {
                        "source_dataset": "notas_becarios",
                        "extraction_enabled": True,
                        "bronze_enabled": True,
                        "silver_enabled": False,
                        "required_for_e2e": False,
                        "extraction_mode": "single",
                        "chunk_size_pages": None,
                        "max_parallel_chunks": 1,
                        "recommended_page_size": 1000,
                        "fallback_page_sizes": [500, 100],
                        "max_page_size_tested_ok": 1000,
                        "page_size_policy": "dataset_safe_default",
                    },
                ]
            }
        }
    }

    checks = resolve_pronabec_checks(
        endpoints_config=endpoints_config,
        orchestration_config=orchestration_config,
        bucket_name="bucket",
        bronze_normalized_template="bronze/pronabec/{dataset}/extraction_date={extraction_date}/data.jsonl",
        extraction_date="2026-06-30",
        scope="e2e",
    )

    assert [check.source_dataset for check in checks] == ["convocatorias"]


def test_resolve_pronabec_checks_respects_bronze_full_scope():
    endpoints_config = {
        "pronabec": {
            "endpoints": [
                {"name": "convocatorias", "enabled": True},
                {"name": "notas_becarios", "enabled": True},
            ]
        }
    }
    orchestration_config = {
        "datasets": {
            "pronabec_api": {
                "extraction_policies": [
                    {
                        "source_dataset": "convocatorias",
                        "extraction_enabled": True,
                        "bronze_enabled": True,
                        "silver_enabled": True,
                        "required_for_e2e": True,
                        "extraction_mode": "single",
                        "chunk_size_pages": None,
                        "max_parallel_chunks": 1,
                        "recommended_page_size": 1000,
                        "fallback_page_sizes": [500, 100],
                        "max_page_size_tested_ok": 1000,
                        "page_size_policy": "dataset_safe_default",
                    },
                    {
                        "source_dataset": "notas_becarios",
                        "extraction_enabled": True,
                        "bronze_enabled": True,
                        "silver_enabled": False,
                        "required_for_e2e": False,
                        "extraction_mode": "single",
                        "chunk_size_pages": None,
                        "max_parallel_chunks": 1,
                        "recommended_page_size": 1000,
                        "fallback_page_sizes": [500, 100],
                        "max_page_size_tested_ok": 1000,
                        "page_size_policy": "dataset_safe_default",
                    },
                ]
            }
        }
    }

    checks = resolve_pronabec_checks(
        endpoints_config=endpoints_config,
        orchestration_config=orchestration_config,
        bucket_name="bucket",
        bronze_normalized_template="bronze/pronabec/{dataset}/extraction_date={extraction_date}/data.jsonl",
        extraction_date="2026-06-30",
        scope="bronze_full",
    )

    assert [check.source_dataset for check in checks] == ["convocatorias", "notas_becarios"]
