from __future__ import annotations

from pipelines.extract_pronabec import (
    build_bronze_dataset_base_path,
    build_success_manifest,
)


def test_build_bronze_dataset_base_path_from_data_jsonl():
    base_path = build_bronze_dataset_base_path(
        "bronze/pronabec/perdida_becas/extraction_date=2026-06-28/data.jsonl"
    )

    assert base_path == "bronze/pronabec/perdida_becas/extraction_date=2026-06-28"


def test_build_success_manifest_contains_control_fields():
    manifest = build_success_manifest(
        dataset_name="perdida_becas",
        extraction_date="2026-06-28",
        run_id="manual_test",
        raw_uri="gs://bucket/bronze/pronabec/perdida_becas/extraction_date=2026-06-28/data_raw.json",
        normalized_uri="gs://bucket/bronze/pronabec/perdida_becas/extraction_date=2026-06-28/data.jsonl",
        records_written=10,
        raw_payload={
            "pages_read": 2,
            "requested_page_size": 1000,
            "effective_page_size": 500,
            "page_start": 1,
            "page_end": 2,
            "reported_records": 10,
            "total_pages": 2,
        },
    )

    assert manifest["source_system"] == "pronabec"
    assert manifest["source_dataset"] == "perdida_becas"
    assert manifest["extraction_date"] == "2026-06-28"
    assert manifest["pipeline_run_id"] == "manual_test"
    assert manifest["status"] == "SUCCESS"
    assert manifest["records_written"] == 10
    assert manifest["pages_read"] == 2
    assert manifest["requested_page_size"] == 1000
    assert manifest["effective_page_size"] == 500
