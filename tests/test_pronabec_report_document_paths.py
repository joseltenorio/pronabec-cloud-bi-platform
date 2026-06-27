from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_CONFIG_PATH = REPO_ROOT / "config" / "pipeline.yaml"
INITIAL_BROZE_VALIDATION_PATH = REPO_ROOT / "docs" / "cloud" / "initial_gcp_bronze_validation.md"


def test_pipeline_config_points_pronabec_documents_to_landing() -> None:
    content = PIPELINE_CONFIG_PATH.read_text(encoding="utf-8")

    assert "bronze/pronabec_reports/_documents" not in content
    assert "landing/pronabec_reports/{source_subset}/_documents/{document_file}" in content


def test_initial_bronze_validation_doc_does_not_claim_pdf_documents_are_in_bronze() -> None:
    content = INITIAL_BROZE_VALIDATION_PATH.read_text(encoding="utf-8")

    assert "bronze/pronabec_reports/_documents" not in content
    assert "landing/pronabec_reports/<source_subset>/_documents/" in content
    assert "Archivos fuente en PDF" not in content
