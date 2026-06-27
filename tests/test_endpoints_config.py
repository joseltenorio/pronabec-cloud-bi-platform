from __future__ import annotations

from pathlib import Path

from pipelines.common.config import load_yaml_config


REPO_ROOT = Path(__file__).resolve().parents[1]
ENDPOINTS_PATH = REPO_ROOT / "config" / "endpoints.yaml"


def test_pronabec_report_documents_stay_in_landing() -> None:
    config = load_yaml_config(ENDPOINTS_PATH)
    documents = config["pronabec_reports"]["documents"]

    assert documents
    landing_subsets = set()

    for document in documents:
        landing_subset = document["landing_subset"]
        landing_subsets.add(landing_subset)

        assert document["storage_prefix"] == "landing/pronabec_reports"

        if "document_storage_path" in document:
            paths = [document["document_storage_path"]]
        else:
            paths = [
                item["document_storage_path"]
                for item in document["documents"]
            ]

        for path in paths:
            assert not path.startswith("bronze/")
            assert path.startswith("landing/pronabec_reports/")
            assert "/_documents/" in path
            assert path.endswith(".pdf")

    assert landing_subsets == {
        "pes_2025",
        "beca18_universitarios_2012_2026",
    }


def test_pronabec_report_datasets_remain_tabular() -> None:
    config = load_yaml_config(ENDPOINTS_PATH)
    documents = config["pronabec_reports"]["documents"]

    datasets = []
    for document in documents:
        datasets.extend(document.get("datasets", []))

    assert len(datasets) == 23
    for dataset in datasets:
        assert dataset["name"].startswith("report_beca18_")
        assert dataset["file_name"].endswith(".csv")
