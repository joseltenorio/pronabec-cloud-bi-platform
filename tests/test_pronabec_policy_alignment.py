from __future__ import annotations

from pathlib import Path

from pipelines.build_pronabec_extraction_plan import build_plan
from pipelines.common.orchestration_config import load_orchestration_config


REPO_ROOT = Path(__file__).resolve().parents[1]


def _base_discovery(dataset_name: str, extraction_mode: str, effective_page_size: int, total_pages: int, total_records: int = 1000) -> dict:
    return {
        "source_system": "pronabec",
        "extraction_date": "2026-06-29",
        "pipeline_run_id": "test-run",
        "source_snapshot_observed_at": "2026-06-29T20:30:00Z",
        "status": "SUCCESS",
        "datasets": [
            {
                "source_dataset": dataset_name,
                "bronze_enabled": True,
                "extraction_enabled": True,
                "silver_enabled": True,
                "required_for_e2e": False,
                "extraction_mode": extraction_mode,
                "recommended_page_size": effective_page_size,
                "fallback_page_sizes": [100],
                "effective_page_size": effective_page_size,
                "total_records": total_records,
                "total_pages": total_pages,
                "status": "SUCCESS",
            }
        ],
    }


def test_actual_policies_align_with_single_and_chunked_modes() -> None:
    orchestration = load_orchestration_config(REPO_ROOT / "config" / "orchestration.yaml")
    policies = {
        item["source_dataset"]: item
        for item in orchestration["datasets"]["pronabec_api"].get("extraction_policies", [])
    }

    assert policies["becarios_pais_estudio"]["extraction_mode"] == "single"
    assert policies["colegios_habiles"]["extraction_mode"] == "single"
    assert policies["convocatorias_carrera_sede"]["extraction_mode"] == "chunked"
    assert policies["convocatorias_carrera_sede"]["chunk_size_pages"] == 10
    assert policies["becarios_provincia"]["allow_record_count_mismatch"] is True


def test_actual_plan_builds_one_chunk_for_becarios_pais_estudio() -> None:
    orchestration = load_orchestration_config(REPO_ROOT / "config" / "orchestration.yaml")
    discovery = _base_discovery(
        "becarios_pais_estudio",
        "single",
        effective_page_size=10000,
        total_pages=9,
        total_records=90000,
    )

    plan = build_plan(discovery, orchestration, "becarios_pais_estudio")

    assert plan["datasets"][0]["source_dataset"] == "becarios_pais_estudio"
    assert plan["datasets"][0]["extraction_mode"] == "single"
    assert plan["datasets"][0]["max_parallel_chunks"] == 1
    assert plan["chunks"] == [
        {
            "chunk_id": "becarios_pais_estudio_0001",
            "source_dataset": "becarios_pais_estudio",
            "page_start": 1,
            "page_end": 9,
            "effective_page_size": 10000,
            "bronze_enabled": True,
            "silver_enabled": True,
            "required_for_e2e": False,
            "output_mode": "chunk",
        }
    ]


def test_actual_plan_builds_eight_chunks_for_convocatorias_carrera_sede() -> None:
    orchestration = load_orchestration_config(REPO_ROOT / "config" / "orchestration.yaml")
    discovery = _base_discovery(
        "convocatorias_carrera_sede",
        "chunked",
        effective_page_size=5000,
        total_pages=80,
        total_records=400000,
    )

    plan = build_plan(discovery, orchestration, "convocatorias_carrera_sede")

    assert plan["datasets"][0]["source_dataset"] == "convocatorias_carrera_sede"
    assert plan["datasets"][0]["extraction_mode"] == "chunked"
    assert plan["datasets"][0]["chunk_size_pages"] == 10
    assert plan["datasets"][0]["max_parallel_chunks"] == 2
    assert len(plan["chunks"]) == 8
    assert plan["chunks"][0]["page_start"] == 1
    assert plan["chunks"][0]["page_end"] == 10
    assert plan["chunks"][1]["page_start"] == 11
    assert plan["chunks"][1]["page_end"] == 20
    assert plan["chunks"][7]["page_start"] == 71
    assert plan["chunks"][7]["page_end"] == 80


def test_disabled_dataset_can_be_planned_when_selected_explicitly() -> None:
    orchestration = load_orchestration_config(REPO_ROOT / "config" / "orchestration.yaml")
    discovery = _base_discovery(
        "convocatorias_carrera_sede",
        "chunked",
        effective_page_size=5000,
        total_pages=80,
        total_records=395633,
    )
    discovery["datasets"][0]["extraction_enabled"] = False

    plan = build_plan(discovery, orchestration, "convocatorias_carrera_sede")

    assert plan["datasets"][0]["source_dataset"] == "convocatorias_carrera_sede"
    assert plan["datasets"][0]["expected_chunks"] == 8
    assert len(plan["chunks"]) == 8
