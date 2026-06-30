# -*- coding: utf-8 -*-
"""Pruebas unitarias detalladas para la división de chunks en el plan de extracción."""

import pytest
from pipelines.build_pronabec_extraction_plan import build_plan
from pipelines.common.config import ConfigError


@pytest.fixture
def base_orchestration():
    return {
        "datasets": {
            "pronabec_api": {
                "extraction_policies": [
                    {
                        "source_dataset": "dataset_1",
                        "extraction_enabled": True,
                        "silver_enabled": True,
                        "extraction_mode": "chunked",
                        "required_for_e2e": True,
                        "chunk_size_pages": 10,
                        "max_parallel_chunks": 2,
                        "recommended_page_size": 1000,
                        "fallback_page_sizes": [500, 100],
                        "page_size_policy": "safe",
                    },
                    {
                        "source_dataset": "dataset_single",
                        "extraction_enabled": True,
                        "silver_enabled": True,
                        "extraction_mode": "single",
                        "required_for_e2e": False,
                        "chunk_size_pages": None,
                        "max_parallel_chunks": 1,
                        "recommended_page_size": 1000,
                        "fallback_page_sizes": [500, 100],
                        "page_size_policy": "safe",
                    }
                ]
            }
        }
    }


@pytest.fixture
def base_discovery():
    return {
        "source_system": "pronabec",
        "extraction_date": "2026-06-29",
        "pipeline_run_id": "test-run",
        "source_snapshot_observed_at": "2026-06-29T20:30:00Z",
        "status": "SUCCESS",
        "datasets": []
    }


def test_chunk_division_rules(base_discovery, base_orchestration):
    # 1. total_pages=80 y chunk_size_pages=10 genera 8 chunks
    disc = dict(base_discovery)
    disc["datasets"] = [
        {
            "source_dataset": "dataset_1",
            "extraction_enabled": True,
            "silver_enabled": True,
            "required_for_e2e": True,
            "extraction_mode": "chunked",
            "recommended_page_size": 1000,
            "fallback_page_sizes": [500],
            "effective_page_size": 1000,
            "total_records": 80000,
            "total_pages": 80,
            "status": "SUCCESS",
        }
    ]
    plan = build_plan(disc, base_orchestration, None)
    assert len(plan["chunks"]) == 8
    assert plan["datasets"][0]["expected_chunks"] == 8
    assert plan["chunks"][0]["page_start"] == 1
    assert plan["chunks"][0]["page_end"] == 10
    assert plan["chunks"][7]["page_start"] == 71
    assert plan["chunks"][7]["page_end"] == 80


def test_single_mode_chunk_ranges(base_discovery, base_orchestration):
    # 2. total_pages=11 y extraction_mode=single genera 1 chunk de 1 a 11
    disc = dict(base_discovery)
    disc["datasets"] = [
        {
            "source_dataset": "dataset_single",
            "extraction_enabled": True,
            "silver_enabled": True,
            "required_for_e2e": False,
            "extraction_mode": "single",
            "recommended_page_size": 1000,
            "fallback_page_sizes": [500],
            "effective_page_size": 1000,
            "total_records": 11000,
            "total_pages": 11,
            "status": "SUCCESS",
        }
    ]
    plan = build_plan(disc, base_orchestration, None)
    assert len(plan["chunks"]) == 1
    assert plan["chunks"][0]["page_start"] == 1
    assert plan["chunks"][0]["page_end"] == 11

    # 3. total_pages=15 y extraction_mode=single genera 1 chunk de 1 a 15
    disc["datasets"][0]["total_pages"] = 15
    plan = build_plan(disc, base_orchestration, None)
    assert len(plan["chunks"]) == 1
    assert plan["chunks"][0]["page_start"] == 1
    assert plan["chunks"][0]["page_end"] == 15

    # 4. total_pages=9 y extraction_mode=single genera 1 chunk de 1 a 9
    disc["datasets"][0]["total_pages"] = 9
    plan = build_plan(disc, base_orchestration, None)
    assert len(plan["chunks"]) == 1
    assert plan["chunks"][0]["page_start"] == 1
    assert plan["chunks"][0]["page_end"] == 9


def test_total_pages_zero_handling(base_discovery, base_orchestration):
    # 5. total_pages=0 se maneja sin división inválida
    disc = dict(base_discovery)
    disc["datasets"] = [
        {
            "source_dataset": "dataset_1",
            "extraction_enabled": True,
            "silver_enabled": True,
            "required_for_e2e": True,
            "extraction_mode": "chunked",
            "recommended_page_size": 1000,
            "fallback_page_sizes": [500],
            "effective_page_size": 1000,
            "total_records": 0,
            "total_pages": 0,
            "status": "SUCCESS",
        }
    ]
    plan = build_plan(disc, base_orchestration, None)
    assert len(plan["chunks"]) == 1
    assert plan["chunks"][0]["page_start"] == 1
    assert plan["chunks"][0]["page_end"] == 0


def test_parameter_propagation(base_discovery, base_orchestration):
    disc = dict(base_discovery)
    disc["datasets"] = [
        {
            "source_dataset": "dataset_1",
            "extraction_enabled": True,
            "silver_enabled": True,
            "required_for_e2e": True,
            "extraction_mode": "chunked",
            "recommended_page_size": 1000,
            "fallback_page_sizes": [500],
            "effective_page_size": 500, # Diferente al recomendado
            "total_records": 1500,
            "total_pages": 3,
            "status": "SUCCESS",
        }
    ]
    plan = build_plan(disc, base_orchestration, None)
    
    # 6. usa effective_page_size desde discovery
    assert plan["chunks"][0]["effective_page_size"] == 500
    
    # 8. required_for_e2e se conserva en chunks
    assert plan["chunks"][0]["required_for_e2e"] is True
    
    # 9. max_parallel_chunks se conserva por dataset
    assert plan["datasets"][0]["max_parallel_chunks"] == 2


def test_does_not_plan_failed_datasets(base_discovery, base_orchestration):
    # 7. no planifica datasets con status FAILED
    disc = dict(base_discovery)
    disc["datasets"] = [
        {
            "source_dataset": "dataset_1",
            "extraction_enabled": True,
            "bronze_enabled": True,
            "silver_enabled": True,
            "required_for_e2e": True,
            "extraction_mode": "chunked",
            "recommended_page_size": 1000,
            "fallback_page_sizes": [500],
            "effective_page_size": 1000,
            "total_records": 1000,
            "total_pages": 1,
            "status": "FAILED", # FAILED!
        }
    ]
    with pytest.raises(ConfigError, match="datasets Bronze habilitados"):
        build_plan(disc, base_orchestration, None)


def test_chunk_id_determinism_and_gaps(base_discovery, base_orchestration):
    disc = dict(base_discovery)
    disc["datasets"] = [
        {
            "source_dataset": "dataset_1",
            "extraction_enabled": True,
            "silver_enabled": True,
            "required_for_e2e": True,
            "extraction_mode": "chunked",
            "recommended_page_size": 1000,
            "fallback_page_sizes": [500],
            "effective_page_size": 1000,
            "total_records": 3500,
            "total_pages": 25,
            "status": "SUCCESS",
        }
    ]
    
    plan1 = build_plan(disc, base_orchestration, None)
    plan2 = build_plan(disc, base_orchestration, None)
    
    # 13. chunk_id es estable/determinístico
    assert plan1["chunks"] == plan2["chunks"]
    assert plan1["chunks"][0]["chunk_id"] == "dataset_1_0001"
    assert plan1["chunks"][1]["chunk_id"] == "dataset_1_0002"
    
    # 14. no hay overlap ni gaps entre chunks
    # 15. page_end nunca supera total_pages
    assert len(plan1["chunks"]) == 3
    assert plan1["chunks"][0]["page_start"] == 1
    assert plan1["chunks"][0]["page_end"] == 10
    assert plan1["chunks"][1]["page_start"] == 11
    assert plan1["chunks"][1]["page_end"] == 20
    assert plan1["chunks"][2]["page_start"] == 21
    assert plan1["chunks"][2]["page_end"] == 25
