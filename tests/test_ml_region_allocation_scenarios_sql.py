from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SQL_PATH = PROJECT_ROOT / "sql" / "ml" / "create_region_allocation_scenarios.sql"


def _read() -> str:
    assert SQL_PATH.exists(), f"Missing SQL file: {SQL_PATH}"
    return SQL_PATH.read_text(encoding="utf-8")


def test_region_allocation_scenarios_sql_exists_and_creates_view() -> None:
    content = _read()

    assert "CREATE OR REPLACE VIEW `{project_id}.{ml_dataset}.region_allocation_scenarios`" in content


def test_region_allocation_scenarios_reads_required_sources() -> None:
    content = _read()

    assert "`{project_id}.{ml_dataset}.budget_scenarios`" in content
    assert "`{project_id}.{ml_dataset}.region_priority_scores_v2`" in content
    assert "`{project_id}.{ml_dataset}.region_priority_scores`" in content
    assert "`{project_id}.{ml_dataset}.region_context_features`" in content
    assert "`{project_id}.{ml_dataset}.budget_forecast_results`" in content


def test_region_allocation_scenarios_calculates_required_metrics() -> None:
    content = _read()

    assert "scenario_raw_score" in content
    assert "allocation_weight" in content
    assert "SAFE_MULTIPLY(allocated.allocation_weight, 100) AS allocation_pct" in content
    assert "RANK() OVER" in content
    assert "scenario_rank" in content
    assert "baseline_rank_v2 - allocated.scenario_rank AS rank_change_vs_v2" in content
    assert "SUM(CAST(forecast_value AS FLOAT64)) AS reference_forecast_budget_amount" in content
    assert "estimated_budget_amount" in content
    assert "estimated_scholarships" in content


def test_region_allocation_scenarios_avoid_forbidden_sources() -> None:
    content = _read().lower()

    assert "bronze." not in content
    assert "presupuesto_mef_departamento" not in content
