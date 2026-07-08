from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SQL_PATH = PROJECT_ROOT / "sql" / "ml" / "create_budget_scenarios.sql"


def _read() -> str:
    assert SQL_PATH.exists(), f"Missing SQL file: {SQL_PATH}"
    return SQL_PATH.read_text(encoding="utf-8")


def test_budget_scenarios_sql_exists_and_creates_view() -> None:
    content = _read()

    assert "CREATE OR REPLACE VIEW `{project_id}.{ml_dataset}.budget_scenarios`" in content
    assert "UNNEST" in content
    assert "scenario_version" in content


def test_budget_scenarios_include_required_scenarios() -> None:
    content = _read()

    for scenario_id in [
        "base_priority",
        "budget_plus_10",
        "budget_plus_20",
        "budget_minus_10",
        "poverty_focus",
        "demand_population_focus",
        "first_generation_focus",
        "balanced_equity_demand",
    ]:
        assert scenario_id in content


def test_budget_scenarios_avoid_forbidden_sources() -> None:
    content = _read().lower()

    assert "bronze." not in content
    assert "presupuesto_mef_departamento" not in content
