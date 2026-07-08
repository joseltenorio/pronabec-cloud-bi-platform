from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SQL_PATH = PROJECT_ROOT / "sql" / "ml" / "create_region_priority_scores_v2.sql"


def test_region_priority_v2_sql_exists() -> None:
    assert SQL_PATH.exists()


def test_region_priority_v2_sql_contract() -> None:
    content = SQL_PATH.read_text(encoding="utf-8")

    assert "CREATE OR REPLACE VIEW `{project_id}.{ml_dataset}.region_priority_scores_v2`" in content
    assert "{project_id}.{ml_dataset}.region_coverage_features" in content
    assert "{project_id}.{silver_dataset}" not in content
    assert "bronze." not in content
    assert "0.60" in content
    assert "0.30" in content
    assert "0.10" in content
    assert "priority_score_v2" in content
    assert "priority_rank_v2" in content
    assert "priority_tier_v2" in content
    assert "regional_context_coverage_v2" in content
    assert "weighted_context_coverage_score" in content
