from __future__ import annotations

import re
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SQL_PATH = PROJECT_ROOT / "sql" / "ml" / "create_region_priority_scores.sql"


def test_region_priority_sql_exists_and_targets_ml_context_features() -> None:
    assert SQL_PATH.exists()
    content = SQL_PATH.read_text(encoding="utf-8")

    assert "CREATE OR REPLACE VIEW" in content
    assert "{project_id}.{ml_dataset}.region_context_features" in content
    assert "{project_id}.{ml_dataset}.region_priority_scores" in content
    assert "{project_id}.{gold_dataset}" not in content
    assert "bronze." not in content
    assert "silver." not in content
    assert "presupuesto_mef_departamento" not in content


def test_region_priority_sql_uses_expected_weights_and_safe_divide() -> None:
    content = SQL_PATH.read_text(encoding="utf-8")
    normalized = " ".join(content.split())

    for weight in ["0.35", "0.25", "0.15", "0.10"]:
        assert weight in content

    assert "SAFE_DIVIDE" in content
    assert "weighted_minmax_context_score" in content
    assert "feature_completeness_score" in content
    assert "score_version" in content
    assert "score_method" in content
    assert "pobreza_score" in content
    assert "demanda_educativa_score" in content
    assert "poblacion_joven_score" in content
    assert "ruralidad_score" in content
    assert "brecha_digital_score" in content
    assert "priority_score" in content
    assert "priority_rank" in content
    assert "priority_tier" in content
    assert "RANK() OVER" in normalized
    assert "WHERE anio BETWEEN 2012 AND 2025" in normalized


def test_region_priority_sql_handles_min_max_and_missing_youth_source() -> None:
    content = SQL_PATH.read_text(encoding="utf-8")

    assert "COALESCE(poblacion_15_29, poblacion_15_24)" in content
    assert "NULLIF(pobreza_max_anual - pobreza_min_anual, 0)" in content
    assert "NULLIF(CAST(demanda_max_anual AS FLOAT64) - CAST(demanda_min_anual AS FLOAT64), 0)" in content
    assert "NULLIF(CAST(joven_max_anual AS FLOAT64) - CAST(joven_min_anual AS FLOAT64), 0)" in content
    assert "NULLIF(ruralidad_max_anual - ruralidad_min_anual, 0)" in content
    assert "NULLIF(brecha_max_anual - brecha_min_anual, 0)" in content
    assert re.search(r"available_weight\s*=\s*0\s+THEN\s+NULL", content, flags=re.IGNORECASE) is not None
