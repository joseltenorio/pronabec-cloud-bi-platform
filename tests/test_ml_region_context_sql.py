from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SQL_PATH = PROJECT_ROOT / "sql" / "ml" / "create_region_context_features.sql"


def test_region_context_sql_exists() -> None:
    assert SQL_PATH.exists()


def test_region_context_sql_uses_silver_sources_only() -> None:
    content = SQL_PATH.read_text(encoding="utf-8")

    assert "{project_id}.{ml_dataset}.region_context_features" in content
    assert "{project_id}.{ml_dataset}.dim_region_mapping" in content
    assert "{project_id}.{silver_dataset}.inei_pobreza_departamental" in content
    assert "{project_id}.{silver_dataset}.inei_population_youth_region" in content
    assert "{project_id}.{silver_dataset}.inei_demographic_indicators_region" in content
    assert "{project_id}.{silver_dataset}.inei_internet_acceso_region" in content
    assert "{project_id}.{silver_dataset}.minedu_matricula_secundaria_departamental" in content
    assert "{project_id}.ml" not in content
    assert "bronze." not in content
    assert "presupuesto_mef_departamento" not in content


def test_region_context_sql_filters_to_v1_year_range_and_required_features() -> None:
    content = SQL_PATH.read_text(encoding="utf-8")

    assert "GENERATE_ARRAY(2012, 2025)" in content
    assert "brecha_digital_pct" in content
    assert "ruralidad_educativa_pct" in content
    assert "feature_completeness_score" in content
    assert "feature_quality_flag" in content
    assert "has_synthetic_values" in content
    assert "synthetic_fields" in content
    assert "source_priority" in content
    assert "CURRENT_TIMESTAMP()" in content


def test_region_context_sql_normalizes_regions_and_anchors_canonical_regions() -> None:
    content = SQL_PATH.read_text(encoding="utf-8")

    assert "TRANSLATE" in content
    assert "REGEXP_REPLACE" in content
    assert "g.region_canonical AS region" in content
    assert "dim_region_mapping" in content
    assert "LIMA METROPOLITANA" not in content
    assert "LIMA PROVINCIAS" not in content
    assert "PROV. CONST. DEL CALLAO" not in content
    assert "PROVINCIA CONSTITUCIONAL DEL CALLAO" not in content


def test_region_context_sql_includes_feature_math() -> None:
    content = SQL_PATH.read_text(encoding="utf-8")

    assert "100 - i.internet_acceso_pct" in content
    assert "COALESCE(pop.poblacion_15_29, pop.poblacion_15_24)" in content
    assert "CAST(pop.poblacion_total AS FLOAT64)" in content
    assert "CAST(m.matricula_5to_rural AS FLOAT64)" in content
    assert "CAST(m.matricula_5to_secundaria AS FLOAT64)" in content
