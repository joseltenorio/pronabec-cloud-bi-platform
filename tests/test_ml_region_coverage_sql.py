from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SQL_PATH = PROJECT_ROOT / "sql" / "ml" / "create_region_coverage_features.sql"


def test_region_coverage_sql_exists() -> None:
    assert SQL_PATH.exists()


def test_region_coverage_sql_contract() -> None:
    content = SQL_PATH.read_text(encoding="utf-8")

    assert "CREATE OR REPLACE VIEW `{project_id}.{ml_dataset}.region_coverage_features`" in content
    assert "{project_id}.{ml_dataset}.region_context_features" in content
    assert "{project_id}.{ml_dataset}.region_priority_scores" in content
    assert "{project_id}.{ml_dataset}.dim_region_mapping" in content
    assert "{project_id}.{silver_dataset}.pronabec_report_beca18_region_postulacion_anual" in content
    assert "{project_id}.{silver_dataset}.pronabec_report_beca18_becas_otorgadas_modalidad_anual" in content
    assert "{project_id}.{silver_dataset}.pronabec_report_beca18_primera_generacion_region" in content
    assert "coverage_base AS (" in content
    assert "coverage_scored AS (" in content
    assert "primera_generacion_ratio_base" in content
    assert "primera_generacion_ratio_base AS primera_generacion_ratio" in content
    assert "bronze." not in content
    assert "presupuesto_mef_departamento" not in content
    assert "regional_becarios_pct" in content
    assert "regional_becarios_estimated" in content
    assert "becas_por_1000_jovenes" in content
    assert "becas_por_1000_matriculados_5to" in content
    assert "coverage_gap_score" in content
    assert "primera_generacion_score" in content
    assert "coverage_data_quality_flag" in content
    assert "coverage_source_method" in content
