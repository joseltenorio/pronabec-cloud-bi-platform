from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
MODEL_SQL_PATH = REPO_ROOT / "sql" / "ml" / "create_budget_forecast_model.sql"
RESULTS_SQL_PATH = REPO_ROOT / "sql" / "ml" / "create_budget_forecast_results.sql"


def _read(path: Path) -> str:
    assert path.exists(), f"Missing SQL file: {path}"
    return path.read_text(encoding="utf-8")


def test_budget_forecast_sql_files_exist() -> None:
    assert MODEL_SQL_PATH.exists()
    assert RESULTS_SQL_PATH.exists()


def test_budget_forecast_model_uses_arima_plus() -> None:
    content = _read(MODEL_SQL_PATH)

    assert "CREATE OR REPLACE MODEL `{project_id}.{ml_dataset}.model_budget_forecast`" in content
    assert "model_type = 'ARIMA_PLUS'" in content
    assert "time_series_timestamp_col = 'periodo_fecha'" in content
    assert "time_series_data_col = 'devengado_total'" in content
    assert "`{project_id}.{silver_dataset}.presupuesto_mef_temporal`" in content
    assert "SUM(CAST(devengado AS FLOAT64)) AS devengado_total" in content
    assert "DATE(ano, mes_numero, 1) AS periodo_fecha" in content


def test_budget_forecast_results_use_ml_forecast() -> None:
    content = _read(RESULTS_SQL_PATH)

    assert "CREATE OR REPLACE VIEW `{project_id}.{ml_dataset}.budget_forecast_results`" in content
    assert "ML.FORECAST" in content
    assert "MODEL `{project_id}.{ml_dataset}.model_budget_forecast`" in content
    assert "STRUCT(12 AS horizon, 0.80 AS confidence_level)" in content
    assert "forecast_version" in content


def test_budget_forecast_sql_avoids_forbidden_sources() -> None:
    combined = (_read(MODEL_SQL_PATH) + "\n" + _read(RESULTS_SQL_PATH)).lower()

    assert "presupuesto_mef_departamento" not in combined
    assert "bronze." not in combined
    assert "{project_id}.silver" not in combined
    assert "{project_id}.ml" not in combined
    assert "pronabec-cloud-bi-platform" not in combined
    assert "create or replace table" not in combined
    assert "drop" not in combined
