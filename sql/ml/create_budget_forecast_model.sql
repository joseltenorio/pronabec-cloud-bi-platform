-- ============================================================================
-- Project Cloud BI Platform
-- BigQuery ML monthly budget forecast model
-- ============================================================================

CREATE OR REPLACE MODEL `{project_id}.{ml_dataset}.model_budget_forecast`
OPTIONS(
  model_type = 'ARIMA_PLUS',
  time_series_timestamp_col = 'periodo_fecha',
  time_series_data_col = 'devengado_total',
  auto_arima = TRUE,
  data_frequency = 'MONTHLY'
) AS
SELECT
  DATE(ano, mes_numero, 1) AS periodo_fecha,
  SUM(CAST(devengado AS FLOAT64)) AS devengado_total
FROM `{project_id}.{silver_dataset}.presupuesto_mef_temporal`
WHERE periodo_tipo = 'MENSUAL'
  AND ano IS NOT NULL
  AND mes_numero BETWEEN 1 AND 12
  AND devengado IS NOT NULL
GROUP BY periodo_fecha;
