-- ============================================================================
-- Project Cloud BI Platform
-- BigQuery ML monthly budget forecast results
-- ============================================================================

CREATE OR REPLACE VIEW `{project_id}.{ml_dataset}.budget_forecast_results` AS
SELECT
  forecast_timestamp,
  forecast_value,
  prediction_interval_lower_bound,
  prediction_interval_upper_bound,
  0.80 AS confidence_level,
  12 AS forecast_horizon_months,
  'model_budget_forecast' AS model_name,
  'budget_forecast_arima_plus_v1' AS forecast_version
FROM ML.FORECAST(
  MODEL `{project_id}.{ml_dataset}.model_budget_forecast`,
  STRUCT(12 AS horizon, 0.80 AS confidence_level)
);
