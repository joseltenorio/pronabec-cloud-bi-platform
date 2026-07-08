-- ============================================================================
-- Project Cloud BI Platform
-- BigQuery ML regional clustering model
-- ============================================================================

CREATE OR REPLACE MODEL `{project_id}.{ml_dataset}.model_region_clusters`
OPTIONS(
  model_type = 'kmeans',
  num_clusters = 4,
  standardize_features = TRUE,
  kmeans_init_method = 'KMEANS++'
) AS
SELECT
  COALESCE(context_priority_score, 0) AS context_priority_score,
  COALESCE(coverage_gap_score, 0) AS coverage_gap_score,
  COALESCE(primera_generacion_score, 0) AS primera_generacion_score,
  COALESCE(pobreza_monetaria_pct, 0) AS pobreza_monetaria_pct,
  COALESCE(matricula_5to_secundaria, 0) AS matricula_5to_secundaria,
  COALESCE(poblacion_15_29, 0) AS poblacion_15_29,
  COALESCE(brecha_digital_pct, 0) AS brecha_digital_pct,
  COALESCE(ruralidad_educativa_pct, 0) AS ruralidad_educativa_pct,
  COALESCE(primera_generacion_ratio, 0) AS primera_generacion_ratio,
  COALESCE(becas_por_1000_jovenes, 0) AS becas_por_1000_jovenes,
  COALESCE(becas_por_1000_matriculados_5to, 0) AS becas_por_1000_matriculados_5to
FROM `{project_id}.{ml_dataset}.region_coverage_features`
WHERE anio BETWEEN 2012 AND 2025
  AND context_priority_score IS NOT NULL;
