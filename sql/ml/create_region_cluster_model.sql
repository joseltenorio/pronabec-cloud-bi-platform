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
  context_priority_score,
  coverage_gap_score,
  primera_generacion_score,
  pobreza_monetaria_pct,
  matricula_5to_secundaria,
  poblacion_15_29,
  brecha_digital_pct,
  ruralidad_educativa_pct,
  primera_generacion_ratio,
  becas_por_1000_jovenes,
  becas_por_1000_matriculados_5to
FROM `{project_id}.{ml_dataset}.region_coverage_features`
WHERE anio BETWEEN 2012 AND 2025
  AND context_priority_score IS NOT NULL
  AND coverage_gap_score IS NOT NULL
  AND (
    primera_generacion_score IS NOT NULL
    OR pobreza_monetaria_pct IS NOT NULL
    OR matricula_5to_secundaria IS NOT NULL
    OR poblacion_15_29 IS NOT NULL
    OR brecha_digital_pct IS NOT NULL
    OR ruralidad_educativa_pct IS NOT NULL
    OR primera_generacion_ratio IS NOT NULL
    OR becas_por_1000_jovenes IS NOT NULL
    OR becas_por_1000_matriculados_5to IS NOT NULL
  );
