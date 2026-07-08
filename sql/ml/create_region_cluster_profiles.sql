-- ============================================================================
-- Project Cloud BI Platform
-- BigQuery ML regional cluster profiles
-- ============================================================================

CREATE OR REPLACE VIEW `{project_id}.{ml_dataset}.region_cluster_profiles` AS
SELECT
  centroid_id,
  cluster_label,
  COUNT(DISTINCT region_canonical) AS regiones_count,
  AVG(priority_score_v2) AS avg_priority_score_v2,
  AVG(context_priority_score) AS avg_context_priority_score,
  AVG(coverage_gap_score) AS avg_coverage_gap_score,
  AVG(primera_generacion_score) AS avg_primera_generacion_score,
  AVG(pobreza_monetaria_pct) AS avg_pobreza_monetaria_pct,
  AVG(CAST(matricula_5to_secundaria AS FLOAT64)) AS avg_matricula_5to_secundaria,
  AVG(brecha_digital_pct) AS avg_brecha_digital_pct,
  AVG(ruralidad_educativa_pct) AS avg_ruralidad_educativa_pct,
  CURRENT_TIMESTAMP() AS created_at
FROM `{project_id}.{ml_dataset}.region_cluster_assignments`
GROUP BY centroid_id, cluster_label;
