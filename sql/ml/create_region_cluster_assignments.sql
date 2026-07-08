-- ============================================================================
-- Project Cloud BI Platform
-- BigQuery ML regional cluster assignments
-- ============================================================================

CREATE OR REPLACE VIEW `{project_id}.{ml_dataset}.region_cluster_assignments` AS
WITH prediction_input AS (
  SELECT
    anio,
    region,
    region_canonical,
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
    becas_por_1000_matriculados_5to,
    created_at
  FROM `{project_id}.{ml_dataset}.region_coverage_features`
  WHERE anio BETWEEN 2012 AND 2025
    AND context_priority_score IS NOT NULL
    AND coverage_gap_score IS NOT NULL
),
predicted AS (
  SELECT
    anio,
    region,
    region_canonical,
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
    becas_por_1000_matriculados_5to,
    created_at,
    CENTROID_ID,
    NEAREST_CENTROIDS_DISTANCE
  FROM ML.PREDICT(
    MODEL `{project_id}.{ml_dataset}.model_region_clusters`,
    (
      SELECT
        anio,
        region,
        region_canonical,
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
        becas_por_1000_matriculados_5to,
        created_at
      FROM prediction_input
    )
  )
)
SELECT
  p.anio,
  p.region,
  p.region_canonical,
  s.priority_score_v2,
  p.context_priority_score,
  p.coverage_gap_score,
  p.primera_generacion_score,
  p.pobreza_monetaria_pct,
  p.matricula_5to_secundaria,
  p.poblacion_15_29,
  p.brecha_digital_pct,
  p.ruralidad_educativa_pct,
  p.primera_generacion_ratio,
  p.becas_por_1000_jovenes,
  p.becas_por_1000_matriculados_5to,
  p.CENTROID_ID AS centroid_id,
  (
    SELECT nearest.distance
    FROM UNNEST(p.NEAREST_CENTROIDS_DISTANCE) AS nearest
    WHERE nearest.centroid_id = p.CENTROID_ID
    LIMIT 1
  ) AS centroid_distance,
  CONCAT('Cluster ', CAST(p.CENTROID_ID AS STRING)) AS cluster_label,
  p.created_at
FROM predicted AS p
LEFT JOIN `{project_id}.{ml_dataset}.region_priority_scores_v2` AS s
  ON s.anio = p.anio
 AND s.region_canonical = p.region_canonical;
