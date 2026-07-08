-- ============================================================================
-- Project Cloud BI Platform
-- ML regional priority score v2
-- ============================================================================

CREATE OR REPLACE VIEW `{project_id}.{ml_dataset}.region_priority_scores_v2` AS
WITH base AS (
  SELECT
    anio,
    region,
    region_canonical,
    context_priority_score,
    coverage_gap_score,
    primera_generacion_score,
    context_priority_rank,
    context_priority_tier,
    coverage_data_quality_flag,
    coverage_source_method,
    coverage_source_notes,
    has_estimated_coverage,
    created_at
  FROM `{project_id}.{ml_dataset}.region_coverage_features`
),
scored AS (
  SELECT
    *,
    (CASE WHEN context_priority_score IS NULL THEN 0 ELSE 0.60 END)
    + (CASE WHEN coverage_gap_score IS NULL THEN 0 ELSE 0.30 END)
    + (CASE WHEN primera_generacion_score IS NULL THEN 0 ELSE 0.10 END) AS available_weight,
    (
      COALESCE(context_priority_score, 0) * 0.60
      + COALESCE(coverage_gap_score, 0) * 0.30
      + COALESCE(primera_generacion_score, 0) * 0.10
    ) AS weighted_score_sum
  FROM base
)
SELECT
  anio,
  region,
  region_canonical,
  context_priority_score,
  coverage_gap_score,
  primera_generacion_score,
  CASE
    WHEN available_weight = 0 THEN NULL
    ELSE SAFE_DIVIDE(weighted_score_sum, available_weight)
  END AS priority_score_v2,
  RANK() OVER (
    PARTITION BY anio
    ORDER BY (
      CASE
        WHEN available_weight = 0 THEN NULL
        ELSE SAFE_DIVIDE(weighted_score_sum, available_weight)
      END
    ) IS NULL,
    CASE
      WHEN available_weight = 0 THEN NULL
      ELSE SAFE_DIVIDE(weighted_score_sum, available_weight)
    END DESC
  ) AS priority_rank_v2,
  CASE
    WHEN CASE
      WHEN available_weight = 0 THEN NULL
      ELSE SAFE_DIVIDE(weighted_score_sum, available_weight)
    END IS NULL THEN 'Insuficiente'
    WHEN CASE
      WHEN available_weight = 0 THEN NULL
      ELSE SAFE_DIVIDE(weighted_score_sum, available_weight)
    END >= 0.80 THEN 'Muy alta'
    WHEN CASE
      WHEN available_weight = 0 THEN NULL
      ELSE SAFE_DIVIDE(weighted_score_sum, available_weight)
    END >= 0.60 THEN 'Alta'
    WHEN CASE
      WHEN available_weight = 0 THEN NULL
      ELSE SAFE_DIVIDE(weighted_score_sum, available_weight)
    END >= 0.40 THEN 'Media'
    ELSE 'Baja'
  END AS priority_tier_v2,
  context_priority_rank,
  context_priority_tier,
  coverage_data_quality_flag,
  coverage_source_method,
  coverage_source_notes,
  has_estimated_coverage,
  'regional_context_coverage_v2' AS score_version,
  'weighted_context_coverage_score' AS score_method,
  created_at
FROM scored;
