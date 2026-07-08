-- ============================================================================
-- Project Cloud BI Platform
-- ML regional priority score
-- ============================================================================

CREATE OR REPLACE VIEW `{project_id}.{ml_dataset}.region_priority_scores` AS
WITH base AS (
  SELECT
    anio,
    region,
    region_canonical,
    pobreza_monetaria_pct,
    poblacion_total,
    poblacion_15_24,
    poblacion_15_29,
    poblacion_joven_pct,
    matricula_5to_secundaria,
    ruralidad_educativa_pct,
    internet_acceso_pct,
    brecha_digital_pct,
    feature_completeness_score,
    feature_quality_flag,
    has_synthetic_values,
    synthetic_fields,
    source_priority,
    created_at
  FROM `{project_id}.{ml_dataset}.region_context_features`
  WHERE anio BETWEEN 2012 AND 2025
),
windowed AS (
  SELECT
    base.*,
    COALESCE(poblacion_15_29, poblacion_15_24) AS poblacion_joven_base,
    MIN(pobreza_monetaria_pct) OVER (PARTITION BY anio) AS pobreza_min_anual,
    MAX(pobreza_monetaria_pct) OVER (PARTITION BY anio) AS pobreza_max_anual,
    MIN(matricula_5to_secundaria) OVER (PARTITION BY anio) AS demanda_min_anual,
    MAX(matricula_5to_secundaria) OVER (PARTITION BY anio) AS demanda_max_anual,
    MIN(COALESCE(poblacion_15_29, poblacion_15_24)) OVER (PARTITION BY anio) AS joven_min_anual,
    MAX(COALESCE(poblacion_15_29, poblacion_15_24)) OVER (PARTITION BY anio) AS joven_max_anual,
    MIN(ruralidad_educativa_pct) OVER (PARTITION BY anio) AS ruralidad_min_anual,
    MAX(ruralidad_educativa_pct) OVER (PARTITION BY anio) AS ruralidad_max_anual,
    MIN(brecha_digital_pct) OVER (PARTITION BY anio) AS brecha_min_anual,
    MAX(brecha_digital_pct) OVER (PARTITION BY anio) AS brecha_max_anual
  FROM base
),
component_scores AS (
  SELECT
    anio,
    region,
    region_canonical,
    pobreza_monetaria_pct,
    poblacion_total,
    poblacion_15_24,
    poblacion_15_29,
    poblacion_joven_pct,
    matricula_5to_secundaria,
    ruralidad_educativa_pct,
    internet_acceso_pct,
    brecha_digital_pct,
    feature_completeness_score,
    feature_quality_flag,
    has_synthetic_values,
    synthetic_fields,
    source_priority,
    created_at,
    CASE
      WHEN pobreza_monetaria_pct IS NULL THEN NULL
      ELSE COALESCE(
        SAFE_DIVIDE(
          pobreza_monetaria_pct - pobreza_min_anual,
          NULLIF(pobreza_max_anual - pobreza_min_anual, 0)
        ),
        0
      )
    END AS pobreza_score,
    CASE
      WHEN matricula_5to_secundaria IS NULL THEN NULL
      ELSE COALESCE(
        SAFE_DIVIDE(
          CAST(matricula_5to_secundaria AS FLOAT64) - CAST(demanda_min_anual AS FLOAT64),
          NULLIF(CAST(demanda_max_anual AS FLOAT64) - CAST(demanda_min_anual AS FLOAT64), 0)
        ),
        0
      )
    END AS demanda_educativa_score,
    CASE
      WHEN poblacion_joven_base IS NULL THEN NULL
      ELSE COALESCE(
        SAFE_DIVIDE(
          CAST(poblacion_joven_base AS FLOAT64) - CAST(joven_min_anual AS FLOAT64),
          NULLIF(CAST(joven_max_anual AS FLOAT64) - CAST(joven_min_anual AS FLOAT64), 0)
        ),
        0
      )
    END AS poblacion_joven_score,
    CASE
      WHEN ruralidad_educativa_pct IS NULL THEN NULL
      ELSE COALESCE(
        SAFE_DIVIDE(
          ruralidad_educativa_pct - ruralidad_min_anual,
          NULLIF(ruralidad_max_anual - ruralidad_min_anual, 0)
        ),
        0
      )
    END AS ruralidad_score,
    CASE
      WHEN brecha_digital_pct IS NULL THEN NULL
      ELSE COALESCE(
        SAFE_DIVIDE(
          brecha_digital_pct - brecha_min_anual,
          NULLIF(brecha_max_anual - brecha_min_anual, 0)
        ),
        0
      )
    END AS brecha_digital_score
  FROM windowed
),
priority_components AS (
  SELECT
    *,
    (CASE WHEN pobreza_score IS NULL THEN 0 ELSE 0.35 END)
    + (CASE WHEN demanda_educativa_score IS NULL THEN 0 ELSE 0.25 END)
    + (CASE WHEN poblacion_joven_score IS NULL THEN 0 ELSE 0.15 END)
    + (CASE WHEN brecha_digital_score IS NULL THEN 0 ELSE 0.15 END)
    + (CASE WHEN ruralidad_score IS NULL THEN 0 ELSE 0.10 END) AS available_weight,
    (
      COALESCE(pobreza_score, 0) * 0.35
      + COALESCE(demanda_educativa_score, 0) * 0.25
      + COALESCE(poblacion_joven_score, 0) * 0.15
      + COALESCE(brecha_digital_score, 0) * 0.15
      + COALESCE(ruralidad_score, 0) * 0.10
    ) AS weighted_score_sum
  FROM component_scores
)
SELECT
  anio,
  region,
  region_canonical,
  pobreza_monetaria_pct,
  poblacion_15_24,
  poblacion_15_29,
  poblacion_joven_pct,
  matricula_5to_secundaria,
  ruralidad_educativa_pct,
  internet_acceso_pct,
  brecha_digital_pct,
  pobreza_score,
  demanda_educativa_score,
  poblacion_joven_score,
  ruralidad_score,
  brecha_digital_score,
  CASE
    WHEN available_weight = 0 THEN NULL
    ELSE SAFE_DIVIDE(weighted_score_sum, available_weight)
  END AS priority_score,
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
  ) AS priority_rank,
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
  END AS priority_tier,
  'regional_context_v1' AS score_version,
  'weighted_minmax_context_score' AS score_method,
  feature_completeness_score,
  feature_quality_flag,
  source_priority,
  has_synthetic_values,
  synthetic_fields,
  created_at
FROM priority_components;
