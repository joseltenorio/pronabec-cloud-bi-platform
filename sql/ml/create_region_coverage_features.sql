-- ============================================================================
-- Project Cloud BI Platform
-- ML regional coverage foundation
-- ============================================================================

CREATE OR REPLACE VIEW `{project_id}.{ml_dataset}.region_coverage_features` AS
WITH
region_mapping AS (
  SELECT
    source_region,
    region_canonical,
    UPPER(
      TRIM(
        REGEXP_REPLACE(
          TRANSLATE(
            UPPER(COALESCE(source_region, '')),
            'ÁÀÂÄÃÉÈÊËÍÌÎÏÓÒÔÖÕÚÙÛÜÑ',
            'AAAAAEEEEIIIIOOOOOUUUUN'
          ),
          r'[^A-Z0-9 ]',
          ' '
        )
      )
    ) AS source_region_key
  FROM `{project_id}.{ml_dataset}.dim_region_mapping`
),
context_base AS (
  SELECT
    anio,
    region,
    region_canonical,
    pobreza_monetaria_pct,
    poblacion_15_24,
    poblacion_15_29,
    matricula_5to_secundaria,
    ruralidad_educativa_pct,
    internet_acceso_pct,
    brecha_digital_pct
  FROM `{project_id}.{ml_dataset}.region_context_features`
),
context_years AS (
  SELECT DISTINCT anio
  FROM context_base
),
priority_v1 AS (
  SELECT
    anio,
    region_canonical,
    priority_score AS context_priority_score,
    priority_rank AS context_priority_rank,
    priority_tier AS context_priority_tier
  FROM `{project_id}.{ml_dataset}.region_priority_scores`
),
regional_share_raw AS (
  SELECT
    CAST(r.ano_convocatoria AS INT64) AS anio,
    m.region_canonical,
    AVG(CAST(r.porcentaje_becarios AS FLOAT64)) AS regional_becarios_pct
  FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_region_postulacion_anual` AS r
  JOIN region_mapping AS m
    ON UPPER(
      TRIM(
        REGEXP_REPLACE(
          TRANSLATE(
            UPPER(COALESCE(r.grupo_region, '')),
            'ÁÀÂÄÃÉÈÊËÍÌÎÏÓÒÔÖÕÚÙÛÜÑ',
            'AAAAAEEEEIIIIOOOOOUUUUN'
          ),
          r'[^A-Z0-9 ]',
          ' '
        )
      )
    ) = m.source_region_key
  WHERE r.porcentaje_becarios IS NOT NULL
  GROUP BY anio, region_canonical
),
regional_share AS (
  SELECT
    anio,
    region_canonical,
    CASE
      WHEN regional_becarios_pct IS NULL THEN NULL
      WHEN regional_becarios_pct > 1 AND regional_becarios_pct <= 100 THEN regional_becarios_pct
      WHEN regional_becarios_pct >= 0 AND regional_becarios_pct <= 1 THEN regional_becarios_pct * 100
      ELSE regional_becarios_pct
    END AS regional_becarios_pct
  FROM regional_share_raw
),
total_becas_anual AS (
  SELECT
    ano_convocatoria AS anio,
    SUM(becas_otorgadas) AS total_becas_anual
  FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_becas_otorgadas_modalidad_anual`
  WHERE becas_otorgadas IS NOT NULL
  GROUP BY ano_convocatoria
),
first_generation_snapshot AS (
  SELECT
    m.region_canonical,
    AVG(CAST(f.ratio_primera_generacion AS FLOAT64)) AS primera_generacion_ratio_raw,
    SUM(f.total_becarios_primera_generacion) AS total_becarios_primera_generacion,
    SUM(f.total_becarios_encuestados) AS total_becarios_encuestados
  FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_primera_generacion_region` AS f
  JOIN region_mapping AS m
    ON UPPER(
      TRIM(
        REGEXP_REPLACE(
          TRANSLATE(
            UPPER(COALESCE(f.region, '')),
            'ÁÀÂÄÃÉÈÊËÍÌÎÏÓÒÔÖÕÚÙÛÜÑ',
            'AAAAAEEEEIIIIOOOOOUUUUN'
          ),
          r'[^A-Z0-9 ]',
          ' '
        )
      )
    ) = m.source_region_key
  GROUP BY m.region_canonical
),
first_generation_by_year AS (
  SELECT
    y.anio,
    s.region_canonical,
    s.primera_generacion_ratio_raw,
    s.total_becarios_primera_generacion,
    s.total_becarios_encuestados
  FROM context_years AS y
  CROSS JOIN first_generation_snapshot AS s
),
coverage_base AS (
  SELECT
    c.anio,
    c.region,
    c.region_canonical,
    c.pobreza_monetaria_pct,
    c.poblacion_15_24,
    c.poblacion_15_29,
    c.matricula_5to_secundaria,
    c.ruralidad_educativa_pct,
    c.internet_acceso_pct,
    c.brecha_digital_pct,
    p.context_priority_score,
    p.context_priority_rank,
    p.context_priority_tier,
    rs.regional_becarios_pct,
    t.total_becas_anual,
    fg.total_becarios_primera_generacion,
    fg.total_becarios_encuestados,
    CASE
      WHEN fg.primera_generacion_ratio_raw IS NULL THEN NULL
      WHEN fg.primera_generacion_ratio_raw > 1 THEN fg.primera_generacion_ratio_raw / 100
      ELSE fg.primera_generacion_ratio_raw
    END AS primera_generacion_ratio_base
  FROM context_base AS c
  LEFT JOIN priority_v1 AS p
    ON p.anio = c.anio
   AND p.region_canonical = c.region_canonical
  LEFT JOIN regional_share AS rs
    ON rs.anio = c.anio
   AND rs.region_canonical = c.region_canonical
  LEFT JOIN total_becas_anual AS t
    ON t.anio = c.anio
  LEFT JOIN first_generation_by_year AS fg
    ON fg.anio = c.anio
   AND fg.region_canonical = c.region_canonical
),
coverage_metrics AS (
  SELECT
    *,
    CASE
      WHEN regional_becarios_pct IS NOT NULL
       AND total_becas_anual IS NOT NULL
        THEN CAST(ROUND(SAFE_MULTIPLY(total_becas_anual, SAFE_DIVIDE(regional_becarios_pct, 100))) AS INT64)
      ELSE NULL
    END AS regional_becarios_estimated
  FROM coverage_base
),
coverage_with_rates AS (
  SELECT
    *,
    SAFE_DIVIDE(CAST(regional_becarios_estimated AS FLOAT64), CAST(COALESCE(poblacion_15_29, poblacion_15_24) AS FLOAT64)) * 1000 AS becas_por_1000_jovenes,
    SAFE_DIVIDE(CAST(regional_becarios_estimated AS FLOAT64), CAST(matricula_5to_secundaria AS FLOAT64)) * 1000 AS becas_por_1000_matriculados_5to,
    CASE
      WHEN matricula_5to_secundaria IS NOT NULL AND regional_becarios_estimated IS NOT NULL
        THEN GREATEST(matricula_5to_secundaria - regional_becarios_estimated, 0)
      ELSE NULL
    END AS demanda_no_cubierta_estimada,
    CASE
      WHEN regional_becarios_estimated IS NOT NULL AND primera_generacion_ratio_base IS NOT NULL THEN 'mixed'
      WHEN regional_becarios_estimated IS NOT NULL THEN 'estimated_from_regional_share'
      WHEN primera_generacion_ratio_base IS NOT NULL THEN 'first_generation_snapshot_only'
      ELSE 'unavailable'
    END AS coverage_source_method,
    CASE
      WHEN regional_becarios_estimated IS NOT NULL AND primera_generacion_ratio_base IS NOT NULL THEN 'regional_share_estimated_from_total_becas_anual; first_generation_snapshot_applied_to_context_years'
      WHEN regional_becarios_estimated IS NOT NULL THEN 'regional_share_estimated_from_total_becas_anual'
      WHEN primera_generacion_ratio_base IS NOT NULL THEN 'first_generation_snapshot_applied_to_context_years'
      ELSE 'coverage_signals_not_available'
    END AS coverage_source_notes,
    CASE
      WHEN regional_becarios_estimated IS NOT NULL THEN TRUE
      WHEN primera_generacion_ratio_base IS NOT NULL THEN FALSE
      ELSE NULL
    END AS has_estimated_coverage
  FROM coverage_metrics
),
coverage_scored AS (
  SELECT
    *,
    CASE
      WHEN primera_generacion_ratio_base IS NULL THEN NULL
      ELSE COALESCE(
        SAFE_DIVIDE(
          primera_generacion_ratio_base
          - MIN(primera_generacion_ratio_base) OVER (PARTITION BY anio),
          NULLIF(
            MAX(primera_generacion_ratio_base) OVER (PARTITION BY anio)
            - MIN(primera_generacion_ratio_base) OVER (PARTITION BY anio),
            0
          )
        ),
        0
      )
    END AS primera_generacion_score,
    CASE
      WHEN becas_por_1000_matriculados_5to IS NULL THEN NULL
      ELSE 1 - COALESCE(
        SAFE_DIVIDE(
          becas_por_1000_matriculados_5to
          - MIN(becas_por_1000_matriculados_5to) OVER (PARTITION BY anio),
          NULLIF(
            MAX(becas_por_1000_matriculados_5to) OVER (PARTITION BY anio)
            - MIN(becas_por_1000_matriculados_5to) OVER (PARTITION BY anio),
            0
          )
        ),
        0
      )
    END AS coverage_gap_score
  FROM coverage_with_rates
),
final_features AS (
  SELECT
    anio,
    region,
    region_canonical,
    pobreza_monetaria_pct,
    poblacion_15_24,
    poblacion_15_29,
    matricula_5to_secundaria,
    ruralidad_educativa_pct,
    internet_acceso_pct,
    brecha_digital_pct,
    context_priority_score,
    context_priority_rank,
    context_priority_tier,
    regional_becarios_pct,
    regional_becarios_estimated,
    total_becas_anual,
    primera_generacion_ratio_base,
    primera_generacion_score,
    total_becarios_primera_generacion,
    total_becarios_encuestados,
    becas_por_1000_jovenes,
    becas_por_1000_matriculados_5to,
    demanda_no_cubierta_estimada,
    coverage_gap_score,
    CASE
      WHEN regional_becarios_pct IS NOT NULL
       AND total_becas_anual IS NOT NULL
       AND regional_becarios_estimated IS NOT NULL
       AND becas_por_1000_matriculados_5to IS NOT NULL
       AND primera_generacion_ratio_base IS NOT NULL
        THEN 'complete'
      WHEN regional_becarios_estimated IS NOT NULL
       AND becas_por_1000_matriculados_5to IS NOT NULL
        THEN 'usable'
      WHEN regional_becarios_estimated IS NOT NULL OR primera_generacion_ratio_base IS NOT NULL
        THEN 'partial'
      ELSE 'insufficient'
    END AS coverage_data_quality_flag,
    coverage_source_method,
    coverage_source_notes,
    has_estimated_coverage,
    CURRENT_TIMESTAMP() AS created_at
  FROM coverage_scored
)
SELECT
  anio,
  region,
  region_canonical,
  pobreza_monetaria_pct,
  poblacion_15_24,
  poblacion_15_29,
  matricula_5to_secundaria,
  ruralidad_educativa_pct,
  internet_acceso_pct,
  brecha_digital_pct,
  context_priority_score,
  context_priority_rank,
  context_priority_tier,
  regional_becarios_pct,
  regional_becarios_estimated,
  total_becas_anual,
  primera_generacion_ratio_base AS primera_generacion_ratio,
  total_becarios_primera_generacion,
  total_becarios_encuestados,
  becas_por_1000_jovenes,
  becas_por_1000_matriculados_5to,
  demanda_no_cubierta_estimada,
  coverage_gap_score,
  primera_generacion_score,
  coverage_data_quality_flag,
  coverage_source_method,
  coverage_source_notes,
  has_estimated_coverage,
  created_at
FROM final_features;
