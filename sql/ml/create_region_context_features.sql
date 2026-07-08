-- ============================================================================
-- Project Cloud BI Platform
-- ML regional context foundation
-- ============================================================================

CREATE OR REPLACE VIEW `{project_id}.{ml_dataset}.region_context_features` AS
WITH
region_mapping AS (
  SELECT
    source_region,
    region_canonical,
    region_scope,
    mapping_rule,
    is_aggregated_region,
    notes,
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
region_dimension AS (
  SELECT DISTINCT region_canonical
  FROM region_mapping
),
calendar_years AS (
  SELECT year AS anio
  FROM UNNEST(GENERATE_ARRAY(2012, 2025)) AS year
),
base_grid AS (
  SELECT
    y.anio,
    r.region_canonical
  FROM calendar_years AS y
  CROSS JOIN region_dimension AS r
),
source_rows AS (
  SELECT
    anio,
    region AS source_region,
    'poverty' AS source_group,
    source_type AS poverty_source_type,
    NULL AS education_source_type,
    NULL AS internet_source_type,
    NULL AS internet_acceso_pct,
    pobreza_monetaria_pct,
    NULL AS poblacion_total,
    NULL AS poblacion_15_24,
    NULL AS poblacion_15_29,
    NULL AS matricula_total,
    NULL AS matricula_publica,
    NULL AS matricula_privada,
    NULL AS matricula_urbana,
    NULL AS matricula_rural
  FROM `{project_id}.{silver_dataset}.inei_pobreza_departamental`

  UNION ALL

  SELECT
    anio,
    region AS source_region,
    'population' AS source_group,
    NULL AS poverty_source_type,
    NULL AS education_source_type,
    NULL AS internet_source_type,
    NULL AS internet_acceso_pct,
    NULL AS pobreza_monetaria_pct,
    poblacion_total,
    poblacion_15_24,
    poblacion_15_29,
    NULL AS matricula_total,
    NULL AS matricula_publica,
    NULL AS matricula_privada,
    NULL AS matricula_urbana,
    NULL AS matricula_rural
  FROM `{project_id}.{silver_dataset}.inei_population_youth_region`

  UNION ALL

  SELECT
    anio,
    region AS source_region,
    'demographic' AS source_group,
    NULL AS poverty_source_type,
    NULL AS education_source_type,
    NULL AS internet_source_type,
    NULL AS internet_acceso_pct,
    NULL AS pobreza_monetaria_pct,
    NULL AS poblacion_total,
    NULL AS poblacion_15_24,
    NULL AS poblacion_15_29,
    NULL AS matricula_total,
    NULL AS matricula_publica,
    NULL AS matricula_privada,
    NULL AS matricula_urbana,
    NULL AS matricula_rural
  FROM `{project_id}.{silver_dataset}.inei_demographic_indicators_region`

  UNION ALL

  SELECT
    anio,
    region AS source_region,
    'internet' AS source_group,
    NULL AS poverty_source_type,
    NULL AS education_source_type,
    source_type AS internet_source_type,
    internet_acceso_pct,
    NULL AS pobreza_monetaria_pct,
    NULL AS poblacion_total,
    NULL AS poblacion_15_24,
    NULL AS poblacion_15_29,
    NULL AS matricula_total,
    NULL AS matricula_publica,
    NULL AS matricula_privada,
    NULL AS matricula_urbana,
    NULL AS matricula_rural
  FROM `{project_id}.{silver_dataset}.inei_internet_acceso_region`

  UNION ALL

  SELECT
    anio,
    COALESCE(region_normalizada, region) AS source_region,
    'education' AS source_group,
    NULL AS poverty_source_type,
    'official' AS education_source_type,
    NULL AS internet_source_type,
    NULL AS internet_acceso_pct,
    NULL AS pobreza_monetaria_pct,
    NULL AS poblacion_total,
    NULL AS poblacion_15_24,
    NULL AS poblacion_15_29,
    matricula_total,
    matricula_publica,
    matricula_privada,
    matricula_urbana,
    matricula_rural
  FROM `{project_id}.{silver_dataset}.minedu_matricula_secundaria_departamental`
  WHERE UPPER(TRIM(grado)) = 'QUINTO_GRADO'
),
normalized_source_rows AS (
  SELECT
    s.*,
    UPPER(
      TRIM(
        REGEXP_REPLACE(
          TRANSLATE(
            UPPER(COALESCE(s.source_region, '')),
            'ÁÀÂÄÃÉÈÊËÍÌÎÏÓÒÔÖÕÚÙÛÜÑ',
            'AAAAAEEEEIIIIOOOOOUUUUN'
          ),
          r'[^A-Z0-9 ]',
          ' '
        )
      )
    ) AS source_region_key
  FROM source_rows AS s
),
mapped_source_rows AS (
  SELECT
    n.anio,
    m.region_canonical,
    n.source_group,
    n.poverty_source_type,
    n.education_source_type,
    n.internet_source_type,
    n.internet_acceso_pct,
    n.pobreza_monetaria_pct,
    n.poblacion_total,
    n.poblacion_15_24,
    n.poblacion_15_29,
    n.matricula_total,
    n.matricula_publica,
    n.matricula_privada,
    n.matricula_urbana,
    n.matricula_rural
  FROM normalized_source_rows AS n
  JOIN region_mapping AS m
    ON n.source_region_key = m.source_region_key
),
pobreza_agg AS (
  SELECT
    anio,
    region_canonical,
    AVG(pobreza_monetaria_pct) AS pobreza_monetaria_pct,
    ANY_VALUE(poverty_source_type) AS poverty_source_type
  FROM mapped_source_rows
  WHERE pobreza_monetaria_pct IS NOT NULL
  GROUP BY anio, region_canonical
),
population_agg AS (
  SELECT
    anio,
    region_canonical,
    SUM(poblacion_total) AS poblacion_total,
    SUM(poblacion_15_24) AS poblacion_15_24,
    SUM(poblacion_15_29) AS poblacion_15_29
  FROM mapped_source_rows
  WHERE poblacion_total IS NOT NULL
     OR poblacion_15_24 IS NOT NULL
     OR poblacion_15_29 IS NOT NULL
  GROUP BY anio, region_canonical
),
internet_agg AS (
  SELECT
    anio,
    region_canonical,
    AVG(internet_acceso_pct) AS internet_acceso_pct,
    ANY_VALUE(internet_source_type) AS internet_source_type
  FROM mapped_source_rows
  WHERE internet_acceso_pct IS NOT NULL
  GROUP BY anio, region_canonical
),
minedu_agg AS (
  SELECT
    anio,
    region_canonical,
    SUM(matricula_total) AS matricula_5to_secundaria,
    SUM(matricula_publica) AS matricula_5to_publica,
    SUM(matricula_privada) AS matricula_5to_privada,
    SUM(matricula_urbana) AS matricula_5to_urbana,
    SUM(matricula_rural) AS matricula_5to_rural,
    ANY_VALUE(education_source_type) AS education_source_type
  FROM mapped_source_rows
  WHERE source_group = 'education'
  GROUP BY anio, region_canonical
),
source_presence AS (
  SELECT
    anio,
    region_canonical,
    COUNT(DISTINCT source_group) AS source_group_count
  FROM mapped_source_rows
  GROUP BY anio, region_canonical
)
SELECT
  g.anio,
  g.region_canonical AS region,
  g.region_canonical,
  p.pobreza_monetaria_pct,
  p.poverty_source_type,
  pop.poblacion_total,
  pop.poblacion_15_24,
  pop.poblacion_15_29,
  CASE
    WHEN COALESCE(pop.poblacion_total, 0) > 0
      AND COALESCE(pop.poblacion_15_29, pop.poblacion_15_24) IS NOT NULL
      THEN SAFE_DIVIDE(
        CAST(COALESCE(pop.poblacion_15_29, pop.poblacion_15_24) AS FLOAT64),
        CAST(pop.poblacion_total AS FLOAT64)
      ) * 100
    ELSE NULL
  END AS poblacion_joven_pct,
  m.matricula_5to_secundaria,
  m.matricula_5to_publica,
  m.matricula_5to_privada,
  m.matricula_5to_urbana,
  m.matricula_5to_rural,
  CASE
    WHEN m.matricula_5to_secundaria IS NOT NULL AND m.matricula_5to_secundaria > 0
      THEN SAFE_DIVIDE(
        CAST(m.matricula_5to_rural AS FLOAT64),
        CAST(m.matricula_5to_secundaria AS FLOAT64)
      ) * 100
    ELSE NULL
  END AS ruralidad_educativa_pct,
  m.education_source_type,
  i.internet_acceso_pct,
  CASE
    WHEN i.internet_acceso_pct IS NOT NULL THEN 100 - i.internet_acceso_pct
    ELSE NULL
  END AS brecha_digital_pct,
  i.internet_source_type,
  SAFE_DIVIDE(
    CAST(
      (
        (CASE WHEN p.pobreza_monetaria_pct IS NOT NULL THEN 1 ELSE 0 END)
        + (CASE WHEN COALESCE(pop.poblacion_15_29, pop.poblacion_15_24) IS NOT NULL THEN 1 ELSE 0 END)
        + (CASE WHEN m.matricula_5to_secundaria IS NOT NULL THEN 1 ELSE 0 END)
        + (CASE WHEN i.internet_acceso_pct IS NOT NULL THEN 1 ELSE 0 END)
        + (CASE WHEN pop.poblacion_total IS NOT NULL THEN 1 ELSE 0 END)
      ) AS FLOAT64
    ),
    5.0
  ) AS feature_completeness_score,
  CASE
    WHEN SAFE_DIVIDE(
      CAST(
        (
          (CASE WHEN p.pobreza_monetaria_pct IS NOT NULL THEN 1 ELSE 0 END)
          + (CASE WHEN COALESCE(pop.poblacion_15_29, pop.poblacion_15_24) IS NOT NULL THEN 1 ELSE 0 END)
          + (CASE WHEN m.matricula_5to_secundaria IS NOT NULL THEN 1 ELSE 0 END)
          + (CASE WHEN i.internet_acceso_pct IS NOT NULL THEN 1 ELSE 0 END)
          + (CASE WHEN pop.poblacion_total IS NOT NULL THEN 1 ELSE 0 END)
        ) AS FLOAT64
      ),
      5.0
    ) >= 0.9 THEN 'complete'
    WHEN SAFE_DIVIDE(
      CAST(
        (
          (CASE WHEN p.pobreza_monetaria_pct IS NOT NULL THEN 1 ELSE 0 END)
          + (CASE WHEN COALESCE(pop.poblacion_15_29, pop.poblacion_15_24) IS NOT NULL THEN 1 ELSE 0 END)
          + (CASE WHEN m.matricula_5to_secundaria IS NOT NULL THEN 1 ELSE 0 END)
          + (CASE WHEN i.internet_acceso_pct IS NOT NULL THEN 1 ELSE 0 END)
          + (CASE WHEN pop.poblacion_total IS NOT NULL THEN 1 ELSE 0 END)
        ) AS FLOAT64
      ),
      5.0
    ) >= 0.7 THEN 'usable'
    WHEN SAFE_DIVIDE(
      CAST(
        (
          (CASE WHEN p.pobreza_monetaria_pct IS NOT NULL THEN 1 ELSE 0 END)
          + (CASE WHEN COALESCE(pop.poblacion_15_29, pop.poblacion_15_24) IS NOT NULL THEN 1 ELSE 0 END)
          + (CASE WHEN m.matricula_5to_secundaria IS NOT NULL THEN 1 ELSE 0 END)
          + (CASE WHEN i.internet_acceso_pct IS NOT NULL THEN 1 ELSE 0 END)
          + (CASE WHEN pop.poblacion_total IS NOT NULL THEN 1 ELSE 0 END)
        ) AS FLOAT64
      ),
      5.0
    ) >= 0.4 THEN 'partial'
    ELSE 'insufficient'
  END AS feature_quality_flag,
  FALSE AS has_synthetic_values,
  NULL AS synthetic_fields,
  CASE
    WHEN COALESCE(sp.source_group_count, 0) > 1 THEN 'mixed'
    WHEN COALESCE(sp.source_group_count, 0) = 1 THEN 'official'
    ELSE 'official'
  END AS source_priority,
  CURRENT_TIMESTAMP() AS created_at
FROM base_grid AS g
LEFT JOIN pobreza_agg AS p
  ON p.anio = g.anio
 AND p.region_canonical = g.region_canonical
LEFT JOIN population_agg AS pop
  ON pop.anio = g.anio
 AND pop.region_canonical = g.region_canonical
LEFT JOIN internet_agg AS i
  ON i.anio = g.anio
 AND i.region_canonical = g.region_canonical
LEFT JOIN minedu_agg AS m
  ON m.anio = g.anio
 AND m.region_canonical = g.region_canonical
LEFT JOIN source_presence AS sp
  ON sp.anio = g.anio
 AND sp.region_canonical = g.region_canonical;
