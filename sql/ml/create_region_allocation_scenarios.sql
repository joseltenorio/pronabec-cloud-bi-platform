-- ============================================================================
-- Project Cloud BI Platform
-- ML prescriptive regional allocation scenarios
-- ============================================================================

CREATE OR REPLACE VIEW `{project_id}.{ml_dataset}.region_allocation_scenarios` AS
WITH regional_features AS (
  SELECT
    v2.anio,
    v2.region,
    v2.region_canonical,
    v2.priority_score_v2,
    v2.context_priority_score,
    COALESCE(v2.coverage_gap_score, 0) AS coverage_gap_score,
    COALESCE(v2.primera_generacion_score, 0) AS primera_generacion_score,
    v2.priority_rank_v2 AS baseline_rank_v2,
    p.pobreza_score AS poverty_score,
    p.demanda_educativa_score AS demand_score,
    p.poblacion_joven_score AS population_score,
    p.brecha_digital_score AS digital_gap_score,
    p.ruralidad_score AS rurality_score
  FROM `{project_id}.{ml_dataset}.region_context_features` AS cf
  INNER JOIN `{project_id}.{ml_dataset}.region_priority_scores_v2` AS v2
    ON cf.anio = v2.anio
    AND cf.region_canonical = v2.region_canonical
  LEFT JOIN `{project_id}.{ml_dataset}.region_priority_scores` AS p
    ON v2.anio = p.anio
    AND v2.region_canonical = p.region_canonical
  WHERE v2.anio BETWEEN 2012 AND 2025
),
forecast_reference AS (
  SELECT
    SUM(CAST(forecast_value AS FLOAT64)) AS reference_forecast_budget_amount
  FROM `{project_id}.{ml_dataset}.budget_forecast_results`
),
annual_cost_reference AS (
  SELECT
    COALESCE(becas.ano, presupuesto.ano) AS anio,
    SAFE_DIVIDE(presupuesto.devengado_total, NULLIF(becas.becas_otorgadas_total, 0)) AS devengado_por_beca
  FROM (
    SELECT
      ano_convocatoria AS ano,
      SUM(becas_otorgadas) AS becas_otorgadas_total
    FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_becas_otorgadas_modalidad_anual`
    GROUP BY ano_convocatoria
  ) AS becas
  FULL OUTER JOIN (
    SELECT
      ano,
      SUM(devengado) AS devengado_total
    FROM `{project_id}.{silver_dataset}.presupuesto_mef`
    GROUP BY ano
  ) AS presupuesto
    ON becas.ano = presupuesto.ano
),
latest_cost_reference AS (
  SELECT
    ARRAY_AGG(
      CAST(devengado_por_beca AS FLOAT64)
      IGNORE NULLS
      ORDER BY anio DESC
      LIMIT 1
    )[SAFE_OFFSET(0)] AS costo_promedio_beca_referencia
  FROM annual_cost_reference
  WHERE devengado_por_beca > 0
),
scored AS (
  SELECT
    rf.anio,
    rf.region,
    rf.region_canonical,
    s.scenario_id,
    s.scenario_name,
    s.scenario_type,
    s.allocation_method,
    s.budget_multiplier,
    s.scholarship_multiplier,
    rf.priority_score_v2,
    rf.context_priority_score,
    rf.coverage_gap_score,
    rf.primera_generacion_score,
    rf.poverty_score,
    rf.demand_score,
    rf.population_score,
    rf.digital_gap_score,
    rf.rurality_score,
    CASE s.allocation_method
      WHEN 'priority_score_v2' THEN COALESCE(rf.priority_score_v2, 0)
      WHEN 'poverty_weighted' THEN
        s.poverty_weight * COALESCE(rf.poverty_score, 0)
        + s.demand_weight * COALESCE(rf.demand_score, 0)
        + s.population_weight * COALESCE(rf.population_score, 0)
        + s.first_generation_weight * COALESCE(rf.primera_generacion_score, 0)
        + s.coverage_weight * COALESCE(rf.coverage_gap_score, 0)
      WHEN 'demand_population_weighted' THEN
        s.demand_weight * COALESCE(rf.demand_score, 0)
        + s.population_weight * COALESCE(rf.population_score, 0)
        + s.poverty_weight * COALESCE(rf.poverty_score, 0)
        + s.first_generation_weight * COALESCE(rf.primera_generacion_score, 0)
        + s.coverage_weight * COALESCE(rf.coverage_gap_score, 0)
      WHEN 'first_generation_weighted' THEN
        s.first_generation_weight * COALESCE(rf.primera_generacion_score, 0)
        + s.poverty_weight * COALESCE(rf.poverty_score, 0)
        + s.demand_weight * COALESCE(rf.demand_score, 0)
        + s.population_weight * COALESCE(rf.population_score, 0)
        + s.coverage_weight * COALESCE(rf.coverage_gap_score, 0)
      WHEN 'multi_factor_weighted' THEN
        s.poverty_weight * COALESCE(rf.poverty_score, 0)
        + s.demand_weight * COALESCE(rf.demand_score, 0)
        + s.population_weight * COALESCE(rf.population_score, 0)
        + s.first_generation_weight * COALESCE(rf.primera_generacion_score, 0)
        + s.coverage_weight * COALESCE(rf.coverage_gap_score, 0)
      ELSE 0
    END AS scenario_raw_score,
    rf.baseline_rank_v2,
    fr.reference_forecast_budget_amount,
    s.scenario_version,
    'Simulacion prescriptiva basada en scores regionales; no es asignacion oficial, prediccion individual ni inferencia causal.' AS scenario_notes,
    s.created_at
  FROM regional_features AS rf
  CROSS JOIN `{project_id}.{ml_dataset}.budget_scenarios` AS s
  CROSS JOIN forecast_reference AS fr
),
allocated AS (
  SELECT
    scored.*,
    SAFE_DIVIDE(
      scenario_raw_score,
      NULLIF(SUM(scenario_raw_score) OVER (PARTITION BY anio, scenario_id), 0)
    ) AS allocation_weight,
    RANK() OVER (
      PARTITION BY anio, scenario_id
      ORDER BY scenario_raw_score DESC, region_canonical
    ) AS scenario_rank
  FROM scored
)
SELECT
  allocated.anio,
  allocated.region,
  allocated.region_canonical,
  allocated.scenario_id,
  allocated.scenario_name,
  allocated.scenario_type,
  allocated.allocation_method,
  allocated.budget_multiplier,
  allocated.scholarship_multiplier,
  allocated.priority_score_v2,
  allocated.context_priority_score,
  allocated.coverage_gap_score,
  allocated.primera_generacion_score,
  allocated.poverty_score,
  allocated.demand_score,
  allocated.population_score,
  allocated.digital_gap_score,
  allocated.rurality_score,
  allocated.scenario_raw_score,
  allocated.allocation_weight,
  SAFE_MULTIPLY(allocated.allocation_weight, 100) AS allocation_pct,
  allocated.scenario_rank,
  allocated.baseline_rank_v2,
  allocated.baseline_rank_v2 - allocated.scenario_rank AS rank_change_vs_v2,
  allocated.reference_forecast_budget_amount,
  allocated.reference_forecast_budget_amount * allocated.budget_multiplier AS scenario_budget_amount,
  allocated.reference_forecast_budget_amount * allocated.budget_multiplier * allocated.allocation_weight AS estimated_budget_amount,
  SAFE_MULTIPLY(
    SAFE_DIVIDE(
      allocated.reference_forecast_budget_amount * allocated.budget_multiplier * allocated.allocation_weight,
      cost.costo_promedio_beca_referencia
    ),
    allocated.scholarship_multiplier
  ) AS estimated_scholarships,
  allocated.scenario_version,
  allocated.scenario_notes,
  allocated.created_at
FROM allocated
CROSS JOIN latest_cost_reference AS cost;
