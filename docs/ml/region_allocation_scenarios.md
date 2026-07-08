# Escenarios de asignacion regional

## Objetivo

`ml.region_allocation_scenarios` simula como se distribuiria un presupuesto forecast entre regiones bajo distintas reglas de priorizacion.

La vista responde preguntas de Power BI sobre cambios de ranking, distribucion porcentual, presupuesto estimado por region y becas estimadas referenciales.

## Fuentes

- `ml.budget_scenarios`
- `ml.region_priority_scores_v2`
- `ml.region_priority_scores`
- `ml.region_context_features`
- `ml.budget_forecast_results`
- `silver.presupuesto_mef`
- `silver.pronabec_report_beca18_becas_otorgadas_modalidad_anual`

No usa Bronze ni `presupuesto_mef_departamento`.

## Grano

`anio + region_canonical + scenario_id`, cubriendo simulacion historica regional 2012-2025.

## Metricas principales

- `scenario_raw_score`: score del escenario segun la regla seleccionada.
- `allocation_weight`: peso normalizado por `anio + scenario_id`.
- `allocation_pct`: porcentaje equivalente al peso.
- `scenario_rank`: ranking regional dentro del escenario.
- `rank_change_vs_v2`: diferencia frente al ranking base v2; positivo significa que la region sube posiciones.
- `reference_forecast_budget_amount`: suma del forecast de los proximos 12 meses.
- `scenario_budget_amount`: forecast de referencia multiplicado por el escenario.
- `estimated_budget_amount`: presupuesto regional estimado.
- `estimated_scholarships`: becas estimadas usando costo promedio referencial por beca.

## Becas estimadas

`estimated_scholarships` usa un costo promedio referencial calculado como `SUM(devengado) / SUM(becas_otorgadas)` para el ultimo año con datos validos en Silver.

Este ratio es financiero agregado e indirecto. No representa costo directo individual ni compromiso presupuestal oficial.

## Limitaciones

- Son escenarios simulados/prescriptivos, no asignaciones oficiales.
- No hay causalidad.
- No hay prediccion individual.
- No se modela comportamiento de estudiantes ni demanda individual.
- Gold solo expone esta vista mediante `gold.vw_predictive_region_allocation_scenarios`.
