# Escenarios presupuestales

## Objetivo

`ml.budget_scenarios` define los escenarios prescriptivos usados para comparar reglas de asignacion regional en Power BI.

Es una vista estatica, versionada y parametrica. No lee Bronze, no crea estudiantes sinteticos y no ejecuta prediccion individual.

## Escenarios

| scenario_id | Enfoque | Multiplicador presupuesto |
| --- | --- | --- |
| `base_priority` | Score regional v2 balanceado | 1.00 |
| `budget_plus_10` | Score regional v2 con presupuesto +10% | 1.10 |
| `budget_plus_20` | Score regional v2 con presupuesto +20% | 1.20 |
| `budget_minus_10` | Score regional v2 con presupuesto -10% | 0.90 |
| `poverty_focus` | Prioridad a pobreza | 1.00 |
| `demand_population_focus` | Prioridad a demanda educativa y poblacion joven | 1.00 |
| `first_generation_focus` | Prioridad a primera generacion | 1.00 |
| `balanced_equity_demand` | Balance equidad-demanda | 1.00 |

## Limitaciones

- Son escenarios simulados/prescriptivos, no asignaciones oficiales.
- No hay causalidad.
- No hay prediccion individual.
- `coverage_gap_score` tiene peso bajo o balanceado porque actualmente no debe dominar la decision.
