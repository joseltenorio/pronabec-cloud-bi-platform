# Score regional de prioridad v2

## Objetivo

`ml.region_priority_scores_v2` mejora el score contextual v1 agregando señales de cobertura PRONABEC y primera generación.

## Fórmula

```text
priority_score_v2 =
0.60 * context_priority_score
+ 0.30 * coverage_gap_score
+ 0.10 * primera_generacion_score
```

La ponderación se ajusta por disponibilidad de componentes, igual que en v1.

## Fuente

- `ml.region_coverage_features`

## Interpretación

- Más brecha de cobertura implica mayor prioridad.
- Más proporción de primera generación implica mayor prioridad.
- El score sigue siendo explicable y no causal.

## Consumo Gold

La vista Gold `gold.vw_predictive_region_priority_scores_v2` expone el score ya calculado y no lo recalcula.

El score tambien se anexa a `ml.region_cluster_assignments` para facilitar el analisis de clusters regionales en Power BI. La interpretacion de clusters debe hacerse con perfiles promedio, no con el numero de centroide.
