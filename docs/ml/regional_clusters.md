# Clusters regionales BigQuery ML

## Objetivo

`ml.model_region_clusters` segmenta regiones con BigQuery ML usando KMeans sobre features regionales agregadas. Es un modelo no supervisado: no predice estudiantes, no estima causalidad y no asigna riesgo individual.

## Fuente

- `ml.region_coverage_features`
- `ml.region_priority_scores_v2` para anexar el score v2 a las asignaciones

## Features usadas

- `context_priority_score`
- `coverage_gap_score`
- `primera_generacion_score`
- `pobreza_monetaria_pct`
- `matricula_5to_secundaria`
- `poblacion_15_29`
- `brecha_digital_pct`
- `ruralidad_educativa_pct`
- `primera_generacion_ratio`
- `becas_por_1000_jovenes`
- `becas_por_1000_matriculados_5to`

## Objetos

- `ml.model_region_clusters`: modelo KMeans con 4 clusters y estandarizacion de features.
- `ml.region_cluster_assignments`: vista con `ML.PREDICT` para asignar centroides a cada region y anio.
- `ml.region_cluster_profiles`: vista de promedios por cluster para interpretar los centroides.
- `gold.vw_predictive_region_clusters`: salida Gold para Power BI.
- `gold.vw_predictive_region_cluster_profiles`: perfil Gold para Power BI.

## Interpretacion

Las etiquetas son genericas (`Cluster 1` a `Cluster 4`). La interpretacion sustantiva debe validarse con los perfiles promedio, no asumirse desde el numero de centroide.

## Limites

- No hay prediccion individual.
- No hay clasificacion supervisada.
- No hay inferencia causal.
- Los clusters dependen de la disponibilidad y calidad de las features regionales agregadas.
