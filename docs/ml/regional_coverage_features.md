# Cobertura regional PRONABEC

## Objetivo

`ml.region_coverage_features` combina el contexto regional INEI/MINEDU con métricas PRONABEC para estimar cobertura relativa, brecha de cobertura y primera generación.

## Fuentes

- `ml.region_context_features`
- `ml.region_priority_scores`
- `ml.dim_region_mapping`
- `silver.pronabec_report_beca18_region_postulacion_anual`
- `silver.pronabec_report_beca18_becas_otorgadas_modalidad_anual`
- `silver.pronabec_report_beca18_primera_generacion_region`

## Nota metodológica

Cuando no existe conteo regional real de becarios, la cobertura se estima desde el porcentaje regional observado y el total anual de becas otorgadas. Ese proxy debe tratarse como una aproximación analítica, no como una medida causal o censal.

## Salidas

- `regional_becarios_pct`
- `regional_becarios_estimated`
- `becas_por_1000_jovenes`
- `becas_por_1000_matriculados_5to`
- `coverage_gap_score`
- `primera_generacion_score`
- `coverage_data_quality_flag`
- `coverage_source_method`

## Alcance

La vista alimenta el modelo KMeans regional (`ml.model_region_clusters`) y el score regional v2. No implementa forecast, simulación presupuestal ni modelos supervisados.
