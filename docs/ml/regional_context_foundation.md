# Fundación regional ML

## Objetivo

Este módulo construye la base regional integrada que servirá como insumo para simulación, priorización territorial y modelado futuro. La intención no es predecir resultados individuales de estudiantes, porque el proyecto no dispone de llaves ni trazabilidad individual suficiente para hacerlo con rigor.

En su lugar, se arma una capa `ml` con contexto regional unificado desde `silver`, combinando pobreza monetaria, población joven, matrícula de secundaria, ruralidad educativa e internet. Esta base prepara la siguiente fase de la plataforma predictiva sin implementar todavía clustering, forecast, regresiones ni modelos supervisados.

## Fuentes

La capa consume exclusivamente tablas Silver:

- `silver.inei_pobreza_departamental`
- `silver.inei_population_youth_region`
- `silver.inei_demographic_indicators_region`
- `silver.inei_internet_acceso_region`
- `silver.minedu_matricula_secundaria_departamental`

No se lee desde Bronze para esta base. No se usa `presupuesto_mef_departamento`.

## Grano

La tabla/vista `ml.region_context_features` se construye al grano:

```text
anio + region_canonical
```

La cobertura v1 se restringe a `2012-2025` y a las 25 regiones canónicas del país, con tratamiento especial para:

- `LIMA METROPOLITANA` -> `LIMA`
- `LIMA PROVINCIAS` -> `LIMA`
- `PROV. CONST. DEL CALLAO` -> `CALLAO`
- `PROVINCIA CONSTITUCIONAL DEL CALLAO` -> `CALLAO`

## Normalización regional

La dimensión `ml.dim_region_mapping` estandariza nombres regionales y conserva trazabilidad:

- `source_region`
- `region_canonical`
- `region_scope`
- `mapping_rule`
- `is_aggregated_region`
- `notes`

La normalización resuelve mayúsculas, tildes, espacios dobles y variantes comunes para que todos los insumos confluyan en un único `region_canonical`.

## Columnas generadas

`ml.region_context_features` expone:

- pobreza monetaria y tipo de fuente
- población total y población joven
- porcentaje de población joven
- matrícula de quinto de secundaria por total, pública, privada, urbana y rural
- ruralidad educativa
- acceso a internet y brecha digital
- score de completitud
- bandera de calidad
- metadata sintética / manual
- prioridad de fuente
- timestamp de materialización

## Criterios metodológicos

- La matrícula de quinto de secundaria se usa como proxy de demanda potencial inmediata.
- El acceso a internet se usa como proxy de brecha digital regional.
- La pobreza monetaria se usa como proxy de vulnerabilidad territorial.
- La población joven se usa como denominador contextual para entender escala y presión demográfica.
- El módulo no infiere causalidad sobre reducción de pobreza ni sobre impacto real de una beca.

## Regional priority score v1

La siguiente salida de esta base es `ml.region_priority_scores`, un score regional explicable que prioriza regiones según necesidad social, demanda educativa, población joven, ruralidad educativa y brecha digital.

Puntos metodológicos:

- No es un modelo individual de estudiantes.
- No es causal ni pretende medir impacto real de una beca.
- Usa únicamente `ml.region_context_features` como fuente.
- Normaliza cada componente por año mediante min-max.
- Ajusta el score final por componentes disponibles cuando faltan valores.
- Conserva `feature_completeness_score` y `feature_quality_flag` para lectura de confiabilidad.
- No incorpora cobertura PRONABEC todavía.

## Datos sintéticos y manuales

La versión actual privilegia datos oficiales/manuales provenientes de Silver. Si en el futuro se rellena un hueco con promedio regional, carry-forward o cualquier otra imputación, el registro deberá marcarse explícitamente con:

- `has_synthetic_values = true`
- `synthetic_fields`
- `source_priority = synthetic_demo` o `mixed`

En v1, la intención es no sintetizar salvo necesidad operativa clara.

## Limitaciones

- No hay predicción individual.
- No hay KMeans, ARIMA_PLUS, regresión ni scoring final en esta rama.
- Lima y Callao se colapsan en una sola unidad canónica cada una.
- La tabla es una base de features, no un modelo ML.

## Próximos pasos

La siguiente rama podrá construir sobre esta base:

- `ml.region_priority_scores`
- `gold.vw_predictive_region_priority_scores`
- escenarios presupuestales
- simulación de asignación regional
- clustering territorial
