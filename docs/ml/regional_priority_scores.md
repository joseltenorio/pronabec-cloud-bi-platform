# Score regional de prioridad predictiva

## 1. Objetivo

`ml.region_priority_scores` resume, de forma interpretable, qué regiones requieren mayor prioridad de intervención según contexto social, presión educativa y brechas estructurales. La primera exposición de negocio se publica en `gold.vw_predictive_region_priority_scores` para Power BI.

## 2. Fuente

La única fuente de cálculo es:

- `ml.region_context_features`

No se lee desde Bronze ni desde Silver de forma directa.

## 3. Grano

El grano lógico es:

```text
anio + region_canonical
```

## 4. Componentes

- `pobreza_monetaria_pct`
- `matricula_5to_secundaria`
- `poblacion_15_29` con fallback a `poblacion_15_24`
- `ruralidad_educativa_pct`
- `brecha_digital_pct`

Todos los componentes se normalizan por año con min-max antes de combinarse.

## 5. Pesos v1

- `pobreza_score`: 0.35
- `demanda_educativa_score`: 0.25
- `poblacion_joven_score`: 0.15
- `brecha_digital_score`: 0.15
- `ruralidad_score`: 0.10

## 6. Fórmula

```text
priority_score =
(
  pobreza_score * 0.35
  + demanda_educativa_score * 0.25
  + poblacion_joven_score * 0.15
  + brecha_digital_score * 0.15
  + ruralidad_score * 0.10
) / available_weight
```

`available_weight` solo suma los pesos de los componentes que sí están disponibles.

## 7. Tiers

- `priority_score >= 0.80` -> `Muy alta`
- `priority_score >= 0.60` -> `Alta`
- `priority_score >= 0.40` -> `Media`
- `priority_score >= 0.20` -> `Baja`
- `priority_score IS NULL` -> `Insuficiente`

## 8. Limitaciones

- No explica causalidad.
- No reemplaza análisis territorial detallado.
- No incorpora cobertura PRONABEC aún.
- Si todos los componentes faltan, el score queda `NULL`.

## 9. Uso en Power BI

La vista Gold expone:

- `priority_score`
- `priority_score_pct`
- `priority_rank`
- `priority_tier`
- `priority_label`

Esto permite tableros de ranking, mapas coropléticos y segmentación por banda de prioridad.

## 10. Próximos pasos

- Agregar cobertura PRONABEC.
- Construir escenarios de asignación.
- Evaluar KMeans territorial.
- Incorporar forecast cuando exista una hipótesis presupuestal estable.
