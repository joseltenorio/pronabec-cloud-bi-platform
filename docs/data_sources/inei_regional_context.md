# Contexto regional INEI

## Alcance

El contexto regional INEI se integra como una fuente externa/manual. Se espera que los archivos CSV ya existan en Cloud Storage bajo:

```text
gs://${GCS_BUCKET_NAME}/landing/inei_reports/
```

Este pipeline cubre únicamente:

```text
Landing -> Bronze -> Silver -> Calidad
```

Las vistas Gold predictivas, BigQuery ML, puntaje regional, cruces con MINEDU y modelado de escenarios presupuestales quedan intencionalmente fuera del alcance de este PR.

## Datasets

| Tabla Silver | Archivo Landing | Propósito |
| --- | --- | --- |
| `silver.inei_population_youth_region` | `inei_population_youth_region.csv` | Indicadores regionales de población joven. |
| `silver.inei_demographic_indicators_region` | `inei_demographic_indicators_region.csv` | Indicadores demográficos regionales. |
| `silver.inei_pobreza_departamental` | `inei_pobreza_departamental_2012_2025.csv` | Porcentaje de pobreza monetaria a nivel departamental. |
| `silver.inei_internet_acceso_region` | `inei_internet_acceso_region_2012_2025_final.csv` | Porcentaje regional de acceso a internet. |

## Flujo

1. `tools.stage_inei_reports` copia cada CSV esperado desde Landing hacia una ruta Bronze particionada.
2. Bronze almacena el CSV original como `data.csv` y escribe `manifest.json` junto con `_SUCCESS`.
3. `pipelines.dataflow_bronze_to_silver` se ejecuta con `source_system=inei_reports` y transforma cada CSV en registros Silver tipados.
4. `sql/quality/data_quality_checks.sql` valida campos requeridos, rangos y claves duplicadas `(anio, region)`.

Las rutas Bronze siguen este patrón:

```text
gs://${GCS_BUCKET_NAME}/bronze/inei_reports/{dataset}/extraction_date=YYYY-MM-DD/data.csv
```

## Uso futuro

Estas tablas Silver están pensadas para alimentar posteriormente una tabla `ml.region_context_features` junto con indicadores regionales de PRONABEC y MINEDU. En trabajos futuros podrán usarse para agrupamiento territorial, análisis de escenarios de cobertura y priorización regional, pero nada de eso se implementa en esta rama.
