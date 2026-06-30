# Cloud Run Jobs

## Proposito

Cloud Run Jobs es la capa de ejecucion serverless para procesos batch de PRONABEC Cloud BI Platform. Ejecuta componentes Python versionados dentro de una imagen Docker comun, manteniendo separadas la orquestacion, la extraccion, el staging, la validacion y la calidad de datos.

## Jobs PRONABEC particionados

### `pronabec-discovery-job`

Ejecuta `python -m pipelines.discover_pronabec`. Calcula `effective_page_size`, observa `total_records` y `total_pages`, y escribe `discovery.json` en `bronze_work/pronabec/_plans/`.

### `pronabec-build-plan-job`

Ejecuta `python -m pipelines.build_pronabec_extraction_plan`. Construye `plan.json` a partir de `discovery.json`.

### `pronabec-run-plan-job`

Ejecuta `python -m pipelines.run_pronabec_extraction_plan`. Lee `plan.json` y ejecuta exactamente los chunks definidos en el plan. Este es el job principal para la extraccion PRONABEC particionada.

### `pronabec-extract-chunk-job`

Ejecuta `python -m pipelines.extract_pronabec`. Se conserva para debug manual o reprocesos de chunks aislados con `SOURCE_DATASET`, `PAGE_START`, `PAGE_END` y `OUTPUT_MODE=chunk`.

### `pronabec-finalize-dataset-job`

Ejecuta `python -m pipelines.finalize_pronabec_dataset`. Consolida los chunks intermedios hacia Bronze final y escribe `data.jsonl`, `manifest.json` y `_SUCCESS`.

### `pronabec-extract-job`

Extractor legado de PRONABEC. Se mantiene por compatibilidad y pruebas antiguas, pero no es la ruta principal del flujo particionado.

## Jobs MEF, reportes, Gold y calidad

- `mef-extract-job`: ejecuta `pipelines.scrape_mef_budget`.
- `pronabec-stage-reports-job`: ejecuta el staging de reportes documentales.
- `bronze-manifest-validation-job`: valida manifests y `_SUCCESS` antes de promover Bronze a Silver.
- `dataflow-pronabec-report-job`: job parametrizable unico para los 23 reportes documentales.
- `gold-publish-job`: publica vistas Gold.
- `gold-validate-job`: valida contratos Gold.
- `quality-checks-job`: ejecuta controles de calidad.

## Estrategia operativa

La extraccion PRONABEC particionada sigue esta secuencia:

```text
pronabec-discovery-job
pronabec-build-plan-job
pronabec-run-plan-job
pronabec-finalize-dataset-job
```

`bronze_work/` es temporal. Dataflow no debe leer `bronze_work`; solo Bronze final consolidado por `pronabec-finalize-dataset-job` es consumible por `validate_bronze_manifests` y por Silver.

## Imagen y despliegue

La plataforma usa una imagen Docker comun para todos los jobs. El deploy script registra o actualiza los jobs con la misma imagen, service account, bucket y variables base, variando solo el modulo Python o los argumentos del contenedor.

Si cambian modulos Python, reconstruya y publique la imagen antes de redeployar los Cloud Run Jobs. Si solo cambian DAG, configuracion o documentos, basta con subir los artefactos de Composer y actualizar variables.
