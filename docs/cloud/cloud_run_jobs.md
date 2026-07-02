# Cloud Run Jobs

## Proposito

Cloud Run Jobs es la capa de ejecucion serverless para procesos batch de PRONABEC Cloud BI Platform. Ejecuta componentes Python versionados dentro de una imagen Docker comun, manteniendo separadas la orquestacion, la extraccion, el staging, la validacion y la calidad de datos.

## Jobs PRONABEC particionados

### `pronabec-discovery-job`

Ejecuta `python -m pipelines.discover_pronabec`. Calcula `effective_page_size`, observa `total_records` y `total_pages`, y escribe `discovery.json` en `bronze_work/pronabec/_plans/` para todos los datasets `bronze_enabled=true`.

### `pronabec-build-plan-job`

Ejecuta `python -m pipelines.build_pronabec_extraction_plan`. Construye `plan.json` a partir de `discovery.json` para todos los datasets Bronze habilitados.

### `pronabec-run-plan-job`

Ejecuta `python -m pipelines.run_pronabec_extraction_plan`. Lee `plan.json` y ejecuta exactamente los chunks definidos en el plan. Este es el job principal para la extraccion PRONABEC particionada.

### `pronabec-extract-chunk-job`

Ejecuta `python -m pipelines.extract_pronabec`. Se conserva para debug manual o reprocesos de chunks aislados con `SOURCE_DATASET`, `PAGE_START`, `PAGE_END` y `OUTPUT_MODE=chunk`.

### `pronabec-finalize-dataset-job`

Ejecuta `python -m pipelines.finalize_pronabec_dataset`. Consolida los chunks intermedios hacia Bronze final y escribe `data.jsonl`, `manifest.json` y `_SUCCESS`.

*Nota: Este job está configurado con 2Gi de memoria para evitar eventos de Out-Of-Memory (OOM) al consolidar grandes volúmenes de datos procedentes de chunks de datasets grandes (como `convocatorias_carrera_sede`).*

## Jobs MEF, reportes, Gold y calidad

- `mef-extract-job`: ejecuta `pipelines.scrape_mef_budget`.
- `pronabec-stage-reports-job`: ejecuta `python -m tools.stage_pronabec_manual_reports --strict --overwrite` para que los imports `pipelines.common.*` funcionen dentro de la imagen.
- `bronze-manifest-validation-job`: valida manifests y `_SUCCESS` antes de promover Bronze a Silver.
- `dataflow-pronabec-report-job`: job parametrizable unico para los 23 reportes documentales.
- `gold-publish-job`: publica vistas Gold.
- `gold-validate-job`: valida contratos Gold.
- `quality-checks-job`: ejecuta controles de calidad.

### Runtime MEF

`mef-extract-job` usa Consulta Amigable y queda configurado durante `scripts/deploy_cloud_run_jobs.ps1`. Composer o una ejecucion manual no deben repetir todo el runtime MEF; basta con pasar `BRONZE_EXTRACTION_DATE` y `PIPELINE_RUN_ID`.

El contrato operativo del job cubre el rango historico 2012-2026, la unidad ejecutora PRONABEC y los 10 breakdown slices:

```text
producto,generica,fuente,rubro,departamento,temporal,producto_temporal,actividad,actividad_temporal,generica_temporal
```

Con ese runtime se esperan 12 salidas Bronze MEF:

```text
presupuesto
presupuesto_hierarchy
presupuesto_producto
presupuesto_generica
presupuesto_fuente
presupuesto_rubro
presupuesto_departamento
presupuesto_temporal
presupuesto_producto_temporal
presupuesto_actividad
presupuesto_actividad_temporal
presupuesto_generica_temporal
```

El valor `MEF_BREAKDOWN_SLICES` contiene comas; el deploy usa delimitador alternativo de `gcloud --set-env-vars` para mantenerlo como una unica variable de entorno.

### PRONABEC reports staging

`pronabec-stage-reports-job` es un unico job parametrizable por `SOURCE_SUBSET`. Los subsets operativos son `pes_2025` y `beca18_universitarios_2012_2026`; cada subset puede contener multiples reportes y no representa un unico dataset.

### Dataflow launchers

Los jobs `dataflow-*` son launchers: arrancan el pipeline Beam desde Cloud Run, pero los workers de Dataflow deben ejecutar con `DATAFLOW_SERVICE_ACCOUNT`. El deploy inyecta esta variable y tambien pasa `--service-account-email` al modulo `pipelines.dataflow_bronze_to_silver`.

La service account del job Cloud Run necesita `roles/iam.serviceAccountUser` sobre `DATAFLOW_SERVICE_ACCOUNT`. La service account worker de Dataflow necesita permisos para Dataflow, GCS y BigQuery. No se debe depender de la Compute default service account.

Los workers de Dataflow no heredan automaticamente el codigo `/app` del launcher. Por eso los jobs Dataflow pasan `DATAFLOW_SETUP_FILE=/app/setup.py` y `--setup-file /app/setup.py`, lo que empaqueta `pipelines`, `pipelines.common` y `pipelines.transforms` para los workers.

Si aparece `ModuleNotFoundError: No module named 'pipelines'`, reconstruya la imagen con `setup.py` incluido y verifique que el job tenga `DATAFLOW_SETUP_FILE` y `--setup-file`.

## Estrategia operativa

La extraccion PRONABEC particionada sigue esta secuencia:

```text
pronabec-discovery-job
pronabec-build-plan-job
pronabec-run-plan-job
pronabec-finalize-dataset-job
```

`bronze_work/` es temporal. Dataflow no debe leer `bronze_work`; solo Bronze final consolidado por `pronabec-finalize-dataset-job` es consumible por `validate_bronze_manifests` y por Silver.

`required_for_e2e` no recorta la descarga Bronze. Si se necesita una ejecucion acotada para debug o pruebas manuales, use `SOURCE_DATASET` en discovery, run-plan o extract-chunk segun corresponda.

## Imagen y despliegue

La plataforma usa una imagen Docker comun para todos los jobs. El deploy script registra o actualiza los jobs con la misma imagen, service account, bucket y variables base, variando solo el modulo Python o los argumentos del contenedor.

Si cambian modulos Python, reconstruya y publique la imagen antes de redeployar los Cloud Run Jobs. Si solo cambian DAG, configuracion o documentos, basta con subir los artefactos de Composer y actualizar variables.
