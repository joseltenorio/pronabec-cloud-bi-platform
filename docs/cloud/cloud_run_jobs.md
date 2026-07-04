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

### Quality checks

`quality-checks-job` ejecuta `python -m pipelines.quality_checks` con argumentos CLI persistidos en `scripts/deploy_cloud_run_jobs.ps1`: `--project-id`, `--silver-dataset`, `--gold-dataset`, `--audit-dataset`, `--pipeline-run-id` y `--fail-on-error`. El `PIPELINE_RUN_ID` se pasa como placeholder runtime para que cada ejecucion manual o Composer conserve trazabilidad.

El runner lee `sql/quality/data_quality_checks.sql` por defecto. El parser divide el archivo por `;` solo en nivel superior, respetando comentarios, strings, backticks y parentesis; no se deben ejecutar fragmentos sueltos como `)`.

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

### PRONABEC reports Dataflow

`dataflow-pronabec-report-job` es un unico launcher parametrizable para reportes documentales PRONABEC. No representa un unico dataset fijo y no se deben crear jobs por reporte.

El deploy registra sentinels:

```text
SOURCE_DATASET=placeholder_dataset
INPUT_PATH=gs://<bucket>/placeholder_path
OUTPUT_TABLE=<project>:<silver>.placeholder_table
```

Estos placeholders solo documentan que el job debe ser sobreescrito en runtime. En ejecucion `DataflowRunner`, el pipeline rechaza la ejecucion antes de construir Dataflow cuando `source_system=pronabec_reports` llega sin `SOURCE_DATASET`, `INPUT_PATH` u `OUTPUT_TABLE` reales, cuando algun valor contiene placeholders, cuando `INPUT_PATH` no empieza con `gs://`, o cuando `OUTPUT_TABLE` no cumple `project:dataset.table`.

Para ejecucion manual sin Composer use `scripts/run_pronabec_reports_dataflow.sh`. El script ejecuta el mismo Cloud Run Job una vez por reporte encontrado en `gs://<bucket>/bronze/pronabec_reports/` y pasa `SOURCE_DATASET`, `INPUT_PATH`, `OUTPUT_TABLE`, `BRONZE_EXTRACTION_DATE`, `PIPELINE_RUN_ID` y `DATAFLOW_SDK_CONTAINER_IMAGE`.

Composer debe aplicar el mismo contrato: una ejecucion de `dataflow-pronabec-report-job` por reporte, siempre con parametros reales. El error `placeholder_path` indica una ejecucion mal parametrizada del launcher, no una falla del worker Dataflow.

### Dataflow launchers

Los jobs `dataflow-*` son launchers: arrancan el pipeline Beam desde Cloud Run, pero los workers de Dataflow deben ejecutar con `DATAFLOW_SERVICE_ACCOUNT`. El deploy inyecta esta variable y tambien pasa `--service-account-email` al modulo `pipelines.dataflow_bronze_to_silver`.

La service account del job Cloud Run necesita `roles/iam.serviceAccountUser` sobre `DATAFLOW_SERVICE_ACCOUNT`. La service account worker de Dataflow necesita permisos para Dataflow, GCS y BigQuery. No se debe depender de la Compute default service account.

Los workers de Dataflow no heredan automaticamente el codigo ni las dependencias del launcher. Por eso los jobs Dataflow pasan `DATAFLOW_SDK_CONTAINER_IMAGE` y `--sdk-container-image`, apuntando a una imagen worker dedicada que instala `requirements-dataflow-worker.txt` y el paquete `pipelines`.

Si aparece `ModuleNotFoundError`, verifique que el job tenga `DATAFLOW_SDK_CONTAINER_IMAGE`, que sus argumentos incluyan `--sdk-container-image` y que la imagen worker haya sido reconstruida con `Dockerfile.dataflow`.

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

## Imagenes y despliegue

La plataforma usa una imagen launcher para Cloud Run Jobs y una imagen worker dedicada para Dataflow. El deploy script registra o actualiza los jobs con la imagen launcher, service account, bucket y variables base, variando el modulo Python o los argumentos del contenedor.

Si cambian modulos Python usados por launchers o `requirements.txt`, reconstruya la imagen launcher antes de redeployar Cloud Run Jobs. Si cambian transforms, `requirements-dataflow-worker.txt` o packaging de Dataflow, reconstruya la imagen worker y luego redeploye Cloud Run Jobs para propagar `DATAFLOW_SDK_CONTAINER_IMAGE`. Si solo cambian DAG, configuracion o documentos, basta con subir los artefactos de Composer y actualizar variables.
