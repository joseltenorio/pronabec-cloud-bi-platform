# Modelo de orquestacion con Cloud Composer

## Proposito

Cloud Composer representa la capa de orquestacion batch de PRONABEC Cloud BI Platform. Composer no contiene logica de negocio ni transformaciones pesadas: coordina Cloud Run Jobs, lanzadores Dataflow y procesos BigQuery mediante dependencias, parametros operativos y control de concurrencia.

El DAG principal es:

```text
pronabec_medallion_batch
```

## Flujo principal

El DAG ya orquesta el flujo PRONABEC particionado:

```text
init_run
  -> discover_pronabec_datasets
  -> build_pronabec_extraction_plan
  -> extract_pronabec_chunks
  -> finalize_pronabec_datasets
  -> validate_bronze_manifests
  -> Dataflow Bronze a Silver
  -> publish_gold_views
  -> validate_gold_contracts
  -> run_quality_checks
  -> finish_run
```

MEF y PRONABEC reports siguen corriendo como ramas Bronze independientes antes de `validate_bronze_manifests`. La compuerta Bronze espera los finalizers PRONABEC, la extraccion MEF y el staging de reportes antes de lanzar cualquier transformacion Dataflow.

## Tareas PRONABEC particionadas

### `discover_pronabec_datasets`

Ejecuta `pronabec-discovery-job`. Genera `discovery.json` en `bronze_work/pronabec/_plans/` con conteos observados, `effective_page_size`, paginas totales y estado por dataset.

### `build_pronabec_extraction_plan`

Ejecuta `pronabec-build-plan-job`. Genera `plan.json` desde `discovery.json`. En esta version el DAG no lee `plan.json` dinamicamente ni usa dynamic task mapping; las tareas de extraccion se crean estaticamente desde `config/orchestration.yaml`.

### `extract_pronabec_<dataset>_<page_start>_<page_end>`

Ejecuta `pronabec-extract-chunk-job` con:

```text
BRONZE_EXTRACTION_DATE
PIPELINE_RUN_ID
SOURCE_DATASET
PAGE_START
PAGE_END
OUTPUT_MODE=chunk
```

Los chunks se escriben en `bronze_work/`.

### `finalize_pronabec_<dataset>`

Ejecuta `pronabec-finalize-dataset-job`. Consolida los chunks en:

```text
bronze/pronabec/{dataset}/extraction_date=YYYY-MM-DD/
```

El finalizer crea `data.jsonl`, `manifest.json` y `_SUCCESS`.

## Bronze work y Dataflow

`bronze_work/` es temporal. Dataflow no lee `bronze_work`.

Dataflow PRONABEC sigue leyendo Bronze final:

```text
bronze/pronabec/{dataset}/extraction_date=YYYY-MM-DD/data.jsonl
```

`validate_bronze_manifests` es la compuerta antes de Silver. Si falta `manifest.json`, falta `_SUCCESS` o la fecha logica no coincide, el flujo debe detenerse antes de Dataflow.

## Parametros del DAG

El DAG acepta parametros para habilitar o deshabilitar ramas sin cambiar codigo:

```text
extraction_date
run_pronabec
run_pronabec_discovery
run_pronabec_build_plan
run_pronabec_chunk_extraction
run_pronabec_finalize
run_mef
run_pronabec_reports_staging
run_bronze_manifest_validation
run_dataflow_pronabec
run_dataflow_mef
run_dataflow_reports
run_gold_publish
run_gold_validation
run_quality
```

`extraction_date` usa `dag_run.conf.get('extraction_date') or ds`. Si `run_pronabec=false`, las subtareas PRONABEC particionadas tambien quedan deshabilitadas.

Ejemplo de `dag_run.conf` para ejecucion manual:

```json
{
  "extraction_date": "2026-06-29",
  "run_pronabec": true,
  "run_pronabec_discovery": true,
  "run_pronabec_build_plan": true,
  "run_pronabec_chunk_extraction": true,
  "run_pronabec_finalize": true,
  "run_mef": true,
  "run_pronabec_reports_staging": true,
  "run_bronze_manifest_validation": true,
  "run_dataflow_pronabec": true,
  "run_dataflow_mef": true,
  "run_dataflow_reports": true,
  "run_gold_publish": true,
  "run_gold_validation": true,
  "run_quality": true
}
```

## Variables Airflow

El DAG resuelve nombres de jobs desde Airflow Variables:

```text
gcp_project_id
gcp_region
gcs_bucket_name
bq_bronze_dataset
bq_silver_dataset
bq_gold_dataset
bq_audit_dataset
pronabec_extract_job_name
pronabec_discovery_job_name
pronabec_build_plan_job_name
pronabec_extract_chunk_job_name
pronabec_finalize_dataset_job_name
mef_extract_job_name
pronabec_reports_stage_job_name
bronze_manifest_validation_job_name
dataflow_pronabec_report_job_name
gold_publish_job_name
gold_validate_job_name
quality_checks_job_name
```

`pronabec-extract-job` permanece registrado por compatibilidad y pruebas manuales antiguas. El DAG principal usa `pronabec-discovery-job`, `pronabec-build-plan-job`, `pronabec-extract-chunk-job` y `pronabec-finalize-dataset-job`.

## Debug operativo

Para depurar el flujo PRONABEC, ejecute primero los Cloud Run Jobs manualmente:

```text
pronabec-discovery-job
pronabec-build-plan-job
pronabec-extract-chunk-job con SOURCE_DATASET, PAGE_START, PAGE_END y OUTPUT_MODE=chunk
pronabec-finalize-dataset-job
```

Despues de validar esos componentes, ejecute el DAG completo.

Si cambian modulos Python, haga rebuild/push de la imagen antes de `scripts/deploy_cloud_run_jobs.ps1`. Si solo cambian DAG, configuracion o documentacion, suba el DAG y archivos de soporte a Composer con `scripts/upload_composer_dag.ps1`.

## Reintentos y concurrencia

El DAG mantiene `catchup=False` y `max_active_runs=1`. Esta configuracion evita backfills automaticos y ejecuciones superpuestas para una misma fecha logica.

## Exclusiones

`convocatorias_carrera_sede` se conserva como Bronze-only y no se promueve a Silver ni Gold. Sus rangos estaticos quedan declarados para pruebas manuales y evolucion futura, pero Dataflow no tiene job Silver para ese dataset.

`presupuesto_departamento`, `presupuesto_fuente` y `presupuesto_rubro` son datasets MEF Bronze-only y estan excluidos del flujo Silver.
