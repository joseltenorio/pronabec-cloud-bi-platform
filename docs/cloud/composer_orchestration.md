# Modelo de orquestacion con Cloud Composer

## Proposito

Cloud Composer representa la capa de orquestacion batch de PRONABEC Cloud BI Platform. Composer no contiene logica de negocio ni transformaciones pesadas: coordina Cloud Run Jobs, lanzadores Dataflow y procesos BigQuery mediante dependencias, parametros operativos y control de concurrencia.

El DAG principal es:

```text
pronabec_medallion_batch
```

## Flujo principal

El DAG orquesta el flujo PRONABEC particionado de forma plan-driven:

```text
init_run
  -> discover_pronabec_datasets
  -> build_pronabec_extraction_plan
  -> run_pronabec_extraction_plan
  -> finalize_pronabec_datasets
  -> validate_bronze_manifests
  -> Dataflow Bronze a Silver
  -> publish_gold_views
  -> validate_gold_contracts
  -> run_quality_checks
  -> finish_run
```

`plan.json` es la fuente de verdad para los chunks ejecutados. Composer no hardcodea `PAGE_END`, no define rangos estaticos por dataset y no lee `plan.json` dinamicamente en runtime. El runner `pronabec-run-plan-job` consume el plan ya construido y ejecuta cada chunk exacto.

MEF y PRONABEC reports siguen corriendo como ramas Bronze independientes antes de `validate_bronze_manifests`. La compuerta Bronze espera los finalizers PRONABEC, la extraccion MEF y el staging de reportes antes de lanzar cualquier transformacion Dataflow.

## Alcances PRONABEC

Las politicas declarativas separan tres conceptos:

```text
bronze_enabled: el dataset debe aterrizar en Bronze.
silver_enabled: el dataset tiene transformacion Silver habilitada.
required_for_e2e: el dataset es obligatorio para el E2E principal.
```

`PRONABEC_EXTRACTION_SCOPE=e2e` es el modo por defecto del DAG y de los jobs plan-driven. En este modo, discovery y build-plan incluyen solo datasets con `bronze_enabled=true` y `required_for_e2e=true`.

`PRONABEC_EXTRACTION_SCOPE=bronze_full` incluye todos los datasets `bronze_enabled=true`, incluidos datasets Bronze-only con `silver_enabled=false`. Estos datasets pueden generar `data.jsonl`, `manifest.json` y `_SUCCESS` en Bronze final, pero no obligan a crear ni ejecutar tareas Dataflow/Silver.

## Tareas PRONABEC particionadas

### `discover_pronabec_datasets`

Ejecuta `pronabec-discovery-job`. Genera `discovery.json` en `bronze_work/pronabec/_plans/` con alcance, conteos observados, `effective_page_size`, paginas totales y estado por dataset.

### `build_pronabec_extraction_plan`

Ejecuta `pronabec-build-plan-job`. Genera `plan.json` a partir de `discovery.json`, preservando `bronze_enabled`, `silver_enabled` y `required_for_e2e` por dataset y chunk.

### `run_pronabec_extraction_plan`

Ejecuta `pronabec-run-plan-job`. Lee `plan.json` y ejecuta los chunks exactos definidos en el plan, sin recomputar rangos en Composer.

### `finalize_pronabec_<dataset>`

Ejecuta `pronabec-finalize-dataset-job`. Consolida los chunks de un dataset PRONABEC en la ubicacion Bronze final, escribiendo `data.jsonl`, `manifest.json` y `_SUCCESS`.

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
pronabec_extraction_scope
run_pronabec
run_pronabec_discovery
run_pronabec_build_plan
run_pronabec_plan_execution
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

`pronabec_extraction_scope` acepta `e2e` o `bronze_full`; el valor por defecto es `e2e`. `run_pronabec_chunk_extraction` se conserva como alias de compatibilidad para `run_pronabec_plan_execution`. `extraction_date` usa `dag_run.conf.get('extraction_date') or ds`. Si `run_pronabec=false`, las subtareas PRONABEC particionadas tambien quedan deshabilitadas.

Ejemplo de `dag_run.conf` para ejecucion manual:

```json
{
  "extraction_date": "2026-06-29",
  "pronabec_extraction_scope": "e2e",
  "run_pronabec": true,
  "run_pronabec_discovery": true,
  "run_pronabec_build_plan": true,
  "run_pronabec_plan_execution": true,
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
pronabec_run_plan_job_name
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

`pronabec-extract-job` permanece registrado por compatibilidad y pruebas manuales antiguas. El DAG principal usa `pronabec-discovery-job`, `pronabec-build-plan-job`, `pronabec-run-plan-job` y `pronabec-finalize-dataset-job`. `pronabec-extract-chunk-job` queda como herramienta manual para debug de chunks aislados.

## Debug operativo

Para depurar el flujo PRONABEC, ejecute primero los Cloud Run Jobs manualmente:

```text
pronabec-discovery-job
pronabec-build-plan-job
pronabec-run-plan-job
pronabec-extract-chunk-job con SOURCE_DATASET, PAGE_START, PAGE_END y OUTPUT_MODE=chunk
pronabec-finalize-dataset-job
```

Despues de validar esos componentes, ejecute el DAG completo.

Si cambian modulos Python, haga rebuild/push de la imagen antes de `scripts/deploy_cloud_run_jobs.ps1`. Si solo cambian DAG, configuracion o documentacion, suba el DAG y archivos de soporte a Composer con `scripts/upload_composer_dag.ps1`.

## Reintentos y concurrencia

El DAG mantiene `catchup=False` y `max_active_runs=1`. Esta configuracion evita backfills automaticos y ejecuciones superpuestas para una misma fecha logica.

## Exclusiones

`convocatorias_carrera_sede` se conserva como Bronze-only y no se promueve a Silver ni Gold. Su plan de extraccion queda definido en `plan.json`, pero no existe job Silver para ese dataset.

`presupuesto_departamento`, `presupuesto_fuente` y `presupuesto_rubro` son datasets MEF Bronze-only y estan excluidos del flujo Silver.
