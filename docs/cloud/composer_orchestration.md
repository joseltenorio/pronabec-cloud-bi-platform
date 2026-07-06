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
  -> bronze_parallel
       -> pronabec_api_bronze
       -> mef_bronze
       -> pronabec_reports_bronze
  -> validate_bronze_manifests
  -> silver_parallel
       -> pronabec_api_silver
       -> mef_silver
       -> pronabec_reports_silver
  -> publish_gold_views
  -> validate_gold_contracts
  -> run_quality_checks
  -> finish_run
```

`plan.json` es la fuente de verdad para los chunks ejecutados. Composer no hardcodea `PAGE_END`, no define rangos estaticos por dataset y no lee `plan.json` dinamicamente en runtime. El runner `pronabec-run-plan-job` consume el plan ya construido y ejecuta cada chunk exacto.

MEF y PRONABEC reports corren como ramas Bronze independientes en paralelo con PRONABEC API Bronze. La compuerta `validate_bronze_manifests` espera los finalizers PRONABEC, la extraccion MEF y el staging de reportes antes de lanzar cualquier transformacion Dataflow.

Despues de la validacion Bronze, Composer lanza en paralelo las ramas Silver/Dataflow: PRONABEC API, MEF y PRONABEC reports. `publish_gold_views` espera que terminen las tres ramas Silver; luego corren `validate_gold_contracts` y `run_quality_checks`.

## Responsabilidades PRONABEC

Las politicas declarativas separan tres conceptos:

```text
bronze_enabled: el dataset debe aterrizar en Bronze.
silver_enabled: el dataset tiene transformacion Silver habilitada.
required_for_e2e: metadata para pruebas E2E, demos o validaciones acotadas.
```

Bronze PRONABEC descarga todos los datasets `bronze_enabled=true`. Esto incluye datasets Bronze-only con `silver_enabled=false`.

Silver transforma solo datasets `silver_enabled=true`.

`required_for_e2e` no filtra discovery, build-plan, run-plan ni finalize. Si se quiere probar un dataset especifico, la forma correcta es usar `SOURCE_DATASET` en la ejecucion manual del job correspondiente.

## Tareas PRONABEC particionadas

### `discover_pronabec_datasets`

Ejecuta `pronabec-discovery-job`. Genera `discovery.json` en `bronze_work/pronabec/_plans/` con conteos observados, `effective_page_size`, paginas totales y estado por dataset.

### `build_pronabec_extraction_plan`

Ejecuta `pronabec-build-plan-job`. Genera `plan.json` a partir de `discovery.json`, preservando `bronze_enabled`, `silver_enabled` y `required_for_e2e` como metadata por dataset y chunk.

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
run_pronabec
run_pronabec_discovery
run_pronabec_build_plan
run_pronabec_plan_execution
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
pronabec_discovery_job_name
pronabec_build_plan_job_name
pronabec_run_plan_job_name
pronabec_finalize_dataset_job_name
mef_extract_job_name
pronabec_reports_stage_job_name
bronze_manifest_validation_job_name
dataflow_pronabec_report_job_name
gold_publish_job_name
gold_validate_job_name
quality_checks_job_name
```

El DAG principal usa `pronabec-discovery-job`, `pronabec-build-plan-job`, `pronabec-run-plan-job` y `pronabec-finalize-dataset-job`. Esa es la unica ruta soportada para PRONABEC API Bronze.

## Ejecucion robusta de Cloud Run Jobs

Composer no usa `gcloud run jobs execute --wait` ni `subprocess` dentro del DAG. Ese patron puede quedarse sin output mientras `gcloud` espera, perder heartbeat en Airflow y provocar retries que lancen ejecuciones duplicadas.

Cada task Cloud Run usa un `PythonOperator` con `run_cloud_run_job_with_polling`. El operador:

```text
1. obtiene credenciales con `google.auth.default`;
2. usa `AuthorizedSession` contra Cloud Run v2 REST API;
3. lanza el Cloud Run Job con `POST /v2/projects/{project}/locations/{region}/jobs/{job}:run`;
4. envia env vars por `overrides.containerOverrides.env`;
5. hace polling del long-running operation;
6. falla si la operation termina con error o si se supera el timeout.
```

Los logs de Composer deben mostrar:

```text
Launching Cloud Run job through REST API. job=<job-name>
Cloud Run env vars: ...
Cloud Run operation: <operation-name>
Cloud Run operation=<operation-name> job=<job-name> elapsed=<seconds> done=<true|false>
Cloud Run operation completed successfully.
Cloud Run execution: <execution-name>
```

## Debug operativo

Para depurar el flujo PRONABEC, ejecute primero los Cloud Run Jobs manualmente:

```text
pronabec-discovery-job
pronabec-build-plan-job
pronabec-run-plan-job
pronabec-finalize-dataset-job
```

Para reprocesar PRONABEC API, reejecute el flujo plan-driven con la misma o una nueva combinacion de `BRONZE_EXTRACTION_DATE` y `PIPELINE_RUN_ID`. Despues de validar esos componentes, ejecute el DAG completo.

Si cambian modulos Python, haga rebuild/push de la imagen antes de `scripts/deploy_cloud_run_jobs.ps1`. Si solo cambian DAG, configuracion o documentacion, suba el DAG y archivos de soporte a Composer con `scripts/upload_composer_dag.ps1`.

## Reintentos y concurrencia

El DAG mantiene `catchup=False`, `max_active_runs=1` y `max_active_tasks=8`. Esta configuracion evita backfills automaticos, ejecuciones superpuestas para una misma fecha logica y paralelismo ilimitado de launchers.

Durante la validacion E2E, el DAG es manual-only (`schedule_interval=None`) para evitar que Airflow dispare scheduled runs antiguos al despausar el DAG y bloquee la corrida manual correcta.

Dispare Composer con fecha y run id explicitos:

```bash
gcloud composer environments run "$COMPOSER_ENVIRONMENT_NAME" \
  --location "$COMPOSER_LOCATION" \
  --project "$GCP_PROJECT_ID" \
  dags trigger -- \
  --run-id "manual_20260702_composer_e2e_hardened_01" \
  --conf '{"extraction_date":"2026-07-02","pipeline_run_id":"manual_20260702"}' \
  pronabec_medallion_batch
```

El DAG propaga `BRONZE_EXTRACTION_DATE` desde `dag_run.conf.extraction_date` y `PIPELINE_RUN_ID` desde `dag_run.conf.pipeline_run_id`. Si `pipeline_run_id` no viene en el trigger, usa `run_id` como fallback.

Composer controla cuantos Cloud Run Jobs/Dataflow launchers se lanzan al mismo tiempo. `Dataflow max_num_workers` controla cuantos workers puede usar cada job Dataflow individual; es una capa distinta y no se hardcodea por dataset en el DAG.

## Exclusiones

`convocatorias_carrera_sede` se conserva como Bronze-only y no se promueve a Silver ni Gold. Su plan de extraccion queda definido en `plan.json`, pero no existe job Silver para ese dataset.

`presupuesto_departamento`, `presupuesto_fuente` y `presupuesto_rubro` son datasets MEF Bronze-only y estan excluidos del flujo Silver.
