# Runbook Operativo: Ejecucion Cloud de Extremo a Extremo (E2E)

## 1. Proposito

Este runbook resume la ejecucion tecnica E2E de PRONABEC Cloud BI Platform en Google Cloud Platform. Cubre imagen Docker, BigQuery, Cloud Run Jobs, Composer, Bronze, Dataflow, Gold y calidad de datos.

> [!IMPORTANT]
> Power BI queda fuera de este runbook.

## 2. Flujo de implementacion

La secuencia operativa recomendada es:

```text
construir imagen
desplegar Cloud Run Jobs
ejecutar discovery
ejecutar build-plan
ejecutar run-plan
ejecutar finalize
validar Bronze
ejecutar Dataflow
publicar Gold
validar Gold
ejecutar calidad
```

La extraccion PRONABEC fue optimizada calibrando `page_size` por endpoint. La carga estimada de paginacion bajo de 6,623 requests a 122 requests, con fallbacks por dataset para endpoints publicos inestables.

## 3. Jobs Cloud Run

Los jobs nuevos de PRONABEC particionado son:

- `pronabec-discovery-job`
- `pronabec-build-plan-job`
- `pronabec-run-plan-job`
- `pronabec-finalize-dataset-job`

La unica ruta soportada para PRONABEC API Bronze es plan-driven: discovery -> build-plan -> run-plan -> finalize. Para reprocesar PRONABEC API, reejecute ese flujo con la misma o una nueva combinacion de `BRONZE_EXTRACTION_DATE` y `PIPELINE_RUN_ID`.

## 4. Composer

Composer orquesta el flujo plan-driven de PRONABEC y aprovecha paralelismo controlado entre ramas independientes:

```text
init_run
  -> pronabec_api_bronze | mef_bronze | pronabec_reports_bronze | inei_reports_bronze
  -> validate_bronze_manifests
  -> pronabec_api_silver | mef_silver | pronabec_reports_silver | inei_reports_silver
  -> publish_gold_views
  -> validate_gold_contracts
  -> run_quality_checks
  -> finish_run
```

Composer no hardcodea rangos. `plan.json` es la fuente de verdad para los chunks.

Bronze PRONABEC descarga todos los datasets `bronze_enabled=true`. Silver solo transforma datasets `silver_enabled=true`. `required_for_e2e` queda como metadata operativa y no recorta discovery, build-plan, run-plan ni finalize.

`bronze-manifest-validation-job` es la barrera obligatoria antes de Silver. Gold espera a que terminen las tres ramas Silver y Calidad corre al final.

El contexto regional INEI se incluye como una rama de reportes manuales/externos. El job de preparacion copia los archivos CSV desde `landing/inei_reports` hacia `bronze/inei_reports/{dataset}/extraction_date=YYYY-MM-DD/data.csv`, escribe `manifest.json` y `_SUCCESS`, y luego el job parametrizable `dataflow-inei-report-job` promueve cada dataset a `silver.inei_*`. Esta rama alimenta Calidad, pero no modifica salidas Gold predictivas en este PR.

Composer paraleliza launchers de Cloud Run/Dataflow con `max_active_runs=1` y `max_active_tasks=8`. Los workers de cada Dataflow job se controlan aparte con la configuracion Dataflow existente, como `DATAFLOW_MAX_NUM_WORKERS`; no se asignan workers por dataset desde el DAG.

Durante la validacion E2E, el DAG es manual-only (`schedule_interval=None`). Esto evita que, al despausar Composer, se cree un scheduled run antiguo que bloquee la corrida manual correcta por `max_active_runs=1`.

Las tasks Cloud Run no usan `gcloud run jobs execute --wait` ni invocan `gcloud` con `subprocess`. Composer usa Cloud Run v2 REST API con `google.auth` y `AuthorizedSession`: lanza el job con `jobs:run`, envia env vars por overrides y hace polling del long-running operation. Esto evita que el launch quede bloqueado en el CLI antes de comenzar el polling.

Trigger recomendado para la corrida validada:

```bash
gcloud composer environments run "$COMPOSER_ENVIRONMENT_NAME" \
  --location "$COMPOSER_LOCATION" \
  --project "$GCP_PROJECT_ID" \
  dags trigger -- \
  --run-id "manual_20260702_composer_e2e_hardened_01" \
  --conf '{"extraction_date":"2026-07-02","pipeline_run_id":"manual_20260702"}' \
  pronabec_medallion_batch
```

Los logs deben confirmar:

```text
BRONZE_EXTRACTION_DATE=2026-07-02
PIPELINE_RUN_ID=manual_20260702
Launching Cloud Run job asynchronously...
Cloud Run launch command completed.
Launching Cloud Run job through REST API. job=<job-name>
Cloud Run operation: <operation-name>
Cloud Run operation=<operation-name> job=<job-name> elapsed=<seconds> done=<true|false>
Cloud Run operation completed successfully.
Cloud Run execution: <execution-name>
```

## 5. Ejecucion manual con gcloud

### Discovery

```powershell
gcloud run jobs execute pronabec-discovery-job `
  --region="$CLOUD_RUN_REGION" `
  --project="$GCP_PROJECT_ID" `
  --update-env-vars="BRONZE_EXTRACTION_DATE=2026-06-29,PIPELINE_RUN_ID=manual_20260629" `
  --wait
```

### Build plan

```powershell
gcloud run jobs execute pronabec-build-plan-job `
  --region="$CLOUD_RUN_REGION" `
  --project="$GCP_PROJECT_ID" `
  --update-env-vars="BRONZE_EXTRACTION_DATE=2026-06-29,PIPELINE_RUN_ID=manual_20260629" `
  --wait
```

### Run plan

```powershell
gcloud run jobs execute pronabec-run-plan-job `
  --region="$CLOUD_RUN_REGION" `
  --project="$GCP_PROJECT_ID" `
  --update-env-vars="BRONZE_EXTRACTION_DATE=2026-06-29,PIPELINE_RUN_ID=manual_20260629" `
  --wait
```

### Finalize

```powershell
gcloud run jobs execute pronabec-finalize-dataset-job `
  --region="$CLOUD_RUN_REGION" `
  --project="$GCP_PROJECT_ID" `
  --update-env-vars="BRONZE_EXTRACTION_DATE=2026-06-29,PIPELINE_RUN_ID=manual_20260629,SOURCE_DATASET=convocatorias_carrera_sede" `
  --wait
```

### MEF Consulta Amigable

`mef-extract-job` debe quedar configurado por `scripts/deploy_cloud_run_jobs.ps1` con el runtime MEF completo. Composer no lee `.env` local, por lo que el deploy del job debe persistir:

```text
MEF_SOURCE_MODE=consulta_amigable
MEF_START_YEAR=2012
MEF_END_YEAR=2026
MEF_TEXT_FILTER=PRONABEC
MEF_PRONABEC_EXECUTORA_CODE=117-1438
MEF_INCLUDE_HIERARCHY=true
MEF_INCLUDE_SPENDING_BREAKDOWNS=true
MEF_BREAKDOWN_SLICES=producto,generica,fuente,rubro,departamento,temporal,producto_temporal,actividad,actividad_temporal,generica_temporal
```

La ejecucion manual solo debe pasar la fecha logica y el run id:

```powershell
gcloud run jobs execute mef-extract-job `
  --region="$CLOUD_RUN_REGION" `
  --project="$GCP_PROJECT_ID" `
  --update-env-vars="BRONZE_EXTRACTION_DATE=2026-06-29,PIPELINE_RUN_ID=manual_20260629" `
  --wait
```

Con el runtime completo se esperan 12 salidas Bronze MEF: `presupuesto`, `presupuesto_hierarchy`, `presupuesto_producto`, `presupuesto_generica`, `presupuesto_fuente`, `presupuesto_rubro`, `presupuesto_departamento`, `presupuesto_temporal`, `presupuesto_producto_temporal`, `presupuesto_actividad`, `presupuesto_actividad_temporal` y `presupuesto_generica_temporal`.

### PRONABEC reports staging

`pronabec-stage-reports-job` ejecuta `python -m tools.stage_pronabec_manual_reports --strict --overwrite`. Se usa modo modulo para que los imports `pipelines.common.*` funcionen dentro de la imagen.

Ejecute un subset por corrida:

```powershell
gcloud run jobs execute pronabec-stage-reports-job `
  --region="$CLOUD_RUN_REGION" `
  --project="$GCP_PROJECT_ID" `
  --update-env-vars="BRONZE_EXTRACTION_DATE=2026-06-29,PIPELINE_RUN_ID=manual_20260629,SOURCE_SUBSET=pes_2025" `
  --wait

gcloud run jobs execute pronabec-stage-reports-job `
  --region="$CLOUD_RUN_REGION" `
  --project="$GCP_PROJECT_ID" `
  --update-env-vars="BRONZE_EXTRACTION_DATE=2026-06-29,PIPELINE_RUN_ID=manual_20260629,SOURCE_SUBSET=beca18_universitarios_2012_2026" `
  --wait
```

`SOURCE_SUBSET` selecciona un grupo documental completo; `pes_2025` y `beca18_universitarios_2012_2026` contienen multiples reportes.

### PRONABEC reports Dataflow sin Composer

`dataflow-pronabec-report-job` es un unico launcher parametrizable. No representa un dataset fijo y no debe ejecutarse sin `SOURCE_DATASET`, `INPUT_PATH` y `OUTPUT_TABLE` reales. Los valores `placeholder_dataset`, `placeholder_path` y `placeholder_table` existen solo como sentinel de deploy para evitar crear 23 Cloud Run Jobs.

Para ejecutar todos los reportes documentales desde Cloud Shell sin Composer:

```bash
export GCP_PROJECT_ID="<project>"
export GCS_BUCKET_NAME="<bucket>"
export BQ_SILVER_DATASET="silver"
export CLOUD_RUN_REGION="<region>"
export DATAFLOW_SDK_CONTAINER_IMAGE="<region>-docker.pkg.dev/<project>/<repo>/pronabec-dataflow-worker:latest"
export BRONZE_EXTRACTION_DATE="2026-06-29"
export PIPELINE_RUN_ID="manual_20260629"

scripts/run_pronabec_reports_dataflow.sh
```

El script lista `gs://${GCS_BUCKET_NAME}/bronze/pronabec_reports/`, verifica `data.csv` por reporte y ejecuta `dataflow-pronabec-report-job` con overrides por dataset. Composer, cuando se use para reports, debe pasar esos mismos parametros por cada reporte. Si aparece `placeholder_path`, la causa es una ejecucion mal parametrizada del launcher, no un problema del worker Dataflow.

### Contexto regional INEI

`inei-stage-reports-job` ejecuta:

```bash
python -m tools.stage_inei_reports --strict --overwrite
```

Archivos esperados en Landing:

```text
landing/inei_reports/inei_population_youth_region.csv
landing/inei_reports/inei_demographic_indicators_region.csv
landing/inei_reports/inei_pobreza_departamental_2012_2025.csv
landing/inei_reports/inei_internet_acceso_region_2012_2025_final.csv
```

Para probar la preparacion manualmente sin desplegar nada nuevo:

```bash
python -m tools.stage_inei_reports \
  --bucket "$GCS_BUCKET_NAME" \
  --landing-prefix "$INEI_REPORTS_LANDING_PREFIX" \
  --bronze-prefix "$INEI_REPORTS_BRONZE_PREFIX" \
  --extraction-date "$(date +%F)" \
  --strict \
  --overwrite
```

No lance Dataflow INEI manualmente salvo que el Cloud Run Job ya esté desplegado y la ejecución tenga overrides reales para `SOURCE_DATASET`, `BRONZE_INPUT_PATH` y `BQ_OUTPUT_TABLE`.

## 6. Service account worker de Dataflow

Los Cloud Run Jobs `dataflow-*` actuan como launchers. Los workers de Dataflow deben usar `DATAFLOW_SERVICE_ACCOUNT`, no la Compute default service account.

Checklist IAM:

```bash
export DATAFLOW_SERVICE_ACCOUNT="pronabec-dataflow-sa@pronabec-cloud-bi-platform.iam.gserviceaccount.com"
export CLOUD_RUN_SERVICE_ACCOUNT="pronabec-cloudrun-sa@pronabec-cloud-bi-platform.iam.gserviceaccount.com"

gcloud iam service-accounts add-iam-policy-binding "$DATAFLOW_SERVICE_ACCOUNT" \
  --project="$GCP_PROJECT_ID" \
  --member="serviceAccount:$CLOUD_RUN_SERVICE_ACCOUNT" \
  --role="roles/iam.serviceAccountUser"

gcloud projects add-iam-policy-binding "$GCP_PROJECT_ID" \
  --member="serviceAccount:$DATAFLOW_SERVICE_ACCOUNT" \
  --role="roles/dataflow.worker"

gcloud projects add-iam-policy-binding "$GCP_PROJECT_ID" \
  --member="serviceAccount:$DATAFLOW_SERVICE_ACCOUNT" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding "$GCP_PROJECT_ID" \
  --member="serviceAccount:$DATAFLOW_SERVICE_ACCOUNT" \
  --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding "$GCP_PROJECT_ID" \
  --member="serviceAccount:$DATAFLOW_SERVICE_ACCOUNT" \
  --role="roles/storage.objectAdmin"
```

Si el Cloud Run Job usa una service account distinta a `CLOUD_RUN_SERVICE_ACCOUNT`, tambien debe tener `roles/iam.serviceAccountUser` sobre `DATAFLOW_SERVICE_ACCOUNT`.

Diagnostico del launcher:

```bash
gcloud run jobs describe dataflow-pronabec-becarios-pais-estudio-job \
  --region="$CLOUD_RUN_REGION" \
  --project="$GCP_PROJECT_ID" \
  --format="value(spec.template.spec.template.spec.serviceAccountName)"
```

El job debe tener `DATAFLOW_SERVICE_ACCOUNT` en sus variables y/o `--service-account-email` en sus argumentos.

## 7. Imagen worker Dataflow

Los workers de Dataflow no heredan automaticamente el codigo ni las dependencias del launcher Cloud Run. Para que puedan importar `pipelines` y dependencias como `ftfy`, los jobs Dataflow deben usar una imagen worker dedicada:

```text
DATAFLOW_SDK_CONTAINER_IMAGE=us-central1-docker.pkg.dev/<project>/<repository>/pronabec-dataflow-worker:<tag>
--sdk-container-image us-central1-docker.pkg.dev/<project>/<repository>/pronabec-dataflow-worker:<tag>
```

`Dockerfile.dataflow` se basa en una imagen oficial Apache Beam SDK Python, instala `requirements-dataflow-worker.txt` e instala el proyecto con `pip install .` usando `pyproject.toml`.

Despues de cambiar transforms, dependencias o packaging de Dataflow, reconstruya la imagen worker antes de redeployar:

```powershell
.\scripts\build_and_push_dataflow_worker_image.ps1
.\scripts\deploy_cloud_run_jobs.ps1
```

Troubleshooting:

```text
ModuleNotFoundError: No module named 'ftfy'
```

Causa: el worker de Dataflow no esta usando la imagen worker correcta o la imagen no tiene `requirements-dataflow-worker.txt` instalado.

Solucion: verificar `DATAFLOW_SDK_CONTAINER_IMAGE` en el Cloud Run Job, revisar `--sdk-container-image` en los argumentos del launcher, confirmar la imagen en Artifact Registry, reconstruir la imagen worker y redeployar Cloud Run Jobs.

## 8. Validacion Bronze

`bronze_work/` es temporal y no debe ser leido por Dataflow. Solo Bronze final consolidado con `manifest.json` y `_SUCCESS` entra a `validate_bronze_manifests` y luego a Silver.

Si se quiere ejecutar un solo dataset por diagnostico, use `SOURCE_DATASET` en el job manual correspondiente. No existe un scope E2E que reduzca la descarga Bronze principal.

## 9. Configuracion manual del DAG

Ejemplo de `dag_run.conf`:

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
  "run_inei_reports_staging": true,
  "run_bronze_manifest_validation": true,
  "run_dataflow_pronabec": true,
  "run_dataflow_mef": true,
  "run_dataflow_reports": true,
  "run_dataflow_inei": true,
  "run_gold_publish": true,
  "run_gold_validation": true,
  "run_quality": true
}
```

## 10. Validacion final

Si cambian modulos Python usados por launchers o `requirements.txt`, reconstruya la imagen launcher. Si cambian transforms, `requirements-dataflow-worker.txt` o packaging de Dataflow, reconstruya la imagen worker. Si solo cambian DAG, configuracion o documentacion, suba los artefactos a Composer y actualice las variables Airflow.

### Controles de calidad

`quality-checks-job` queda desplegado con los argumentos requeridos por `pipelines.quality_checks`: `--project-id`, `--silver-dataset`, `--gold-dataset`, `--audit-dataset`, `--pipeline-run-id` y `--fail-on-error`. Para reintentar solo calidad despues de Gold validate:

```bash
gcloud run jobs execute quality-checks-job \
  --region="$CLOUD_RUN_REGION" \
  --project="$GCP_PROJECT_ID" \
  --update-env-vars="PIPELINE_RUN_ID=${PIPELINE_RUN_ID}" \
  --wait
```

Para ventanas de prueba controladas, Composer puede eliminarse y recrearse con los comandos oficiales del proyecto:

```bash
gcloud composer environments delete "$COMPOSER_ENVIRONMENT_NAME" --location "$COMPOSER_LOCATION"
gcloud composer environments create "$COMPOSER_ENVIRONMENT_NAME" --location "$COMPOSER_LOCATION"
```

Despues de recrear el entorno, configure las variables Airflow con `scripts/configure_airflow_variables.ps1`.
