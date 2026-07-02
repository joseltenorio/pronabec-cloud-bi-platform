# Runbook Operativo: Ejecucion Cloud de Extremo a Extremo (E2E)

## 1. Proposito

Este runbook resume la ejecucion tecnica E2E de PRONABEC Cloud BI Platform en Google Cloud Platform. Cubre imagen Docker, BigQuery, Cloud Run Jobs, Composer, Bronze, Dataflow, Gold y calidad de datos.

> [!IMPORTANT]
> Power BI queda fuera de este runbook.

## 2. Flujo de implementacion

La secuencia operativa recomendada es:

```text
build image
deploy Cloud Run Jobs
run discovery
run build-plan
run run-plan
run finalize
validate Bronze
run Dataflow
publish Gold
validate Gold
run quality
```

La extraccion PRONABEC fue optimizada calibrando `page_size` por endpoint. La carga estimada de paginacion bajo de 6,623 requests a 122 requests, con fallbacks por dataset para endpoints publicos inestables.

## 3. Jobs Cloud Run

Los jobs nuevos de PRONABEC particionado son:

- `pronabec-discovery-job`
- `pronabec-build-plan-job`
- `pronabec-run-plan-job`
- `pronabec-extract-chunk-job`
- `pronabec-finalize-dataset-job`

`pronabec-extract-chunk-job` se mantiene solo para debug manual o reproceso puntual de chunks aislados. El flujo principal usa `pronabec-run-plan-job`.

## 4. Composer

Composer ya orquesta el flujo plan-driven de PRONABEC:

```text
discover_pronabec_datasets
build_pronabec_extraction_plan
run_pronabec_extraction_plan
finalize_pronabec_datasets
validate_bronze_manifests
dataflow_pronabec
dataflow_mef
dataflow_reports
publish_gold_views
validate_gold_contracts
run_quality_checks
```

Composer no hardcodea rangos. `plan.json` es la fuente de verdad para los chunks.

Bronze PRONABEC descarga todos los datasets `bronze_enabled=true`. Silver solo transforma datasets `silver_enabled=true`. `required_for_e2e` queda como metadata operativa y no recorta discovery, build-plan, run-plan ni finalize.

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

### Extract chunk aislado

```powershell
gcloud run jobs execute pronabec-extract-chunk-job `
  --region="$CLOUD_RUN_REGION" `
  --project="$GCP_PROJECT_ID" `
  --update-env-vars="BRONZE_EXTRACTION_DATE=2026-06-29,PIPELINE_RUN_ID=manual_20260629,SOURCE_DATASET=convocatorias_carrera_sede,PAGE_START=1,PAGE_END=10,OUTPUT_MODE=chunk" `
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

`Dockerfile.dataflow` se basa en una imagen oficial Apache Beam SDK Python, instala `requirements.txt` e instala el proyecto con `pip install .` usando `pyproject.toml`.

Despues de cambiar transforms, dependencias o packaging de Dataflow, reconstruya la imagen worker antes de redeployar:

```powershell
.\scripts\build_and_push_dataflow_worker_image.ps1
.\scripts\deploy_cloud_run_jobs.ps1
```

Troubleshooting:

```text
ModuleNotFoundError: No module named 'ftfy'
```

Causa: el worker de Dataflow no esta usando la imagen worker correcta o la imagen no tiene `requirements.txt` instalado.

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
  "run_bronze_manifest_validation": true,
  "run_dataflow_pronabec": true,
  "run_dataflow_mef": true,
  "run_dataflow_reports": true,
  "run_gold_publish": true,
  "run_gold_validation": true,
  "run_quality": true
}
```

`run_pronabec_chunk_extraction` queda como alias de compatibilidad para `run_pronabec_plan_execution`.

## 10. Validacion final

Si cambian modulos Python usados por launchers, reconstruya la imagen launcher. Si cambian transforms, dependencias o packaging de Dataflow, reconstruya la imagen worker. Si solo cambian DAG, configuracion o documentacion, suba los artefactos a Composer y actualice las variables Airflow.

Para ventanas de prueba controladas, Composer puede eliminarse y recrearse con los comandos oficiales del proyecto:

```bash
gcloud composer environments delete "$COMPOSER_ENVIRONMENT_NAME" --location "$COMPOSER_LOCATION"
gcloud composer environments create "$COMPOSER_ENVIRONMENT_NAME" --location "$COMPOSER_LOCATION"
```

Despues de recrear el entorno, configure las variables Airflow con `scripts/configure_airflow_variables.ps1`.
