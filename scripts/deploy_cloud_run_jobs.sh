#!/usr/bin/env bash
set -euo pipefail

require_env() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "ERROR: required environment variable ${name} is not set." >&2
    exit 1
  fi
}

log() {
  echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] $*"
}

env_or() {
  local name="$1"
  local default="$2"
  printf '%s' "${!name:-$default}"
}

join_by() {
  local delimiter="$1"
  shift
  local first=true
  local item
  for item in "$@"; do
    if [[ "$first" == true ]]; then
      printf '%s' "$item"
      first=false
    else
      printf '%s%s' "$delimiter" "$item"
    fi
  done
}

join_cloud_run_env_vars() {
  printf '^|^%s' "$(join_by '|' "$@")"
}

cloud_run_job_exists() {
  local job_name="$1"
  gcloud run jobs describe "$job_name" \
    --project "$PROJECT_ID" \
    --region "$REGION" \
    --format "value(name)" >/dev/null 2>&1
}

upsert_cloud_run_job() {
  local job_name="$1"
  local description="$2"
  local args_ref_name="$3"
  local env_ref_name="$4"
  local task_timeout_seconds="${5:-3600}"
  local memory="${6:-512Mi}"
  local cpu="${7:-1}"
  local -n container_args_ref="$args_ref_name"
  local -n set_env_vars_ref="$env_ref_name"

  local env_vars
  env_vars="$(join_cloud_run_env_vars "${BASE_ENV_VARS[@]}" "${set_env_vars_ref[@]}")"
  local joined_args
  joined_args="$(join_by ',' "${container_args_ref[@]}")"

  local common_args=(
    --project "$PROJECT_ID"
    --region "$REGION"
    --image "$CLOUD_RUN_IMAGE"
    --service-account "$SERVICE_ACCOUNT"
    --set-env-vars "$env_vars"
    --max-retries "1"
    --task-timeout "${task_timeout_seconds}s"
    --memory "$memory"
    --cpu "$cpu"
    "--args=${joined_args}"
    --quiet
  )

  if cloud_run_job_exists "$job_name"; then
    log "Updating Cloud Run Job: ${job_name}"
    gcloud run jobs update "$job_name" "${common_args[@]}"
  else
    log "Creating Cloud Run Job: ${job_name}"
    gcloud run jobs create "$job_name" "${common_args[@]}"
  fi

  log "Job configured: ${job_name} - ${description}"
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$PROJECT_ROOT"

PROJECT_ID="${GCP_PROJECT_ID:-}"
REGION="${CLOUD_RUN_JOBS_REGION:-${CLOUD_RUN_REGION:-${GCP_REGION:-}}}"
SERVICE_ACCOUNT="${CLOUD_RUN_JOBS_SERVICE_ACCOUNT:-${CLOUD_RUN_SERVICE_ACCOUNT:-}}"
BUCKET_NAME="${GCS_BUCKET_NAME:-${GCS_BUCKET:-}}"
BQ_LOCATION="${BQ_LOCATION:-US}"

BRONZE_DATASET="${BQ_BRONZE_DATASET:-bronze}"
SILVER_DATASET="${BQ_SILVER_DATASET:-silver}"
GOLD_DATASET="${BQ_GOLD_DATASET:-gold}"
AUDIT_DATASET="${BQ_AUDIT_DATASET:-audit}"
ML_DATASET="${BQ_ML_DATASET:-ml}"

DATAFLOW_TEMP_LOCATION="${DATAFLOW_TEMP_LOCATION:-}"
DATAFLOW_STAGING_LOCATION="${DATAFLOW_STAGING_LOCATION:-}"
DATAFLOW_SERVICE_ACCOUNT="${DATAFLOW_SERVICE_ACCOUNT:-}"
DATAFLOW_SDK_CONTAINER_IMAGE="${DATAFLOW_SDK_CONTAINER_IMAGE:-}"
ARTIFACT_REGION="${ARTIFACT_REGISTRY_REGION:-${ARTIFACT_REGISTRY_LOCATION:-${GCP_REGION:-}}}"
ARTIFACT_REPOSITORY="${ARTIFACT_REGISTRY_REPOSITORY:-pronabec-containers}"
DATAFLOW_WORKER_IMAGE_NAME="${DATAFLOW_WORKER_IMAGE_NAME:-pronabec-dataflow-worker}"
DATAFLOW_WORKER_IMAGE_TAG="${DATAFLOW_WORKER_IMAGE_TAG:-latest}"
DATAFLOW_WORKER_IMAGE="${DATAFLOW_WORKER_IMAGE:-}"

MEF_SOURCE_MODE="${MEF_SOURCE_MODE:-}"
MEF_CONSULTA_AMIGABLE_BASE_URL="${MEF_CONSULTA_AMIGABLE_BASE_URL:-https://apps5.mineco.gob.pe/transparencia/Navegador/}"
MEF_START_YEAR="${MEF_START_YEAR:-}"
MEF_END_YEAR="${MEF_END_YEAR:-}"
MEF_TEXT_FILTER="${MEF_TEXT_FILTER:-}"
MEF_TIMEOUT_SECONDS="${MEF_TIMEOUT_SECONDS:-90}"
MEF_PRONABEC_EXECUTORA_CODE="${MEF_PRONABEC_EXECUTORA_CODE:-}"
MEF_PRONABEC_EXECUTORA_NAME="${MEF_PRONABEC_EXECUTORA_NAME:-PROGRAMA NACIONAL DE BECAS Y CREDITO EDUCATIVO}"
MEF_INCLUDE_HIERARCHY="${MEF_INCLUDE_HIERARCHY:-true}"
MEF_INCLUDE_SPENDING_BREAKDOWNS="${MEF_INCLUDE_SPENDING_BREAKDOWNS:-true}"
MEF_BREAKDOWN_SLICES="${MEF_BREAKDOWN_SLICES:-producto,generica,fuente,rubro,departamento,temporal,producto_temporal,actividad,actividad_temporal,generica_temporal}"

PRONABEC_DISCOVERY_JOB_NAME="$(env_or PRONABEC_DISCOVERY_JOB_NAME pronabec-discovery-job)"
PRONABEC_BUILD_PLAN_JOB_NAME="$(env_or PRONABEC_BUILD_PLAN_JOB_NAME pronabec-build-plan-job)"
PRONABEC_RUN_PLAN_JOB_NAME="$(env_or PRONABEC_RUN_PLAN_JOB_NAME pronabec-run-plan-job)"
PRONABEC_FINALIZE_DATASET_JOB_NAME="$(env_or PRONABEC_FINALIZE_DATASET_JOB_NAME pronabec-finalize-dataset-job)"
MEF_EXTRACT_JOB_NAME="$(env_or MEF_EXTRACT_JOB_NAME mef-extract-job)"
PRONABEC_REPORTS_STAGE_JOB_NAME="$(env_or PRONABEC_REPORTS_STAGE_JOB_NAME pronabec-stage-reports-job)"
INEI_REPORTS_STAGE_JOB_NAME="$(env_or INEI_REPORTS_STAGE_JOB_NAME inei-stage-reports-job)"
BRONZE_MANIFEST_VALIDATION_JOB_NAME="$(env_or BRONZE_MANIFEST_VALIDATION_JOB_NAME bronze-manifest-validation-job)"
GOLD_PUBLISH_JOB_NAME="$(env_or GOLD_PUBLISH_JOB_NAME gold-publish-job)"
GOLD_VALIDATE_JOB_NAME="$(env_or GOLD_VALIDATE_JOB_NAME gold-validate-job)"
QUALITY_CHECKS_JOB_NAME="$(env_or QUALITY_CHECKS_JOB_NAME quality-checks-job)"

DATAFLOW_PRONABEC_CONVOCATORIAS_JOB_NAME="$(env_or DATAFLOW_PRONABEC_CONVOCATORIAS_JOB_NAME dataflow-pronabec-convocatorias-job)"
DATAFLOW_PRONABEC_UBIGEO_POSTULACION_JOB_NAME="$(env_or DATAFLOW_PRONABEC_UBIGEO_POSTULACION_JOB_NAME dataflow-pronabec-ubigeo-postulacion-job)"
DATAFLOW_PRONABEC_BECARIOS_PAIS_ESTUDIO_JOB_NAME="$(env_or DATAFLOW_PRONABEC_BECARIOS_PAIS_ESTUDIO_JOB_NAME dataflow-pronabec-becarios-pais-estudio-job)"
DATAFLOW_PRONABEC_COLEGIOS_HABILES_JOB_NAME="$(env_or DATAFLOW_PRONABEC_COLEGIOS_HABILES_JOB_NAME dataflow-pronabec-colegios-habiles-job)"
DATAFLOW_PRONABEC_BECARIOS_PROVINCIA_JOB_NAME="$(env_or DATAFLOW_PRONABEC_BECARIOS_PROVINCIA_JOB_NAME dataflow-pronabec-becarios-provincia-job)"
DATAFLOW_MEF_PRESUPUESTO_JOB_NAME="$(env_or DATAFLOW_MEF_PRESUPUESTO_JOB_NAME dataflow-mef-presupuesto-job)"
DATAFLOW_MEF_PRESUPUESTO_TEMPORAL_JOB_NAME="$(env_or DATAFLOW_MEF_PRESUPUESTO_TEMPORAL_JOB_NAME dataflow-mef-presupuesto-temporal-job)"
DATAFLOW_MEF_PRODUCTO_JOB_NAME="$(env_or DATAFLOW_MEF_PRODUCTO_JOB_NAME dataflow-mef-producto-job)"
DATAFLOW_MEF_PRODUCTO_TEMPORAL_JOB_NAME="$(env_or DATAFLOW_MEF_PRODUCTO_TEMPORAL_JOB_NAME dataflow-mef-producto-temporal-job)"
DATAFLOW_MEF_ACTIVIDAD_JOB_NAME="$(env_or DATAFLOW_MEF_ACTIVIDAD_JOB_NAME dataflow-mef-actividad-job)"
DATAFLOW_MEF_ACTIVIDAD_TEMPORAL_JOB_NAME="$(env_or DATAFLOW_MEF_ACTIVIDAD_TEMPORAL_JOB_NAME dataflow-mef-actividad-temporal-job)"
DATAFLOW_MEF_GENERICA_JOB_NAME="$(env_or DATAFLOW_MEF_GENERICA_JOB_NAME dataflow-mef-generica-job)"
DATAFLOW_MEF_GENERICA_TEMPORAL_JOB_NAME="$(env_or DATAFLOW_MEF_GENERICA_TEMPORAL_JOB_NAME dataflow-mef-generica-temporal-job)"
DATAFLOW_MEF_HIERARCHY_JOB_NAME="$(env_or DATAFLOW_MEF_HIERARCHY_JOB_NAME dataflow-mef-hierarchy-job)"
DATAFLOW_PRONABEC_REPORT_JOB_NAME="$(env_or DATAFLOW_PRONABEC_REPORT_JOB_NAME dataflow-pronabec-report-job)"
DATAFLOW_INEI_REPORT_JOB_NAME="$(env_or DATAFLOW_INEI_REPORT_JOB_NAME dataflow-inei-report-job)"
MINEDU_ESCALE_EXTRACT_JOB_NAME="$(env_or MINEDU_ESCALE_EXTRACT_JOB_NAME minedu-escale-extract-job)"
DATAFLOW_MINEDU_ESCALE_JOB_NAME="$(env_or DATAFLOW_MINEDU_ESCALE_JOB_NAME dataflow-minedu-escale-job)"

PRONABEC_REPORTS_LANDING_PREFIX="${PRONABEC_REPORTS_LANDING_PREFIX:-landing/pronabec_reports}"
PRONABEC_REPORTS_BRONZE_PREFIX="${PRONABEC_REPORTS_BRONZE_PREFIX:-bronze/pronabec_reports}"
INEI_REPORTS_LANDING_PREFIX="${INEI_REPORTS_LANDING_PREFIX:-landing/inei_reports}"
INEI_REPORTS_BRONZE_PREFIX="${INEI_REPORTS_BRONZE_PREFIX:-bronze/inei_reports}"
MINEDU_ESCALE_START_YEAR="${MINEDU_ESCALE_START_YEAR:-2012}"
MINEDU_ESCALE_END_YEAR="${MINEDU_ESCALE_END_YEAR:-2025}"

require_env PROJECT_ID
require_env REGION
require_env CLOUD_RUN_IMAGE
require_env SERVICE_ACCOUNT
require_env BUCKET_NAME
require_env DATAFLOW_TEMP_LOCATION
require_env DATAFLOW_STAGING_LOCATION
require_env DATAFLOW_SERVICE_ACCOUNT
require_env MEF_SOURCE_MODE
require_env MEF_START_YEAR
require_env MEF_END_YEAR
require_env MEF_TEXT_FILTER
require_env MEF_PRONABEC_EXECUTORA_CODE

if [[ -z "$DATAFLOW_SDK_CONTAINER_IMAGE" ]]; then
  if [[ -z "$DATAFLOW_WORKER_IMAGE" && -n "$ARTIFACT_REGION" ]]; then
    DATAFLOW_WORKER_IMAGE="${ARTIFACT_REGION}-docker.pkg.dev/${PROJECT_ID}/${ARTIFACT_REPOSITORY}/${DATAFLOW_WORKER_IMAGE_NAME}:${DATAFLOW_WORKER_IMAGE_TAG}"
  fi
  DATAFLOW_SDK_CONTAINER_IMAGE="$DATAFLOW_WORKER_IMAGE"
fi
require_env DATAFLOW_SDK_CONTAINER_IMAGE

BASE_ENV_VARS=(
  "GCP_PROJECT_ID=${PROJECT_ID}"
  "GCS_BUCKET=${BUCKET_NAME}"
  "GCS_BUCKET_NAME=${BUCKET_NAME}"
  "BQ_BRONZE_DATASET=${BRONZE_DATASET}"
  "BQ_SILVER_DATASET=${SILVER_DATASET}"
  "BQ_GOLD_DATASET=${GOLD_DATASET}"
  "BQ_AUDIT_DATASET=${AUDIT_DATASET}"
  "BQ_LOCATION=${BQ_LOCATION}"
  "PRONABEC_REPORTS_LANDING_PREFIX=${PRONABEC_REPORTS_LANDING_PREFIX}"
  "PRONABEC_REPORTS_BRONZE_PREFIX=${PRONABEC_REPORTS_BRONZE_PREFIX}"
  "INEI_REPORTS_LANDING_PREFIX=${INEI_REPORTS_LANDING_PREFIX}"
  "INEI_REPORTS_BRONZE_PREFIX=${INEI_REPORTS_BRONZE_PREFIX}"
  "MINEDU_ESCALE_START_YEAR=${MINEDU_ESCALE_START_YEAR}"
  "MINEDU_ESCALE_END_YEAR=${MINEDU_ESCALE_END_YEAR}"
  "PRONABEC_REQUEST_TIMEOUT_SECONDS=${PRONABEC_REQUEST_TIMEOUT_SECONDS:-180}"
  "PRONABEC_MAX_RETRIES=${PRONABEC_MAX_RETRIES:-5}"
  "PRONABEC_BACKOFF_BASE_SECONDS=${PRONABEC_BACKOFF_BASE_SECONDS:-10}"
  "PRONABEC_BACKOFF_MAX_SECONDS=${PRONABEC_BACKOFF_MAX_SECONDS:-120}"
  "STRUCTURED_LOGGING=true"
  "LOG_LEVEL=INFO"
)

EMPTY_ENV=()
DATAFLOW_ENV_VARS=(
  "DATAFLOW_TEMP_LOCATION=${DATAFLOW_TEMP_LOCATION}"
  "DATAFLOW_STAGING_LOCATION=${DATAFLOW_STAGING_LOCATION}"
  "DATAFLOW_SERVICE_ACCOUNT=${DATAFLOW_SERVICE_ACCOUNT}"
  "DATAFLOW_SDK_CONTAINER_IMAGE=${DATAFLOW_SDK_CONTAINER_IMAGE}"
)
DATAFLOW_COMMON_ARGS=(
  -m pipelines.dataflow_bronze_to_silver
  --runner DataflowRunner
  --project "$PROJECT_ID"
  --region "$REGION"
  --temp-location "$DATAFLOW_TEMP_LOCATION"
  --staging-location "$DATAFLOW_STAGING_LOCATION"
  --service-account-email "$DATAFLOW_SERVICE_ACCOUNT"
  --sdk-container-image "$DATAFLOW_SDK_CONTAINER_IMAGE"
  --dlq-output-root "gs://${BUCKET_NAME}/dlq"
)

ARGS_PRONABEC_DISCOVERY=(-m pipelines.discover_pronabec)
upsert_cloud_run_job "$PRONABEC_DISCOVERY_JOB_NAME" "Discovery de datasets PRONABEC para planificacion particionada" ARGS_PRONABEC_DISCOVERY EMPTY_ENV 10800

ARGS_PRONABEC_BUILD_PLAN=(-m pipelines.build_pronabec_extraction_plan)
upsert_cloud_run_job "$PRONABEC_BUILD_PLAN_JOB_NAME" "Construccion del plan de extraccion PRONABEC particionado" ARGS_PRONABEC_BUILD_PLAN EMPTY_ENV 7200

ARGS_PRONABEC_RUN_PLAN=(-m pipelines.run_pronabec_extraction_plan)
upsert_cloud_run_job "$PRONABEC_RUN_PLAN_JOB_NAME" "Ejecucion del plan de chunks PRONABEC desde plan.json" ARGS_PRONABEC_RUN_PLAN EMPTY_ENV 14400

ARGS_PRONABEC_FINALIZE=(-m pipelines.finalize_pronabec_dataset)
upsert_cloud_run_job "$PRONABEC_FINALIZE_DATASET_JOB_NAME" "Consolidacion final de chunks PRONABEC hacia Bronze" ARGS_PRONABEC_FINALIZE EMPTY_ENV 7200 2Gi 1

ENV_MEF_EXTRACT=(
  "MEF_SOURCE_MODE=${MEF_SOURCE_MODE}"
  "MEF_CONSULTA_AMIGABLE_BASE_URL=${MEF_CONSULTA_AMIGABLE_BASE_URL}"
  "MEF_START_YEAR=${MEF_START_YEAR}"
  "MEF_END_YEAR=${MEF_END_YEAR}"
  "MEF_TEXT_FILTER=${MEF_TEXT_FILTER}"
  "MEF_TIMEOUT_SECONDS=${MEF_TIMEOUT_SECONDS}"
  "MEF_PRONABEC_EXECUTORA_CODE=${MEF_PRONABEC_EXECUTORA_CODE}"
  "MEF_PRONABEC_EXECUTORA_NAME=${MEF_PRONABEC_EXECUTORA_NAME}"
  "MEF_INCLUDE_HIERARCHY=${MEF_INCLUDE_HIERARCHY}"
  "MEF_INCLUDE_SPENDING_BREAKDOWNS=${MEF_INCLUDE_SPENDING_BREAKDOWNS}"
  "MEF_BREAKDOWN_SLICES=${MEF_BREAKDOWN_SLICES}"
)
ARGS_MEF_EXTRACT=(-m pipelines.scrape_mef_budget)
upsert_cloud_run_job "$MEF_EXTRACT_JOB_NAME" "Extraccion batch MEF hacia Bronze" ARGS_MEF_EXTRACT ENV_MEF_EXTRACT

ARGS_REPORTS_STAGE=(-m tools.stage_pronabec_manual_reports --strict --overwrite)
upsert_cloud_run_job "$PRONABEC_REPORTS_STAGE_JOB_NAME" "Staging PRONABEC reports desde GCS Landing hacia Bronze" ARGS_REPORTS_STAGE EMPTY_ENV

ARGS_INEI_STAGE=(-m tools.stage_inei_reports --strict --overwrite)
upsert_cloud_run_job "$INEI_REPORTS_STAGE_JOB_NAME" "Staging INEI regional reports desde GCS Landing hacia Bronze" ARGS_INEI_STAGE EMPTY_ENV

ARGS_MINEDU_ESCALE_EXTRACT=(-m pipelines.scrape_minedu_escale)
upsert_cloud_run_job "$MINEDU_ESCALE_EXTRACT_JOB_NAME" "Extraccion MINEDU ESCALE matricula secundaria hacia Bronze" ARGS_MINEDU_ESCALE_EXTRACT EMPTY_ENV 7200

ARGS_BRONZE_VALIDATION=(-m pipelines.validate_bronze_manifests)
upsert_cloud_run_job "$BRONZE_MANIFEST_VALIDATION_JOB_NAME" "Validacion de manifests Bronze antes de promover a Silver" ARGS_BRONZE_VALIDATION EMPTY_ENV

ARGS_GOLD_PUBLISH=(-m pipelines.publish_gold_views)
upsert_cloud_run_job "$GOLD_PUBLISH_JOB_NAME" "Publicacion idempotente de vistas Gold analiticas" ARGS_GOLD_PUBLISH EMPTY_ENV

ARGS_GOLD_VALIDATE=(-m pipelines.validate_gold)
upsert_cloud_run_job "$GOLD_VALIDATE_JOB_NAME" "Validacion de contratos Gold analiticos" ARGS_GOLD_VALIDATE EMPTY_ENV

ARGS_DF_PRONABEC_CONVOCATORIAS=("${DATAFLOW_COMMON_ARGS[@]}" --source-system pronabec --source-dataset convocatorias --input-path "gs://${BUCKET_NAME}/bronze/pronabec/convocatorias/extraction_date=\${BRONZE_EXTRACTION_DATE}/data.jsonl" --input-format jsonl --output-table "${PROJECT_ID}:${SILVER_DATASET}.pronabec_convocatorias" --summary-output-path "gs://${BUCKET_NAME}/audit/processing_summary/pronabec_convocatorias_\${BRONZE_EXTRACTION_DATE}.json")
upsert_cloud_run_job "$DATAFLOW_PRONABEC_CONVOCATORIAS_JOB_NAME" "Lanzador Dataflow PRONABEC convocatorias Bronze a Silver" ARGS_DF_PRONABEC_CONVOCATORIAS DATAFLOW_ENV_VARS 7200

ARGS_DF_PRONABEC_UBIGEO=("${DATAFLOW_COMMON_ARGS[@]}" --source-system pronabec --source-dataset ubigeo_postulacion --input-path "gs://${BUCKET_NAME}/bronze/pronabec/ubigeo_postulacion/extraction_date=\${BRONZE_EXTRACTION_DATE}/data.jsonl" --input-format jsonl --output-table "${PROJECT_ID}:${SILVER_DATASET}.pronabec_ubigeo_postulacion" --summary-output-path "gs://${BUCKET_NAME}/audit/processing_summary/pronabec_ubigeo_postulacion_\${BRONZE_EXTRACTION_DATE}.json")
upsert_cloud_run_job "$DATAFLOW_PRONABEC_UBIGEO_POSTULACION_JOB_NAME" "Lanzador Dataflow PRONABEC ubigeo postulacion Bronze a Silver" ARGS_DF_PRONABEC_UBIGEO DATAFLOW_ENV_VARS 7200

ARGS_DF_PRONABEC_BECARIOS_PAIS=("${DATAFLOW_COMMON_ARGS[@]}" --source-system pronabec --source-dataset becarios_pais_estudio --input-path "gs://${BUCKET_NAME}/bronze/pronabec/becarios_pais_estudio/extraction_date=\${BRONZE_EXTRACTION_DATE}/data.jsonl" --input-format jsonl --output-table "${PROJECT_ID}:${SILVER_DATASET}.pronabec_becarios_pais_estudio" --summary-output-path "gs://${BUCKET_NAME}/audit/processing_summary/pronabec_becarios_pais_estudio_\${BRONZE_EXTRACTION_DATE}.json")
upsert_cloud_run_job "$DATAFLOW_PRONABEC_BECARIOS_PAIS_ESTUDIO_JOB_NAME" "Lanzador Dataflow PRONABEC becarios pais estudio Bronze a Silver" ARGS_DF_PRONABEC_BECARIOS_PAIS DATAFLOW_ENV_VARS 7200

ARGS_DF_PRONABEC_COLEGIOS=("${DATAFLOW_COMMON_ARGS[@]}" --source-system pronabec --source-dataset colegios_habiles --input-path "gs://${BUCKET_NAME}/bronze/pronabec/colegios_habiles/extraction_date=\${BRONZE_EXTRACTION_DATE}/data.jsonl" --input-format jsonl --output-table "${PROJECT_ID}:${SILVER_DATASET}.pronabec_colegios_elegibles" --summary-output-path "gs://${BUCKET_NAME}/audit/processing_summary/pronabec_colegios_elegibles_\${BRONZE_EXTRACTION_DATE}.json")
upsert_cloud_run_job "$DATAFLOW_PRONABEC_COLEGIOS_HABILES_JOB_NAME" "Lanzador Dataflow PRONABEC colegios habiles Bronze a Silver" ARGS_DF_PRONABEC_COLEGIOS DATAFLOW_ENV_VARS 7200

ARGS_DF_PRONABEC_BECARIOS_PROVINCIA=("${DATAFLOW_COMMON_ARGS[@]}" --source-system pronabec --source-dataset becarios_provincia --input-path "gs://${BUCKET_NAME}/bronze/pronabec/becarios_provincia/extraction_date=\${BRONZE_EXTRACTION_DATE}/data.jsonl" --input-format jsonl --output-table "${PROJECT_ID}:${SILVER_DATASET}.pronabec_beca18_becarios_provincia_2016" --summary-output-path "gs://${BUCKET_NAME}/audit/processing_summary/pronabec_beca18_becarios_provincia_2016_\${BRONZE_EXTRACTION_DATE}.json")
upsert_cloud_run_job "$DATAFLOW_PRONABEC_BECARIOS_PROVINCIA_JOB_NAME" "Lanzador Dataflow PRONABEC becarios provincia Bronze a Silver" ARGS_DF_PRONABEC_BECARIOS_PROVINCIA DATAFLOW_ENV_VARS 7200

ARGS_DF_MEF_PRESUPUESTO=("${DATAFLOW_COMMON_ARGS[@]}" --source-system mef --source-dataset presupuesto --input-path "gs://${BUCKET_NAME}/bronze/mef/presupuesto/extraction_date=\${BRONZE_EXTRACTION_DATE}/year=*/data.csv" --input-format csv --output-table "${PROJECT_ID}:${SILVER_DATASET}.presupuesto_mef" --summary-output-path "gs://${BUCKET_NAME}/audit/processing_summary/presupuesto_mef_\${BRONZE_EXTRACTION_DATE}.json")
upsert_cloud_run_job "$DATAFLOW_MEF_PRESUPUESTO_JOB_NAME" "Lanzador Dataflow MEF presupuesto Bronze a Silver" ARGS_DF_MEF_PRESUPUESTO DATAFLOW_ENV_VARS 7200

ARGS_DF_MEF_PRESUPUESTO_TEMPORAL=("${DATAFLOW_COMMON_ARGS[@]}" --source-system mef --source-dataset presupuesto_temporal --input-path "gs://${BUCKET_NAME}/bronze/mef/presupuesto_temporal/extraction_date=\${BRONZE_EXTRACTION_DATE}/year=*/data.csv" --input-format csv --output-table "${PROJECT_ID}:${SILVER_DATASET}.presupuesto_mef_temporal" --summary-output-path "gs://${BUCKET_NAME}/audit/processing_summary/presupuesto_mef_temporal_\${BRONZE_EXTRACTION_DATE}.json")
upsert_cloud_run_job "$DATAFLOW_MEF_PRESUPUESTO_TEMPORAL_JOB_NAME" "Lanzador Dataflow MEF presupuesto temporal Bronze a Silver" ARGS_DF_MEF_PRESUPUESTO_TEMPORAL DATAFLOW_ENV_VARS 7200

ARGS_DF_MEF_PRODUCTO=("${DATAFLOW_COMMON_ARGS[@]}" --source-system mef --source-dataset presupuesto_producto --input-path "gs://${BUCKET_NAME}/bronze/mef/presupuesto_producto/extraction_date=\${BRONZE_EXTRACTION_DATE}/year=*/data.csv" --input-format csv --output-table "${PROJECT_ID}:${SILVER_DATASET}.presupuesto_mef_producto" --summary-output-path "gs://${BUCKET_NAME}/audit/processing_summary/presupuesto_mef_producto_\${BRONZE_EXTRACTION_DATE}.json")
upsert_cloud_run_job "$DATAFLOW_MEF_PRODUCTO_JOB_NAME" "Lanzador Dataflow MEF producto Bronze a Silver" ARGS_DF_MEF_PRODUCTO DATAFLOW_ENV_VARS 7200

ARGS_DF_MEF_PRODUCTO_TEMPORAL=("${DATAFLOW_COMMON_ARGS[@]}" --source-system mef --source-dataset presupuesto_producto_temporal --input-path "gs://${BUCKET_NAME}/bronze/mef/presupuesto_producto_temporal/extraction_date=\${BRONZE_EXTRACTION_DATE}/year=*/data.csv" --input-format csv --output-table "${PROJECT_ID}:${SILVER_DATASET}.presupuesto_mef_producto_temporal" --summary-output-path "gs://${BUCKET_NAME}/audit/processing_summary/presupuesto_mef_producto_temporal_\${BRONZE_EXTRACTION_DATE}.json")
upsert_cloud_run_job "$DATAFLOW_MEF_PRODUCTO_TEMPORAL_JOB_NAME" "Lanzador Dataflow MEF producto temporal Bronze a Silver" ARGS_DF_MEF_PRODUCTO_TEMPORAL DATAFLOW_ENV_VARS 7200

ARGS_DF_MEF_ACTIVIDAD=("${DATAFLOW_COMMON_ARGS[@]}" --source-system mef --source-dataset presupuesto_actividad --input-path "gs://${BUCKET_NAME}/bronze/mef/presupuesto_actividad/extraction_date=\${BRONZE_EXTRACTION_DATE}/year=*/data.csv" --input-format csv --output-table "${PROJECT_ID}:${SILVER_DATASET}.presupuesto_mef_actividad" --summary-output-path "gs://${BUCKET_NAME}/audit/processing_summary/presupuesto_mef_actividad_\${BRONZE_EXTRACTION_DATE}.json")
upsert_cloud_run_job "$DATAFLOW_MEF_ACTIVIDAD_JOB_NAME" "Lanzador Dataflow MEF actividad Bronze a Silver" ARGS_DF_MEF_ACTIVIDAD DATAFLOW_ENV_VARS 7200

ARGS_DF_MEF_ACTIVIDAD_TEMPORAL=("${DATAFLOW_COMMON_ARGS[@]}" --source-system mef --source-dataset presupuesto_actividad_temporal --input-path "gs://${BUCKET_NAME}/bronze/mef/presupuesto_actividad_temporal/extraction_date=\${BRONZE_EXTRACTION_DATE}/year=*/data.csv" --input-format csv --output-table "${PROJECT_ID}:${SILVER_DATASET}.presupuesto_mef_actividad_temporal" --summary-output-path "gs://${BUCKET_NAME}/audit/processing_summary/presupuesto_mef_actividad_temporal_\${BRONZE_EXTRACTION_DATE}.json")
upsert_cloud_run_job "$DATAFLOW_MEF_ACTIVIDAD_TEMPORAL_JOB_NAME" "Lanzador Dataflow MEF actividad temporal Bronze a Silver" ARGS_DF_MEF_ACTIVIDAD_TEMPORAL DATAFLOW_ENV_VARS 7200

ARGS_DF_MEF_GENERICA=("${DATAFLOW_COMMON_ARGS[@]}" --source-system mef --source-dataset presupuesto_generica --input-path "gs://${BUCKET_NAME}/bronze/mef/presupuesto_generica/extraction_date=\${BRONZE_EXTRACTION_DATE}/year=*/data.csv" --input-format csv --output-table "${PROJECT_ID}:${SILVER_DATASET}.presupuesto_mef_generica" --summary-output-path "gs://${BUCKET_NAME}/audit/processing_summary/presupuesto_mef_generica_\${BRONZE_EXTRACTION_DATE}.json")
upsert_cloud_run_job "$DATAFLOW_MEF_GENERICA_JOB_NAME" "Lanzador Dataflow MEF generica Bronze a Silver" ARGS_DF_MEF_GENERICA DATAFLOW_ENV_VARS 7200

ARGS_DF_MEF_GENERICA_TEMPORAL=("${DATAFLOW_COMMON_ARGS[@]}" --source-system mef --source-dataset presupuesto_generica_temporal --input-path "gs://${BUCKET_NAME}/bronze/mef/presupuesto_generica_temporal/extraction_date=\${BRONZE_EXTRACTION_DATE}/year=*/data.csv" --input-format csv --output-table "${PROJECT_ID}:${SILVER_DATASET}.presupuesto_mef_generica_temporal" --summary-output-path "gs://${BUCKET_NAME}/audit/processing_summary/presupuesto_mef_generica_temporal_\${BRONZE_EXTRACTION_DATE}.json")
upsert_cloud_run_job "$DATAFLOW_MEF_GENERICA_TEMPORAL_JOB_NAME" "Lanzador Dataflow MEF generica temporal Bronze a Silver" ARGS_DF_MEF_GENERICA_TEMPORAL DATAFLOW_ENV_VARS 7200

ARGS_DF_MEF_HIERARCHY=("${DATAFLOW_COMMON_ARGS[@]}" --source-system mef --source-dataset presupuesto_hierarchy --input-path "gs://${BUCKET_NAME}/bronze/mef/presupuesto_hierarchy/extraction_date=\${BRONZE_EXTRACTION_DATE}/year=*/data.csv" --input-format csv --output-table "${PROJECT_ID}:${SILVER_DATASET}.presupuesto_mef_hierarchy" --summary-output-path "gs://${BUCKET_NAME}/audit/processing_summary/presupuesto_mef_hierarchy_\${BRONZE_EXTRACTION_DATE}.json")
upsert_cloud_run_job "$DATAFLOW_MEF_HIERARCHY_JOB_NAME" "Lanzador Dataflow MEF jerarquia Bronze a Silver" ARGS_DF_MEF_HIERARCHY DATAFLOW_ENV_VARS 7200

ENV_DF_PRONABEC_REPORT=("${DATAFLOW_ENV_VARS[@]}" "SOURCE_DATASET=placeholder_dataset" "INPUT_PATH=gs://${BUCKET_NAME}/placeholder_path" "OUTPUT_TABLE=${PROJECT_ID}:${SILVER_DATASET}.placeholder_table")
ARGS_DF_PRONABEC_REPORT=("${DATAFLOW_COMMON_ARGS[@]}" --source-system pronabec_reports --source-dataset "\${SOURCE_DATASET}" --input-path "\${INPUT_PATH}" --input-format csv --output-table "\${OUTPUT_TABLE}" --summary-output-path "gs://${BUCKET_NAME}/audit/processing_summary/\${SOURCE_DATASET}_\${BRONZE_EXTRACTION_DATE}.json")
upsert_cloud_run_job "$DATAFLOW_PRONABEC_REPORT_JOB_NAME" "Lanzador Dataflow parametrizable para PRONABEC reports Bronze a Silver" ARGS_DF_PRONABEC_REPORT ENV_DF_PRONABEC_REPORT 7200

ENV_DF_INEI_REPORT=("${DATAFLOW_ENV_VARS[@]}" "SOURCE_SYSTEM=inei_reports" "SOURCE_DATASET=inei_population_youth_region" "BRONZE_INPUT_PATH=gs://${BUCKET_NAME}/${INEI_REPORTS_BRONZE_PREFIX}/\${SOURCE_DATASET}/extraction_date=\${BRONZE_EXTRACTION_DATE}/data.csv" "BQ_OUTPUT_TABLE=${PROJECT_ID}:${SILVER_DATASET}.\${SOURCE_DATASET}")
ARGS_DF_INEI_REPORT=("${DATAFLOW_COMMON_ARGS[@]}" --source-system "\${SOURCE_SYSTEM}" --source-dataset "\${SOURCE_DATASET}" --input-path "\${BRONZE_INPUT_PATH}" --input-format csv --output-table "\${BQ_OUTPUT_TABLE}" --summary-output-path "gs://${BUCKET_NAME}/audit/processing_summary/\${SOURCE_DATASET}_\${BRONZE_EXTRACTION_DATE}.json")
upsert_cloud_run_job "$DATAFLOW_INEI_REPORT_JOB_NAME" "Lanzador Dataflow parametrizable para INEI regional reports Bronze a Silver" ARGS_DF_INEI_REPORT ENV_DF_INEI_REPORT 7200

ARGS_DF_MINEDU_ESCALE=("${DATAFLOW_COMMON_ARGS[@]}" --source-system minedu_escale --source-dataset minedu_matricula_secundaria_departamental --input-path "gs://${BUCKET_NAME}/bronze/minedu/escale_matricula_secundaria/extraction_date=\${BRONZE_EXTRACTION_DATE}/data.csv" --input-format csv --output-table "${PROJECT_ID}:${SILVER_DATASET}.minedu_matricula_secundaria_departamental" --summary-output-path "gs://${BUCKET_NAME}/audit/processing_summary/minedu_matricula_secundaria_departamental_\${BRONZE_EXTRACTION_DATE}.json")
upsert_cloud_run_job "$DATAFLOW_MINEDU_ESCALE_JOB_NAME" "Lanzador Dataflow MINEDU ESCALE Bronze a Silver" ARGS_DF_MINEDU_ESCALE DATAFLOW_ENV_VARS 7200

ARGS_QUALITY=(-m pipelines.quality_checks --project-id "$PROJECT_ID" --silver-dataset "$SILVER_DATASET" --gold-dataset "$GOLD_DATASET" --audit-dataset "$AUDIT_DATASET" --ml-dataset "$ML_DATASET" --extraction-date "\${BRONZE_EXTRACTION_DATE}" --pipeline-run-id "\${PIPELINE_RUN_ID}" --fail-on-error)
upsert_cloud_run_job "$QUALITY_CHECKS_JOB_NAME" "Ejecucion batch de controles de calidad BigQuery" ARGS_QUALITY EMPTY_ENV

log "Cloud Run Jobs configured successfully."
