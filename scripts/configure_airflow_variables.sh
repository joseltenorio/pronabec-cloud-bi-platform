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

set_airflow_variable() {
  local key="$1"
  local value="$2"
  log "Setting Airflow Variable: ${key}=${value}"
  gcloud composer environments run "$COMPOSER_ENVIRONMENT_NAME" \
    --location "$COMPOSER_LOCATION" \
    --project "$GCP_PROJECT_ID" \
    variables set -- "$key" "$value"
}

COMPOSER_ENVIRONMENT_NAME="${COMPOSER_ENVIRONMENT_NAME:-pronabec-composer}"
COMPOSER_LOCATION="${COMPOSER_LOCATION:-${GCP_REGION:-}}"
GCP_REGION_VALUE="${GCP_REGION:-}"
CLOUD_RUN_REGION_VALUE="${CLOUD_RUN_REGION:-${GCP_REGION:-}}"
GCS_BUCKET_NAME_VALUE="${GCS_BUCKET_NAME:-${GCS_BUCKET:-}}"

BQ_BRONZE_DATASET_VALUE="${BQ_BRONZE_DATASET:-bronze}"
BQ_SILVER_DATASET_VALUE="${BQ_SILVER_DATASET:-silver}"
BQ_GOLD_DATASET_VALUE="${BQ_GOLD_DATASET:-gold}"
BQ_AUDIT_DATASET_VALUE="${BQ_AUDIT_DATASET:-audit}"

PRONABEC_DISCOVERY_JOB_NAME_VALUE="${PRONABEC_DISCOVERY_JOB_NAME:-pronabec-discovery-job}"
PRONABEC_BUILD_PLAN_JOB_NAME_VALUE="${PRONABEC_BUILD_PLAN_JOB_NAME:-pronabec-build-plan-job}"
PRONABEC_RUN_PLAN_JOB_NAME_VALUE="${PRONABEC_RUN_PLAN_JOB_NAME:-pronabec-run-plan-job}"
PRONABEC_FINALIZE_DATASET_JOB_NAME_VALUE="${PRONABEC_FINALIZE_DATASET_JOB_NAME:-pronabec-finalize-dataset-job}"
MEF_EXTRACT_JOB_NAME_VALUE="${MEF_EXTRACT_JOB_NAME:-mef-extract-job}"
PRONABEC_REPORTS_STAGE_JOB_NAME_VALUE="${PRONABEC_REPORTS_STAGE_JOB_NAME:-pronabec-stage-reports-job}"
BRONZE_MANIFEST_VALIDATION_JOB_NAME_VALUE="${BRONZE_MANIFEST_VALIDATION_JOB_NAME:-bronze-manifest-validation-job}"
DATAFLOW_PRONABEC_REPORT_JOB_NAME_VALUE="${DATAFLOW_PRONABEC_REPORT_JOB_NAME:-dataflow-pronabec-report-job}"
DATAFLOW_PRONABEC_CONVOCATORIAS_JOB_NAME_VALUE="${DATAFLOW_PRONABEC_CONVOCATORIAS_JOB_NAME:-dataflow-pronabec-convocatorias-job}"
DATAFLOW_PRONABEC_UBIGEO_POSTULACION_JOB_NAME_VALUE="${DATAFLOW_PRONABEC_UBIGEO_POSTULACION_JOB_NAME:-dataflow-pronabec-ubigeo-postulacion-job}"
DATAFLOW_PRONABEC_BECARIOS_PAIS_ESTUDIO_JOB_NAME_VALUE="${DATAFLOW_PRONABEC_BECARIOS_PAIS_ESTUDIO_JOB_NAME:-dataflow-pronabec-becarios-pais-estudio-job}"
DATAFLOW_PRONABEC_COLEGIOS_HABILES_JOB_NAME_VALUE="${DATAFLOW_PRONABEC_COLEGIOS_HABILES_JOB_NAME:-dataflow-pronabec-colegios-habiles-job}"
DATAFLOW_PRONABEC_BECARIOS_PROVINCIA_JOB_NAME_VALUE="${DATAFLOW_PRONABEC_BECARIOS_PROVINCIA_JOB_NAME:-dataflow-pronabec-becarios-provincia-job}"
DATAFLOW_MEF_PRESUPUESTO_JOB_NAME_VALUE="${DATAFLOW_MEF_PRESUPUESTO_JOB_NAME:-dataflow-mef-presupuesto-job}"
DATAFLOW_MEF_PRESUPUESTO_TEMPORAL_JOB_NAME_VALUE="${DATAFLOW_MEF_PRESUPUESTO_TEMPORAL_JOB_NAME:-dataflow-mef-presupuesto-temporal-job}"
DATAFLOW_MEF_PRODUCTO_JOB_NAME_VALUE="${DATAFLOW_MEF_PRODUCTO_JOB_NAME:-dataflow-mef-producto-job}"
DATAFLOW_MEF_PRODUCTO_TEMPORAL_JOB_NAME_VALUE="${DATAFLOW_MEF_PRODUCTO_TEMPORAL_JOB_NAME:-dataflow-mef-producto-temporal-job}"
DATAFLOW_MEF_ACTIVIDAD_JOB_NAME_VALUE="${DATAFLOW_MEF_ACTIVIDAD_JOB_NAME:-dataflow-mef-actividad-job}"
DATAFLOW_MEF_ACTIVIDAD_TEMPORAL_JOB_NAME_VALUE="${DATAFLOW_MEF_ACTIVIDAD_TEMPORAL_JOB_NAME:-dataflow-mef-actividad-temporal-job}"
DATAFLOW_MEF_GENERICA_JOB_NAME_VALUE="${DATAFLOW_MEF_GENERICA_JOB_NAME:-dataflow-mef-generica-job}"
DATAFLOW_MEF_GENERICA_TEMPORAL_JOB_NAME_VALUE="${DATAFLOW_MEF_GENERICA_TEMPORAL_JOB_NAME:-dataflow-mef-generica-temporal-job}"
DATAFLOW_MEF_HIERARCHY_JOB_NAME_VALUE="${DATAFLOW_MEF_HIERARCHY_JOB_NAME:-dataflow-mef-hierarchy-job}"
GOLD_PUBLISH_JOB_NAME_VALUE="${GOLD_PUBLISH_JOB_NAME:-gold-publish-job}"
GOLD_VALIDATE_JOB_NAME_VALUE="${GOLD_VALIDATE_JOB_NAME:-gold-validate-job}"
QUALITY_CHECKS_JOB_NAME_VALUE="${QUALITY_CHECKS_JOB_NAME:-quality-checks-job}"

require_env GCP_PROJECT_ID
require_env COMPOSER_ENVIRONMENT_NAME
require_env COMPOSER_LOCATION
require_env GCP_REGION_VALUE
require_env CLOUD_RUN_REGION_VALUE
require_env GCS_BUCKET_NAME_VALUE
require_env DATAFLOW_SDK_CONTAINER_IMAGE

./scripts/check_composer_environment.sh --allow-missing

if ! gcloud composer environments describe "$COMPOSER_ENVIRONMENT_NAME" \
  --location "$COMPOSER_LOCATION" \
  --project "$GCP_PROJECT_ID" >/dev/null 2>&1; then
  echo "Composer environment '$COMPOSER_ENVIRONMENT_NAME' does not exist. Create it first with scripts/create_composer_environment.sh or enable Create Composer environment in the manual deploy workflow." >&2
  exit 1
fi

set_airflow_variable gcp_project_id "$GCP_PROJECT_ID"
set_airflow_variable gcp_region "$GCP_REGION_VALUE"
set_airflow_variable cloud_run_region "$CLOUD_RUN_REGION_VALUE"
set_airflow_variable gcs_bucket_name "$GCS_BUCKET_NAME_VALUE"
set_airflow_variable bq_bronze_dataset "$BQ_BRONZE_DATASET_VALUE"
set_airflow_variable bq_silver_dataset "$BQ_SILVER_DATASET_VALUE"
set_airflow_variable bq_gold_dataset "$BQ_GOLD_DATASET_VALUE"
set_airflow_variable bq_audit_dataset "$BQ_AUDIT_DATASET_VALUE"
set_airflow_variable dataflow_sdk_container_image "$DATAFLOW_SDK_CONTAINER_IMAGE"
set_airflow_variable pronabec_discovery_job_name "$PRONABEC_DISCOVERY_JOB_NAME_VALUE"
set_airflow_variable pronabec_build_plan_job_name "$PRONABEC_BUILD_PLAN_JOB_NAME_VALUE"
set_airflow_variable pronabec_run_plan_job_name "$PRONABEC_RUN_PLAN_JOB_NAME_VALUE"
set_airflow_variable pronabec_finalize_dataset_job_name "$PRONABEC_FINALIZE_DATASET_JOB_NAME_VALUE"
set_airflow_variable mef_extract_job_name "$MEF_EXTRACT_JOB_NAME_VALUE"
set_airflow_variable pronabec_reports_stage_job_name "$PRONABEC_REPORTS_STAGE_JOB_NAME_VALUE"
set_airflow_variable bronze_manifest_validation_job_name "$BRONZE_MANIFEST_VALIDATION_JOB_NAME_VALUE"
set_airflow_variable dataflow_pronabec_report_job_name "$DATAFLOW_PRONABEC_REPORT_JOB_NAME_VALUE"
set_airflow_variable dataflow_pronabec_convocatorias_job_name "$DATAFLOW_PRONABEC_CONVOCATORIAS_JOB_NAME_VALUE"
set_airflow_variable dataflow_pronabec_ubigeo_postulacion_job_name "$DATAFLOW_PRONABEC_UBIGEO_POSTULACION_JOB_NAME_VALUE"
set_airflow_variable dataflow_pronabec_becarios_pais_estudio_job_name "$DATAFLOW_PRONABEC_BECARIOS_PAIS_ESTUDIO_JOB_NAME_VALUE"
set_airflow_variable dataflow_pronabec_colegios_habiles_job_name "$DATAFLOW_PRONABEC_COLEGIOS_HABILES_JOB_NAME_VALUE"
set_airflow_variable dataflow_pronabec_becarios_provincia_job_name "$DATAFLOW_PRONABEC_BECARIOS_PROVINCIA_JOB_NAME_VALUE"
set_airflow_variable dataflow_mef_presupuesto_job_name "$DATAFLOW_MEF_PRESUPUESTO_JOB_NAME_VALUE"
set_airflow_variable dataflow_mef_presupuesto_temporal_job_name "$DATAFLOW_MEF_PRESUPUESTO_TEMPORAL_JOB_NAME_VALUE"
set_airflow_variable dataflow_mef_producto_job_name "$DATAFLOW_MEF_PRODUCTO_JOB_NAME_VALUE"
set_airflow_variable dataflow_mef_producto_temporal_job_name "$DATAFLOW_MEF_PRODUCTO_TEMPORAL_JOB_NAME_VALUE"
set_airflow_variable dataflow_mef_actividad_job_name "$DATAFLOW_MEF_ACTIVIDAD_JOB_NAME_VALUE"
set_airflow_variable dataflow_mef_actividad_temporal_job_name "$DATAFLOW_MEF_ACTIVIDAD_TEMPORAL_JOB_NAME_VALUE"
set_airflow_variable dataflow_mef_generica_job_name "$DATAFLOW_MEF_GENERICA_JOB_NAME_VALUE"
set_airflow_variable dataflow_mef_generica_temporal_job_name "$DATAFLOW_MEF_GENERICA_TEMPORAL_JOB_NAME_VALUE"
set_airflow_variable dataflow_mef_hierarchy_job_name "$DATAFLOW_MEF_HIERARCHY_JOB_NAME_VALUE"
set_airflow_variable gold_publish_job_name "$GOLD_PUBLISH_JOB_NAME_VALUE"
set_airflow_variable gold_validate_job_name "$GOLD_VALIDATE_JOB_NAME_VALUE"
set_airflow_variable quality_checks_job_name "$QUALITY_CHECKS_JOB_NAME_VALUE"

log "Airflow Variables configured successfully."
