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

run_bq_sql_file() {
  local sql_path="$1"
  local step_name="$2"

  if [[ ! -f "$sql_path" ]]; then
    echo "ERROR: SQL file not found for ${step_name}: ${sql_path}" >&2
    exit 1
  fi
  if [[ ! -s "$sql_path" ]]; then
    echo "ERROR: SQL file is empty for ${step_name}: ${sql_path}" >&2
    exit 1
  fi

  log "Executing BigQuery SQL: ${step_name}"
  bq query \
    --project_id="$GCP_PROJECT_ID" \
    --use_legacy_sql=false \
    --location "$BQ_LOCATION" \
    --quiet < "$sql_path"
  log "BigQuery SQL completed: ${step_name}"
}

resolve_latest_bronze_extraction_date() {
  local latest_path
  latest_path="$(
    gcloud storage ls "gs://${GCS_BUCKET_NAME}/bronze/mef/presupuesto/extraction_date=*/" 2>/dev/null \
      | sed -E 's#^gs://.*/extraction_date=([^/]+)/$#\1#' \
      | sort \
      | tail -n 1
  )"
  if [[ -z "$latest_path" ]]; then
    echo "ERROR: BRONZE_EXTRACTION_DATE=latest requested, but no MEF Bronze extraction_date folders were found." >&2
    exit 1
  fi
  printf '%s' "$latest_path"
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$PROJECT_ROOT"

SKIP_GENERATED_DDL=false
SKIP_RENDER=false
SKIP_GOLD=false
EXECUTE_BRONZE_EXTERNAL_TABLES=true

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-generated-ddl) SKIP_GENERATED_DDL=true ;;
    --skip-render) SKIP_RENDER=true ;;
    --skip-gold) SKIP_GOLD=true ;;
    --help)
      cat <<'USAGE'
Usage: scripts/deploy_bigquery_sql.sh [--skip-generated-ddl] [--skip-render] [--skip-gold]
USAGE
      exit 0
      ;;
    *) echo "ERROR: unknown argument: $1" >&2; exit 1 ;;
  esac
  shift
done

GCS_BUCKET_NAME="${GCS_BUCKET_NAME:-${GCS_BUCKET:-}}"
BQ_LOCATION="${BQ_LOCATION:-US}"
REQUIRE_BRONZE_EXTERNAL_TABLES="${REQUIRE_BRONZE_EXTERNAL_TABLES:-false}"

require_env GCP_PROJECT_ID
require_env GCS_BUCKET_NAME
require_env BQ_LOCATION

export PROJECT_ID="$GCP_PROJECT_ID"
export BUCKET_NAME="$GCS_BUCKET_NAME"
export OUTPUT_DIR="build/generated/sql"

if [[ "${BRONZE_EXTRACTION_DATE:-}" == "latest" ]]; then
  BRONZE_EXTRACTION_DATE="$(resolve_latest_bronze_extraction_date)"
  export BRONZE_EXTRACTION_DATE
  log "Resolved latest BRONZE_EXTRACTION_DATE=${BRONZE_EXTRACTION_DATE}"
fi

if [[ "$SKIP_GENERATED_DDL" == false ]]; then
  if [[ -n "${BRONZE_EXTRACTION_DATE:-}" ]]; then
    scripts/generate_bigquery_ddl.sh --deploy
    EXECUTE_BRONZE_EXTERNAL_TABLES=true
  elif [[ "$REQUIRE_BRONZE_EXTERNAL_TABLES" == "true" ]]; then
    scripts/generate_bigquery_ddl.sh --deploy
  else
    log "BRONZE_EXTRACTION_DATE is not set; generating static DDL and skipping Bronze external tables."
    scripts/generate_bigquery_ddl.sh --ci
    EXECUTE_BRONZE_EXTERNAL_TABLES=false
  fi
fi

if [[ "$SKIP_RENDER" == false ]]; then
  scripts/render_sql_templates.sh
fi

GENERATED_SQL_DIR="build/generated/sql"

run_bq_sql_file "${GENERATED_SQL_DIR}/create_datasets.rendered.sql" "Datasets BigQuery"

if [[ "$SKIP_GENERATED_DDL" == false ]]; then
  if [[ "$EXECUTE_BRONZE_EXTERNAL_TABLES" == true ]]; then
    run_bq_sql_file "${GENERATED_SQL_DIR}/create_bronze_external_tables.sql" "Tablas externas Bronze"
  else
    log "Skipping Bronze external tables because no runtime BRONZE_EXTRACTION_DATE was provided."
  fi
  run_bq_sql_file "${GENERATED_SQL_DIR}/create_silver_tables.sql" "Tablas Silver"
fi

run_bq_sql_file "${GENERATED_SQL_DIR}/create_audit_tables.rendered.sql" "Tablas Audit"
run_bq_sql_file "${GENERATED_SQL_DIR}/create_dim_region_mapping.rendered.sql" "Dimensión ML de regiones"
run_bq_sql_file "${GENERATED_SQL_DIR}/create_region_context_features.rendered.sql" "Features ML regionales"
run_bq_sql_file "${GENERATED_SQL_DIR}/create_region_priority_scores.rendered.sql" "Score ML de prioridad regional"

if [[ "$SKIP_GOLD" == false ]]; then
  run_bq_sql_file "${GENERATED_SQL_DIR}/create_gold_views.rendered.sql" "Vistas Gold"
fi

log "BigQuery SQL deployment completed successfully."
