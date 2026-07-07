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

usage() {
  cat <<'USAGE'
Usage: scripts/run_cloud_deploy.sh [--bigquery] [--images] [--jobs] [--composer] [--all] [--help]

Options:
  --bigquery   Deploy BigQuery datasets/tables/views.
  --images     Build and push main and Dataflow worker images.
  --jobs       Deploy Cloud Run Jobs.
  --composer   Upload Composer DAG/support files and configure Airflow Variables.
  --all        Run bigquery -> images -> jobs -> composer.
  --help       Show this help.
USAGE
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$PROJECT_ROOT"

RUN_BIGQUERY=false
RUN_IMAGES=false
RUN_JOBS=false
RUN_COMPOSER=false

if [[ $# -eq 0 ]]; then
  usage
  exit 1
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bigquery) RUN_BIGQUERY=true ;;
    --images) RUN_IMAGES=true ;;
    --jobs) RUN_JOBS=true ;;
    --composer) RUN_COMPOSER=true ;;
    --all)
      RUN_BIGQUERY=true
      RUN_IMAGES=true
      RUN_JOBS=true
      RUN_COMPOSER=true
      ;;
    --help)
      usage
      exit 0
      ;;
    *) echo "ERROR: unknown argument: $1" >&2; usage; exit 1 ;;
  esac
  shift
done

if [[ "$RUN_BIGQUERY" == true ]]; then
  log "Deploying BigQuery SQL."
  scripts/deploy_bigquery_sql.sh
fi

if [[ "$RUN_IMAGES" == true ]]; then
  log "Building and pushing images."
  scripts/build_and_push_image.sh
  scripts/build_and_push_dataflow_worker_image.sh
fi

if [[ "$RUN_JOBS" == true ]]; then
  log "Deploying Cloud Run Jobs."
  scripts/deploy_cloud_run_jobs.sh
fi

if [[ "$RUN_COMPOSER" == true ]]; then
  log "Deploying Composer DAG and variables."
  scripts/upload_composer_dag.sh
  scripts/configure_airflow_variables.sh
fi

log "Selected deployment steps completed."
