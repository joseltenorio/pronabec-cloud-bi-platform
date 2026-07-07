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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$PROJECT_ROOT"

COMPOSER_LOCATION="${COMPOSER_LOCATION:-${GCP_REGION:-}}"
DAG_PATH="${DAG_PATH:-dags/pronabec_medallion_batch_dag.py}"

require_env GCP_PROJECT_ID
require_env COMPOSER_LOCATION
require_env COMPOSER_ENVIRONMENT_NAME

if [[ ! -f "$DAG_PATH" ]]; then
  echo "ERROR: DAG file not found: ${DAG_PATH}" >&2
  exit 1
fi

DAG_BUCKET="$(
  gcloud composer environments describe "$COMPOSER_ENVIRONMENT_NAME" \
    --location "$COMPOSER_LOCATION" \
    --project "$GCP_PROJECT_ID" \
    --format="value(config.dagGcsPrefix)"
)"

if [[ -z "$DAG_BUCKET" ]]; then
  echo "ERROR: could not resolve Composer DAG bucket." >&2
  exit 1
fi

log "Uploading DAG to Composer: ${DAG_BUCKET}/"
gcloud storage cp "$DAG_PATH" "$DAG_BUCKET/" --quiet

log "Syncing tracked Composer support files from config/ and pipelines/"
while IFS= read -r relative_path; do
  [[ -f "$relative_path" ]] || continue
  gcloud storage cp "$relative_path" "${DAG_BUCKET}/${relative_path}" --quiet
done < <(git ls-files config pipelines)

log "Composer DAG uploaded successfully: ${DAG_BUCKET}/$(basename "$DAG_PATH")"
