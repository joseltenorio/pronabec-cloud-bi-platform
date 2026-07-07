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

require_env GCP_PROJECT_ID
require_env CLOUD_RUN_IMAGE

log "Building and pushing main Cloud Run image: ${CLOUD_RUN_IMAGE}"
gcloud builds submit \
  --tag "$CLOUD_RUN_IMAGE" \
  --project "$GCP_PROJECT_ID" \
  .

log "Main Cloud Run image published: ${CLOUD_RUN_IMAGE}"
