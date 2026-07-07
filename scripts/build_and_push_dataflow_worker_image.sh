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

if [[ -z "${DATAFLOW_WORKER_IMAGE:-}" ]]; then
  ARTIFACT_REGION="${ARTIFACT_REGISTRY_REGION:-${ARTIFACT_REGISTRY_LOCATION:-${GCP_REGION:-}}}"
  ARTIFACT_REPOSITORY="${ARTIFACT_REGISTRY_REPOSITORY:-pronabec-containers}"
  DATAFLOW_WORKER_IMAGE_NAME="${DATAFLOW_WORKER_IMAGE_NAME:-pronabec-dataflow-worker}"
  DATAFLOW_WORKER_IMAGE_TAG="${DATAFLOW_WORKER_IMAGE_TAG:-latest}"
  if [[ -n "${GCP_PROJECT_ID:-}" && -n "$ARTIFACT_REGION" ]]; then
    DATAFLOW_WORKER_IMAGE="${ARTIFACT_REGION}-docker.pkg.dev/${GCP_PROJECT_ID}/${ARTIFACT_REPOSITORY}/${DATAFLOW_WORKER_IMAGE_NAME}:${DATAFLOW_WORKER_IMAGE_TAG}"
  fi
fi

require_env GCP_PROJECT_ID
require_env DATAFLOW_WORKER_IMAGE

log "Building and pushing Dataflow worker image with Cloud Build: ${DATAFLOW_WORKER_IMAGE}"
gcloud builds submit \
  --project "$GCP_PROJECT_ID" \
  --config cloudbuild.dataflow.yaml \
  --substitutions "_DATAFLOW_WORKER_IMAGE=${DATAFLOW_WORKER_IMAGE}" \
  .

log "Dataflow worker image published: ${DATAFLOW_WORKER_IMAGE}"
