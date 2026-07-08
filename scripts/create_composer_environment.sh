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
require_env COMPOSER_LOCATION
require_env COMPOSER_ENVIRONMENT_NAME
require_env COMPOSER_SERVICE_ACCOUNT

COMPOSER_IMAGE_VERSION="${COMPOSER_IMAGE_VERSION:-composer-3-airflow-2}"

if ./scripts/check_composer_environment.sh --allow-missing; then
  if gcloud composer environments describe "$COMPOSER_ENVIRONMENT_NAME" \
    --location "$COMPOSER_LOCATION" \
    --project "$GCP_PROJECT_ID" >/dev/null 2>&1; then
    log "Composer environment already exists. Skipping creation."
    exit 0
  fi
fi

log "Creating Composer environment '$COMPOSER_ENVIRONMENT_NAME' in '$COMPOSER_LOCATION'."
gcloud composer environments create "$COMPOSER_ENVIRONMENT_NAME" \
  --location "$COMPOSER_LOCATION" \
  --project "$GCP_PROJECT_ID" \
  --image-version="${COMPOSER_IMAGE_VERSION}" \
  --service-account="$COMPOSER_SERVICE_ACCOUNT" \
  --quiet

log "Composer environment creation requested. Waiting for environment to become ready."
gcloud composer environments describe "$COMPOSER_ENVIRONMENT_NAME" \
  --location "$COMPOSER_LOCATION" \
  --project "$GCP_PROJECT_ID" \
  --format="value(state)"

log "Composer environment '$COMPOSER_ENVIRONMENT_NAME' created or already ready."
