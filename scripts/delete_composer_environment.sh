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

if ./scripts/check_composer_environment.sh --allow-missing; then
  if ! gcloud composer environments describe "$COMPOSER_ENVIRONMENT_NAME" \
    --location "$COMPOSER_LOCATION" \
    --project "$GCP_PROJECT_ID" >/dev/null 2>&1; then
    log "Composer environment does not exist. Nothing to delete."
    exit 0
  fi
fi

log "Deleting Composer environment '$COMPOSER_ENVIRONMENT_NAME' in '$COMPOSER_LOCATION'."
gcloud composer environments delete "$COMPOSER_ENVIRONMENT_NAME" \
  --location "$COMPOSER_LOCATION" \
  --project "$GCP_PROJECT_ID" \
  --quiet

if gcloud composer environments describe "$COMPOSER_ENVIRONMENT_NAME" \
  --location "$COMPOSER_LOCATION" \
  --project "$GCP_PROJECT_ID" >/dev/null 2>&1; then
  echo "ERROR: Composer environment '$COMPOSER_ENVIRONMENT_NAME' still exists after delete." >&2
  exit 1
fi

log "Composer environment '$COMPOSER_ENVIRONMENT_NAME' deleted successfully."
