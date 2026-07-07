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

warn() {
  echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] WARNING: $*" >&2
}

fail() {
  echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] ERROR: $*" >&2
  exit 1
}

ALLOW_MISSING=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --allow-missing) ALLOW_MISSING=true ;;
    --help)
      cat <<'USAGE'
Usage: scripts/check_composer_environment.sh [--allow-missing]
USAGE
      exit 0
      ;;
    *) fail "Unknown argument: $1" ;;
  esac
  shift
done

require_env GCP_PROJECT_ID
require_env COMPOSER_LOCATION
require_env COMPOSER_ENVIRONMENT_NAME

if gcloud composer environments describe "$COMPOSER_ENVIRONMENT_NAME" \
  --location "$COMPOSER_LOCATION" \
  --project "$GCP_PROJECT_ID" >/dev/null 2>&1; then
  log "Composer environment '$COMPOSER_ENVIRONMENT_NAME' exists in '$COMPOSER_LOCATION'."
  exit 0
fi

if [[ "$ALLOW_MISSING" == true ]]; then
  warn "Composer environment '$COMPOSER_ENVIRONMENT_NAME' does not exist in '$COMPOSER_LOCATION'."
  exit 0
fi

fail "Composer environment '$COMPOSER_ENVIRONMENT_NAME' does not exist in '$COMPOSER_LOCATION'."
