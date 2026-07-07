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

PROJECT_ID="${PROJECT_ID:-${GCP_PROJECT_ID:-}}"
BUCKET_NAME="${BUCKET_NAME:-${GCS_BUCKET_NAME:-${GCS_BUCKET:-}}}"
OUTPUT_DIR="${OUTPUT_DIR:-build/generated/sql}"

require_env PROJECT_ID
require_env BUCKET_NAME

mkdir -p "$OUTPUT_DIR"

args=(
  tools/generate_bigquery_ddl.py
  --project-id "$PROJECT_ID"
  --bucket "$BUCKET_NAME"
  --output-dir "$OUTPUT_DIR"
)

if [[ -n "${BRONZE_EXTRACTION_DATE:-}" ]]; then
  args+=(--bronze-extraction-date "$BRONZE_EXTRACTION_DATE")
fi

log "Generating BigQuery DDL into ${OUTPUT_DIR}"
python "${args[@]}"

[[ -f "${OUTPUT_DIR}/create_bronze_external_tables.sql" ]]
[[ -f "${OUTPUT_DIR}/create_silver_tables.sql" ]]

log "BigQuery DDL generated successfully."
