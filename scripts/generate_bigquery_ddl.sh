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
GENERATION_MODE="deploy"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ci) GENERATION_MODE="ci" ;;
    --deploy) GENERATION_MODE="deploy" ;;
    --help)
      cat <<'USAGE'
Usage: scripts/generate_bigquery_ddl.sh [--ci|--deploy]

Modes:
  --ci      Generate static validation DDL without requiring runtime Bronze dates.
  --deploy  Generate operational deployment DDL. Requires BRONZE_EXTRACTION_DATE for MEF.
USAGE
      exit 0
      ;;
    *) echo "ERROR: unknown argument: $1" >&2; exit 1 ;;
  esac
  shift
done

require_env PROJECT_ID
require_env BUCKET_NAME

if [[ "$GENERATION_MODE" == "deploy" && -z "${BRONZE_EXTRACTION_DATE:-}" ]]; then
  echo "ERROR: BRONZE_EXTRACTION_DATE is required in deploy generation mode for MEF external table DDL." >&2
  exit 1
fi

mkdir -p "$OUTPUT_DIR"

args=(
  tools/generate_bigquery_ddl.py
  --project-id "$PROJECT_ID"
  --bucket "$BUCKET_NAME"
  --output-dir "$OUTPUT_DIR"
  --generation-mode "$GENERATION_MODE"
)

if [[ -n "${BRONZE_EXTRACTION_DATE:-}" ]]; then
  args+=(--bronze-extraction-date "$BRONZE_EXTRACTION_DATE")
fi

log "Generating BigQuery DDL into ${OUTPUT_DIR}"
python "${args[@]}"

[[ -f "${OUTPUT_DIR}/create_bronze_external_tables.sql" ]]
[[ -f "${OUTPUT_DIR}/create_silver_tables.sql" ]]

log "BigQuery DDL generated successfully."
