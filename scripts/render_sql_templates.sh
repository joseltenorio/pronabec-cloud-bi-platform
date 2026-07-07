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
BRONZE_DATASET="${BQ_BRONZE_DATASET:-bronze}"
SILVER_DATASET="${BQ_SILVER_DATASET:-silver}"
GOLD_DATASET="${BQ_GOLD_DATASET:-gold}"
AUDIT_DATASET="${BQ_AUDIT_DATASET:-audit}"
ML_DATASET="${BQ_ML_DATASET:-ml}"
OUTPUT_DIR="${OUTPUT_DIR:-build/generated/sql}"

require_env PROJECT_ID

args=(
  tools/render_sql_templates.py
  --project-id "$PROJECT_ID"
  --bronze-dataset "$BRONZE_DATASET"
  --silver-dataset "$SILVER_DATASET"
  --gold-dataset "$GOLD_DATASET"
  --audit-dataset "$AUDIT_DATASET"
  --ml-dataset "$ML_DATASET"
  --output-dir "$OUTPUT_DIR"
)

for source_file in "$@"; do
  args+=(--source-file "$source_file")
done

log "Rendering SQL templates into ${OUTPUT_DIR}"
python "${args[@]}"
