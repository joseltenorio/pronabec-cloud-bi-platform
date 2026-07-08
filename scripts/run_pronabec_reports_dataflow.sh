#!/usr/bin/env bash
set -euo pipefail

required_vars=(
  GCP_PROJECT_ID
  GCS_BUCKET_NAME
  BQ_SILVER_DATASET
  CLOUD_RUN_REGION
  DATAFLOW_SDK_CONTAINER_IMAGE
  BRONZE_EXTRACTION_DATE
  PIPELINE_RUN_ID
)

for var_name in "${required_vars[@]}"; do
  if [[ -z "${!var_name:-}" ]]; then
    echo "ERROR: ${var_name} is required." >&2
    exit 2
  fi
done

reports_root="gs://${GCS_BUCKET_NAME}/bronze/pronabec_reports/"
processed_count=0
skipped_count=0
failed_count=0

echo "Listing PRONABEC report datasets from ${reports_root}"

if ! mapfile -t report_paths < <(gsutil ls "${reports_root}"); then
  echo "ERROR: Could not list ${reports_root}" >&2
  exit 1
fi

for report_path in "${report_paths[@]}"; do
  report_path="${report_path%/}"
  report_dataset="${report_path##*/}"

  if [[ -z "${report_dataset}" ]]; then
    continue
  fi

  input_path="gs://${GCS_BUCKET_NAME}/bronze/pronabec_reports/${report_dataset}/extraction_date=${BRONZE_EXTRACTION_DATE}/data.csv"
  output_table="${GCP_PROJECT_ID}:${BQ_SILVER_DATASET}.pronabec_${report_dataset}"

  if ! gsutil -q stat "${input_path}"; then
    echo "SKIP ${report_dataset}: ${input_path} does not exist."
    skipped_count=$((skipped_count + 1))
    continue
  fi

  echo "RUN ${report_dataset}: ${input_path} -> ${output_table}"

  if gcloud run jobs execute dataflow-pronabec-report-job \
    --region="${CLOUD_RUN_REGION}" \
    --project="${GCP_PROJECT_ID}" \
    --update-env-vars="BRONZE_EXTRACTION_DATE=${BRONZE_EXTRACTION_DATE},PIPELINE_RUN_ID=${PIPELINE_RUN_ID},SOURCE_DATASET=${report_dataset},INPUT_PATH=${input_path},OUTPUT_TABLE=${output_table},DATAFLOW_SDK_CONTAINER_IMAGE=${DATAFLOW_SDK_CONTAINER_IMAGE}" \
    --wait; then
    processed_count=$((processed_count + 1))
  else
    echo "FAILED ${report_dataset}: Cloud Run job execution failed." >&2
    failed_count=$((failed_count + 1))
  fi
done

echo "Summary:"
echo "processed_count=${processed_count}"
echo "skipped_count=${skipped_count}"
echo "failed_count=${failed_count}"

if [[ "${failed_count}" -gt 0 ]]; then
  exit 1
fi
