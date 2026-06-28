# scripts/configure_airflow_variables.ps1

[CmdletBinding()]
param(
    [string]$ProjectId = $env:GCP_PROJECT_ID,
    [string]$ComposerEnvironmentName = $(if ($env:COMPOSER_ENVIRONMENT_NAME) { $env:COMPOSER_ENVIRONMENT_NAME } else { "pronabec-composer" }),
    [string]$ComposerLocation = $(if ($env:COMPOSER_LOCATION) { $env:COMPOSER_LOCATION } else { $env:GCP_REGION }),
    [string]$GcpRegion = $env:GCP_REGION,
    [string]$CloudRunRegion = $(if ($env:CLOUD_RUN_REGION) { $env:CLOUD_RUN_REGION } else { $env:GCP_REGION }),
    [string]$GcsBucketName = $(if ($env:GCS_BUCKET_NAME) { $env:GCS_BUCKET_NAME } else { $env:GCS_BUCKET }),

    [string]$BronzeDataset = $(if ($env:BQ_BRONZE_DATASET) { $env:BQ_BRONZE_DATASET } else { "bronze" }),
    [string]$SilverDataset = $(if ($env:BQ_SILVER_DATASET) { $env:BQ_SILVER_DATASET } else { "silver" }),
    [string]$GoldDataset = $(if ($env:BQ_GOLD_DATASET) { $env:BQ_GOLD_DATASET } else { "gold" }),
    [string]$AuditDataset = $(if ($env:BQ_AUDIT_DATASET) { $env:BQ_AUDIT_DATASET } else { "audit" }),

    [string]$PronabecExtractJobName = $(if ($env:PRONABEC_EXTRACT_JOB_NAME) { $env:PRONABEC_EXTRACT_JOB_NAME } else { "pronabec-extract-job" }),
    [string]$MefExtractJobName = $(if ($env:MEF_EXTRACT_JOB_NAME) { $env:MEF_EXTRACT_JOB_NAME } else { "mef-extract-job" }),
    [string]$PronabecReportsStageJobName = $(if ($env:PRONABEC_REPORTS_STAGE_JOB_NAME) { $env:PRONABEC_REPORTS_STAGE_JOB_NAME } else { "pronabec-stage-reports-job" }),
    [string]$BronzeManifestValidationJobName = $(if ($env:BRONZE_MANIFEST_VALIDATION_JOB_NAME) { $env:BRONZE_MANIFEST_VALIDATION_JOB_NAME } else { "bronze-manifest-validation-job" }),
    [string]$DataflowPronabecReportJobName = $(if ($env:DATAFLOW_PRONABEC_REPORT_JOB_NAME) { $env:DATAFLOW_PRONABEC_REPORT_JOB_NAME } else { "dataflow-pronabec-report-job" }),
    [string]$GoldPublishJobName = $(if ($env:GOLD_PUBLISH_JOB_NAME) { $env:GOLD_PUBLISH_JOB_NAME } else { "gold-publish-job" }),
    [string]$GoldValidateJobName = $(if ($env:GOLD_VALIDATE_JOB_NAME) { $env:GOLD_VALIDATE_JOB_NAME } else { "gold-validate-job" }),
    [string]$QualityChecksJobName = $(if ($env:QUALITY_CHECKS_JOB_NAME) { $env:QUALITY_CHECKS_JOB_NAME } else { "quality-checks-job" })
)

$ErrorActionPreference = "Stop"

function Assert-RequiredValue {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name,

        [AllowEmptyString()]
        [string]$Value
    )

    if ([string]::IsNullOrWhiteSpace($Value)) {
        throw "Falta valor requerido: $Name"
    }
}

function Set-AirflowVariable {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name,

        [Parameter(Mandatory = $true)]
        [string]$Value
    )

    Write-Host "Configurando Airflow Variable: $Name=$Value"

    gcloud composer environments run $ComposerEnvironmentName `
        --location $ComposerLocation `
        variables set -- $Name $Value

    if ($LASTEXITCODE -ne 0) {
        throw "Falló configuración de Airflow Variable: $Name"
    }
}

Assert-RequiredValue -Name "ProjectId" -Value $ProjectId
Assert-RequiredValue -Name "ComposerEnvironmentName" -Value $ComposerEnvironmentName
Assert-RequiredValue -Name "ComposerLocation" -Value $ComposerLocation
Assert-RequiredValue -Name "GcpRegion" -Value $GcpRegion
Assert-RequiredValue -Name "CloudRunRegion" -Value $CloudRunRegion
Assert-RequiredValue -Name "GcsBucketName" -Value $GcsBucketName

$Variables = [ordered]@{
    "gcp_project_id" = $ProjectId
    "gcp_region" = $GcpRegion
    "cloud_run_region" = $CloudRunRegion
    "gcs_bucket_name" = $GcsBucketName

    "bq_bronze_dataset" = $BronzeDataset
    "bq_silver_dataset" = $SilverDataset
    "bq_gold_dataset" = $GoldDataset
    "bq_audit_dataset" = $AuditDataset

    "pronabec_extract_job_name" = $PronabecExtractJobName
    "mef_extract_job_name" = $MefExtractJobName
    "pronabec_reports_stage_job_name" = $PronabecReportsStageJobName
    "bronze_manifest_validation_job_name" = $BronzeManifestValidationJobName
    "dataflow_pronabec_report_job_name" = $DataflowPronabecReportJobName

    "gold_publish_job_name" = $GoldPublishJobName
    "gold_validate_job_name" = $GoldValidateJobName
    "quality_checks_job_name" = $QualityChecksJobName
}

foreach ($Entry in $Variables.GetEnumerator()) {
    Set-AirflowVariable -Name $Entry.Key -Value $Entry.Value
}

Write-Host "Airflow Variables configuradas correctamente."