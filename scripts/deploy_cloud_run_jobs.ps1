# scripts/deploy_cloud_run_jobs.ps1

[CmdletBinding()]
param(
    [string]$ProjectId = $env:GCP_PROJECT_ID,
    [string]$Region = $(if ($env:CLOUD_RUN_JOBS_REGION) { $env:CLOUD_RUN_JOBS_REGION } else { $env:GCP_REGION }),
    [string]$Image = $env:CLOUD_RUN_IMAGE,
    [string]$ServiceAccount = $env:CLOUD_RUN_JOBS_SERVICE_ACCOUNT,

    [string]$BucketName = $(if ($env:GCS_BUCKET_NAME) { $env:GCS_BUCKET_NAME } else { $env:GCS_BUCKET }),

    [string]$BronzeDataset = $(if ($env:BQ_BRONZE_DATASET) { $env:BQ_BRONZE_DATASET } else { "bronze" }),
    [string]$SilverDataset = $(if ($env:BQ_SILVER_DATASET) { $env:BQ_SILVER_DATASET } else { "silver" }),
    [string]$GoldDataset = $(if ($env:BQ_GOLD_DATASET) { $env:BQ_GOLD_DATASET } else { "gold" }),
    [string]$AuditDataset = $(if ($env:BQ_AUDIT_DATASET) { $env:BQ_AUDIT_DATASET } else { "audit" }),

    [string]$PronabecJobName = "pronabec-extract-job",
    [string]$MefJobName = "mef-extract-job",
    [string]$QualityJobName = "quality-checks-job"
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
        throw "Falta configurar $Name. Define el parámetro correspondiente o la variable de entorno asociada."
    }
}

function Test-CloudRunJobExists {
    param(
        [Parameter(Mandatory = $true)]
        [string]$JobName
    )

    $result = gcloud run jobs describe $JobName `
        --project $ProjectId `
        --region $Region `
        --format "value(name)" `
        2>$null

    return -not [string]::IsNullOrWhiteSpace($result)
}

function Upsert-CloudRunJob {
    param(
        [Parameter(Mandatory = $true)]
        [string]$JobName,

        [Parameter(Mandatory = $true)]
        [string[]]$Args,

        [Parameter(Mandatory = $true)]
        [string]$Description
    )

    $CommonArgs = @(
        "--project", $ProjectId,
        "--region", $Region,
        "--image", $Image,
        "--service-account", $ServiceAccount,
        "--set-env-vars", "GCP_PROJECT_ID=$ProjectId,GCS_BUCKET=$BucketName,BQ_BRONZE_DATASET=$BronzeDataset,BQ_SILVER_DATASET=$SilverDataset,BQ_GOLD_DATASET=$GoldDataset,BQ_AUDIT_DATASET=$AuditDataset,STRUCTURED_LOGGING=true,LOG_LEVEL=INFO",
        "--max-retries", "1",
        "--task-timeout", "3600s"
    )

    $JoinedArgs = ($Args -join ",")

    if (Test-CloudRunJobExists -JobName $JobName) {
        Write-Host "Actualizando Cloud Run Job: $JobName"

        gcloud run jobs update $JobName `
            @CommonArgs `
            --args $JoinedArgs `
            --quiet
    }
    else {
        Write-Host "Creando Cloud Run Job: $JobName"

        gcloud run jobs create $JobName `
            @CommonArgs `
            --args $JoinedArgs `
            --quiet
    }

    Write-Host "Job configurado: $JobName - $Description"
}

Assert-RequiredValue -Name "ProjectId" -Value $ProjectId
Assert-RequiredValue -Name "Region" -Value $Region
Assert-RequiredValue -Name "Image" -Value $Image
Assert-RequiredValue -Name "ServiceAccount" -Value $ServiceAccount
Assert-RequiredValue -Name "BucketName" -Value $BucketName

Upsert-CloudRunJob `
    -JobName $PronabecJobName `
    -Description "Extracción batch PRONABEC hacia Bronze" `
    -Args @(
        "-m",
        "pipelines.extract_pronabec"
    )

Upsert-CloudRunJob `
    -JobName $MefJobName `
    -Description "Extracción batch MEF hacia Bronze" `
    -Args @(
        "-m",
        "pipelines.scrape_mef_budget"
    )

Upsert-CloudRunJob `
    -JobName $QualityJobName `
    -Description "Ejecución batch de controles de calidad BigQuery" `
    -Args @(
        "-m",
        "pipelines.quality_checks"
    )

Write-Host "Cloud Run Jobs configurados correctamente."