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

    [string]$DataflowTempLocation = $(if ($env:DATAFLOW_TEMP_LOCATION) { $env:DATAFLOW_TEMP_LOCATION } else { "" }),
    [string]$DataflowStagingLocation = $(if ($env:DATAFLOW_STAGING_LOCATION) { $env:DATAFLOW_STAGING_LOCATION } else { "" }),

    [string]$PronabecJobName = "pronabec-extract-job",
    [string]$MefJobName = "mef-extract-job",
    [string]$QualityJobName = "quality-checks-job",

    [string]$DataflowPronabecConvocatoriasJobName = "dataflow-pronabec-convocatorias-job",
    [string]$DataflowMefPresupuestoJobName = "dataflow-mef-presupuesto-job",
    [string]$DataflowReportUniversitariosJobName = "dataflow-report-universitarios-job"
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

function Join-CloudRunArgs {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Args
    )

    return ($Args -join ",")
}

function Upsert-CloudRunJob {
    param(
        [Parameter(Mandatory = $true)]
        [string]$JobName,

        [Parameter(Mandatory = $true)]
        [string[]]$Args,

        [Parameter(Mandatory = $true)]
        [string]$Description,

        [int]$TaskTimeoutSeconds = 3600
    )

    $CommonArgs = @(
        "--project", $ProjectId,
        "--region", $Region,
        "--image", $Image,
        "--service-account", $ServiceAccount,
        "--set-env-vars", "GCP_PROJECT_ID=$ProjectId,GCS_BUCKET=$BucketName,BQ_BRONZE_DATASET=$BronzeDataset,BQ_SILVER_DATASET=$SilverDataset,BQ_GOLD_DATASET=$GoldDataset,BQ_AUDIT_DATASET=$AuditDataset,DATAFLOW_TEMP_LOCATION=$DataflowTempLocation,DATAFLOW_STAGING_LOCATION=$DataflowStagingLocation,STRUCTURED_LOGGING=true,LOG_LEVEL=INFO",
        "--max-retries", "1",
        "--task-timeout", "$($TaskTimeoutSeconds)s"
    )

    $JoinedArgs = Join-CloudRunArgs -Args $Args

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
Assert-RequiredValue -Name "DataflowTempLocation" -Value $DataflowTempLocation
Assert-RequiredValue -Name "DataflowStagingLocation" -Value $DataflowStagingLocation

$DataflowCommonArgs = @(
    "-m",
    "pipelines.dataflow_bronze_to_silver",
    "--runner",
    "DataflowRunner",
    "--project",
    $ProjectId,
    "--region",
    $Region,
    "--temp-location",
    $DataflowTempLocation,
    "--staging-location",
    $DataflowStagingLocation,
    "--dlq-output-root",
    "gs://$BucketName/dlq"
)

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
    -JobName $DataflowPronabecConvocatoriasJobName `
    -Description "Lanzador Dataflow PRONABEC convocatorias Bronze a Silver" `
    -TaskTimeoutSeconds 7200 `
    -Args @(
        $DataflowCommonArgs +
        @(
            "--source-system",
            "pronabec",
            "--source-dataset",
            "convocatorias",
            "--input-path",
            "gs://$BucketName/bronze/pronabec/convocatorias/extraction_date=`${BRONZE_EXTRACTION_DATE}/data.jsonl",
            "--input-format",
            "jsonl",
            "--output-table",
            "$ProjectId`:$SilverDataset.pronabec_convocatorias",
            "--summary-output-path",
            "gs://$BucketName/audit/processing_summary/pronabec_convocatorias_`${BRONZE_EXTRACTION_DATE}.json"
        )
    )

Upsert-CloudRunJob `
    -JobName $DataflowMefPresupuestoJobName `
    -Description "Lanzador Dataflow MEF presupuesto Bronze a Silver" `
    -TaskTimeoutSeconds 7200 `
    -Args @(
        $DataflowCommonArgs +
        @(
            "--source-system",
            "mef",
            "--source-dataset",
            "presupuesto",
            "--input-path",
            "gs://$BucketName/bronze/mef/presupuesto/extraction_date=`${BRONZE_EXTRACTION_DATE}/year=*/data.csv",
            "--input-format",
            "csv",
            "--output-table",
            "$ProjectId`:$SilverDataset.presupuesto_mef",
            "--summary-output-path",
            "gs://$BucketName/audit/processing_summary/presupuesto_mef_`${BRONZE_EXTRACTION_DATE}.json"
        )
    )

Upsert-CloudRunJob `
    -JobName $DataflowReportUniversitariosJobName `
    -Description "Lanzador Dataflow PRONABEC reports universitarios Bronze a Silver" `
    -TaskTimeoutSeconds 7200 `
    -Args @(
        $DataflowCommonArgs +
        @(
            "--source-system",
            "pronabec_reports",
            "--source-dataset",
            "report_beca18_universitarios_universidad_anual",
            "--input-path",
            "gs://$BucketName/bronze/pronabec_reports/report_beca18_universitarios_universidad_anual/extraction_date=`${BRONZE_EXTRACTION_DATE}/data.csv",
            "--input-format",
            "csv",
            "--output-table",
            "$ProjectId`:$SilverDataset.pronabec_report_beca18_universitarios_universidad_anual",
            "--summary-output-path",
            "gs://$BucketName/audit/processing_summary/pronabec_report_beca18_universitarios_universidad_anual_`${BRONZE_EXTRACTION_DATE}.json"
        )
    )

Upsert-CloudRunJob `
    -JobName $QualityJobName `
    -Description "Ejecución batch de controles de calidad BigQuery" `
    -Args @(
        "-m",
        "pipelines.quality_checks"
    )

Write-Host "Cloud Run Jobs configurados correctamente."