# scripts/deploy_cloud_run_jobs.ps1

[CmdletBinding()]
param(
    [string]$ProjectId = $env:GCP_PROJECT_ID,
    [string]$Region = $(if ($env:CLOUD_RUN_JOBS_REGION) { $env:CLOUD_RUN_JOBS_REGION } else { $env:GCP_REGION }),
    [string]$Image = $env:CLOUD_RUN_IMAGE,
    [string]$ServiceAccount = $env:CLOUD_RUN_JOBS_SERVICE_ACCOUNT,

    [string]$BucketName = $(if ($env:GCS_BUCKET_NAME) { $env:GCS_BUCKET_NAME } else { $env:GCS_BUCKET }),
    [string]$Location = $(if ($env:BQ_LOCATION) { $env:BQ_LOCATION } else { "US" }),

    [string]$BronzeDataset = $(if ($env:BQ_BRONZE_DATASET) { $env:BQ_BRONZE_DATASET } else { "bronze" }),
    [string]$SilverDataset = $(if ($env:BQ_SILVER_DATASET) { $env:BQ_SILVER_DATASET } else { "silver" }),
    [string]$GoldDataset = $(if ($env:BQ_GOLD_DATASET) { $env:BQ_GOLD_DATASET } else { "gold" }),
    [string]$AuditDataset = $(if ($env:BQ_AUDIT_DATASET) { $env:BQ_AUDIT_DATASET } else { "audit" }),

    [string]$DataflowTempLocation = $(if ($env:DATAFLOW_TEMP_LOCATION) { $env:DATAFLOW_TEMP_LOCATION } else { "" }),
    [string]$DataflowStagingLocation = $(if ($env:DATAFLOW_STAGING_LOCATION) { $env:DATAFLOW_STAGING_LOCATION } else { "" }),

    [string]$PronabecJobName = "pronabec-extract-job",
    [string]$MefJobName = "mef-extract-job",
    [string]$PronabecReportsStageJobName = $(if ($env:PRONABEC_REPORTS_STAGE_JOB_NAME) { $env:PRONABEC_REPORTS_STAGE_JOB_NAME } else { "pronabec-stage-reports-job" }),
    [string]$GoldPublishJobName = $(if ($env:GOLD_PUBLISH_JOB_NAME) { $env:GOLD_PUBLISH_JOB_NAME } else { "gold-publish-job" }),
    [string]$GoldValidateJobName = $(if ($env:GOLD_VALIDATE_JOB_NAME) { $env:GOLD_VALIDATE_JOB_NAME } else { "gold-validate-job" }),
    [string]$QualityJobName = "quality-checks-job",

    [string]$PronabecReportsLandingPrefix = $(if ($env:PRONABEC_REPORTS_LANDING_PREFIX) { $env:PRONABEC_REPORTS_LANDING_PREFIX } else { "landing/pronabec_reports" }),
    [string]$PronabecReportsBronzePrefix = $(if ($env:PRONABEC_REPORTS_BRONZE_PREFIX) { $env:PRONABEC_REPORTS_BRONZE_PREFIX } else { "bronze/pronabec_reports" }),

    [string]$DataflowPronabecConvocatoriasJobName = "dataflow-pronabec-convocatorias-job",
    [string]$DataflowPronabecUbigeoPostulacionJobName = "dataflow-pronabec-ubigeo-postulacion-job",
    [string]$DataflowPronabecBecariosPaisEstudioJobName = "dataflow-pronabec-becarios-pais-estudio-job",
    [string]$DataflowPronabecColegiosHabilesJobName = "dataflow-pronabec-colegios-habiles-job",
    [string]$DataflowPronabecBecariosProvinciaJobName = "dataflow-pronabec-becarios-provincia-job",
    [string]$DataflowMefPresupuestoJobName = "dataflow-mef-presupuesto-job",
    [string]$DataflowMefPresupuestoTemporalJobName = "dataflow-mef-presupuesto-temporal-job",
    [string]$DataflowMefProductoJobName = "dataflow-mef-producto-job",
    [string]$DataflowMefProductoTemporalJobName = "dataflow-mef-producto-temporal-job",
    [string]$DataflowMefActividadJobName = "dataflow-mef-actividad-job",
    [string]$DataflowMefActividadTemporalJobName = "dataflow-mef-actividad-temporal-job",
    [string]$DataflowMefGenericaJobName = "dataflow-mef-generica-job",
    [string]$DataflowMefGenericaTemporalJobName = "dataflow-mef-generica-temporal-job",
    [string]$DataflowMefHierarchyJobName = "dataflow-mef-hierarchy-job",
    [string]$DataflowPronabecReportJobName = "dataflow-pronabec-report-job"
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

        [string[]]$SetEnvVars = @(),

        [int]$TaskTimeoutSeconds = 3600
    )

    $BaseEnvVars = @(
        "GCP_PROJECT_ID=$ProjectId",
        "GCS_BUCKET=$BucketName",
        "GCS_BUCKET_NAME=$BucketName",
        "BQ_BRONZE_DATASET=$BronzeDataset",
        "BQ_SILVER_DATASET=$SilverDataset",
        "BQ_GOLD_DATASET=$GoldDataset",
        "BQ_AUDIT_DATASET=$AuditDataset",
        "BQ_LOCATION=$Location",
        "DATAFLOW_TEMP_LOCATION=$DataflowTempLocation",
        "DATAFLOW_STAGING_LOCATION=$DataflowStagingLocation",
        "PRONABEC_REPORTS_LANDING_PREFIX=$PronabecReportsLandingPrefix",
        "PRONABEC_REPORTS_BRONZE_PREFIX=$PronabecReportsBronzePrefix",
        "STRUCTURED_LOGGING=true",
        "LOG_LEVEL=INFO"
    )
    $EnvVars = ($BaseEnvVars + $SetEnvVars) -join ","

    $CommonArgs = @(
        "--project", $ProjectId,
        "--region", $Region,
        "--image", $Image,
        "--service-account", $ServiceAccount,
        "--set-env-vars", $EnvVars,
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
    -JobName $PronabecReportsStageJobName `
    -Description "Staging PRONABEC reports desde GCS Landing hacia Bronze" `
    -Args @(
        "tools/stage_pronabec_manual_reports.py",
        "--strict",
        "--overwrite"
    )

Upsert-CloudRunJob `
    -JobName $GoldPublishJobName `
    -Description "Publicacion idempotente de vistas Gold analiticas" `
    -Args @(
        "-m",
        "pipelines.publish_gold_views"
    )

Upsert-CloudRunJob `
    -JobName $GoldValidateJobName `
    -Description "Validacion de contratos Gold analiticos" `
    -Args @(
        "-m",
        "pipelines.validate_gold"
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
    -JobName $DataflowPronabecUbigeoPostulacionJobName `
    -Description "Lanzador Dataflow PRONABEC ubigeo postulación Bronze a Silver" `
    -TaskTimeoutSeconds 7200 `
    -Args @(
        $DataflowCommonArgs +
        @(
            "--source-system",
            "pronabec",
            "--source-dataset",
            "ubigeo_postulacion",
            "--input-path",
            "gs://$BucketName/bronze/pronabec/ubigeo_postulacion/extraction_date=`${BRONZE_EXTRACTION_DATE}/data.jsonl",
            "--input-format",
            "jsonl",
            "--output-table",
            "$ProjectId`:$SilverDataset.pronabec_ubigeo_postulacion",
            "--summary-output-path",
            "gs://$BucketName/audit/processing_summary/pronabec_ubigeo_postulacion_`${BRONZE_EXTRACTION_DATE}.json"
        )
    )

Upsert-CloudRunJob `
    -JobName $DataflowPronabecBecariosPaisEstudioJobName `
    -Description "Lanzador Dataflow PRONABEC becarios país estudio Bronze a Silver" `
    -TaskTimeoutSeconds 7200 `
    -Args @(
        $DataflowCommonArgs +
        @(
            "--source-system",
            "pronabec",
            "--source-dataset",
            "becarios_pais_estudio",
            "--input-path",
            "gs://$BucketName/bronze/pronabec/becarios_pais_estudio/extraction_date=`${BRONZE_EXTRACTION_DATE}/data.jsonl",
            "--input-format",
            "jsonl",
            "--output-table",
            "$ProjectId`:$SilverDataset.pronabec_becarios_pais_estudio",
            "--summary-output-path",
            "gs://$BucketName/audit/processing_summary/pronabec_becarios_pais_estudio_`${BRONZE_EXTRACTION_DATE}.json"
        )
    )

Upsert-CloudRunJob `
    -JobName $DataflowPronabecColegiosHabilesJobName `
    -Description "Lanzador Dataflow PRONABEC colegios hábiles Bronze a Silver" `
    -TaskTimeoutSeconds 7200 `
    -Args @(
        $DataflowCommonArgs +
        @(
            "--source-system",
            "pronabec",
            "--source-dataset",
            "colegios_habiles",
            "--input-path",
            "gs://$BucketName/bronze/pronabec/colegios_habiles/extraction_date=`${BRONZE_EXTRACTION_DATE}/data.jsonl",
            "--input-format",
            "jsonl",
            "--output-table",
            "$ProjectId`:$SilverDataset.pronabec_colegios_elegibles",
            "--summary-output-path",
            "gs://$BucketName/audit/processing_summary/pronabec_colegios_elegibles_`${BRONZE_EXTRACTION_DATE}.json"
        )
    )

Upsert-CloudRunJob `
    -JobName $DataflowPronabecBecariosProvinciaJobName `
    -Description "Lanzador Dataflow PRONABEC becarios provincia Bronze a Silver" `
    -TaskTimeoutSeconds 7200 `
    -Args @(
        $DataflowCommonArgs +
        @(
            "--source-system",
            "pronabec",
            "--source-dataset",
            "becarios_provincia",
            "--input-path",
            "gs://$BucketName/bronze/pronabec/becarios_provincia/extraction_date=`${BRONZE_EXTRACTION_DATE}/data.jsonl",
            "--input-format",
            "jsonl",
            "--output-table",
            "$ProjectId`:$SilverDataset.pronabec_beca18_becarios_provincia_2016",
            "--summary-output-path",
            "gs://$BucketName/audit/processing_summary/pronabec_beca18_becarios_provincia_2016_`${BRONZE_EXTRACTION_DATE}.json"
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
    -JobName $DataflowMefPresupuestoTemporalJobName `
    -Description "Lanzador Dataflow MEF presupuesto temporal Bronze a Silver" `
    -TaskTimeoutSeconds 7200 `
    -Args @(
        $DataflowCommonArgs +
        @(
            "--source-system",
            "mef",
            "--source-dataset",
            "presupuesto_temporal",
            "--input-path",
            "gs://$BucketName/bronze/mef/presupuesto_temporal/extraction_date=`${BRONZE_EXTRACTION_DATE}/year=*/data.csv",
            "--input-format",
            "csv",
            "--output-table",
            "$ProjectId`:$SilverDataset.presupuesto_mef_temporal",
            "--summary-output-path",
            "gs://$BucketName/audit/processing_summary/presupuesto_mef_temporal_`${BRONZE_EXTRACTION_DATE}.json"
        )
    )

Upsert-CloudRunJob `
    -JobName $DataflowMefProductoJobName `
    -Description "Lanzador Dataflow MEF producto Bronze a Silver" `
    -TaskTimeoutSeconds 7200 `
    -Args @(
        $DataflowCommonArgs +
        @(
            "--source-system",
            "mef",
            "--source-dataset",
            "presupuesto_producto",
            "--input-path",
            "gs://$BucketName/bronze/mef/presupuesto_producto/extraction_date=`${BRONZE_EXTRACTION_DATE}/year=*/data.csv",
            "--input-format",
            "csv",
            "--output-table",
            "$ProjectId`:$SilverDataset.presupuesto_mef_producto",
            "--summary-output-path",
            "gs://$BucketName/audit/processing_summary/presupuesto_mef_producto_`${BRONZE_EXTRACTION_DATE}.json"
        )
    )

Upsert-CloudRunJob `
    -JobName $DataflowMefProductoTemporalJobName `
    -Description "Lanzador Dataflow MEF producto temporal Bronze a Silver" `
    -TaskTimeoutSeconds 7200 `
    -Args @(
        $DataflowCommonArgs +
        @(
            "--source-system",
            "mef",
            "--source-dataset",
            "presupuesto_producto_temporal",
            "--input-path",
            "gs://$BucketName/bronze/mef/presupuesto_producto_temporal/extraction_date=`${BRONZE_EXTRACTION_DATE}/year=*/data.csv",
            "--input-format",
            "csv",
            "--output-table",
            "$ProjectId`:$SilverDataset.presupuesto_mef_producto_temporal",
            "--summary-output-path",
            "gs://$BucketName/audit/processing_summary/presupuesto_mef_producto_temporal_`${BRONZE_EXTRACTION_DATE}.json"
        )
    )

Upsert-CloudRunJob `
    -JobName $DataflowMefActividadJobName `
    -Description "Lanzador Dataflow MEF actividad Bronze a Silver" `
    -TaskTimeoutSeconds 7200 `
    -Args @(
        $DataflowCommonArgs +
        @(
            "--source-system",
            "mef",
            "--source-dataset",
            "presupuesto_actividad",
            "--input-path",
            "gs://$BucketName/bronze/mef/presupuesto_actividad/extraction_date=`${BRONZE_EXTRACTION_DATE}/year=*/data.csv",
            "--input-format",
            "csv",
            "--output-table",
            "$ProjectId`:$SilverDataset.presupuesto_mef_actividad",
            "--summary-output-path",
            "gs://$BucketName/audit/processing_summary/presupuesto_mef_actividad_`${BRONZE_EXTRACTION_DATE}.json"
        )
    )

Upsert-CloudRunJob `
    -JobName $DataflowMefActividadTemporalJobName `
    -Description "Lanzador Dataflow MEF actividad temporal Bronze a Silver" `
    -TaskTimeoutSeconds 7200 `
    -Args @(
        $DataflowCommonArgs +
        @(
            "--source-system",
            "mef",
            "--source-dataset",
            "presupuesto_actividad_temporal",
            "--input-path",
            "gs://$BucketName/bronze/mef/presupuesto_actividad_temporal/extraction_date=`${BRONZE_EXTRACTION_DATE}/year=*/data.csv",
            "--input-format",
            "csv",
            "--output-table",
            "$ProjectId`:$SilverDataset.presupuesto_mef_actividad_temporal",
            "--summary-output-path",
            "gs://$BucketName/audit/processing_summary/presupuesto_mef_actividad_temporal_`${BRONZE_EXTRACTION_DATE}.json"
        )
    )

Upsert-CloudRunJob `
    -JobName $DataflowMefGenericaJobName `
    -Description "Lanzador Dataflow MEF genérica Bronze a Silver" `
    -TaskTimeoutSeconds 7200 `
    -Args @(
        $DataflowCommonArgs +
        @(
            "--source-system",
            "mef",
            "--source-dataset",
            "presupuesto_generica",
            "--input-path",
            "gs://$BucketName/bronze/mef/presupuesto_generica/extraction_date=`${BRONZE_EXTRACTION_DATE}/year=*/data.csv",
            "--input-format",
            "csv",
            "--output-table",
            "$ProjectId`:$SilverDataset.presupuesto_mef_generica",
            "--summary-output-path",
            "gs://$BucketName/audit/processing_summary/presupuesto_mef_generica_`${BRONZE_EXTRACTION_DATE}.json"
        )
    )

Upsert-CloudRunJob `
    -JobName $DataflowMefGenericaTemporalJobName `
    -Description "Lanzador Dataflow MEF genérica temporal Bronze a Silver" `
    -TaskTimeoutSeconds 7200 `
    -Args @(
        $DataflowCommonArgs +
        @(
            "--source-system",
            "mef",
            "--source-dataset",
            "presupuesto_generica_temporal",
            "--input-path",
            "gs://$BucketName/bronze/mef/presupuesto_generica_temporal/extraction_date=`${BRONZE_EXTRACTION_DATE}/year=*/data.csv",
            "--input-format",
            "csv",
            "--output-table",
            "$ProjectId`:$SilverDataset.presupuesto_mef_generica_temporal",
            "--summary-output-path",
            "gs://$BucketName/audit/processing_summary/presupuesto_mef_generica_temporal_`${BRONZE_EXTRACTION_DATE}.json"
        )
    )

Upsert-CloudRunJob `
    -JobName $DataflowMefHierarchyJobName `
    -Description "Lanzador Dataflow MEF jerarquía Bronze a Silver" `
    -TaskTimeoutSeconds 7200 `
    -Args @(
        $DataflowCommonArgs +
        @(
            "--source-system",
            "mef",
            "--source-dataset",
            "presupuesto_hierarchy",
            "--input-path",
            "gs://$BucketName/bronze/mef/presupuesto_hierarchy/extraction_date=`${BRONZE_EXTRACTION_DATE}/year=*/data.csv",
            "--input-format",
            "csv",
            "--output-table",
            "$ProjectId`:$SilverDataset.presupuesto_mef_hierarchy",
            "--summary-output-path",
            "gs://$BucketName/audit/processing_summary/presupuesto_mef_hierarchy_`${BRONZE_EXTRACTION_DATE}.json"
        )
    )

Upsert-CloudRunJob `
    -JobName $DataflowPronabecReportJobName `
    -Description "Lanzador Dataflow parametrizable para PRONABEC reports Bronze a Silver" `
    -TaskTimeoutSeconds 7200 `
    -SetEnvVars @(
        "SOURCE_DATASET=placeholder_dataset",
        "INPUT_PATH=gs://$BucketName/placeholder_path",
        "OUTPUT_TABLE=$ProjectId`:$SilverDataset.placeholder_table"
    ) `
    -Args @(
        $DataflowCommonArgs +
        @(
            "--source-system",
            "pronabec_reports",
            "--source-dataset",
            "`${SOURCE_DATASET}",
            "--input-path",
            "`${INPUT_PATH}",
            "--input-format",
            "csv",
            "--output-table",
            "`${OUTPUT_TABLE}",
            "--summary-output-path",
            "gs://$BucketName/audit/processing_summary/`${SOURCE_DATASET}_`${BRONZE_EXTRACTION_DATE}.json"
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
