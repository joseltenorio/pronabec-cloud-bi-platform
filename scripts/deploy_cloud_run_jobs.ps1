# scripts/deploy_cloud_run_jobs.ps1

[CmdletBinding()]
param(
    [string]$ProjectId = $env:GCP_PROJECT_ID,
    [string]$Region = $(if ($env:CLOUD_RUN_JOBS_REGION) { $env:CLOUD_RUN_JOBS_REGION } elseif ($env:CLOUD_RUN_REGION) { $env:CLOUD_RUN_REGION } else { $env:GCP_REGION }),
    [string]$Image = $env:CLOUD_RUN_IMAGE,
    [string]$ServiceAccount = $(if ($env:CLOUD_RUN_JOBS_SERVICE_ACCOUNT) { $env:CLOUD_RUN_JOBS_SERVICE_ACCOUNT } else { $env:CLOUD_RUN_SERVICE_ACCOUNT }),

    [string]$BucketName = $(if ($env:GCS_BUCKET_NAME) { $env:GCS_BUCKET_NAME } else { $env:GCS_BUCKET }),
    [string]$Location = $(if ($env:BQ_LOCATION) { $env:BQ_LOCATION } else { "US" }),

    [string]$BronzeDataset = $(if ($env:BQ_BRONZE_DATASET) { $env:BQ_BRONZE_DATASET } else { "bronze" }),
    [string]$SilverDataset = $(if ($env:BQ_SILVER_DATASET) { $env:BQ_SILVER_DATASET } else { "silver" }),
    [string]$GoldDataset = $(if ($env:BQ_GOLD_DATASET) { $env:BQ_GOLD_DATASET } else { "gold" }),
    [string]$AuditDataset = $(if ($env:BQ_AUDIT_DATASET) { $env:BQ_AUDIT_DATASET } else { "audit" }),

    [string]$DataflowTempLocation = $(if ($env:DATAFLOW_TEMP_LOCATION) { $env:DATAFLOW_TEMP_LOCATION } else { "" }),
    [string]$DataflowStagingLocation = $(if ($env:DATAFLOW_STAGING_LOCATION) { $env:DATAFLOW_STAGING_LOCATION } else { "" }),
    [string]$DataflowServiceAccount = $env:DATAFLOW_SERVICE_ACCOUNT,
    [string]$DataflowSdkContainerImage = $env:DATAFLOW_SDK_CONTAINER_IMAGE,
    [string]$ArtifactRegion = $(if ($env:ARTIFACT_REGISTRY_REGION) { $env:ARTIFACT_REGISTRY_REGION } elseif ($env:ARTIFACT_REGISTRY_LOCATION) { $env:ARTIFACT_REGISTRY_LOCATION } else { $env:GCP_REGION }),
    [string]$ArtifactRepository = $(if ($env:ARTIFACT_REGISTRY_REPOSITORY) { $env:ARTIFACT_REGISTRY_REPOSITORY } else { "pronabec-containers" }),
    [string]$DataflowWorkerImageName = $(if ($env:DATAFLOW_WORKER_IMAGE_NAME) { $env:DATAFLOW_WORKER_IMAGE_NAME } else { "pronabec-dataflow-worker" }),
    [string]$DataflowWorkerImageTag = $(if ($env:DATAFLOW_WORKER_IMAGE_TAG) { $env:DATAFLOW_WORKER_IMAGE_TAG } else { "latest" }),

    [string]$MefJobName = $(if ($env:MEF_EXTRACT_JOB_NAME) { $env:MEF_EXTRACT_JOB_NAME } else { "mef-extract-job" }),
    [string]$PronabecReportsStageJobName = $(if ($env:PRONABEC_REPORTS_STAGE_JOB_NAME) { $env:PRONABEC_REPORTS_STAGE_JOB_NAME } else { "pronabec-stage-reports-job" }),
    [string]$BronzeManifestValidationJobName = $(if ($env:BRONZE_MANIFEST_VALIDATION_JOB_NAME) { $env:BRONZE_MANIFEST_VALIDATION_JOB_NAME } else { "bronze-manifest-validation-job" }),
    [string]$GoldPublishJobName = $(if ($env:GOLD_PUBLISH_JOB_NAME) { $env:GOLD_PUBLISH_JOB_NAME } else { "gold-publish-job" }),
    [string]$GoldValidateJobName = $(if ($env:GOLD_VALIDATE_JOB_NAME) { $env:GOLD_VALIDATE_JOB_NAME } else { "gold-validate-job" }),
    [string]$QualityJobName = $(if ($env:QUALITY_CHECKS_JOB_NAME) { $env:QUALITY_CHECKS_JOB_NAME } else { "quality-checks-job" }),

    [string]$PronabecDiscoveryJobName = $(if ($env:PRONABEC_DISCOVERY_JOB_NAME) { $env:PRONABEC_DISCOVERY_JOB_NAME } else { "pronabec-discovery-job" }),
    [string]$PronabecBuildPlanJobName = $(if ($env:PRONABEC_BUILD_PLAN_JOB_NAME) { $env:PRONABEC_BUILD_PLAN_JOB_NAME } else { "pronabec-build-plan-job" }),
    [string]$PronabecRunPlanJobName = $(if ($env:PRONABEC_RUN_PLAN_JOB_NAME) { $env:PRONABEC_RUN_PLAN_JOB_NAME } else { "pronabec-run-plan-job" }),
    [string]$PronabecFinalizeDatasetJobName = $(if ($env:PRONABEC_FINALIZE_DATASET_JOB_NAME) { $env:PRONABEC_FINALIZE_DATASET_JOB_NAME } else { "pronabec-finalize-dataset-job" }),

    [string]$PronabecReportsLandingPrefix = $(if ($env:PRONABEC_REPORTS_LANDING_PREFIX) { $env:PRONABEC_REPORTS_LANDING_PREFIX } else { "landing/pronabec_reports" }),
    [string]$PronabecReportsBronzePrefix = $(if ($env:PRONABEC_REPORTS_BRONZE_PREFIX) { $env:PRONABEC_REPORTS_BRONZE_PREFIX } else { "bronze/pronabec_reports" }),

    [string]$MefSourceMode = $env:MEF_SOURCE_MODE,
    [string]$MefConsultaAmigableBaseUrl = $(if ($env:MEF_CONSULTA_AMIGABLE_BASE_URL) { $env:MEF_CONSULTA_AMIGABLE_BASE_URL } else { "https://apps5.mineco.gob.pe/transparencia/Navegador/" }),
    [string]$MefStartYear = $env:MEF_START_YEAR,
    [string]$MefEndYear = $env:MEF_END_YEAR,
    [string]$MefTextFilter = $env:MEF_TEXT_FILTER,
    [string]$MefTimeoutSeconds = $(if ($env:MEF_TIMEOUT_SECONDS) { $env:MEF_TIMEOUT_SECONDS } else { "90" }),
    [string]$MefPronabecEjecutoraCode = $env:MEF_PRONABEC_EXECUTORA_CODE,
    [string]$MefPronabecEjecutoraName = $(if ($env:MEF_PRONABEC_EXECUTORA_NAME) { $env:MEF_PRONABEC_EXECUTORA_NAME } else { "PROGRAMA NACIONAL DE BECAS Y CREDITO EDUCATIVO" }),
    [string]$MefIncludeHierarchy = $(if ($env:MEF_INCLUDE_HIERARCHY) { $env:MEF_INCLUDE_HIERARCHY } else { "true" }),
    [string]$MefIncludeSpendingBreakdowns = $(if ($env:MEF_INCLUDE_SPENDING_BREAKDOWNS) { $env:MEF_INCLUDE_SPENDING_BREAKDOWNS } else { "true" }),
    [string]$MefBreakdownSlices = $(if ($env:MEF_BREAKDOWN_SLICES) { $env:MEF_BREAKDOWN_SLICES } else { "producto,generica,fuente,rubro,departamento,temporal,producto_temporal,actividad,actividad_temporal,generica_temporal" }),

    [string]$DataflowPronabecConvocatoriasJobName = $(if ($env:DATAFLOW_PRONABEC_CONVOCATORIAS_JOB_NAME) { $env:DATAFLOW_PRONABEC_CONVOCATORIAS_JOB_NAME } else { "dataflow-pronabec-convocatorias-job" }),
    [string]$DataflowPronabecUbigeoPostulacionJobName = $(if ($env:DATAFLOW_PRONABEC_UBIGEO_POSTULACION_JOB_NAME) { $env:DATAFLOW_PRONABEC_UBIGEO_POSTULACION_JOB_NAME } else { "dataflow-pronabec-ubigeo-postulacion-job" }),
    [string]$DataflowPronabecBecariosPaisEstudioJobName = $(if ($env:DATAFLOW_PRONABEC_BECARIOS_PAIS_ESTUDIO_JOB_NAME) { $env:DATAFLOW_PRONABEC_BECARIOS_PAIS_ESTUDIO_JOB_NAME } else { "dataflow-pronabec-becarios-pais-estudio-job" }),
    [string]$DataflowPronabecColegiosHabilesJobName = $(if ($env:DATAFLOW_PRONABEC_COLEGIOS_HABILES_JOB_NAME) { $env:DATAFLOW_PRONABEC_COLEGIOS_HABILES_JOB_NAME } else { "dataflow-pronabec-colegios-habiles-job" }),
    [string]$DataflowPronabecBecariosProvinciaJobName = $(if ($env:DATAFLOW_PRONABEC_BECARIOS_PROVINCIA_JOB_NAME) { $env:DATAFLOW_PRONABEC_BECARIOS_PROVINCIA_JOB_NAME } else { "dataflow-pronabec-becarios-provincia-job" }),
    [string]$DataflowMefPresupuestoJobName = $(if ($env:DATAFLOW_MEF_PRESUPUESTO_JOB_NAME) { $env:DATAFLOW_MEF_PRESUPUESTO_JOB_NAME } else { "dataflow-mef-presupuesto-job" }),
    [string]$DataflowMefPresupuestoTemporalJobName = $(if ($env:DATAFLOW_MEF_PRESUPUESTO_TEMPORAL_JOB_NAME) { $env:DATAFLOW_MEF_PRESUPUESTO_TEMPORAL_JOB_NAME } else { "dataflow-mef-presupuesto-temporal-job" }),
    [string]$DataflowMefProductoJobName = $(if ($env:DATAFLOW_MEF_PRODUCTO_JOB_NAME) { $env:DATAFLOW_MEF_PRODUCTO_JOB_NAME } else { "dataflow-mef-producto-job" }),
    [string]$DataflowMefProductoTemporalJobName = $(if ($env:DATAFLOW_MEF_PRODUCTO_TEMPORAL_JOB_NAME) { $env:DATAFLOW_MEF_PRODUCTO_TEMPORAL_JOB_NAME } else { "dataflow-mef-producto-temporal-job" }),
    [string]$DataflowMefActividadJobName = $(if ($env:DATAFLOW_MEF_ACTIVIDAD_JOB_NAME) { $env:DATAFLOW_MEF_ACTIVIDAD_JOB_NAME } else { "dataflow-mef-actividad-job" }),
    [string]$DataflowMefActividadTemporalJobName = $(if ($env:DATAFLOW_MEF_ACTIVIDAD_TEMPORAL_JOB_NAME) { $env:DATAFLOW_MEF_ACTIVIDAD_TEMPORAL_JOB_NAME } else { "dataflow-mef-actividad-temporal-job" }),
    [string]$DataflowMefGenericaJobName = $(if ($env:DATAFLOW_MEF_GENERICA_JOB_NAME) { $env:DATAFLOW_MEF_GENERICA_JOB_NAME } else { "dataflow-mef-generica-job" }),
    [string]$DataflowMefGenericaTemporalJobName = $(if ($env:DATAFLOW_MEF_GENERICA_TEMPORAL_JOB_NAME) { $env:DATAFLOW_MEF_GENERICA_TEMPORAL_JOB_NAME } else { "dataflow-mef-generica-temporal-job" }),
    [string]$DataflowMefHierarchyJobName = $(if ($env:DATAFLOW_MEF_HIERARCHY_JOB_NAME) { $env:DATAFLOW_MEF_HIERARCHY_JOB_NAME } else { "dataflow-mef-hierarchy-job" }),
    [string]$DataflowPronabecReportJobName = $(if ($env:DATAFLOW_PRONABEC_REPORT_JOB_NAME) { $env:DATAFLOW_PRONABEC_REPORT_JOB_NAME } else { "dataflow-pronabec-report-job" })
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
        throw "Falta configurar $Name. Define el parÃ¡metro correspondiente o la variable de entorno asociada."
    }
}

function Invoke-GCloud {
    param(
        [Parameter(Mandatory = $true)]
        [string]$StepName,

        [Parameter(Mandatory = $true)]
        [scriptblock]$Command
    )

    & $Command

    if ($LASTEXITCODE -ne 0) {
        throw "FallÃ³ el paso '$StepName' con cÃ³digo de salida $LASTEXITCODE."
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
        [string[]]$ContainerArgs
    )

    return ($ContainerArgs -join ",")
}

function Join-CloudRunEnvVars {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$EnvVars
    )

    return "^|^" + ($EnvVars -join "|")
}

function Resolve-DataflowSdkContainerImage {
    if (-not [string]::IsNullOrWhiteSpace($DataflowSdkContainerImage)) {
        return $DataflowSdkContainerImage
    }

    if (
        [string]::IsNullOrWhiteSpace($ArtifactRegion) -or
        [string]::IsNullOrWhiteSpace($ProjectId) -or
        [string]::IsNullOrWhiteSpace($ArtifactRepository) -or
        [string]::IsNullOrWhiteSpace($DataflowWorkerImageName) -or
        [string]::IsNullOrWhiteSpace($DataflowWorkerImageTag)
    ) {
        throw "Falta configurar DATAFLOW_SDK_CONTAINER_IMAGE o las variables ARTIFACT_REGISTRY_REGION, GCP_PROJECT_ID, ARTIFACT_REGISTRY_REPOSITORY, DATAFLOW_WORKER_IMAGE_NAME y DATAFLOW_WORKER_IMAGE_TAG."
    }

    return "$ArtifactRegion-docker.pkg.dev/$ProjectId/$ArtifactRepository/$DataflowWorkerImageName`:$DataflowWorkerImageTag"
}

function Upsert-CloudRunJob {
    param(
        [Parameter(Mandatory = $true)]
        [string]$JobName,

        [Parameter(Mandatory = $true)]
        [string[]]$ContainerArgs,

        [Parameter(Mandatory = $true)]
        [string]$Description,

        [string[]]$SetEnvVars = @(),

        [int]$TaskTimeoutSeconds = 3600,

        [string]$Memory = "512Mi",

        [string]$Cpu = "1"
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
        "PRONABEC_REPORTS_LANDING_PREFIX=$PronabecReportsLandingPrefix",
        "PRONABEC_REPORTS_BRONZE_PREFIX=$PronabecReportsBronzePrefix",
        "PRONABEC_REQUEST_TIMEOUT_SECONDS=$(if ($env:PRONABEC_REQUEST_TIMEOUT_SECONDS) { $env:PRONABEC_REQUEST_TIMEOUT_SECONDS } else { "180" })",
        "PRONABEC_MAX_RETRIES=$(if ($env:PRONABEC_MAX_RETRIES) { $env:PRONABEC_MAX_RETRIES } else { "5" })",
        "PRONABEC_BACKOFF_BASE_SECONDS=$(if ($env:PRONABEC_BACKOFF_BASE_SECONDS) { $env:PRONABEC_BACKOFF_BASE_SECONDS } else { "10" })",
        "PRONABEC_BACKOFF_MAX_SECONDS=$(if ($env:PRONABEC_BACKOFF_MAX_SECONDS) { $env:PRONABEC_BACKOFF_MAX_SECONDS } else { "120" })",
        "STRUCTURED_LOGGING=true",
        "LOG_LEVEL=INFO"
    )

    $EnvVars = Join-CloudRunEnvVars -EnvVars ($BaseEnvVars + $SetEnvVars)

    $CommonArgs = @(
        "--project", $ProjectId,
        "--region", $Region,
        "--image", $Image,
        "--service-account", $ServiceAccount,
        "--set-env-vars", $EnvVars,
        "--max-retries", "1",
        "--task-timeout", "$($TaskTimeoutSeconds)s",
        "--memory", $Memory,
        "--cpu", $Cpu
    )

    $JoinedArgs = Join-CloudRunArgs -ContainerArgs $ContainerArgs
    $ArgsFlag = "--args=$JoinedArgs"

    if (Test-CloudRunJobExists -JobName $JobName) {
        Write-Host "Actualizando Cloud Run Job: $JobName"

        Invoke-GCloud -StepName "Actualizando Cloud Run Job $JobName" -Command {
            gcloud run jobs update $JobName `
                @CommonArgs `
                $ArgsFlag `
                --quiet
        }
    }
    else {
        Write-Host "Creando Cloud Run Job: $JobName"

        Invoke-GCloud -StepName "Creando Cloud Run Job $JobName" -Command {
            gcloud run jobs create $JobName `
                @CommonArgs `
                $ArgsFlag `
                --quiet
        }
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
Assert-RequiredValue -Name "DATAFLOW_SERVICE_ACCOUNT" -Value $DataflowServiceAccount
Assert-RequiredValue -Name "MEF_SOURCE_MODE" -Value $MefSourceMode
Assert-RequiredValue -Name "MEF_START_YEAR" -Value $MefStartYear
Assert-RequiredValue -Name "MEF_END_YEAR" -Value $MefEndYear
Assert-RequiredValue -Name "MEF_TEXT_FILTER" -Value $MefTextFilter
Assert-RequiredValue -Name "MEF_PRONABEC_EXECUTORA_CODE" -Value $MefPronabecEjecutoraCode

$ResolvedDataflowSdkContainerImage = Resolve-DataflowSdkContainerImage
Assert-RequiredValue -Name "DATAFLOW_SDK_CONTAINER_IMAGE" -Value $ResolvedDataflowSdkContainerImage

$DataflowEnvVars = @(
    "DATAFLOW_TEMP_LOCATION=$DataflowTempLocation",
    "DATAFLOW_STAGING_LOCATION=$DataflowStagingLocation",
    "DATAFLOW_SERVICE_ACCOUNT=$DataflowServiceAccount",
    "DATAFLOW_SDK_CONTAINER_IMAGE=$ResolvedDataflowSdkContainerImage"
)

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
    "--service-account-email",
    $DataflowServiceAccount,
    "--sdk-container-image",
    $ResolvedDataflowSdkContainerImage,
    "--dlq-output-root",
    "gs://$BucketName/dlq"
)

Upsert-CloudRunJob `
    -JobName $PronabecDiscoveryJobName `
    -Description "Discovery de datasets PRONABEC para planificacion particionada" `
    -ContainerArgs @(
        "-m",
        "pipelines.discover_pronabec"
    )

Upsert-CloudRunJob `
    -JobName $PronabecBuildPlanJobName `
    -Description "Construccion del plan de extraccion PRONABEC particionado" `
    -ContainerArgs @(
        "-m",
        "pipelines.build_pronabec_extraction_plan"
    )

Upsert-CloudRunJob `
    -JobName $PronabecRunPlanJobName `
    -Description "Ejecucion del plan de chunks PRONABEC desde plan.json" `
    -ContainerArgs @(
        "-m",
        "pipelines.run_pronabec_extraction_plan"
    )

Upsert-CloudRunJob `
    -JobName $PronabecFinalizeDatasetJobName `
    -Description "Consolidacion final de chunks PRONABEC hacia Bronze" `
    -Memory "2Gi" `
    -Cpu "1" `
    -ContainerArgs @(
        "-m",
        "pipelines.finalize_pronabec_dataset"
    )

Upsert-CloudRunJob `
    -JobName $MefJobName `
    -Description "ExtracciÃ³n batch MEF hacia Bronze" `
    -SetEnvVars @(
        "MEF_SOURCE_MODE=$MefSourceMode",
        "MEF_CONSULTA_AMIGABLE_BASE_URL=$MefConsultaAmigableBaseUrl",
        "MEF_START_YEAR=$MefStartYear",
        "MEF_END_YEAR=$MefEndYear",
        "MEF_TEXT_FILTER=$MefTextFilter",
        "MEF_TIMEOUT_SECONDS=$MefTimeoutSeconds",
        "MEF_PRONABEC_EXECUTORA_CODE=$MefPronabecEjecutoraCode",
        "MEF_PRONABEC_EXECUTORA_NAME=$MefPronabecEjecutoraName",
        "MEF_INCLUDE_HIERARCHY=$MefIncludeHierarchy",
        "MEF_INCLUDE_SPENDING_BREAKDOWNS=$MefIncludeSpendingBreakdowns",
        "MEF_BREAKDOWN_SLICES=$MefBreakdownSlices"
    ) `
    -ContainerArgs @(
        "-m",
        "pipelines.scrape_mef_budget"
    )

Upsert-CloudRunJob `
    -JobName $PronabecReportsStageJobName `
    -Description "Staging PRONABEC reports desde GCS Landing hacia Bronze" `
    -ContainerArgs @(
        "-m",
        "tools.stage_pronabec_manual_reports",
        "--strict",
        "--overwrite"
    )

Upsert-CloudRunJob `
    -JobName $BronzeManifestValidationJobName `
    -Description "Validacion de manifests Bronze antes de promover a Silver" `
    -ContainerArgs @(
        "-m",
        "pipelines.validate_bronze_manifests"
    )

Upsert-CloudRunJob `
    -JobName $GoldPublishJobName `
    -Description "Publicacion idempotente de vistas Gold analiticas" `
    -ContainerArgs @(
        "-m",
        "pipelines.publish_gold_views"
    )

Upsert-CloudRunJob `
    -JobName $GoldValidateJobName `
    -Description "Validacion de contratos Gold analiticos" `
    -ContainerArgs @(
        "-m",
        "pipelines.validate_gold"
    )

Upsert-CloudRunJob `
    -JobName $DataflowPronabecConvocatoriasJobName `
    -Description "Lanzador Dataflow PRONABEC convocatorias Bronze a Silver" `
    -TaskTimeoutSeconds 7200 `
    -SetEnvVars $DataflowEnvVars `
    -ContainerArgs @(
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
    -Description "Lanzador Dataflow PRONABEC ubigeo postulaciÃ³n Bronze a Silver" `
    -TaskTimeoutSeconds 7200 `
    -SetEnvVars $DataflowEnvVars `
    -ContainerArgs @(
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
    -Description "Lanzador Dataflow PRONABEC becarios paÃ­s estudio Bronze a Silver" `
    -TaskTimeoutSeconds 7200 `
    -SetEnvVars $DataflowEnvVars `
    -ContainerArgs @(
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
    -Description "Lanzador Dataflow PRONABEC colegios hÃ¡biles Bronze a Silver" `
    -TaskTimeoutSeconds 7200 `
    -SetEnvVars $DataflowEnvVars `
    -ContainerArgs @(
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
    -SetEnvVars $DataflowEnvVars `
    -ContainerArgs @(
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
    -SetEnvVars $DataflowEnvVars `
    -ContainerArgs @(
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
    -SetEnvVars $DataflowEnvVars `
    -ContainerArgs @(
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
    -SetEnvVars $DataflowEnvVars `
    -ContainerArgs @(
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
    -SetEnvVars $DataflowEnvVars `
    -ContainerArgs @(
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
    -SetEnvVars $DataflowEnvVars `
    -ContainerArgs @(
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
    -SetEnvVars $DataflowEnvVars `
    -ContainerArgs @(
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
    -Description "Lanzador Dataflow MEF genÃ©rica Bronze a Silver" `
    -TaskTimeoutSeconds 7200 `
    -SetEnvVars $DataflowEnvVars `
    -ContainerArgs @(
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
    -Description "Lanzador Dataflow MEF genÃ©rica temporal Bronze a Silver" `
    -TaskTimeoutSeconds 7200 `
    -SetEnvVars $DataflowEnvVars `
    -ContainerArgs @(
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
    -Description "Lanzador Dataflow MEF jerarquÃ­a Bronze a Silver" `
    -TaskTimeoutSeconds 7200 `
    -SetEnvVars $DataflowEnvVars `
    -ContainerArgs @(
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
        $DataflowEnvVars +
        @(
        "SOURCE_DATASET=placeholder_dataset",
        "INPUT_PATH=gs://$BucketName/placeholder_path",
        "OUTPUT_TABLE=$ProjectId`:$SilverDataset.placeholder_table"
        )
    ) `
    -ContainerArgs @(
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
    -Description "EjecuciÃ³n batch de controles de calidad BigQuery" `
    -ContainerArgs @(
        "-m",
        "pipelines.quality_checks",
        "--project-id",
        $ProjectId,
        "--silver-dataset",
        $SilverDataset,
        "--gold-dataset",
        $GoldDataset,
        "--audit-dataset",
        $AuditDataset,
        "--extraction-date",
        "`${BRONZE_EXTRACTION_DATE}",
        "--pipeline-run-id",
        "`${PIPELINE_RUN_ID}",
        "--fail-on-error"
    )

Write-Host "Cloud Run Jobs configurados correctamente."


