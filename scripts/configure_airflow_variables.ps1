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
    [string]$PronabecDiscoveryJobName = $(if ($env:PRONABEC_DISCOVERY_JOB_NAME) { $env:PRONABEC_DISCOVERY_JOB_NAME } else { "pronabec-discovery-job" }),
    [string]$PronabecBuildPlanJobName = $(if ($env:PRONABEC_BUILD_PLAN_JOB_NAME) { $env:PRONABEC_BUILD_PLAN_JOB_NAME } else { "pronabec-build-plan-job" }),
    [string]$PronabecExtractChunkJobName = $(if ($env:PRONABEC_EXTRACT_CHUNK_JOB_NAME) { $env:PRONABEC_EXTRACT_CHUNK_JOB_NAME } else { "pronabec-extract-chunk-job" }),
    [string]$PronabecFinalizeDatasetJobName = $(if ($env:PRONABEC_FINALIZE_DATASET_JOB_NAME) { $env:PRONABEC_FINALIZE_DATASET_JOB_NAME } else { "pronabec-finalize-dataset-job" }),
    [string]$MefExtractJobName = $(if ($env:MEF_EXTRACT_JOB_NAME) { $env:MEF_EXTRACT_JOB_NAME } else { "mef-extract-job" }),
    [string]$PronabecReportsStageJobName = $(if ($env:PRONABEC_REPORTS_STAGE_JOB_NAME) { $env:PRONABEC_REPORTS_STAGE_JOB_NAME } else { "pronabec-stage-reports-job" }),
    [string]$BronzeManifestValidationJobName = $(if ($env:BRONZE_MANIFEST_VALIDATION_JOB_NAME) { $env:BRONZE_MANIFEST_VALIDATION_JOB_NAME } else { "bronze-manifest-validation-job" }),
    [string]$DataflowPronabecReportJobName = $(if ($env:DATAFLOW_PRONABEC_REPORT_JOB_NAME) { $env:DATAFLOW_PRONABEC_REPORT_JOB_NAME } else { "dataflow-pronabec-report-job" }),
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
    "pronabec_discovery_job_name" = $PronabecDiscoveryJobName
    "pronabec_build_plan_job_name" = $PronabecBuildPlanJobName
    "pronabec_extract_chunk_job_name" = $PronabecExtractChunkJobName
    "pronabec_finalize_dataset_job_name" = $PronabecFinalizeDatasetJobName
    "mef_extract_job_name" = $MefExtractJobName
    "pronabec_reports_stage_job_name" = $PronabecReportsStageJobName
    "bronze_manifest_validation_job_name" = $BronzeManifestValidationJobName
    "dataflow_pronabec_report_job_name" = $DataflowPronabecReportJobName
    "dataflow_pronabec_convocatorias_job_name" = $DataflowPronabecConvocatoriasJobName
    "dataflow_pronabec_ubigeo_postulacion_job_name" = $DataflowPronabecUbigeoPostulacionJobName
    "dataflow_pronabec_becarios_pais_estudio_job_name" = $DataflowPronabecBecariosPaisEstudioJobName
    "dataflow_pronabec_colegios_habiles_job_name" = $DataflowPronabecColegiosHabilesJobName
    "dataflow_pronabec_becarios_provincia_job_name" = $DataflowPronabecBecariosProvinciaJobName
    "dataflow_mef_presupuesto_job_name" = $DataflowMefPresupuestoJobName
    "dataflow_mef_presupuesto_temporal_job_name" = $DataflowMefPresupuestoTemporalJobName
    "dataflow_mef_producto_job_name" = $DataflowMefProductoJobName
    "dataflow_mef_producto_temporal_job_name" = $DataflowMefProductoTemporalJobName
    "dataflow_mef_actividad_job_name" = $DataflowMefActividadJobName
    "dataflow_mef_actividad_temporal_job_name" = $DataflowMefActividadTemporalJobName
    "dataflow_mef_generica_job_name" = $DataflowMefGenericaJobName
    "dataflow_mef_generica_temporal_job_name" = $DataflowMefGenericaTemporalJobName
    "dataflow_mef_hierarchy_job_name" = $DataflowMefHierarchyJobName

    "gold_publish_job_name" = $GoldPublishJobName
    "gold_validate_job_name" = $GoldValidateJobName
    "quality_checks_job_name" = $QualityChecksJobName
}

foreach ($Entry in $Variables.GetEnumerator()) {
    Set-AirflowVariable -Name $Entry.Key -Value $Entry.Value
}

Write-Host "Airflow Variables configuradas correctamente."
