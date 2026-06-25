# scripts/deploy_bigquery_sql.ps1

[CmdletBinding()]
param(
    [string]$ProjectId = $env:GCP_PROJECT_ID,
    [string]$BucketName = $(if ($env:GCS_BUCKET_NAME) { $env:GCS_BUCKET_NAME } else { $env:GCS_BUCKET }),
    [string]$BronzeExtractionDate = $env:BRONZE_EXTRACTION_DATE,
    [string]$Location = $(if ($env:BQ_LOCATION) { $env:BQ_LOCATION } else { "US" }),

    [string]$BronzeDataset = $(if ($env:BQ_BRONZE_DATASET) { $env:BQ_BRONZE_DATASET } else { "bronze" }),
    [string]$SilverDataset = $(if ($env:BQ_SILVER_DATASET) { $env:BQ_SILVER_DATASET } else { "silver" }),
    [string]$GoldDataset = $(if ($env:BQ_GOLD_DATASET) { $env:BQ_GOLD_DATASET } else { "gold" }),
    [string]$AuditDataset = $(if ($env:BQ_AUDIT_DATASET) { $env:BQ_AUDIT_DATASET } else { "audit" }),
    [string]$MlDataset = $(if ($env:BQ_ML_DATASET) { $env:BQ_ML_DATASET } else { "ml" }),

    [switch]$SkipGeneratedDdl,
    [switch]$SkipRender,
    [switch]$SkipGold
)

$ErrorActionPreference = "Stop"

function Resolve-ProjectRoot {
    $scriptPath = Split-Path -Parent $PSCommandPath
    return (Resolve-Path (Join-Path $scriptPath "..")).Path
}

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

function Invoke-BqSqlFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$SqlPath,

        [Parameter(Mandatory = $true)]
        [string]$StepName
    )

    if (-not (Test-Path $SqlPath)) {
        throw "No existe el archivo SQL requerido para '$StepName': $SqlPath"
    }

    Write-Host "Ejecutando BigQuery SQL: $StepName"

    $SqlContent = Get-Content -Raw -Encoding UTF8 -Path $SqlPath

    if ([string]::IsNullOrWhiteSpace($SqlContent)) {
        throw "El archivo SQL está vacío para '$StepName': $SqlPath"
    }

    bq query `
        --project_id="$ProjectId" `
        --location="$Location" `
        --use_legacy_sql=false `
        --quiet `
        $SqlContent
}

$ProjectRoot = Resolve-ProjectRoot
Set-Location $ProjectRoot

Assert-RequiredValue -Name "ProjectId" -Value $ProjectId
Assert-RequiredValue -Name "Location" -Value $Location

if (-not $SkipGeneratedDdl) {
    Assert-RequiredValue -Name "BucketName" -Value $BucketName

    $GenerateArgs = @(
        "-ProjectId", $ProjectId,
        "-BucketName", $BucketName,
        "-OutputDir", "build/generated/sql"
    )

    if (-not [string]::IsNullOrWhiteSpace($BronzeExtractionDate)) {
        $GenerateArgs += @("-BronzeExtractionDate", $BronzeExtractionDate)
    }

    & "$ProjectRoot/scripts/generate_bigquery_ddl.ps1" @GenerateArgs
}

if (-not $SkipRender) {
    & "$ProjectRoot/scripts/render_sql_templates.ps1" `
        -ProjectId $ProjectId `
        -BronzeDataset $BronzeDataset `
        -SilverDataset $SilverDataset `
        -GoldDataset $GoldDataset `
        -AuditDataset $AuditDataset `
        -MlDataset $MlDataset `
        -OutputDir "build/generated/sql"
}

$GeneratedSqlDir = Join-Path $ProjectRoot "build/generated/sql"

$CreateDatasetsSql = Join-Path $GeneratedSqlDir "create_datasets.rendered.sql"
$BronzeGeneratedSql = Join-Path $GeneratedSqlDir "create_bronze_external_tables.sql"
$SilverGeneratedSql = Join-Path $GeneratedSqlDir "create_silver_tables.sql"
$CreateAuditSql = Join-Path $GeneratedSqlDir "create_audit_tables.rendered.sql"
$CreateGoldSql = Join-Path $GeneratedSqlDir "create_gold_views.rendered.sql"

Invoke-BqSqlFile -SqlPath $CreateDatasetsSql -StepName "Datasets BigQuery"

if (-not $SkipGeneratedDdl) {
    Invoke-BqSqlFile -SqlPath $BronzeGeneratedSql -StepName "Tablas externas Bronze"
    Invoke-BqSqlFile -SqlPath $SilverGeneratedSql -StepName "Tablas Silver"
}

Invoke-BqSqlFile -SqlPath $CreateAuditSql -StepName "Tablas Audit"

if (-not $SkipGold) {
    Invoke-BqSqlFile -SqlPath $CreateGoldSql -StepName "Vistas Gold"
}

Write-Host "Despliegue SQL BigQuery finalizado correctamente."