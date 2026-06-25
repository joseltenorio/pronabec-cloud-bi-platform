# scripts/generate_bigquery_ddl.ps1

[CmdletBinding()]
param(
    [string]$ProjectId = $env:GCP_PROJECT_ID,
    [string]$BucketName = $(if ($env:GCS_BUCKET_NAME) { $env:GCS_BUCKET_NAME } else { $env:GCS_BUCKET }),
    [string]$BronzeExtractionDate = $env:BRONZE_EXTRACTION_DATE,
    [string]$OutputDir = "build/generated/sql"
)

$ErrorActionPreference = "Stop"

function Resolve-ProjectRoot {
    $scriptPath = Split-Path -Parent $PSCommandPath
    return (Resolve-Path (Join-Path $scriptPath "..")).Path
}

$ProjectRoot = Resolve-ProjectRoot
Set-Location $ProjectRoot

if ([string]::IsNullOrWhiteSpace($ProjectId)) {
    throw "Falta configurar ProjectId. Define -ProjectId o la variable GCP_PROJECT_ID."
}

if ([string]::IsNullOrWhiteSpace($BucketName)) {
    throw "Falta configurar BucketName. Define -BucketName, GCS_BUCKET_NAME o GCS_BUCKET."
}

$ResolvedOutputDir = Join-Path $ProjectRoot $OutputDir
New-Item -ItemType Directory -Force -Path $ResolvedOutputDir | Out-Null

$Arguments = @(
    "tools/generate_bigquery_ddl.py",
    "--project-id", $ProjectId,
    "--bucket", $BucketName,
    "--output-dir", $ResolvedOutputDir
)

if (-not [string]::IsNullOrWhiteSpace($BronzeExtractionDate)) {
    $Arguments += @("--bronze-extraction-date", $BronzeExtractionDate)
}

python @Arguments

$BronzeSql = Join-Path $ResolvedOutputDir "create_bronze_external_tables.sql"
$SilverSql = Join-Path $ResolvedOutputDir "create_silver_tables.sql"

if (-not (Test-Path $BronzeSql)) {
    throw "No se generó el archivo esperado: $BronzeSql"
}

if (-not (Test-Path $SilverSql)) {
    throw "No se generó el archivo esperado: $SilverSql"
}

Write-Host "DDL BigQuery generado correctamente."
Write-Host "Bronze: $BronzeSql"
Write-Host "Silver: $SilverSql"