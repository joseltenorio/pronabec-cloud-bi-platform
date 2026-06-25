# scripts/render_sql_templates.ps1

[CmdletBinding()]
param(
    [string]$ProjectId = $env:GCP_PROJECT_ID,
    [string]$BronzeDataset = $(if ($env:BQ_BRONZE_DATASET) { $env:BQ_BRONZE_DATASET } else { "bronze" }),
    [string]$SilverDataset = $(if ($env:BQ_SILVER_DATASET) { $env:BQ_SILVER_DATASET } else { "silver" }),
    [string]$GoldDataset = $(if ($env:BQ_GOLD_DATASET) { $env:BQ_GOLD_DATASET } else { "gold" }),
    [string]$AuditDataset = $(if ($env:BQ_AUDIT_DATASET) { $env:BQ_AUDIT_DATASET } else { "audit" }),
    [string]$MlDataset = $(if ($env:BQ_ML_DATASET) { $env:BQ_ML_DATASET } else { "ml" }),
    [string]$OutputDir = "build/generated/sql",
    [string[]]$SourceFile = @()
)

$ErrorActionPreference = "Stop"

function Resolve-ProjectRoot {
    $scriptPath = Split-Path -Parent $PSCommandPath
    return (Resolve-Path (Join-Path $scriptPath "..")).Path
}

$ProjectRoot = Resolve-ProjectRoot
Set-Location $ProjectRoot

$Arguments = @(
    "tools/render_sql_templates.py",
    "--project-id", $ProjectId,
    "--bronze-dataset", $BronzeDataset,
    "--silver-dataset", $SilverDataset,
    "--gold-dataset", $GoldDataset,
    "--audit-dataset", $AuditDataset,
    "--ml-dataset", $MlDataset,
    "--output-dir", $OutputDir
)

foreach ($file in $SourceFile) {
    $Arguments += @("--source-file", $file)
}

python @Arguments