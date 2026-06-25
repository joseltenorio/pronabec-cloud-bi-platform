# scripts/upload_composer_dag.ps1

[CmdletBinding()]
param(
    [string]$ProjectId = $env:GCP_PROJECT_ID,
    [string]$Location = $(if ($env:COMPOSER_LOCATION) { $env:COMPOSER_LOCATION } else { $env:GCP_REGION }),
    [string]$EnvironmentName = $env:COMPOSER_ENVIRONMENT_NAME,
    [string]$DagPath = "dags/pronabec_medallion_batch_dag.py"
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

function Invoke-NativeCommand {
    param(
        [Parameter(Mandatory = $true)]
        [string]$StepName,

        [Parameter(Mandatory = $true)]
        [scriptblock]$Command
    )

    Write-Host $StepName
    & $Command

    if ($LASTEXITCODE -ne 0) {
        throw "Falló el paso '$StepName' con código de salida $LASTEXITCODE."
    }
}

$ProjectRoot = Resolve-ProjectRoot
Set-Location $ProjectRoot

Assert-RequiredValue -Name "ProjectId" -Value $ProjectId
Assert-RequiredValue -Name "Location" -Value $Location
Assert-RequiredValue -Name "EnvironmentName" -Value $EnvironmentName

$ResolvedDagPath = Join-Path $ProjectRoot $DagPath

if (-not (Test-Path $ResolvedDagPath)) {
    throw "No existe el DAG indicado: $ResolvedDagPath"
}

$DagBucket = gcloud composer environments describe $EnvironmentName `
    --project $ProjectId `
    --location $Location `
    --format "value(config.dagGcsPrefix)"

if ([string]::IsNullOrWhiteSpace($DagBucket)) {
    throw "No se pudo resolver el bucket de DAGs para Composer."
}

Invoke-NativeCommand `
    -StepName "Subiendo DAG a Composer..." `
    -Command {
        gcloud storage cp $ResolvedDagPath "$DagBucket/"
    }

Write-Host "DAG subido correctamente:"
Write-Host "$DagBucket/$(Split-Path -Leaf $ResolvedDagPath)"