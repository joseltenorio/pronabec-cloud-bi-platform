# scripts/build_and_push_dataflow_worker_image.ps1

[CmdletBinding()]
param(
    [string]$GcpProjectId = $env:GCP_PROJECT_ID,
    [string]$ArtifactRegion = $(if ($env:ARTIFACT_REGISTRY_REGION) { $env:ARTIFACT_REGISTRY_REGION } elseif ($env:ARTIFACT_REGISTRY_LOCATION) { $env:ARTIFACT_REGISTRY_LOCATION } else { $env:GCP_REGION }),
    [string]$ArtifactRepository = $(if ($env:ARTIFACT_REGISTRY_REPOSITORY) { $env:ARTIFACT_REGISTRY_REPOSITORY } else { "pronabec-containers" }),
    [string]$DataflowWorkerImageName = $(if ($env:DATAFLOW_WORKER_IMAGE_NAME) { $env:DATAFLOW_WORKER_IMAGE_NAME } else { "pronabec-dataflow-worker" }),
    [string]$DataflowWorkerImageTag = $(if ($env:DATAFLOW_WORKER_IMAGE_TAG) { $env:DATAFLOW_WORKER_IMAGE_TAG } else { "latest" })
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
        throw "Falta configurar $Name. Define el parametro correspondiente o la variable de entorno asociada."
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
        throw "Fallo el paso '$StepName' con codigo de salida $LASTEXITCODE."
    }
}

$ProjectRoot = Resolve-ProjectRoot
Set-Location $ProjectRoot

Assert-RequiredValue -Name "GcpProjectId" -Value $GcpProjectId
Assert-RequiredValue -Name "ArtifactRegion" -Value $ArtifactRegion
Assert-RequiredValue -Name "ArtifactRepository" -Value $ArtifactRepository
Assert-RequiredValue -Name "DataflowWorkerImageName" -Value $DataflowWorkerImageName
Assert-RequiredValue -Name "DataflowWorkerImageTag" -Value $DataflowWorkerImageTag

$DataflowWorkerImage = "$ArtifactRegion-docker.pkg.dev/$GcpProjectId/$ArtifactRepository/$DataflowWorkerImageName`:$DataflowWorkerImageTag"

Write-Host "Imagen worker Dataflow destino:"
Write-Host $DataflowWorkerImage

Invoke-NativeCommand `
    -StepName "Construyendo y publicando imagen worker Dataflow con Cloud Build..." `
    -Command {
        gcloud builds submit `
            --config cloudbuild.dataflow.yaml `
            --substitutions "_DATAFLOW_WORKER_IMAGE=$DataflowWorkerImage" `
            .
    }

Write-Host "Imagen worker Dataflow publicada correctamente:"
Write-Host $DataflowWorkerImage
