# scripts/build_and_push_image.ps1

[CmdletBinding()]
param(
    [string]$ProjectId = $env:GCP_PROJECT_ID,
    [string]$Region = $(if ($env:ARTIFACT_REGISTRY_REGION) { $env:ARTIFACT_REGISTRY_REGION } else { $env:GCP_REGION }),
    [string]$Repository = $(if ($env:ARTIFACT_REGISTRY_REPOSITORY) { $env:ARTIFACT_REGISTRY_REPOSITORY } else { "project-cloud-bi" }),
    [string]$ImageName = $(if ($env:ARTIFACT_IMAGE_NAME) { $env:ARTIFACT_IMAGE_NAME } else { "pronabec-cloud-bi-platform" }),
    [string]$ImageTag = $(if ($env:ARTIFACT_IMAGE_TAG) { $env:ARTIFACT_IMAGE_TAG } else { "latest" })
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
Assert-RequiredValue -Name "Region" -Value $Region
Assert-RequiredValue -Name "Repository" -Value $Repository
Assert-RequiredValue -Name "ImageName" -Value $ImageName
Assert-RequiredValue -Name "ImageTag" -Value $ImageTag

$ImageUri = "$Region-docker.pkg.dev/$ProjectId/$Repository/$ImageName`:$ImageTag"

Write-Host "Imagen destino:"
Write-Host $ImageUri

Invoke-NativeCommand `
    -StepName "Configurando autenticación Docker para Artifact Registry..." `
    -Command {
        gcloud auth configure-docker "$Region-docker.pkg.dev" --quiet
    }

Invoke-NativeCommand `
    -StepName "Construyendo imagen Docker..." `
    -Command {
        docker build --tag $ImageUri .
    }

Invoke-NativeCommand `
    -StepName "Publicando imagen en Artifact Registry..." `
    -Command {
        docker push $ImageUri
    }

Write-Host "Imagen publicada correctamente:"
Write-Host $ImageUri