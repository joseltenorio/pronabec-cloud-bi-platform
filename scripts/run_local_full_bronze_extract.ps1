param(
  [string]$ExtractionDate = "2026-06-14",
  [ValidateSet("Quick", "Full")]
  [string]$Mode = "Quick",
  [string[]]$PronabecDatasets = @(),
  [int]$MaxPages = 5,
  [int]$StartYear = 2012,
  [int]$EndYear = 2026,
  [string]$OutputDir = "tmp",
  [switch]$SkipPronabec,
  [switch]$SkipMef,
  [switch]$Clean
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

if ($StartYear -gt $EndYear) {
  throw "StartYear no puede ser mayor que EndYear."
}

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$PythonExe = Join-Path $RepoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $PythonExe)) {
  $PythonExe = "python"
}

$ResolvedOutputDir = Join-Path $RepoRoot $OutputDir
$BronzeDir = Join-Path $ResolvedOutputDir "bronze"
$ProfilingDir = Join-Path $ResolvedOutputDir "profiling"
$MefSlices = @(
  "presupuesto",
  "presupuesto_hierarchy",
  "presupuesto_producto",
  "presupuesto_generica",
  "presupuesto_fuente",
  "presupuesto_rubro",
  "presupuesto_departamento",
  "presupuesto_temporal",
  "presupuesto_producto_temporal",
  "presupuesto_actividad",
  "presupuesto_actividad_temporal",
  "presupuesto_generica_temporal"
)
$BreakdownSlices = "producto,generica,fuente,rubro,departamento,temporal,producto_temporal,actividad,actividad_temporal,generica_temporal"
$PronabecTimeoutSeconds = "180"

if ($Mode -eq "Quick") {
  $EffectiveStartYear = if ($PSBoundParameters.ContainsKey("StartYear")) { $StartYear } else { 2026 }
  $EffectiveEndYear = if ($PSBoundParameters.ContainsKey("EndYear")) { $EndYear } else { 2026 }
} else {
  $EffectiveStartYear = $StartYear
  $EffectiveEndYear = $EndYear
}

if ($EffectiveStartYear -gt $EffectiveEndYear) {
  throw "StartYear no puede ser mayor que EndYear."
}

function Format-Command {
  param([string[]]$CommandParts)

  return ($CommandParts | ForEach-Object {
      if ($_ -match "\s") {
        '"' + $_ + '"'
      } else {
        $_
      }
    }) -join " "
}

function Invoke-LoggedCommand {
  param([string[]]$CommandParts)

  Write-Host ""
  Write-Host ">> $(Format-Command $CommandParts)"
  & $CommandParts[0] @($CommandParts[1..($CommandParts.Count - 1)])
  if ($LASTEXITCODE -ne 0) {
    throw "Comando fallido con exit code ${LASTEXITCODE}: $(Format-Command $CommandParts)"
  }
}

function Move-MefSliceIntoYearPartition {
  param(
    [string]$SliceName,
    [int]$Year
  )

  $ExtractionDir = Join-Path $BronzeDir "mef\$SliceName\extraction_date=$ExtractionDate"
  $DataPath = Join-Path $ExtractionDir "data.csv"
  $MetadataPath = Join-Path $ExtractionDir "extraction_metadata.json"
  $YearDir = Join-Path $ExtractionDir "year=$Year"
  $DestinationDataPath = Join-Path $YearDir "data.csv"
  $DestinationMetadataPath = Join-Path $YearDir "extraction_metadata.json"

  if ((Test-Path $DestinationDataPath) -and (Test-Path $DestinationMetadataPath)) {
    Write-Host "MEF $SliceName anio $Year already partitioned -> $YearDir"
    return
  }

  if (-not (Test-Path $DataPath)) {
    throw "No se encontro data.csv para MEF slice '$SliceName' anio $Year en $ExtractionDir"
  }
  if (-not (Test-Path $MetadataPath)) {
    throw "No se encontro extraction_metadata.json para MEF slice '$SliceName' anio $Year en $ExtractionDir"
  }

  New-Item -ItemType Directory -Force -Path $YearDir | Out-Null

  if (Test-Path $DestinationDataPath) {
    Remove-Item -LiteralPath $DestinationDataPath -Force
  }
  if (Test-Path $DestinationMetadataPath) {
    Remove-Item -LiteralPath $DestinationMetadataPath -Force
  }

  Move-Item -LiteralPath $DataPath -Destination $DestinationDataPath
  Move-Item -LiteralPath $MetadataPath -Destination $DestinationMetadataPath
  Write-Host "MEF $SliceName anio $Year -> $YearDir"
}

Write-Host "Project Cloud BI Platform - local full Bronze extraction"
Write-Host "ExtractionDate: $ExtractionDate"
Write-Host "Mode: $Mode"
Write-Host "StartYear: $EffectiveStartYear"
Write-Host "EndYear: $EffectiveEndYear"
Write-Host "OutputDir: $OutputDir"
Write-Host "SkipPronabec: $SkipPronabec"
Write-Host "SkipMef: $SkipMef"

if ($Mode -eq "Full") {
  Write-Host "WARNING: Full mode may take several hours because some PRONABEC datasets have thousands of pages."
}

if ($Clean) {
  if ($OutputDir -ne "tmp") {
    throw "Por seguridad, -Clean solo puede usarse con -OutputDir tmp. Solo se permite borrar tmp/bronze y tmp/profiling."
  }

  $AllowedCleanTargets = @($BronzeDir, $ProfilingDir)
  foreach ($Target in $AllowedCleanTargets) {
    Write-Host "Clean target permitido: $Target"
    if (Test-Path $Target) {
      Remove-Item -LiteralPath $Target -Recurse -Force
    }
  }
}

New-Item -ItemType Directory -Force -Path $ResolvedOutputDir | Out-Null

if ($SkipPronabec) {
  Write-Host "PRONABEC extraction skipped."
} elseif ($Mode -eq "Quick") {
  Write-Host "PRONABEC Quick mode: max pages per dataset = $MaxPages"
  $BasePronabecCommand = @(
    $PythonExe,
    "-m",
    "pipelines.extract_pronabec",
    "--extraction-date",
    $ExtractionDate,
    "--timeout",
    $PronabecTimeoutSeconds,
    "--max-pages",
    ([string]$MaxPages),
    "--dry-run",
    "--output-dir",
    $OutputDir
  )

  if ($PronabecDatasets.Count -gt 0) {
    Write-Host "PRONABEC Quick mode: filtering datasets: $($PronabecDatasets -join ', ')"
    foreach ($Dataset in $PronabecDatasets) {
      Invoke-LoggedCommand ($BasePronabecCommand + @("--dataset", $Dataset))
    }
  } else {
    Write-Host "PRONABEC Quick mode: extracting all enabled datasets with a page limit."
    Invoke-LoggedCommand $BasePronabecCommand
  }
} else {
  Write-Host "PRONABEC Full mode: no page limit."
  $BasePronabecCommand = @(
    $PythonExe,
    "-m",
    "pipelines.extract_pronabec",
    "--extraction-date",
    $ExtractionDate,
    "--timeout",
    $PronabecTimeoutSeconds,
    "--dry-run",
    "--output-dir",
    $OutputDir
  )

  if ($PronabecDatasets.Count -gt 0) {
    Write-Host "PRONABEC Full mode: filtering datasets: $($PronabecDatasets -join ', ')"
    foreach ($Dataset in $PronabecDatasets) {
      Invoke-LoggedCommand ($BasePronabecCommand + @("--dataset", $Dataset))
    }
  } else {
    Write-Host "PRONABEC Full mode: extracting all enabled datasets."
    Invoke-LoggedCommand $BasePronabecCommand
  }
}

if ($SkipMef) {
  Write-Host "MEF extraction skipped."
} else {
  foreach ($Year in $EffectiveStartYear..$EffectiveEndYear) {
    Invoke-LoggedCommand @(
      $PythonExe,
      "-m",
      "pipelines.scrape_mef_budget",
      "--consulta-amigable",
      "--extraction-date",
      $ExtractionDate,
      "--start-year",
      ([string]$Year),
      "--end-year",
      ([string]$Year),
      "--include-hierarchy",
      "--include-spending-breakdowns",
      "--breakdown-slices",
      $BreakdownSlices,
      "--dry-run",
      "--output-dir",
      $OutputDir
    )

    foreach ($SliceName in $MefSlices) {
      Move-MefSliceIntoYearPartition -SliceName $SliceName -Year $Year
    }
  }
}

Write-Host ""
Write-Host "Extraccion local Bronze completada."
Write-Host "PRONABEC: $(Join-Path $BronzeDir 'pronabec')"
Write-Host "MEF: $(Join-Path $BronzeDir 'mef')"
