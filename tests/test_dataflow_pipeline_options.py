"""
Pruebas unitarias para validar las opciones y argumentos del pipeline de Dataflow.
"""

from __future__ import annotations

import pytest

from pipelines.dataflow_bronze_to_silver import parse_arguments, validate_arguments


def test_direct_runner_accepts_minimal_local_config() -> None:
    """
    Valida que DirectRunner acepte una configuración local mínima sin requerir GCP options.
    """
    argv = [
        "--source-system", "pronabec_reports",
        "--source-dataset", "report_beca18_universitarios_universidad_anual",
        "--extraction-date", "2026-06-15",
        "--input-path", "tmp/data.csv",
        "--input-format", "csv",
        "--output-table", "test-project:silver.some_table",
        "--runner", "DirectRunner",
        "--dry-run",
    ]
    args, _ = parse_arguments(argv)
    
    # No debería lanzar ninguna excepción al validar
    validate_arguments(args)
    assert args.runner == "DirectRunner"
    assert args.dry_run is True


def test_dataflow_runner_requires_cloud_config() -> None:
    """
    Valida que DataflowRunner exija las opciones de la nube y falle si faltan.
    """
    base_argv = [
        "--source-system", "pronabec_reports",
        "--source-dataset", "report_beca18_universitarios_universidad_anual",
        "--extraction-date", "2026-06-15",
        "--input-path", "tmp/data.csv",
        "--input-format", "csv",
        "--output-table", "test-project:silver.some_table",
        "--runner", "DataflowRunner",
    ]
    
    # Falta project, region, temp_location, staging_location
    args, _ = parse_arguments(base_argv)
    with pytest.raises(ValueError) as excinfo:
        validate_arguments(args)
    
    assert "Los siguientes argumentos son requeridos para DataflowRunner" in str(excinfo.value)
    assert "--project" in str(excinfo.value)
    assert "--region" in str(excinfo.value)
    assert "--temp-location" in str(excinfo.value)
    assert "--staging-location" in str(excinfo.value)


def test_dataflow_runner_passes_with_full_cloud_config() -> None:
    """
    Valida que DataflowRunner pase la validación si se definen todas las opciones GCP.
    """
    argv = [
        "--source-system", "pronabec_reports",
        "--source-dataset", "report_beca18_universitarios_universidad_anual",
        "--extraction-date", "2026-06-15",
        "--input-path", "gs://bucket/data.csv",
        "--input-format", "csv",
        "--output-table", "test-project:silver.some_table",
        "--runner", "DataflowRunner",
        "--project", "test-project",
        "--region", "us-central1",
        "--temp-location", "gs://bucket/temp",
        "--staging-location", "gs://bucket/staging",
    ]
    args, _ = parse_arguments(argv)
    
    # Debería validar sin excepciones
    validate_arguments(args)


def test_invalid_input_format_raises_error() -> None:
    """
    Valida que formatos de entrada no soportados lancen un error claro.
    """
    argv = [
        "--source-system", "pronabec_reports",
        "--source-dataset", "report_beca18_universitarios_universidad_anual",
        "--extraction-date", "2026-06-15",
        "--input-path", "tmp/data.parquet",
        "--input-format", "parquet",
        "--output-table", "test-project:silver.some_table",
        "--runner", "DirectRunner",
    ]
    args, _ = parse_arguments(argv)
    with pytest.raises(ValueError) as excinfo:
        validate_arguments(args)
        
    assert "Formato de entrada inválido: parquet" in str(excinfo.value)


def test_missing_critical_argument_raises_error() -> None:
    """
    Valida que la falta de un argumento crítico lance un error descriptivo.
    """
    # Sin --source-system
    argv = [
        "--source-dataset", "report_beca18_universitarios_universidad_anual",
        "--extraction-date", "2026-06-15",
        "--input-path", "tmp/data.csv",
        "--input-format", "csv",
        "--output-table", "test-project:silver.some_table",
        "--runner", "DirectRunner",
    ]
    args, _ = parse_arguments(argv)
    with pytest.raises(ValueError) as excinfo:
        validate_arguments(args)
        
    assert "El argumento crítico --source-system es requerido." in str(excinfo.value)
