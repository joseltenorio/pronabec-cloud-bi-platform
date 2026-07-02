"""
Pruebas unitarias para validar las opciones y argumentos del pipeline de Dataflow.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pipelines.dataflow_bronze_to_silver import (
    build_pipeline_options,
    parse_arguments,
    resolve_runtime_arguments,
    validate_arguments,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
PYPROJECT_FILE = REPO_ROOT / "pyproject.toml"


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
        "--service-account-email", "test-dataflow-sa@test-project.iam.gserviceaccount.com",
        "--sdk-container-image", "us-central1-docker.pkg.dev/test-project/repo/dataflow-worker:latest",
        "--dry-run",
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
        "--service-account-email", "test-dataflow-sa@test-project.iam.gserviceaccount.com",
        "--sdk-container-image", "us-central1-docker.pkg.dev/test-project/repo/dataflow-worker:latest",
    ]
    args, _ = parse_arguments(argv)
    
    # Debería validar sin excepciones
    validate_arguments(args)


def test_dataflow_runner_requires_service_account_email() -> None:
    """
    Valida que DataflowRunner no use implicitamente la service account default de Compute.
    """
    argv = [
        "--source-system", "pronabec",
        "--source-dataset", "becarios_pais_estudio",
        "--extraction-date", "2026-07-02",
        "--input-path", "gs://bucket/bronze/pronabec/becarios_pais_estudio/data.jsonl",
        "--input-format", "jsonl",
        "--output-table", "test-project:silver.pronabec_becarios_pais_estudio",
        "--runner", "DataflowRunner",
        "--project", "test-project",
        "--region", "us-central1",
        "--temp-location", "gs://bucket/temp",
        "--staging-location", "gs://bucket/staging",
        "--sdk-container-image", "us-central1-docker.pkg.dev/test-project/repo/dataflow-worker:latest",
    ]
    args, _ = parse_arguments(argv)

    with pytest.raises(ValueError) as excinfo:
        validate_arguments(args)

    assert "DATAFLOW_SERVICE_ACCOUNT o --service-account-email" in str(excinfo.value)


def test_dataflow_runner_requires_sdk_container_image() -> None:
    argv = [
        "--source-system", "pronabec",
        "--source-dataset", "becarios_pais_estudio",
        "--extraction-date", "2026-07-02",
        "--input-path", "gs://bucket/bronze/pronabec/becarios_pais_estudio/data.jsonl",
        "--input-format", "jsonl",
        "--output-table", "test-project:silver.pronabec_becarios_pais_estudio",
        "--runner", "DataflowRunner",
        "--project", "test-project",
        "--region", "us-central1",
        "--temp-location", "gs://bucket/temp",
        "--staging-location", "gs://bucket/staging",
        "--service-account-email", "test-dataflow-sa@test-project.iam.gserviceaccount.com",
    ]
    args, _ = parse_arguments(argv)

    with pytest.raises(ValueError) as excinfo:
        validate_arguments(args)

    assert "DATAFLOW_SDK_CONTAINER_IMAGE o --sdk-container-image" in str(excinfo.value)


def test_runtime_arguments_resolve_dataflow_service_account(monkeypatch) -> None:
    monkeypatch.setenv(
        "DATAFLOW_SERVICE_ACCOUNT",
        "test-dataflow-sa@test-project.iam.gserviceaccount.com",
    )

    argv = [
        "--source-system", "pronabec",
        "--source-dataset", "becarios_pais_estudio",
        "--extraction-date", "2026-07-02",
        "--input-path", "gs://bucket/bronze/pronabec/becarios_pais_estudio/data.jsonl",
        "--input-format", "jsonl",
        "--output-table", "test-project:silver.pronabec_becarios_pais_estudio",
        "--runner", "DataflowRunner",
        "--project", "test-project",
        "--region", "us-central1",
        "--temp-location", "gs://bucket/temp",
        "--staging-location", "gs://bucket/staging",
        "--sdk-container-image", "us-central1-docker.pkg.dev/test-project/repo/dataflow-worker:latest",
    ]
    args, _ = parse_arguments(argv)

    resolved = resolve_runtime_arguments(args)

    assert resolved.service_account_email == "test-dataflow-sa@test-project.iam.gserviceaccount.com"
    validate_arguments(resolved)


def test_runtime_arguments_resolve_dataflow_sdk_container_image(monkeypatch) -> None:
    monkeypatch.setenv(
        "DATAFLOW_SDK_CONTAINER_IMAGE",
        "us-central1-docker.pkg.dev/test-project/repo/dataflow-worker:latest",
    )

    argv = [
        "--source-system", "pronabec",
        "--source-dataset", "becarios_pais_estudio",
        "--extraction-date", "2026-07-02",
        "--input-path", "gs://bucket/bronze/pronabec/becarios_pais_estudio/data.jsonl",
        "--input-format", "jsonl",
        "--output-table", "test-project:silver.pronabec_becarios_pais_estudio",
        "--runner", "DirectRunner",
    ]
    args, _ = parse_arguments(argv)

    resolved = resolve_runtime_arguments(args)

    assert resolved.sdk_container_image == "us-central1-docker.pkg.dev/test-project/repo/dataflow-worker:latest"


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
        "--temp-location", "gs://bucket/temp",
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
        "--temp-location", "gs://bucket/temp",
    ]
    args, _ = parse_arguments(argv)
    with pytest.raises(ValueError) as excinfo:
        validate_arguments(args)
        
    assert "El argumento crítico --source-system es requerido." in str(excinfo.value)


def test_build_pipeline_options_propagates_temp_location() -> None:
    """
    Valida que build_pipeline_options propague temp_location en DirectRunner.
    """
    argv = [
        "--source-system", "pronabec_reports",
        "--source-dataset", "report_beca18_universitarios_universidad_anual",
        "--extraction-date", "2026-06-15",
        "--input-path", "tmp/data.csv",
        "--input-format", "csv",
        "--output-table", "test-project:silver.some_table",
        "--runner", "DirectRunner",
        "--temp-location", "gs://test-bucket/temp",
    ]
    args, pipeline_args = parse_arguments(argv)
    options = build_pipeline_options(args, pipeline_args)
    
    # Extraer diccionario de opciones
    opt_dict = options.get_all_options()
    assert opt_dict.get("temp_location") == "gs://test-bucket/temp"


def test_build_pipeline_options_propagates_dataflow_service_account() -> None:
    argv = [
        "--source-system", "pronabec",
        "--source-dataset", "becarios_pais_estudio",
        "--extraction-date", "2026-07-02",
        "--input-path", "gs://bucket/bronze/pronabec/becarios_pais_estudio/data.jsonl",
        "--input-format", "jsonl",
        "--output-table", "test-project:silver.pronabec_becarios_pais_estudio",
        "--runner", "DataflowRunner",
        "--project", "test-project",
        "--region", "us-central1",
        "--temp-location", "gs://bucket/temp",
        "--staging-location", "gs://bucket/staging",
        "--service-account-email", "test-dataflow-sa@test-project.iam.gserviceaccount.com",
        "--sdk-container-image", "us-central1-docker.pkg.dev/test-project/repo/dataflow-worker:latest",
    ]
    args, pipeline_args = parse_arguments(argv)
    options = build_pipeline_options(args, pipeline_args)

    opt_dict = options.get_all_options()
    assert opt_dict.get("service_account_email") == "test-dataflow-sa@test-project.iam.gserviceaccount.com"
    assert opt_dict.get("sdk_container_image") == "us-central1-docker.pkg.dev/test-project/repo/dataflow-worker:latest"
    assert opt_dict.get("setup" + "_file") is None
    assert opt_dict.get("requirements" + "_file") is None
    assert opt_dict.get("save_main_session") is True


def test_dataflow_runner_adds_runner_v2_without_duplicate() -> None:
    argv = [
        "--source-system", "pronabec",
        "--source-dataset", "becarios_pais_estudio",
        "--extraction-date", "2026-07-02",
        "--input-path", "gs://bucket/bronze/pronabec/becarios_pais_estudio/data.jsonl",
        "--input-format", "jsonl",
        "--output-table", "test-project:silver.pronabec_becarios_pais_estudio",
        "--runner", "DataflowRunner",
        "--project", "test-project",
        "--region", "us-central1",
        "--temp-location", "gs://bucket/temp",
        "--staging-location", "gs://bucket/staging",
        "--service-account-email", "test-dataflow-sa@test-project.iam.gserviceaccount.com",
        "--sdk-container-image", "us-central1-docker.pkg.dev/test-project/repo/dataflow-worker:latest",
        "--experiments", "shuffle_mode=service,use_runner_v2",
    ]
    args, pipeline_args = parse_arguments(argv)
    options = build_pipeline_options(args, pipeline_args)

    experiments = options.get_all_options().get("experiments")
    assert experiments.count("use_runner_v2") == 1
    assert "shuffle_mode=service" in experiments


def test_direct_runner_does_not_require_sdk_container_image() -> None:
    argv = [
        "--source-system", "pronabec",
        "--source-dataset", "becarios_pais_estudio",
        "--extraction-date", "2026-07-02",
        "--input-path", "tmp/data.jsonl",
        "--input-format", "jsonl",
        "--output-table", "test-project:silver.pronabec_becarios_pais_estudio",
        "--runner", "DirectRunner",
        "--dry-run",
    ]
    args, _ = parse_arguments(argv)

    validate_arguments(args)


def test_pyproject_packages_pipelines_modules() -> None:
    pyproject = PYPROJECT_FILE.read_text(encoding="utf-8")

    assert '[tool.setuptools.packages.find]' in pyproject
    assert '"pipelines"' in pyproject
    assert '"pipelines.common"' in pyproject
    assert '"pipelines.transforms"' in pyproject


def test_runtime_arguments_resolve_cloud_run_environment(monkeypatch) -> None:
    monkeypatch.setenv("SOURCE_DATASET", "report_dataset")
    monkeypatch.setenv("BRONZE_EXTRACTION_DATE", "2026-06-28")
    monkeypatch.setenv("INPUT_PATH", "gs://bucket/bronze/report_dataset/data.csv")
    monkeypatch.setenv("OUTPUT_TABLE", "project:silver.report_dataset")
    monkeypatch.setenv("PIPELINE_RUN_ID", "manual__2026-06-28")

    argv = [
        "--source-system", "pronabec_reports",
        "--source-dataset", "${SOURCE_DATASET}",
        "--input-path", "${INPUT_PATH}",
        "--input-format", "csv",
        "--output-table", "${OUTPUT_TABLE}",
        "--summary-output-path", "gs://bucket/audit/${SOURCE_DATASET}_${BRONZE_EXTRACTION_DATE}.json",
        "--runner", "DirectRunner",
        "--dry-run",
    ]
    args, _ = parse_arguments(argv)

    resolved = resolve_runtime_arguments(args)

    assert resolved.source_dataset == "report_dataset"
    assert resolved.extraction_date == "2026-06-28"
    assert resolved.input_path == "gs://bucket/bronze/report_dataset/data.csv"
    assert resolved.output_table == "project:silver.report_dataset"
    assert resolved.pipeline_run_id == "manual__2026-06-28"
    assert resolved.summary_output_path == "gs://bucket/audit/report_dataset_2026-06-28.json"
    validate_arguments(resolved)


def test_runtime_arguments_reject_unresolved_placeholder(monkeypatch) -> None:
    monkeypatch.delenv("INPUT_PATH", raising=False)

    args, _ = parse_arguments(
        [
            "--source-system", "pronabec_reports",
            "--source-dataset", "report_dataset",
            "--extraction-date", "2026-06-28",
            "--input-path", "${INPUT_PATH}",
            "--input-format", "csv",
            "--output-table", "project:silver.report_dataset",
            "--runner", "DirectRunner",
            "--dry-run",
        ]
    )

    with pytest.raises(ValueError) as excinfo:
        resolve_runtime_arguments(args)

    assert "Variable de entorno requerida no definida: INPUT_PATH" in str(excinfo.value)
