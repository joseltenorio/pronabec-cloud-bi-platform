"""
Pruebas unitarias para la herramienta local de staging de reportes manuales PRONABEC.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tools.stage_pronabec_manual_reports import (
    dataset_name_from_landing_filename,
    expected_reports_for_subset,
    find_gcs_csv,
    resolve_gcs_runtime_options,
    stage_reports_gcs,
    stage_reports,
    validate_source_subset,
)


@pytest.fixture
def mock_input_dir(tmp_path: Path) -> Path:
    """Fixture que crea un directorio temporal de entrada."""
    input_dir = tmp_path / "input_manual"
    input_dir.mkdir()
    return input_dir


@pytest.fixture
def mock_output_dir(tmp_path: Path) -> Path:
    """Fixture que crea un directorio temporal de salida."""
    output_dir = tmp_path / "output_bronze"
    output_dir.mkdir()
    return output_dir


def test_dataset_name_from_landing_filename_removes_pronabec_prefix() -> None:
    assert (
        dataset_name_from_landing_filename("pronabec_report_beca18_sexo_anual.csv")
        == "report_beca18_sexo_anual"
    )


def test_dataset_name_from_landing_filename_handles_universitarios_carrera() -> None:
    assert (
        dataset_name_from_landing_filename(
            "pronabec_report_beca18_universitarios_carrera_anual.csv"
        )
        == "report_beca18_universitarios_carrera_anual"
    )


def test_dataset_name_from_landing_filename_rejects_non_csv() -> None:
    with pytest.raises(ValueError) as excinfo:
        dataset_name_from_landing_filename("_documents/source.pdf")

    assert "no es un CSV de datos" in str(excinfo.value)


def test_expected_reports_for_subset_returns_registered_reports() -> None:
    reports = expected_reports_for_subset("beca18_universitarios_2012_2026")

    assert reports == [
        "report_beca18_universitarios_universidad_anual",
        "report_beca18_universitarios_carrera_anual",
    ]


def test_validate_source_subset_rejects_unknown_subset() -> None:
    with pytest.raises(ValueError) as excinfo:
        validate_source_subset("unknown_subset")

    assert "source_subset no soportado" in str(excinfo.value)


def test_resolve_gcs_runtime_options_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GCS_BUCKET_NAME", "bucket")
    monkeypatch.setenv("PRONABEC_REPORTS_LANDING_PREFIX", "landing/pronabec_reports")
    monkeypatch.setenv("PRONABEC_REPORTS_BRONZE_PREFIX", "bronze/pronabec_reports")
    monkeypatch.setenv("SOURCE_SUBSET", "pes_2025")
    monkeypatch.setenv("BRONZE_EXTRACTION_DATE", "2026-06-26")

    input_uri, output_uri, extraction_date, source_subset = resolve_gcs_runtime_options(
        input_uri=None,
        output_uri=None,
        extraction_date=None,
        source_subset=None,
    )

    assert input_uri == "gs://bucket/landing/pronabec_reports/pes_2025"
    assert output_uri == "gs://bucket/bronze/pronabec_reports"
    assert extraction_date == "2026-06-26"
    assert source_subset == "pes_2025"


def test_resolve_gcs_runtime_options_uses_gcs_bucket_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GCS_BUCKET_NAME", raising=False)
    monkeypatch.setenv("GCS_BUCKET", "fallback-bucket")
    monkeypatch.setenv("SOURCE_SUBSET", "pes_2025")
    monkeypatch.setenv("BRONZE_EXTRACTION_DATE", "2026-06-26")

    input_uri, output_uri, _, _ = resolve_gcs_runtime_options(
        input_uri=None,
        output_uri=None,
        extraction_date=None,
        source_subset=None,
    )

    assert input_uri == "gs://fallback-bucket/landing/pronabec_reports/pes_2025"
    assert output_uri == "gs://fallback-bucket/bronze/pronabec_reports"


def test_resolve_gcs_runtime_options_rejects_missing_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in (
        "GCS_BUCKET_NAME",
        "GCS_BUCKET",
        "SOURCE_SUBSET",
        "BRONZE_EXTRACTION_DATE",
    ):
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(ValueError) as excinfo:
        resolve_gcs_runtime_options(
            input_uri=None,
            output_uri=None,
            extraction_date=None,
            source_subset=None,
        )

    assert (
        "Modo GCS por entorno requiere GCS_BUCKET_NAME, SOURCE_SUBSET y BRONZE_EXTRACTION_DATE"
        in str(excinfo.value)
    )


def test_resolve_gcs_runtime_options_rejects_missing_bucket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GCS_BUCKET_NAME", raising=False)
    monkeypatch.delenv("GCS_BUCKET", raising=False)
    monkeypatch.setenv("SOURCE_SUBSET", "pes_2025")
    monkeypatch.setenv("BRONZE_EXTRACTION_DATE", "2026-06-26")

    with pytest.raises(ValueError) as excinfo:
        resolve_gcs_runtime_options(None, None, None, None)

    assert "GCS_BUCKET_NAME" in str(excinfo.value)


def test_resolve_gcs_runtime_options_rejects_missing_source_subset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GCS_BUCKET_NAME", "bucket")
    monkeypatch.delenv("SOURCE_SUBSET", raising=False)
    monkeypatch.setenv("BRONZE_EXTRACTION_DATE", "2026-06-26")

    with pytest.raises(ValueError) as excinfo:
        resolve_gcs_runtime_options(None, None, None, None)

    assert "SOURCE_SUBSET" in str(excinfo.value)


def test_resolve_gcs_runtime_options_rejects_missing_extraction_date(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GCS_BUCKET_NAME", "bucket")
    monkeypatch.setenv("SOURCE_SUBSET", "pes_2025")
    monkeypatch.delenv("BRONZE_EXTRACTION_DATE", raising=False)

    with pytest.raises(ValueError) as excinfo:
        resolve_gcs_runtime_options(None, None, None, None)

    assert "BRONZE_EXTRACTION_DATE" in str(excinfo.value)


def test_resolve_gcs_runtime_options_keeps_explicit_gcs_args(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in ("GCS_BUCKET_NAME", "GCS_BUCKET"):
        monkeypatch.delenv(name, raising=False)

    resolved = resolve_gcs_runtime_options(
        input_uri="gs://bucket/landing/pronabec_reports/pes_2025",
        output_uri="gs://bucket/bronze/pronabec_reports",
        extraction_date="2026-06-26",
        source_subset="pes_2025",
    )

    assert resolved == (
        "gs://bucket/landing/pronabec_reports/pes_2025",
        "gs://bucket/bronze/pronabec_reports",
        "2026-06-26",
        "pes_2025",
    )


def test_stage_university_report_success(mock_input_dir: Path, mock_output_dir: Path) -> None:
    """
    Valida el staging exitoso de report_beca18_universitarios_universidad_anual.
    """
    # Crear archivo CSV manual de prueba
    csv_content = (
        "Universidad,2012,2013,2026 (*),Total\n"
        "UNIVERSIDAD NACIONAL,1,2,3,6\n"
        "Total general,1,2,3,6\n"
    )
    source_file = mock_input_dir / "beca18_becarios_universidad_2012_2026.csv"
    source_file.write_text(csv_content, encoding="utf-8")

    staged, skipped, missing = stage_reports(
        input_dir=str(mock_input_dir),
        output_dir=str(mock_output_dir),
        extraction_date="2026-06-15",
        report_name="report_beca18_universitarios_universidad_anual",
    )

    assert staged == 1
    assert skipped == 0
    assert missing == 0

    # Validar que los archivos de salida existan
    target_dir = (
        mock_output_dir
        / "report_beca18_universitarios_universidad_anual"
        / "extraction_date=2026-06-15"
    )
    target_csv = target_dir / "data.csv"
    target_meta = target_dir / "extraction_metadata.json"

    assert target_csv.exists()
    assert target_meta.exists()

    # Validar contenido mapeado al contrato Bronze
    import csv
    with open(target_csv, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        headers = next(reader)
        rows = list(reader)

    # Validar cabeceras esperadas según esquema Bronze (formato ancho + metadata)
    assert "universidad" in headers
    assert "anio_2012" in headers
    assert "anio_2013" in headers
    assert "anio_2026_preliminar" in headers
    assert "total" in headers
    assert "source_document_file" in headers
    assert "source_document_title" in headers
    assert "extraction_method" in headers

    # Validar que se conserva el formato ancho y el número de filas (no se hace unpivot)
    assert len(rows) == 2

    # Validar valores preservados
    first_row = dict(zip(headers, rows[0]))
    assert first_row["universidad"] == "UNIVERSIDAD NACIONAL"
    assert first_row["anio_2012"] == "1"
    assert first_row["anio_2013"] == "2"
    assert first_row["anio_2026_preliminar"] == "3"
    assert first_row["total"] == "6"

    # Validar metadata documental presente por fila
    assert first_row["source_document_file"] == "8170922-beca-18-cantidad-de-becarios-segun-universidad-de-estudio-2012-2026.pdf"
    assert first_row["source_document_title"] == "Beca 18 - cantidad de becarios según universidad de estudio 2012-2026"
    assert first_row["extraction_method"] == "manual_csv"

    # Validar metadatos en extraction_metadata.json
    with open(target_meta, "r", encoding="utf-8") as f:
        metadata = json.load(f)

    assert metadata["source_system"] == "pronabec_reports"
    assert metadata["source_dataset"] == "report_beca18_universitarios_universidad_anual"
    assert metadata["source_file_name"] == "beca18_becarios_universidad_2012_2026.csv"
    assert metadata["bronze_file_name"] == "data.csv"
    assert metadata["extraction_date"] == "2026-06-15"
    assert metadata["extraction_method"] == "manual_csv"
    assert metadata["row_count"] == 2


def test_stage_career_report_success_recursive(mock_input_dir: Path, mock_output_dir: Path) -> None:
    """
    Valida el staging exitoso de report_beca18_universitarios_carrera_anual
    ubicado en una subcarpeta recursiva.
    """
    # Crear estructura de subcarpeta y archivo
    sub_dir = mock_input_dir / "subfolder" / "beca18"
    sub_dir.mkdir(parents=True)
    
    csv_content = (
        "Carrera de Estudio,2012,2026 (*),Total\n"
        "INGENIERÍA DE SISTEMAS,-,5,5\n"
        "Total general,0,5,5\n"
    )
    source_file = sub_dir / "beca18_becarios_universidades_carrera_2012_2026.csv"
    source_file.write_text(csv_content, encoding="utf-8")

    staged, skipped, missing = stage_reports(
        input_dir=str(mock_input_dir),
        output_dir=str(mock_output_dir),
        extraction_date="2026-06-15",
        report_name="report_beca18_universitarios_carrera_anual",
    )

    assert staged == 1
    assert skipped == 0
    assert missing == 0

    target_dir = (
        mock_output_dir
        / "report_beca18_universitarios_carrera_anual"
        / "extraction_date=2026-06-15"
    )
    target_csv = target_dir / "data.csv"
    assert target_csv.exists()

    # Validar contenido mapeado al contrato Bronze
    import csv
    with open(target_csv, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        headers = next(reader)
        rows = list(reader)

    # Validar estructura y preservación de formato ancho
    assert "carrera_estudio" in headers
    assert "anio_2012" in headers
    assert "anio_2026_preliminar" in headers
    assert "total" in headers
    assert "source_document_file" in headers

    assert len(rows) == 2

    first_row = dict(zip(headers, rows[0]))
    assert first_row["carrera_estudio"] == "INGENIERÍA DE SISTEMAS"
    assert first_row["anio_2012"] == "-"
    assert first_row["anio_2026_preliminar"] == "5"
    assert first_row["total"] == "5"
    assert first_row["source_document_file"] == "8170922-beca-18-cantidad-de-becarios-en-universidades-segun-carrera-de-estudio-2012-2026.pdf"


def test_stage_all_reports_some_missing(mock_input_dir: Path, mock_output_dir: Path) -> None:
    """
    Valida que al ejecutar sin report_name no falle todo si falta un archivo manual,
    sino que stagee los que sí existan y marque los faltantes como missing.
    """
    # Solo creamos el de universidad
    csv_content = "universidad,total\nVal1,Val2"
    source_file = mock_input_dir / "beca18_becarios_universidad_2012_2026.csv"
    source_file.write_text(csv_content, encoding="utf-8")

    from tools.stage_pronabec_manual_reports import MANUAL_REPORT_SOURCES

    staged, skipped, missing = stage_reports(
        input_dir=str(mock_input_dir),
        output_dir=str(mock_output_dir),
        extraction_date="2026-06-15",
    )

    assert staged == 1
    assert skipped == 0
    assert missing == len(MANUAL_REPORT_SOURCES) - 1


def test_report_name_filter(mock_input_dir: Path, mock_output_dir: Path) -> None:
    """
    Valida que al especificar --report-name solo se stagee ese reporte específico,
    incluso si el otro archivo fuente también existe en el input_dir.
    """
    # Crear ambos archivos manuales
    csv_content_uni = "universidad,total\nVal1,Val2"
    csv_content_car = "carrera de estudio,total\nVal1,Val2"
    (mock_input_dir / "beca18_becarios_universidad_2012_2026.csv").write_text(csv_content_uni)
    (mock_input_dir / "beca18_becarios_universidades_carrera_2012_2026.csv").write_text(csv_content_car)

    staged, skipped, missing = stage_reports(
        input_dir=str(mock_input_dir),
        output_dir=str(mock_output_dir),
        extraction_date="2026-06-15",
        report_name="report_beca18_universitarios_universidad_anual",
    )

    assert staged == 1
    # Solo el de universidad debe haberse creado físicamente
    assert (
        mock_output_dir
        / "report_beca18_universitarios_universidad_anual"
        / "extraction_date=2026-06-15"
        / "data.csv"
    ).exists()
    assert not (
        mock_output_dir
        / "report_beca18_universitarios_carrera_anual"
    ).exists()


def test_explicit_report_missing_file_raises_error(mock_input_dir: Path, mock_output_dir: Path) -> None:
    """
    Valida que se lance FileNotFoundError si se solicita un reporte explícito
    pero su archivo manual no existe en input_dir.
    """
    with pytest.raises(FileNotFoundError) as excinfo:
        stage_reports(
            input_dir=str(mock_input_dir),
            output_dir=str(mock_output_dir),
            extraction_date="2026-06-15",
            report_name="report_beca18_universitarios_universidad_anual",
        )
    assert "No se encontró el archivo fuente para el reporte" in str(excinfo.value)


def test_overwrite_modes(mock_input_dir: Path, mock_output_dir: Path) -> None:
    """
    Valida que falle si ya existe el archivo destino y no se habilita overwrite,
    y que reemplace correctamente si se habilita.
    """
    csv_content = "universidad,total\nVal1,Val2"
    source_file = mock_input_dir / "beca18_becarios_universidad_2012_2026.csv"
    source_file.write_text(csv_content)

    # 1. Primera ejecución exitosa
    staged, skipped, missing = stage_reports(
        input_dir=str(mock_input_dir),
        output_dir=str(mock_output_dir),
        extraction_date="2026-06-15",
        report_name="report_beca18_universitarios_universidad_anual",
    )
    assert staged == 1

    # 2. Segunda ejecución sin overwrite debe saltarlo (o fallar si es explícito con report_name)
    with pytest.raises(FileExistsError) as excinfo:
        stage_reports(
            input_dir=str(mock_input_dir),
            output_dir=str(mock_output_dir),
            extraction_date="2026-06-15",
            report_name="report_beca18_universitarios_universidad_anual",
            overwrite=False,
        )
    assert "ya existe y no se especificó --overwrite" in str(excinfo.value)

    # Si es ejecución general, incrementa skipped
    staged_gen, skipped_gen, missing_gen = stage_reports(
        input_dir=str(mock_input_dir),
        output_dir=str(mock_output_dir),
        extraction_date="2026-06-15",
        overwrite=False,
    )
    assert staged_gen == 0
    assert skipped_gen == 1

    # 3. Segunda ejecución con overwrite=True debe sobreescribir con éxito
    staged_ov, skipped_ov, missing_ov = stage_reports(
        input_dir=str(mock_input_dir),
        output_dir=str(mock_output_dir),
        extraction_date="2026-06-15",
        report_name="report_beca18_universitarios_universidad_anual",
        overwrite=True,
    )
    assert staged_ov == 1
    assert skipped_ov == 0


def test_invalid_extraction_date_format(mock_input_dir: Path, mock_output_dir: Path) -> None:
    """Valida error si el formato de la fecha es inválido."""
    with pytest.raises(ValueError) as excinfo:
        stage_reports(
            input_dir=str(mock_input_dir),
            output_dir=str(mock_output_dir),
            extraction_date="2026/06/15",
        )
    assert "Formato de fecha inválido" in str(excinfo.value)


def test_non_existent_input_dir(mock_output_dir: Path) -> None:
    """Valida error si el directorio de entrada no existe."""
    with pytest.raises(FileNotFoundError) as excinfo:
        stage_reports(
            input_dir="non_existent_path_dir_12345",
            output_dir=str(mock_output_dir),
            extraction_date="2026-06-15",
        )
    assert "El directorio de entrada no existe" in str(excinfo.value)


def test_unknown_report_name(mock_input_dir: Path, mock_output_dir: Path) -> None:
    """Valida error si se pasa un report_name desconocido."""
    with pytest.raises(ValueError) as excinfo:
        stage_reports(
            input_dir=str(mock_input_dir),
            output_dir=str(mock_output_dir),
            extraction_date="2026-06-15",
            report_name="unknown_report_dataset_name",
        )
    assert "Reporte desconocido o no soportado" in str(excinfo.value)


def test_empty_csv_raises_error(mock_input_dir: Path, mock_output_dir: Path) -> None:
    """Valida error si el archivo CSV de origen está completamente vacío."""
    source_file = mock_input_dir / "beca18_becarios_universidad_2012_2026.csv"
    source_file.write_text("")  # Archivo vacío

    with pytest.raises(ValueError) as excinfo:
        stage_reports(
            input_dir=str(mock_input_dir),
            output_dir=str(mock_output_dir),
            extraction_date="2026-06-15",
            report_name="report_beca18_universitarios_universidad_anual",
        )
    assert "El archivo CSV está vacío" in str(excinfo.value)


def test_stage_pes_2025_metadata_and_mapping(mock_input_dir: Path, mock_output_dir: Path) -> None:
    """
    Valida el staging de un reporte de PES 2025 con metadatos específicos.
    """
    csv_content = "\ufeffAño,Modalidad,Becas\n2025,Beca 18,150\n"
    source_file = mock_input_dir / "pronabec_report_beca18_becas_otorgadas_modalidad_anual.csv"
    source_file.write_text(csv_content, encoding="utf-8-sig")

    # Copiar un PDF simulado
    pdf_file = mock_input_dir / "7219175-panorama-de-estudios-sociales-pronabec.pdf"
    pdf_file.write_text("dummy PDF")

    staged, skipped, missing = stage_reports(
        input_dir=str(mock_input_dir),
        output_dir=str(mock_output_dir),
        extraction_date="2026-06-15",
        report_name="report_beca18_becas_otorgadas_modalidad_anual",
        strict=True,
    )

    assert staged == 1
    assert skipped == 0
    assert missing == 0

    target_dir = (
        mock_output_dir
        / "report_beca18_becas_otorgadas_modalidad_anual"
        / "extraction_date=2026-06-15"
    )
    target_csv = target_dir / "data.csv"
    target_meta = target_dir / "extraction_metadata.json"

    assert target_csv.exists()
    assert target_meta.exists()

    # Validar conservación de BOM y contenido
    assert target_csv.read_text(encoding="utf-8-sig") == csv_content

    # Validar metadatos requeridos de PES 2025
    with open(target_meta, "r", encoding="utf-8") as f:
        meta = json.load(f)

    assert meta["source_system"] == "pronabec_reports"
    assert meta["source_dataset"] == "report_beca18_becas_otorgadas_modalidad_anual"
    assert meta["source_subset"] == "pes_2025"
    assert meta["source_document_file"] == "7219175-panorama-de-estudios-sociales-pronabec.pdf"
    assert meta["records_read"] == 1
    assert meta["records_written"] == 1
    assert "file_sha256" in meta
    assert "ingestion_timestamp" in meta


def test_stage_gcs_report_success(monkeypatch: pytest.MonkeyPatch) -> None:
    source_uri = (
        "gs://bucket/landing/pronabec_reports/pes_2025/"
        "pronabec_report_beca18_sexo_anual.csv"
    )
    written: dict[str, bytes | str] = {}

    monkeypatch.setattr(
        "tools.stage_pronabec_manual_reports.list_gcs_objects",
        lambda uri: [source_uri],
    )
    monkeypatch.setattr(
        "tools.stage_pronabec_manual_reports.read_gcs_bytes",
        lambda uri: b"anio,sexo,becarios\n2025,F,10\n",
    )
    monkeypatch.setattr(
        "tools.stage_pronabec_manual_reports.write_gcs_bytes",
        lambda uri, content, content_type=None: written.update({uri: content}),
    )
    monkeypatch.setattr(
        "tools.stage_pronabec_manual_reports.write_gcs_text",
        lambda uri, content: written.update({uri: content}),
    )

    staged, skipped, missing = stage_reports_gcs(
        input_uri="gs://bucket/landing/pronabec_reports/pes_2025",
        output_uri="gs://bucket/bronze/pronabec_reports",
        extraction_date="2026-06-26",
        report_name="report_beca18_sexo_anual",
        source_subset="pes_2025",
        strict=True,
    )

    target_csv_uri = (
        "gs://bucket/bronze/pronabec_reports/report_beca18_sexo_anual/"
        "extraction_date=2026-06-26/data.csv"
    )
    target_meta_uri = (
        "gs://bucket/bronze/pronabec_reports/report_beca18_sexo_anual/"
        "extraction_date=2026-06-26/extraction_metadata.json"
    )

    assert (staged, skipped, missing) == (1, 0, 0)
    assert written[target_csv_uri] == b"anio,sexo,becarios\n2025,F,10\n"

    metadata = json.loads(written[target_meta_uri])
    assert metadata["source_system"] == "pronabec_reports"
    assert metadata["source_subset"] == "pes_2025"
    assert metadata["source_dataset"] == "report_beca18_sexo_anual"
    assert metadata["extraction_date"] == "2026-06-26"
    assert metadata["source_method"] == "manual_landing_csv"
    assert metadata["landing_uri"] == source_uri
    assert metadata["row_count"] == 1
    assert "sha256" in metadata


def test_find_gcs_csv_ignores_documents_and_pdfs(monkeypatch: pytest.MonkeyPatch) -> None:
    objects = [
        "gs://bucket/landing/pronabec_reports/pes_2025/_documents/source.pdf",
        "gs://bucket/landing/pronabec_reports/pes_2025/report_beca18_sexo_anual.pdf",
        "gs://bucket/landing/pronabec_reports/pes_2025/pronabec_report_beca18_sexo_anual.csv",
    ]
    monkeypatch.setattr(
        "tools.stage_pronabec_manual_reports.list_gcs_objects",
        lambda uri: objects,
    )

    source_uri = find_gcs_csv(
        "gs://bucket/landing/pronabec_reports/pes_2025",
        ["pronabec_report_beca18_sexo_anual.csv"],
    )

    assert source_uri == (
        "gs://bucket/landing/pronabec_reports/pes_2025/"
        "pronabec_report_beca18_sexo_anual.csv"
    )


def test_stage_gcs_rejects_unknown_subset() -> None:
    with pytest.raises(ValueError) as excinfo:
        stage_reports_gcs(
            input_uri="gs://bucket/landing/pronabec_reports/unknown",
            output_uri="gs://bucket/bronze/pronabec_reports",
            extraction_date="2026-06-26",
            source_subset="unknown",
        )

    assert "source_subset no soportado" in str(excinfo.value)


def test_stage_gcs_strict_raises_when_expected_csv_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "tools.stage_pronabec_manual_reports.list_gcs_objects",
        lambda uri: [
            "gs://bucket/landing/pronabec_reports/pes_2025/_documents/source.pdf",
        ],
    )

    with pytest.raises(FileNotFoundError) as excinfo:
        stage_reports_gcs(
            input_uri="gs://bucket/landing/pronabec_reports/pes_2025",
            output_uri="gs://bucket/bronze/pronabec_reports",
            extraction_date="2026-06-26",
            report_name="report_beca18_sexo_anual",
            source_subset="pes_2025",
            strict=True,
        )

    assert "No se encontró el CSV fuente" in str(excinfo.value)


def test_stage_gcs_uses_tempfile_without_repo_tmp(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source_uri = (
        "gs://bucket/landing/pronabec_reports/pes_2025/"
        "pronabec_report_beca18_sexo_anual.csv"
    )
    temp_calls: list[dict[str, object]] = []

    class FakeTemporaryDirectory:
        def __init__(self, *args: object, **kwargs: object) -> None:
            temp_calls.append({"args": args, "kwargs": kwargs})
            self.path = tmp_path / "cloud_temp"

        def __enter__(self) -> str:
            self.path.mkdir()
            return str(self.path)

        def __exit__(self, *args: object) -> None:
            return None

    monkeypatch.setattr(
        "tools.stage_pronabec_manual_reports.tempfile.TemporaryDirectory",
        FakeTemporaryDirectory,
    )
    monkeypatch.setattr(
        "tools.stage_pronabec_manual_reports.list_gcs_objects",
        lambda uri: [source_uri] if uri.endswith("pes_2025") else [],
    )
    monkeypatch.setattr(
        "tools.stage_pronabec_manual_reports.read_gcs_bytes",
        lambda uri: b"anio,sexo,becarios\n2025,F,10\n",
    )
    monkeypatch.setattr(
        "tools.stage_pronabec_manual_reports.write_gcs_bytes",
        lambda uri, content, content_type=None: None,
    )
    monkeypatch.setattr(
        "tools.stage_pronabec_manual_reports.write_gcs_text",
        lambda uri, content: None,
    )

    staged, _, _ = stage_reports_gcs(
        input_uri="gs://bucket/landing/pronabec_reports/pes_2025",
        output_uri="gs://bucket/bronze/pronabec_reports",
        extraction_date="2026-06-26",
        report_name="report_beca18_sexo_anual",
        source_subset="pes_2025",
        strict=True,
        overwrite=True,
    )

    assert staged == 1
    assert temp_calls
    assert temp_calls[0]["kwargs"].get("prefix") == "stage_pronabec_reports_gcs_"
    assert "dir" not in temp_calls[0]["kwargs"]


def test_deploy_script_stage_reports_job_avoids_shell_literals() -> None:
    script = Path("scripts/deploy_cloud_run_jobs.ps1").read_text(encoding="utf-8")
    start = script.index("-JobName $PronabecReportsStageJobName")
    end = script.index("Upsert-CloudRunJob `", start + 1)
    stage_job_block = script[start:end]

    assert "`${SOURCE_SUBSET}" not in stage_job_block
    assert "`${BRONZE_EXTRACTION_DATE}" not in stage_job_block
    assert "--overwrite" in stage_job_block
    assert "GCS_BUCKET_NAME" in script
    assert "PRONABEC_REPORTS_LANDING_PREFIX" in script
    assert "PRONABEC_REPORTS_BRONZE_PREFIX" in script


def test_stage_pes_2025_strict_raises_error_if_any_missing(mock_input_dir: Path, mock_output_dir: Path) -> None:
    """
    Valida que al ejecutarse en modo estricto para pes_2025, si falta algún CSV obligatorio,
    se lance FileNotFoundError.
    """
    # Solo creamos uno de los 21
    source_file = mock_input_dir / "pronabec_report_beca18_becas_otorgadas_modalidad_anual.csv"
    source_file.write_text("Año,Modalidad,Becas\n2025,Beca 18,100\n", encoding="utf-8")

    with pytest.raises(FileNotFoundError) as excinfo:
        stage_reports(
            input_dir=str(mock_input_dir),
            output_dir=str(mock_output_dir),
            extraction_date="2026-06-15",
            source_subset="pes_2025",
            strict=True,
        )
    assert "No se encontró el archivo fuente para el reporte" in str(excinfo.value)


def test_stage_pes_2025_non_strict_tolerates_missing(mock_input_dir: Path, mock_output_dir: Path) -> None:
    """
    Valida que en modo no estricto para pes_2025, tolere archivos faltantes incrementando missing_count.
    """
    # Solo creamos uno de los 21
    source_file = mock_input_dir / "pronabec_report_beca18_becas_otorgadas_modalidad_anual.csv"
    source_file.write_text("Año,Modalidad,Becas\n2025,Beca 18,100\n", encoding="utf-8")

    staged, skipped, missing = stage_reports(
        input_dir=str(mock_input_dir),
        output_dir=str(mock_output_dir),
        extraction_date="2026-06-15",
        source_subset="pes_2025",
        strict=False,
    )

    assert staged == 1
    assert missing == 20  # Los otros 20 faltan y se omiten sin fallar


def test_stage_all_reports_success_joint(mock_input_dir: Path, mock_output_dir: Path) -> None:
    """
    Valida que al ejecutar sin filtros (sin report_name ni source_subset),
    cuando todos los archivos fuente de ambas subfamilias (PES 2025 y universitarios)
    están presentes, se procesan los 23 reportes completos con éxito.
    """
    from tools.stage_pronabec_manual_reports import MANUAL_REPORT_SOURCES

    # Crear mock files para cada uno de los reportes en el registry
    for report_id, config in MANUAL_REPORT_SOURCES.items():
        if "source_subset" in config and config["source_subset"] == "pes_2025":
            # Los PES 2025 ya contienen metadatos en su CSV de origen
            csv_content = (
                "modalidad,ano_convocatoria,becas_otorgadas,source_document_file,"
                "source_document_title,source_page,source_figure,extraction_method\n"
                "ORDINARIA,2012,3973,doc.pdf,title,7,1,manual_csv\n"
            )
        else:
            # Los universitarios se reescriben a Bronze con metadatos
            csv_content = (
                "Universidad,2012,Total\n"
                "UNIVERSIDAD NACIONAL,1,1\n"
            )

        file_path = mock_input_dir / config["filename"]
        file_path.write_text(csv_content, encoding="utf-8")

    staged, skipped, missing = stage_reports(
        input_dir=str(mock_input_dir),
        output_dir=str(mock_output_dir),
        extraction_date="2026-06-17",
        strict=True,
    )

    assert staged == len(MANUAL_REPORT_SOURCES)
    assert skipped == 0
    assert missing == 0

    # Validar que los 23 datasets tengan sus directorios y archivos generados
    for report_id in MANUAL_REPORT_SOURCES:
        path = mock_output_dir / report_id / "extraction_date=2026-06-17"
        assert (path / "data.csv").exists()
        assert (path / "extraction_metadata.json").exists()


def test_stage_subset_filtering(mock_input_dir: Path, mock_output_dir: Path) -> None:
    """
    Valida que al pasar --source-subset se filtren adecuadamente los reportes.
    - pes_2025 debe stagear 21 reportes.
    - beca18_universitarios_2012_2026 debe stagear 2 reportes.
    """
    from tools.stage_pronabec_manual_reports import MANUAL_REPORT_SOURCES

    # Crear mock files para todos los 23 reportes registrados
    for report_id, config in MANUAL_REPORT_SOURCES.items():
        if "source_subset" in config and config["source_subset"] == "pes_2025":
            csv_content = (
                "modalidad,ano_convocatoria,becas_otorgadas,source_document_file,"
                "source_document_title,source_page,source_figure,extraction_method\n"
                "ORDINARIA,2012,3973,doc.pdf,title,7,1,manual_csv\n"
            )
        else:
            csv_content = (
                "Universidad,2012,Total\n"
                "UNIVERSIDAD NACIONAL,1,1\n"
            )
        file_path = mock_input_dir / config["filename"]
        file_path.write_text(csv_content, encoding="utf-8")

    # 1. Probar filtrado por pes_2025
    out_pes = mock_output_dir / "pes_2025"
    out_pes.mkdir()
    staged_pes, skipped_pes, missing_pes = stage_reports(
        input_dir=str(mock_input_dir),
        output_dir=str(out_pes),
        extraction_date="2026-06-17",
        source_subset="pes_2025",
        strict=True,
    )
    assert staged_pes == 21
    assert skipped_pes == 0
    assert missing_pes == 0

    # 2. Probar filtrado por beca18_universitarios_2012_2026
    out_uni = mock_output_dir / "uni"
    out_uni.mkdir()
    staged_uni, skipped_uni, missing_uni = stage_reports(
        input_dir=str(mock_input_dir),
        output_dir=str(out_uni),
        extraction_date="2026-06-17",
        source_subset="beca18_universitarios_2012_2026",
        strict=True,
    )
    assert staged_uni == 2
    assert skipped_uni == 0
    assert missing_uni == 0



