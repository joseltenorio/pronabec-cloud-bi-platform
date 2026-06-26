"""
Herramienta de staging para preparar archivos CSV manuales de PRONABEC en layout Bronze local.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipelines.common.gcs import (
    join_gcs_uri,
    list_gcs_objects,
    read_gcs_bytes,
    write_gcs_bytes,
    write_gcs_text,
)

SUPPORTED_SOURCE_SUBSETS = (
    "pes_2025",
    "beca18_universitarios_2012_2026",
)
DEFAULT_REPORTS_LANDING_PREFIX = "landing/pronabec_reports"
DEFAULT_REPORTS_BRONZE_PREFIX = "bronze/pronabec_reports"

# Lista de los 21 reportes del Panorama de Estudios Sociales (PES 2025)
PES_2025_REPORTS = [
    "report_beca18_autoidentificacion_etnica_modalidad_2025",
    "report_beca18_becas_otorgadas_modalidad_anual",
    "report_beca18_colegio_gestion_2025",
    "report_beca18_enp_promedio_caracteristica_2025",
    "report_beca18_enp_promedio_region_2025",
    "report_beca18_lengua_materna_modalidad_2025",
    "report_beca18_migracion_region_acumulada",
    "report_beca18_migracion_region_anual",
    "report_beca18_no_continuaria_sin_beca_caracteristica_2025",
    "report_beca18_padres_nivel_educativo_2025",
    "report_beca18_periodo_ingreso_ies_genero_2025",
    "report_beca18_preparacion_ies_meses_caracteristica_2025",
    "report_beca18_preparacion_ies_tipo_2025",
    "report_beca18_primera_generacion_region",
    "report_beca18_razones_eleccion_carrera_gestion_ies_2025",
    "report_beca18_razones_eleccion_carrera_sexo_2025",
    "report_beca18_razones_eleccion_ies_gestion_2025",
    "report_beca18_region_postulacion_2025",
    "report_beca18_region_postulacion_acumulada",
    "report_beca18_region_postulacion_anual",
    "report_beca18_sexo_anual",
]

# Mapeo de reportes manuales soportados
MANUAL_REPORT_SOURCES = {
    "report_beca18_universitarios_universidad_anual": {
        "filename": "beca18_becarios_universidad_2012_2026.csv",
        "alternative_filenames": [
            "pronabec_report_beca18_universitarios_universidad_anual.csv"
        ],
        "source_document_title": "Beca 18 - cantidad de becarios según universidad de estudio 2012-2026",
        "source_publication_url": "https://www.gob.pe/institucion/pronabec/informes-publicaciones/8170922-beca-18-cantidad-de-becarios-universitarios",
        "extraction_method": "manual_csv",
        "source_document_file": "8170922-beca-18-cantidad-de-becarios-segun-universidad-de-estudio-2012-2026.pdf",
        "source_page": "1",
        "source_table": "1",
        "source_subset": "beca18_universitarios_2012_2026",
    },
    "report_beca18_universitarios_carrera_anual": {
        "filename": "beca18_becarios_universidades_carrera_2012_2026.csv",
        "alternative_filenames": [
            "pronabec_report_beca18_universitarios_carrera_anual.csv"
        ],
        "source_document_title": "Beca 18 - cantidad de becarios en universidades según carrera de estudio 2012-2026",
        "source_publication_url": "https://www.gob.pe/institucion/pronabec/informes-publicaciones/8170922-beca-18-cantidad-de-becarios-universitarios",
        "extraction_method": "manual_csv",
        "source_document_file": "8170922-beca-18-cantidad-de-becarios-en-universidades-segun-carrera-de-estudio-2012-2026.pdf",
        "source_page": "1",
        "source_table": "1",
        "source_subset": "beca18_universitarios_2012_2026",
    },
}

# Agregar dinámicamente los reportes PES 2025 al mapeo
for report_id in PES_2025_REPORTS:
    MANUAL_REPORT_SOURCES[report_id] = {
        "filename": f"pronabec_{report_id}.csv",
        "alternative_filenames": [
            f"{report_id}.csv"
        ],
        "source_document_title": f"Panorama de Estudios Sociales - Beca 18 ({report_id})",
        "source_document_file": "7219175-panorama-de-estudios-sociales-pronabec.pdf",
        "source_publication_url": None,
        "extraction_method": "manual_csv",
        "source_subset": "pes_2025",
    }


def dataset_name_from_landing_filename(filename: str) -> str:
    """Deriva el dataset Bronze desde un CSV de Landing."""
    path_name = Path(filename).name
    if path_name.lower().endswith(".pdf"):
        raise ValueError(f"El archivo no es un CSV de datos: {filename}")
    if not path_name.lower().endswith(".csv"):
        raise ValueError(f"El archivo debe tener extensión .csv: {filename}")

    dataset_name = path_name[:-4]
    if dataset_name.startswith("pronabec_"):
        dataset_name = dataset_name.removeprefix("pronabec_")

    if not dataset_name:
        raise ValueError(f"No se pudo derivar dataset desde: {filename}")

    return dataset_name


def validate_source_subset(source_subset: str) -> None:
    """Valida que el subset de reportes sea soportado."""
    if source_subset not in SUPPORTED_SOURCE_SUBSETS:
        valid_values = ", ".join(SUPPORTED_SOURCE_SUBSETS)
        raise ValueError(
            f"source_subset no soportado: {source_subset}. Valores válidos: {valid_values}"
        )


def expected_reports_for_subset(source_subset: str) -> list[str]:
    """Devuelve los datasets esperados para un subset de reportes PRONABEC."""
    validate_source_subset(source_subset)
    return [
        report_id
        for report_id, config in MANUAL_REPORT_SOURCES.items()
        if config.get("source_subset") == source_subset
    ]


def validate_date(date_str: str) -> None:
    """Valida que la fecha tenga el formato YYYY-MM-DD."""
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(
            f"Formato de fecha inválido: {date_str}. Debe ser YYYY-MM-DD."
        ) from e


def get_env_value(name: str) -> str | None:
    """Obtiene una variable de entorno no vacía."""
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return None
    return value.strip()


def resolve_gcs_runtime_options(
    input_uri: str | None,
    output_uri: str | None,
    extraction_date: str | None,
    source_subset: str | None,
) -> tuple[str, str, str, str]:
    """Resuelve opciones GCS explícitas o derivadas desde variables de entorno."""
    bucket_name = get_env_value("GCS_BUCKET_NAME") or get_env_value("GCS_BUCKET")
    resolved_source_subset = source_subset or get_env_value("SOURCE_SUBSET")
    resolved_extraction_date = extraction_date or get_env_value("BRONZE_EXTRACTION_DATE")
    landing_prefix = (
        get_env_value("PRONABEC_REPORTS_LANDING_PREFIX")
        or DEFAULT_REPORTS_LANDING_PREFIX
    ).strip("/")
    bronze_prefix = (
        get_env_value("PRONABEC_REPORTS_BRONZE_PREFIX")
        or DEFAULT_REPORTS_BRONZE_PREFIX
    ).strip("/")

    if not resolved_source_subset or not resolved_extraction_date:
        raise ValueError(
            "Modo GCS por entorno requiere GCS_BUCKET_NAME, SOURCE_SUBSET y BRONZE_EXTRACTION_DATE."
        )

    if (not input_uri or not output_uri) and not bucket_name:
        raise ValueError(
            "Modo GCS por entorno requiere GCS_BUCKET_NAME, SOURCE_SUBSET y BRONZE_EXTRACTION_DATE."
        )

    resolved_input_uri = input_uri
    if not resolved_input_uri:
        resolved_input_uri = f"gs://{bucket_name}/{landing_prefix}/{resolved_source_subset}"

    resolved_output_uri = output_uri
    if not resolved_output_uri:
        resolved_output_uri = f"gs://{bucket_name}/{bronze_prefix}"

    return (
        resolved_input_uri,
        resolved_output_uri,
        resolved_extraction_date,
        resolved_source_subset,
    )


def find_file_recursively(root_dir: Path, target_names: list[str]) -> Path | None:
    """Busca un archivo por sus nombres posibles de forma recursiva."""
    for path in root_dir.rglob("*"):
        if path.is_file() and path.name in target_names:
            return path
    return None


def find_gcs_csv(input_uri: str, target_names: list[str]) -> str | None:
    """Busca un CSV esperado dentro de una URI Landing, ignorando documentos."""
    for object_uri in list_gcs_objects(input_uri):
        object_name = object_uri.rsplit("/", 1)[-1]
        if "/_documents/" in object_uri:
            continue
        if not object_name.lower().endswith(".csv"):
            continue
        if object_name in target_names:
            return object_uri
    return None


def calculate_sha256(file_path: Path) -> str:
    """Calcula el hash SHA-256 de un archivo de forma eficiente."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def calculate_bytes_sha256(content: bytes) -> str:
    """Calcula SHA-256 para contenido en memoria."""
    return hashlib.sha256(content).hexdigest()


def analyze_csv(file_path: Path) -> tuple[int, int, list[str]]:
    """
    Analiza un archivo CSV para obtener la cantidad de filas de datos,
    cantidad de columnas y los nombres de las columnas.
    Prueba diferentes codificaciones de manera defensiva.
    """
    encodings = ["utf-8-sig", "utf-8", "latin-1"]
    for encoding in encodings:
        try:
            with open(file_path, mode="r", encoding=encoding) as f:
                # Comprobar si el archivo está vacío
                first_char = f.read(1)
                if not first_char:
                    raise ValueError("El archivo CSV está vacío.")
                f.seek(0)

                reader = csv.reader(f)
                try:
                    headers = next(reader)
                except StopIteration as e:
                    raise ValueError("El archivo CSV no tiene cabeceras.") from e

                if not headers or all(h.strip() == "" for h in headers):
                    raise ValueError("Cabeceras inválidas en el archivo CSV.")

                row_count = 0
                for _ in reader:
                    row_count += 1

                return row_count, len(headers), [h.strip() for h in headers]
        except UnicodeDecodeError:
            continue
        except Exception as e:
            raise e

    raise UnicodeDecodeError(
        "utf-8",
        b"",
        0,
        0,
        f"No se pudo decodificar el archivo CSV {file_path} con codificaciones soportadas.",
    )


def stage_csv_with_metadata(
    source_file: Path,
    target_csv_path: Path,
    schema_columns: list[str],
    config: dict[str, Any],
) -> tuple[int, int, list[str]]:
    """
    Lee el archivo CSV de origen, mapea sus cabeceras a los nombres del esquema Bronze,
    agrega las columnas de metadatos documentales requeridas por fila y guarda el resultado.
    """
    encodings = ["utf-8-sig", "utf-8", "latin-1"]
    source_rows = []
    source_headers = []

    # Intentar leer con codificaciones defensivas
    for encoding in encodings:
        try:
            with open(source_file, mode="r", encoding=encoding) as f:
                # Comprobar si el archivo está vacío
                first_char = f.read(1)
                if not first_char:
                    raise ValueError("El archivo CSV está vacío.")
                f.seek(0)
                reader = csv.reader(f)
                source_headers = [h.strip() for h in next(reader)]
                source_rows = list(reader)
                break
        except UnicodeDecodeError:
            continue
    else:
        raise UnicodeDecodeError(
            "utf-8", b"", 0, 0, f"No se pudo decodificar el archivo CSV {source_file}."
        )

    # Mapeo de cabeceras
    header_mapping = {}
    for h in source_headers:
        h_clean = h.strip().lstrip('\ufeff').lower()
        if h_clean in ("universidad", "carrera de estudio", "carrera_estudio"):
            header_mapping[h] = "universidad" if "universidad" in schema_columns else "carrera_estudio"
        elif h_clean.isdigit():
            header_mapping[h] = f"anio_{h_clean}"
        elif h_clean == "2026 (*)":
            header_mapping[h] = "anio_2026_preliminar"
        elif h_clean in ("total", "total general"):
            header_mapping[h] = "total"
        else:
            header_mapping[h] = h_clean

    # Validación defensiva de la dimensión principal
    mapped_targets = list(header_mapping.values())
    if "carrera_estudio" in schema_columns and "carrera_estudio" not in mapped_targets:
        raise ValueError(f"Falla de staging: No se mapeó 'carrera_estudio'. Cabeceras fuente: {source_headers}")
    if "universidad" in schema_columns and "universidad" not in mapped_targets:
        raise ValueError(f"Falla de staging: No se mapeó 'universidad'. Cabeceras fuente: {source_headers}")

    # Escribir el nuevo CSV con las columnas en el orden exacto del esquema
    with open(target_csv_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(schema_columns)

        for row in source_rows:
            row_dict = {}
            for idx, h in enumerate(source_headers):
                if idx < len(row):
                    target_col = header_mapping.get(h, h)
                    row_dict[target_col] = row[idx]

            # Rellenar metadatos documentales
            for col in schema_columns:
                if col not in row_dict:
                    if col in config:
                        row_dict[col] = config[col]
                    elif col == "source_page":
                        row_dict[col] = config.get("source_page", "1")
                    elif col == "source_table":
                        row_dict[col] = config.get("source_table", "1")
                    else:
                        row_dict[col] = ""

            # Escribir fila mapeada en el orden del esquema
            new_row = [row_dict.get(col, "") for col in schema_columns]
            writer.writerow(new_row)

    return len(source_rows), len(schema_columns), schema_columns


def target_configs(
    report_name: str | None = None,
    source_subset: str | None = None,
) -> dict[str, dict[str, Any]]:
    """Resuelve los reportes objetivo para staging."""
    if source_subset:
        validate_source_subset(source_subset)

    targets: dict[str, dict[str, Any]] = {}
    if report_name:
        if report_name not in MANUAL_REPORT_SOURCES:
            raise ValueError(f"Reporte desconocido o no soportado: {report_name}")
        targets[report_name] = MANUAL_REPORT_SOURCES[report_name]
        return targets

    for report_id, config in MANUAL_REPORT_SOURCES.items():
        if source_subset:
            if report_id in expected_reports_for_subset(source_subset):
                targets[report_id] = config
        else:
            targets[report_id] = config

    return targets


def should_rewrite_with_metadata(target_key: str, source_file: Path) -> tuple[bool, list[str]]:
    """Determina si un CSV debe reescribirse para agregar metadata documental."""
    project_root = Path(__file__).resolve().parent.parent
    schema_path = project_root / "config" / "schemas" / "bronze" / f"{target_key}_schema.json"

    should_rewrite = False
    schema_columns: list[str] = []

    if target_key in (
        "report_beca18_universitarios_universidad_anual",
        "report_beca18_universitarios_carrera_anual",
    ) and schema_path.exists():
        try:
            with open(schema_path, "r", encoding="utf-8") as f:
                schema_data = json.load(f)
            schema_columns = [field["name"] for field in schema_data]

            _, _, source_cols = analyze_csv(source_file)
            if "source_document_file" in schema_columns and "source_document_file" not in source_cols:
                should_rewrite = True
        except Exception as e:
            print(f"Warning leyendo esquema o CSV para {target_key}: {e}")

    return should_rewrite, schema_columns


def build_metadata(
    target_key: str,
    source_file_name: str,
    target_csv_ref: str,
    extraction_date: str,
    config: dict[str, Any],
    row_count: int,
    col_count: int,
    columns: list[str],
    sha256: str,
    source_subset: str | None = None,
    source_path: str | None = None,
    landing_uri: str | None = None,
    output_path: str | None = None,
    source_method: str | None = None,
) -> dict[str, Any]:
    """Construye metadata común para staging local y GCS."""
    staging_time = datetime.now(timezone.utc).isoformat()
    resolved_subset = source_subset or config.get("source_subset")
    resolved_method = source_method or config["extraction_method"]

    metadata = {
        "source_system": "pronabec_reports",
        "source_dataset": target_key,
        "source_file_name": source_file_name,
        "bronze_file_name": "data.csv",
        "extraction_date": extraction_date,
        "staging_timestamp": staging_time,
        "ingestion_timestamp": staging_time,
        "staged_at": staging_time,
        "created_at": staging_time,
        "extraction_method": resolved_method,
        "source_method": resolved_method,
        "source_document_title": config["source_document_title"],
        "source_publication_url": config.get("source_publication_url"),
        "row_count": row_count,
        "records_read": row_count,
        "records_written": row_count,
        "column_count": col_count,
        "columns": columns,
        "sha256": sha256,
        "content_sha256": sha256,
        "file_sha256": sha256,
    }

    if source_path:
        metadata["source_path"] = source_path
        metadata["input_path"] = source_path
    if landing_uri:
        metadata["landing_uri"] = landing_uri
    if output_path:
        metadata["output_path"] = output_path
    else:
        metadata["output_path"] = target_csv_ref

    if resolved_subset:
        metadata["source_subset"] = resolved_subset
        metadata["source_file"] = source_file_name
    if "source_document_file" in config:
        metadata["source_document_file"] = config["source_document_file"]

    return metadata


def stage_reports(
    input_dir: str,
    output_dir: str,
    extraction_date: str,
    report_name: str | None = None,
    overwrite: bool = False,
    source_subset: str | None = None,
    strict: bool = False,
) -> tuple[int, int, int]:
    """
    Ejecuta el staging de los reportes manuales copiándolos a la carpeta de salida
    Bronze local y generando sus metadatos correspondientes.
    """
    validate_date(extraction_date)
    if source_subset:
        validate_source_subset(source_subset)

    input_path = Path(input_dir)
    if not input_path.exists() or not input_path.is_dir():
        raise FileNotFoundError(f"El directorio de entrada no existe: {input_dir}")

    output_path = Path(output_dir)

    staged_count = 0
    skipped_count = 0
    missing_count = 0

    targets = target_configs(report_name=report_name, source_subset=source_subset)

    for target_key, config in targets.items():
        possible_names = [config["filename"]] + config.get("alternative_filenames", [])
        source_file = find_file_recursively(input_path, possible_names)

        if not source_file:
            if report_name or strict:
                raise FileNotFoundError(
                    f"No se encontró el archivo fuente para el reporte '{target_key}' "
                    f"bajo la ruta '{input_dir}' (buscando {possible_names})."
                )
            print(f"Skipped {target_key}: Archivo fuente no encontrado.")
            missing_count += 1
            continue

        # Directorio y archivo destino
        target_dataset_dir = output_path / target_key / f"extraction_date={extraction_date}"
        target_csv_path = target_dataset_dir / "data.csv"
        target_metadata_path = target_dataset_dir / "extraction_metadata.json"

        # Validar sobreescritura
        if target_csv_path.exists() and not overwrite:
            if report_name or strict:
                raise FileExistsError(
                    f"El archivo destino ya existe y no se especificó --overwrite: {target_csv_path}"
                )
            print(f"Skipped {target_key}: El archivo destino ya existe y no se especificó --overwrite.")
            skipped_count += 1
            continue

        # Crear directorios
        target_dataset_dir.mkdir(parents=True, exist_ok=True)

        should_rewrite, schema_columns = should_rewrite_with_metadata(target_key, source_file)

        if should_rewrite:
            try:
                row_count, col_count, columns = stage_csv_with_metadata(
                    source_file=source_file,
                    target_csv_path=target_csv_path,
                    schema_columns=schema_columns,
                    config=config,
                )
            except Exception as e:
                if target_csv_path.exists():
                    target_csv_path.unlink()
                raise ValueError(f"Error procesando CSV con metadatos {source_file}: {str(e)}") from e
        else:
            # Copia conservadora binaria
            shutil.copyfile(source_file, target_csv_path)

            # Analizar el archivo final copiado para los metadatos
            try:
                row_count, col_count, columns = analyze_csv(target_csv_path)
            except Exception as e:
                # Si falla la lectura, removemos el archivo copiado para no dejar Bronze inconsistente
                if target_csv_path.exists():
                    target_csv_path.unlink()
                raise ValueError(f"Error analizando el archivo CSV {source_file}: {str(e)}") from e

        sha256 = calculate_sha256(target_csv_path)
        metadata = build_metadata(
            target_key=target_key,
            source_file_name=source_file.name,
            target_csv_ref=str(target_csv_path.resolve()),
            extraction_date=extraction_date,
            config=config,
            row_count=row_count,
            col_count=col_count,
            columns=columns,
            sha256=sha256,
            source_path=str(source_file.resolve()),
            output_path=str(target_csv_path.resolve()),
        )

        # Guardar metadatos en JSON formateado
        with open(target_metadata_path, "w", encoding="utf-8") as meta_f:
            json.dump(metadata, meta_f, indent=2, ensure_ascii=False)

        print(f"Staged {target_key}")
        print(f"  source: {source_file}")
        print(f"  target: {target_csv_path}")
        print(f"  rows: {row_count}")
        print(f"  columns: {col_count}")

        staged_count += 1

    return staged_count, skipped_count, missing_count


def stage_reports_gcs(
    input_uri: str,
    output_uri: str,
    extraction_date: str,
    report_name: str | None = None,
    overwrite: bool = False,
    source_subset: str | None = None,
    strict: bool = False,
) -> tuple[int, int, int]:
    """Ejecuta staging desde GCS Landing hacia GCS Bronze."""
    validate_date(extraction_date)
    if not source_subset:
        raise ValueError("--source-subset es obligatorio para staging GCS.")
    validate_source_subset(source_subset)

    targets = target_configs(report_name=report_name, source_subset=source_subset)
    staged_count = 0
    skipped_count = 0
    missing_count = 0

    with tempfile.TemporaryDirectory(prefix="stage_pronabec_reports_gcs_") as work_dir:
        work_path = Path(work_dir)

        for target_key, config in targets.items():
            possible_names = [config["filename"]] + config.get("alternative_filenames", [])
            source_uri = find_gcs_csv(input_uri, possible_names)

            if not source_uri:
                if report_name or strict:
                    raise FileNotFoundError(
                        f"No se encontró el CSV fuente para el reporte '{target_key}' "
                        f"bajo la ruta '{input_uri}' (buscando {possible_names})."
                    )
                print(f"Skipped {target_key}: Archivo fuente no encontrado.")
                missing_count += 1
                continue

            source_file_name = source_uri.rsplit("/", 1)[-1]
            source_file = work_path / source_file_name
            target_csv_path = work_path / f"{target_key}_data.csv"
            source_file.write_bytes(read_gcs_bytes(source_uri))

            target_dataset_uri = join_gcs_uri(
                output_uri,
                target_key,
                f"extraction_date={extraction_date}",
            )
            target_csv_uri = join_gcs_uri(target_dataset_uri, "data.csv")
            target_metadata_uri = join_gcs_uri(target_dataset_uri, "extraction_metadata.json")

            if not overwrite:
                existing_outputs = set(list_gcs_objects(target_dataset_uri))
                if target_csv_uri in existing_outputs:
                    if report_name or strict:
                        raise FileExistsError(
                            f"El archivo destino ya existe y no se especificó --overwrite: {target_csv_uri}"
                        )
                    print(f"Skipped {target_key}: El archivo destino ya existe y no se especificó --overwrite.")
                    skipped_count += 1
                    continue

            should_rewrite, schema_columns = should_rewrite_with_metadata(target_key, source_file)
            if should_rewrite:
                row_count, col_count, columns = stage_csv_with_metadata(
                    source_file=source_file,
                    target_csv_path=target_csv_path,
                    schema_columns=schema_columns,
                    config=config,
                )
            else:
                shutil.copyfile(source_file, target_csv_path)
                row_count, col_count, columns = analyze_csv(target_csv_path)

            staged_bytes = target_csv_path.read_bytes()
            sha256 = calculate_bytes_sha256(staged_bytes)
            metadata = build_metadata(
                target_key=target_key,
                source_file_name=source_file_name,
                target_csv_ref=target_csv_uri,
                extraction_date=extraction_date,
                config=config,
                row_count=row_count,
                col_count=col_count,
                columns=columns,
                sha256=sha256,
                source_subset=source_subset,
                landing_uri=source_uri,
                output_path=target_csv_uri,
                source_method="manual_landing_csv",
            )

            write_gcs_bytes(target_csv_uri, staged_bytes, content_type="text/csv")
            write_gcs_text(
                target_metadata_uri,
                json.dumps(metadata, indent=2, ensure_ascii=False),
            )

            print(f"Staged {target_key}")
            print(f"  source: {source_uri}")
            print(f"  target: {target_csv_uri}")
            print(f"  rows: {row_count}")
            print(f"  columns: {col_count}")

            staged_count += 1

    return staged_count, skipped_count, missing_count


def main() -> None:
    """Función de entrada para ejecución CLI."""
    parser = argparse.ArgumentParser(
        description="Stagea reportes manuales PRONABEC como archivos Bronze locales o GCS."
    )
    parser.add_argument(
        "--input-dir",
        default=None,
        help="Directorio de origen que contiene los reportes CSV manuales.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directorio destino Bronze local.",
    )
    parser.add_argument(
        "--input-uri",
        default=None,
        help="URI GCS Landing de origen que contiene los reportes CSV.",
    )
    parser.add_argument(
        "--output-uri",
        default=None,
        help="URI GCS Bronze destino.",
    )
    parser.add_argument(
        "--extraction-date",
        default=None,
        help="Fecha de extracción en formato YYYY-MM-DD.",
    )
    parser.add_argument(
        "--report-name",
        default=None,
        help="Opcional. Nombre de un reporte específico a stagear.",
    )
    parser.add_argument(
        "--source-subset",
        default=None,
        help="Opcional. Filtrar por un subconjunto específico (ej. pes_2025).",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Opcional. Sobrescribe archivos existentes en la carpeta de destino.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Opcional. Lanza un error si no se encuentra alguno de los reportes esperados.",
    )

    args = parser.parse_args()

    try:
        local_mode = args.input_dir is not None or args.output_dir is not None
        explicit_gcs_mode = args.input_uri is not None or args.output_uri is not None
        env_gcs_mode = not local_mode and not explicit_gcs_mode
        gcs_mode = explicit_gcs_mode or env_gcs_mode

        if local_mode and explicit_gcs_mode:
            raise ValueError("Use solo modo local (--input-dir/--output-dir) o solo modo GCS (--input-uri/--output-uri).")
        if local_mode and (not args.input_dir or not args.output_dir):
            raise ValueError("Modo local requiere --input-dir y --output-dir.")
        if local_mode and not args.extraction_date:
            raise ValueError("Modo local requiere --extraction-date.")

        if gcs_mode:
            input_uri, output_uri, extraction_date, source_subset = resolve_gcs_runtime_options(
                input_uri=args.input_uri,
                output_uri=args.output_uri,
                extraction_date=args.extraction_date,
                source_subset=args.source_subset,
            )
            staged, skipped, missing = stage_reports_gcs(
                input_uri=input_uri,
                output_uri=output_uri,
                extraction_date=extraction_date,
                report_name=args.report_name,
                overwrite=args.overwrite,
                source_subset=source_subset,
                strict=args.strict,
            )
        else:
            staged, skipped, missing = stage_reports(
                input_dir=args.input_dir,
                output_dir=args.output_dir,
                extraction_date=args.extraction_date,
                report_name=args.report_name,
                overwrite=args.overwrite,
                source_subset=args.source_subset,
                strict=args.strict,
            )
        print("\nStaging finalizado:")
        print(f"Reports staged: {staged}")
        print(f"Reports skipped: {skipped}")
        print(f"Reports missing: {missing}")
    except Exception as e:
        print(f"Error ejecutando staging: {str(e)}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
