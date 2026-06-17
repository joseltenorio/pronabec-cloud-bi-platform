"""
Herramienta de staging para preparar archivos CSV manuales de PRONABEC en layout Bronze local.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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


def validate_date(date_str: str) -> None:
    """Valida que la fecha tenga el formato YYYY-MM-DD."""
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(
            f"Formato de fecha inválido: {date_str}. Debe ser YYYY-MM-DD."
        ) from e


def find_file_recursively(root_dir: Path, target_names: list[str]) -> Path | None:
    """Busca un archivo por sus nombres posibles de forma recursiva."""
    for path in root_dir.rglob("*"):
        if path.is_file() and path.name in target_names:
            return path
    return None


def calculate_sha256(file_path: Path) -> str:
    """Calcula el hash SHA-256 de un archivo de forma eficiente."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def analyze_csv(file_path: Path) -> tuple[int, int, list[str]]:
    """
    Analiza un archivo CSV para obtener la cantidad de filas de datos,
    cantidad de columnas y los nombres de las columnas.
    Prueba diferentes codificaciones de manera defensiva.
    """
    encodings = ["utf-8", "utf-8-sig", "latin-1"]
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
    encodings = ["utf-8", "utf-8-sig", "latin-1"]
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
        h_clean = h.strip().lower()
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

    input_path = Path(input_dir)
    if not input_path.exists() or not input_path.is_dir():
        raise FileNotFoundError(f"El directorio de entrada no existe: {input_dir}")

    output_path = Path(output_dir)

    staged_count = 0
    skipped_count = 0
    missing_count = 0

    # Filtrar reportes según parámetro y subconjunto
    targets = {}
    if report_name:
        if report_name not in MANUAL_REPORT_SOURCES:
            raise ValueError(f"Reporte desconocido o no soportado: {report_name}")
        targets[report_name] = MANUAL_REPORT_SOURCES[report_name]
    else:
        for k, v in MANUAL_REPORT_SOURCES.items():
            if source_subset:
                if v.get("source_subset") == source_subset:
                    targets[k] = v
            else:
                # Si no se define subset, solo se stagean los que no pertenecen a ningún subset
                # para mantener compatibilidad con las pruebas existentes.
                if v.get("source_subset") is None:
                    targets[k] = v

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

        # Verificar si existe esquema Bronze para validar si faltan columnas de metadatos documentales
        project_root = Path(__file__).resolve().parent.parent
        schema_path = project_root / "config" / "schemas" / "bronze" / f"{target_key}_schema.json"

        should_rewrite = False
        schema_columns = []

        if target_key in ("report_beca18_universitarios_universidad_anual", "report_beca18_universitarios_carrera_anual") and schema_path.exists():
            try:
                with open(schema_path, "r", encoding="utf-8") as f:
                    schema_data = json.load(f)
                schema_columns = [field["name"] for field in schema_data]

                # Leer cabeceras del CSV de origen para ver si ya contiene "source_document_file"
                _, _, source_cols = analyze_csv(source_file)
                if "source_document_file" in schema_columns and "source_document_file" not in source_cols:
                    should_rewrite = True
            except Exception as e:
                print(f"Warning leyendo esquema o CSV para {target_key}: {e}")

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

        # Hash SHA-256 reproducible
        sha256 = calculate_sha256(target_csv_path)
        staging_time = datetime.now(timezone.utc).isoformat()

        # Metadatos de extracción
        metadata = {
            "source_system": "pronabec_reports",
            "source_dataset": target_key,
            "source_file_name": source_file.name,
            "bronze_file_name": "data.csv",
            "input_path": str(source_file.resolve()),
            "output_path": str(target_csv_path.resolve()),
            "extraction_date": extraction_date,
            "staging_timestamp": staging_time,
            "ingestion_timestamp": staging_time,
            "staged_at": staging_time,
            "extraction_method": config["extraction_method"],
            "source_document_title": config["source_document_title"],
            "source_publication_url": config.get("source_publication_url"),
            "row_count": row_count,
            "records_read": row_count,
            "records_written": row_count,
            "column_count": col_count,
            "columns": columns,
            "content_sha256": sha256,
            "file_sha256": sha256,
        }

        # Campos adicionales del subset
        if "source_subset" in config:
            metadata["source_subset"] = config["source_subset"]
            metadata["source_file"] = source_file.name
        if "source_document_file" in config:
            metadata["source_document_file"] = config["source_document_file"]

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


def main() -> None:
    """Función de entrada para ejecución CLI."""
    parser = argparse.ArgumentParser(
        description="Stagea reportes manuales PRONABEC como archivos Bronze locales."
    )
    parser.add_argument(
        "--input-dir",
        required=True,
        help="Directorio de origen que contiene los reportes CSV manuales.",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directorio destino Bronze local.",
    )
    parser.add_argument(
        "--extraction-date",
        required=True,
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
