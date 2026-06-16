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
    },
    "report_beca18_universitarios_carrera_anual": {
        "filename": "beca18_becarios_universidades_carrera_2012_2026.csv",
        "alternative_filenames": [
            "pronabec_report_beca18_universitarios_carrera_anual.csv"
        ],
        "source_document_title": "Beca 18 - cantidad de becarios en universidades según carrera de estudio 2012-2026",
        "source_publication_url": "https://www.gob.pe/institucion/pronabec/informes-publicaciones/8170922-beca-18-cantidad-de-becarios-universitarios",
        "extraction_method": "manual_csv",
    },
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


def stage_reports(
    input_dir: str,
    output_dir: str,
    extraction_date: str,
    report_name: str | None = None,
    overwrite: bool = False,
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

    # Filtrar reportes según parámetro
    targets = {}
    if report_name:
        if report_name not in MANUAL_REPORT_SOURCES:
            raise ValueError(f"Reporte desconocido o no soportado: {report_name}")
        targets[report_name] = MANUAL_REPORT_SOURCES[report_name]
    else:
        targets = MANUAL_REPORT_SOURCES

    for target_key, config in targets.items():
        possible_names = [config["filename"]] + config.get("alternative_filenames", [])
        source_file = find_file_recursively(input_path, possible_names)

        if not source_file:
            if report_name:
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
            if report_name:
                raise FileExistsError(
                    f"El archivo destino ya existe y no se especificó --overwrite: {target_csv_path}"
                )
            print(f"Skipped {target_key}: El archivo destino ya existe y no se especificó --overwrite.")
            skipped_count += 1
            continue

        # Crear directorios
        target_dataset_dir.mkdir(parents=True, exist_ok=True)

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

        # Metadatos de extracción
        metadata = {
            "source_system": "pronabec_reports",
            "source_dataset": target_key,
            "source_file_name": source_file.name,
            "bronze_file_name": "data.csv",
            "input_path": str(source_file.resolve()),
            "output_path": str(target_csv_path.resolve()),
            "extraction_date": extraction_date,
            "staging_timestamp": datetime.now(timezone.utc).isoformat(),
            "extraction_method": config["extraction_method"],
            "source_document_title": config["source_document_title"],
            "source_publication_url": config.get("source_publication_url"),
            "row_count": row_count,
            "column_count": col_count,
            "columns": columns,
            "content_sha256": sha256,
        }

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
        "--overwrite",
        action="store_true",
        help="Opcional. Sobrescribe archivos existentes en la carpeta de destino.",
    )

    args = parser.parse_args()

    try:
        staged, skipped, missing = stage_reports(
            input_dir=args.input_dir,
            output_dir=args.output_dir,
            extraction_date=args.extraction_date,
            report_name=args.report_name,
            overwrite=args.overwrite,
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
