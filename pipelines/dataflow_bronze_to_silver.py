"""
Pipeline principal de Apache Beam / Dataflow para procesar datos de Bronze a Silver.
"""

from __future__ import annotations

import argparse
import io
import json
import csv
from datetime import datetime, timezone
from typing import Any

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from pipelines.common.logging import setup_structured_logger
from pipelines.transforms.base import add_technical_metadata

logger = setup_structured_logger("dataflow_bronze_to_silver", level="INFO")


class ReadCsvDoFn(beam.DoFn):
    """DoFn para leer un archivo CSV desde almacenamiento local o GCS."""

    def process(self, path: str) -> Any:
        from apache_beam.io.filesystems import FileSystems

        try:
            with FileSystems.open(path) as f:
                text_file = io.TextIOWrapper(f, encoding="utf-8")
                reader = csv.DictReader(text_file)
                for row in reader:
                    yield dict(row)
        except Exception as e:
            logger.error(f"Error leyendo archivo CSV {path}: {str(e)}")
            raise e


class ReadJsonlDoFn(beam.DoFn):
    """DoFn para leer un archivo JSONL desde almacenamiento local o GCS."""

    def process(self, path: str) -> Any:
        from apache_beam.io.filesystems import FileSystems

        try:
            with FileSystems.open(path) as f:
                text_file = io.TextIOWrapper(f, encoding="utf-8")
                for line in text_file:
                    line = line.strip()
                    if line:
                        yield json.loads(line)
        except Exception as e:
            logger.error(f"Error leyendo archivo JSONL {path}: {str(e)}")
            raise e


def parse_arguments(argv: list[str] | None = None) -> tuple[argparse.Namespace, list[str]]:
    """
    Define y parsea los argumentos de la línea de comandos para el pipeline.
    """
    parser = argparse.ArgumentParser(
        description="Pipeline Apache Beam para transición de datos Bronze a Silver."
    )
    parser.add_argument(
        "--source-system",
        required=False,
        default=None,
        help="Sistema origen (ej. pronabec_reports).",
    )
    parser.add_argument(
        "--source-dataset",
        required=False,
        default=None,
        help="Dataset origen de los datos.",
    )
    parser.add_argument(
        "--extraction-date",
        required=False,
        default=None,
        help="Fecha de extracción en formato YYYY-MM-DD.",
    )
    parser.add_argument(
        "--input-path",
        required=False,
        default=None,
        help="Ruta de entrada (local o GCS) al archivo de origen.",
    )
    parser.add_argument(
        "--input-format",
        required=False,
        default=None,
        help="Formato del archivo de entrada: 'csv' o 'jsonl'.",
    )
    parser.add_argument(
        "--output-table",
        required=False,
        default=None,
        help="Nombre completo de la tabla BigQuery de salida (project:dataset.table).",
    )
    parser.add_argument(
        "--runner",
        required=False,
        default="DirectRunner",
        help="Runner a utilizar para la ejecución del pipeline (DirectRunner o DataflowRunner).",
    )
    parser.add_argument(
        "--project",
        required=False,
        default=None,
        help="ID del proyecto GCP (requerido para DataflowRunner).",
    )
    parser.add_argument(
        "--region",
        required=False,
        default=None,
        help="Región de ejecución en GCP (requerida para DataflowRunner).",
    )
    parser.add_argument(
        "--temp-location",
        required=False,
        default=None,
        help="Ruta de GCS temporal para Dataflow (requerida para DataflowRunner).",
    )
    parser.add_argument(
        "--staging-location",
        required=False,
        default=None,
        help="Ruta de GCS staging para Dataflow (requerida para DataflowRunner).",
    )
    parser.add_argument(
        "--pipeline-run-id",
        required=False,
        default=None,
        help="ID único de la corrida del pipeline.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Evita la escritura física y procesa los datos en memoria/logs.",
    )

    args, pipeline_args = parser.parse_known_args(argv)
    return args, pipeline_args


def validate_arguments(args: argparse.Namespace) -> None:
    """
    Valida las opciones ingresadas por el usuario.
    """
    # 1. Validar argumentos críticos requeridos
    critical_fields = [
        ("source_system", "--source-system"),
        ("source_dataset", "--source-dataset"),
        ("extraction_date", "--extraction-date"),
        ("input_path", "--input-path"),
        ("input_format", "--input-format"),
        ("output_table", "--output-table"),
        ("runner", "--runner"),
    ]
    for field, arg_name in critical_fields:
        val = getattr(args, field, None)
        if val is None or str(val).strip() == "":
            raise ValueError(f"El argumento crítico {arg_name} es requerido.")

    # 2. Validar formato de entrada
    if args.input_format not in ("csv", "jsonl"):
        raise ValueError(
            f"Formato de entrada inválido: {args.input_format}. Debe ser 'csv' o 'jsonl'."
        )

    # 3. Validar runner
    if args.runner not in ("DirectRunner", "DataflowRunner"):
        raise ValueError(
            f"Runner inválido: {args.runner}. Debe ser 'DirectRunner' o 'DataflowRunner'."
        )

    # 4. Validar parámetros específicos si es DataflowRunner
    if args.runner == "DataflowRunner":
        missing_cloud_params = []
        if not args.project:
            missing_cloud_params.append("--project")
        if not args.region:
            missing_cloud_params.append("--region")
        if not args.temp_location:
            missing_cloud_params.append("--temp-location")
        if not args.staging_location:
            missing_cloud_params.append("--staging-location")

        if missing_cloud_params:
            raise ValueError(
                "Los siguientes argumentos son requeridos para DataflowRunner: "
                f"{', '.join(missing_cloud_params)}."
            )


def build_pipeline_options(
    args: argparse.Namespace,
    pipeline_args: list[str],
) -> PipelineOptions:
    """
    Construye las opciones del pipeline Apache Beam combinando argumentos.
    """
    options_dict = {
        "runner": args.runner,
    }
    if args.runner == "DataflowRunner":
        options_dict.update({
            "project": args.project,
            "region": args.region,
            "temp_location": args.temp_location,
            "staging_location": args.staging_location,
        })

    return PipelineOptions(pipeline_args, **options_dict)


def run(argv: list[str] | None = None) -> None:
    """
    Función principal de ejecución del pipeline.
    """
    args, pipeline_args = parse_arguments(argv)
    validate_arguments(args)

    pipeline_options = build_pipeline_options(args, pipeline_args)
    pipeline_run_id = args.pipeline_run_id or f"run_{int(datetime.now(timezone.utc).timestamp())}"

    logger.info(
        "Iniciando pipeline Bronze a Silver",
        extra_fields={
            "source_system": args.source_system,
            "source_dataset": args.source_dataset,
            "runner": args.runner,
            "dry_run": args.dry_run,
        },
    )

    with beam.Pipeline(options=pipeline_options) as p:
        # 1. Lectura de datos según formato
        if args.input_format == "csv":
            records = (
                p
                | "Start CSV Read" >> beam.Create([args.input_path])
                | "Read CSV File" >> beam.ParDo(ReadCsvDoFn())
            )
        else:
            records = (
                p
                | "Start JSONL Read" >> beam.Create([args.input_path])
                | "Read JSONL File" >> beam.ParDo(ReadJsonlDoFn())
            )

        # 2. Transformación placeholder (Identidad + Metadata técnica)
        transformed = (
            records
            | "Apply Technical Metadata" >> beam.Map(
                add_technical_metadata,
                source_system=args.source_system,
                source_dataset=args.source_dataset,
                extraction_date=args.extraction_date,
                pipeline_run_id=pipeline_run_id,
            )
        )

        # 3. Escritura o Consumo final
        # Por ahora no escribimos físicamente a BigQuery, registramos el comportamiento.
        if args.dry_run:
            (
                transformed
                | "Log Records (Dry-Run)" >> beam.Map(
                    lambda x: logger.info(f"Registro procesado (Dry-run): {x}")
                )
            )
        else:
            # Comportamiento temporal sin BigQuery real
            (
                transformed
                | "Log Records (Standard)" >> beam.Map(
                    lambda x: logger.info(f"Registro procesado (Standard): {x}")
                )
            )


if __name__ == "__main__":
    run()
