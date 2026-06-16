"""
Pipeline principal de Apache Beam / Dataflow para procesar datos de Bronze a Silver.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
from datetime import datetime, timezone
from typing import Any

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from pipelines.common.bigquery import build_bigquery_write_config, build_bigquery_write_transform
from pipelines.common.logging import setup_structured_logger, log_event
from pipelines.transforms.pronabec_reports import REPORT_SPECS, transform_pronabec_report_record
from pipelines.transforms.base import add_technical_metadata
from pipelines.transforms.mef import transform_mef_record
from pipelines.transforms.pronabec import transform_pronabec_record

logger = setup_structured_logger("dataflow_bronze_to_silver", level="INFO")


class ReadCsvDoFn(beam.DoFn):
    """DoFn para leer un archivo CSV desde almacenamiento local o GCS."""

    def process(self, path: str) -> Any:
        from apache_beam.io.filesystems import FileSystems

        try:
            with FileSystems.open(path) as f:
                text_file = io.TextIOWrapper(f, encoding="utf-8-sig")
                reader = csv.DictReader(text_file)
                for row in reader:
                    yield dict(row)
        except Exception as e:
            logger.error(f"Error leyendo archivo CSV {path}: {str(e)}")
            raise e


class ReadJsonlDoFn(beam.DoFn):
    """DoFn para leer un archivo JSONL desde almacenamiento local o GCS."""

    def process(
        self,
        path: str,
        source_system: str | None = None,
        source_dataset: str | None = None,
        extraction_date: str | None = None,
        pipeline_run_id: str | None = None,
        ingestion_timestamp: str | None = None,
    ) -> Any:
        from apache_beam.io.filesystems import FileSystems
        from pipelines.common.dlq import build_rejected_record

        try:
            with FileSystems.open(path) as f:
                text_file = io.TextIOWrapper(f, encoding="utf-8")
                for line in text_file:
                    line = line.strip()
                    if line:
                        try:
                            yield json.loads(line)
                        except Exception as e:
                            if source_system is None:
                                raise e
                            rejected = build_rejected_record(
                                raw_record={"raw_line": line},
                                source_system=source_system,
                                source_dataset=source_dataset or "",
                                extraction_date=extraction_date or "",
                                pipeline_run_id=pipeline_run_id or "",
                                ingestion_timestamp=ingestion_timestamp,
                                processing_stage="parse",
                                error_code="PARSE_ERROR",
                                error_message=f"JSON decode error: {str(e)}",
                                exception_type=type(e).__name__,
                            )
                            yield beam.pvalue.TaggedOutput("rejected", rejected)
        except Exception as e:
            logger.error(f"Error leyendo archivo JSONL {path}: {str(e)}")
            raise e


class TransformBronzeRecordsDoFn(beam.DoFn):
    """
    DoFn para aplicar transformaciones Bronze a Silver defensivamente.

    Los registros exitosos se emiten por la salida principal.
    Las fallas se emiten por TaggedOutput("rejected") hacia el DLQ.
    """

    def process(
        self,
        record: dict[str, Any],
        source_system: str,
        source_dataset: str,
        extraction_date: str,
        pipeline_run_id: str,
        ingestion_timestamp: str,
    ) -> Any:
        from pipelines.common.dlq import build_rejected_record
        from pipelines.dataflow_bronze_to_silver import transform_bronze_records

        try:
            transformed_list = transform_bronze_records(
                record,
                source_system=source_system,
                source_dataset=source_dataset,
                extraction_date=extraction_date,
                pipeline_run_id=pipeline_run_id,
                ingestion_timestamp=ingestion_timestamp,
            )

            # Validar claves contra el esquema Silver si está disponible
            schema_keys = None
            try:
                from pathlib import Path
                repo_root = Path(__file__).resolve().parent.parent
                schema_name = source_dataset
                if source_system == "mef":
                    from pipelines.transforms.mef import MEF_SPECS
                    spec = MEF_SPECS.get(source_dataset)
                    if spec:
                        schema_name = spec.target_dataset
                elif source_system in ("pronabec_reports", "pronabec"):
                    schema_name = f"pronabec_{source_dataset}"

                schema_path = repo_root / "config" / "schemas" / "silver" / f"{schema_name}_schema.json"
                if schema_path.exists():
                    schema_data = json.loads(schema_path.read_text(encoding="utf-8"))
                    schema_keys = {field["name"] for field in schema_data}
            except Exception as e:
                logger.warning(f"No se pudo cargar el esquema Silver para validar {source_dataset}: {str(e)}")

            for res in transformed_list:
                if not isinstance(res, dict) or not res:
                    rejected = build_rejected_record(
                        raw_record=record,
                        source_system=source_system,
                        source_dataset=source_dataset,
                        extraction_date=extraction_date,
                        pipeline_run_id=pipeline_run_id,
                        ingestion_timestamp=ingestion_timestamp,
                        processing_stage="transform",
                        error_code="INVALID_RECORD",
                        error_message="El resultado de la transformacion esta vacio o no es un diccionario.",
                    )
                    yield beam.pvalue.TaggedOutput("rejected", rejected)
                    continue

                if schema_keys:
                    res_keys = set(res.keys())
                    if not res_keys.issubset(schema_keys):
                        extra_keys = sorted(res_keys - schema_keys)
                        rejected = build_rejected_record(
                            raw_record=record,
                            source_system=source_system,
                            source_dataset=source_dataset,
                            extraction_date=extraction_date,
                            pipeline_run_id=pipeline_run_id,
                            ingestion_timestamp=ingestion_timestamp,
                            processing_stage="validation",
                            error_code="SCHEMA_MISMATCH",
                            error_message=f"El registro contiene columnas no presentes en el esquema Silver: {extra_keys}",
                            failed_field=extra_keys[0],
                            failed_value=res.get(extra_keys[0]),
                            partial_record=res,
                        )
                        yield beam.pvalue.TaggedOutput("rejected", rejected)
                        continue

                yield res

        except Exception as e:
            logger.warning(f"Fallo al transformar registro en {source_dataset}: {str(e)}")
            rejected = build_rejected_record(
                raw_record=record,
                source_system=source_system,
                source_dataset=source_dataset,
                extraction_date=extraction_date,
                pipeline_run_id=pipeline_run_id,
                ingestion_timestamp=ingestion_timestamp,
                processing_stage="transform",
                error_code="TRANSFORM_ERROR",
                error_message=str(e),
                exception_type=type(e).__name__,
            )
            yield beam.pvalue.TaggedOutput("rejected", rejected)


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
        "--write-disposition",
        required=False,
        default="WRITE_APPEND",
        help="Disposicion de escritura para BigQuery.",
    )
    parser.add_argument(
        "--create-disposition",
        required=False,
        default="CREATE_NEVER",
        help="Disposicion de creacion para BigQuery.",
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
    parser.add_argument(
        "--dlq-output-root",
        required=False,
        default="tmp/dlq",
        help="Ruta base (local o GCS) para escribir registros rechazados en el DLQ.",
    )
    parser.add_argument(
        "--disable-dlq",
        action="store_true",
        help="Desactiva la escritura de registros rechazados en el DLQ.",
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

    if not args.dry_run and not args.output_table:
        raise ValueError(
            "El argumento --output-table es requerido cuando --dry-run no esta activo."
        )

    if args.output_table:
        build_bigquery_write_config(
            args.output_table,
            write_disposition=args.write_disposition,
            create_disposition=args.create_disposition,
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


def transform_bronze_record(
    record: dict[str, Any],
    source_system: str,
    source_dataset: str,
    extraction_date: str,
    pipeline_run_id: str,
    ingestion_timestamp: str | None = None,
) -> dict[str, Any]:
    """Apply a supported Bronze to Silver transform, or metadata-only fallback."""
    if source_system == "pronabec":
        return transform_pronabec_record(
            source_dataset,
            record,
            {
                "extraction_date": extraction_date,
                "ingestion_timestamp": ingestion_timestamp,
                "pipeline_run_id": pipeline_run_id,
            },
        )
    if source_system == "mef":
        return transform_mef_record(
            source_dataset,
            record,
            {
                "extraction_date": extraction_date,
                "ingestion_timestamp": ingestion_timestamp,
                "pipeline_run_id": pipeline_run_id,
            },
        )

    return add_technical_metadata(
        record,
        source_system=source_system,
        source_dataset=source_dataset,
        extraction_date=extraction_date,
        pipeline_run_id=pipeline_run_id,
    )


def transform_bronze_records(
    record: dict[str, Any],
    source_system: str,
    source_dataset: str,
    extraction_date: str,
    pipeline_run_id: str,
    ingestion_timestamp: str | None = None,
) -> list[dict[str, Any]]:
    """Apply transforms that may emit one or more Silver records."""
    if source_system == "pronabec_reports" and source_dataset in REPORT_SPECS:
        return transform_pronabec_report_record(
            source_dataset,
            record,
            {
                "extraction_date": extraction_date,
                "ingestion_timestamp": ingestion_timestamp,
                "pipeline_run_id": pipeline_run_id,
            },
        )
    return [
        transform_bronze_record(
            record,
            source_system=source_system,
            source_dataset=source_dataset,
            extraction_date=extraction_date,
            pipeline_run_id=pipeline_run_id,
            ingestion_timestamp=ingestion_timestamp,
        )
    ]


def run(argv: list[str] | None = None) -> None:
    """
    Función principal de ejecución del pipeline.
    """
    args, pipeline_args = parse_arguments(argv)
    validate_arguments(args)

    pipeline_options = build_pipeline_options(args, pipeline_args)
    pipeline_run_id = args.pipeline_run_id or f"run_{int(datetime.now(timezone.utc).timestamp())}"
    ingestion_timestamp = datetime.now(timezone.utc).isoformat()

    log_event(
        logger,
        "INFO",
        "Iniciando pipeline Bronze a Silver",
        source_system=args.source_system,
        source_dataset=args.source_dataset,
        runner=args.runner,
        dry_run=args.dry_run,
    )

    with beam.Pipeline(options=pipeline_options) as p:
        # 1. Lectura de datos según formato
        if args.input_format == "csv":
            parsed_records = (
                p
                | "Start CSV Read" >> beam.Create([args.input_path])
                | "Read CSV File" >> beam.ParDo(ReadCsvDoFn())
            )
            parse_rejected = p | "Empty CSV Rejected" >> beam.Create([])
        else:
            jsonl_outputs = (
                p
                | "Start JSONL Read" >> beam.Create([args.input_path])
                | "Read JSONL File" >> beam.ParDo(
                    ReadJsonlDoFn(),
                    source_system=args.source_system,
                    source_dataset=args.source_dataset,
                    extraction_date=args.extraction_date,
                    pipeline_run_id=pipeline_run_id,
                    ingestion_timestamp=ingestion_timestamp,
                ).with_outputs("rejected", main="main")
            )
            parsed_records = jsonl_outputs.main
            parse_rejected = jsonl_outputs.rejected

        # 2. Transformación con control de errores a DLQ
        transform_outputs = (
            parsed_records
            | "Apply Bronze to Silver Transform" >> beam.ParDo(
                TransformBronzeRecordsDoFn(),
                source_system=args.source_system,
                source_dataset=args.source_dataset,
                extraction_date=args.extraction_date,
                pipeline_run_id=pipeline_run_id,
                ingestion_timestamp=ingestion_timestamp,
            ).with_outputs("rejected", main="main")
        )
        transformed = transform_outputs.main
        transform_rejected = transform_outputs.rejected

        # Fusionar errores de parseo y transformación
        rejected_records = (
            (parse_rejected, transform_rejected)
            | "Merge Rejected Records" >> beam.Flatten()
        )

        # 3. Escritura o Consumo final de válidos
        if args.dry_run:
            (
                transformed
                | "Log Records (Dry-Run)" >> beam.Map(
                    lambda x: logger.info(f"Registro procesado (Dry-run): {x}")
                )
            )
        else:
            bq_config = build_bigquery_write_config(
                args.output_table,
                write_disposition=args.write_disposition,
                create_disposition=args.create_disposition,
            )
            log_event(
                logger,
                "INFO",
                "Escribiendo registros Silver en BigQuery",
                output_table=bq_config.output_table,
                write_disposition=bq_config.write_disposition,
                create_disposition=bq_config.create_disposition,
            )
            (
                transformed
                | "Write Silver Records to BigQuery" >> build_bigquery_write_transform(
                    bq_config
                )
            )

        # 4. Escritura al DLQ
        if not args.disable_dlq:
            from pipelines.common.dlq import build_dlq_path, serialize_rejected_record
            dlq_path = build_dlq_path(
                output_root=args.dlq_output_root,
                source_system=args.source_system,
                source_dataset=args.source_dataset,
                extraction_date=args.extraction_date,
            )
            
            # Serialize y escribir
            serialized_rejected = (
                rejected_records
                | "Serialize Rejected" >> beam.Map(serialize_rejected_record)
            )
            
            prefix = dlq_path.removesuffix(".jsonl")
            (
                serialized_rejected
                | "Write Rejected to DLQ" >> beam.io.WriteToText(
                    prefix,
                    file_name_suffix=".jsonl",
                    shard_name_template="",
                )
            )


if __name__ == "__main__":
    run()
