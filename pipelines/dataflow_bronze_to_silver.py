"""
Pipeline principal de Apache Beam / Dataflow para procesar datos de Bronze a Silver.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
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

ENV_PLACEHOLDER_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


class ReadCsvDoFn(beam.DoFn):
    """DoFn para leer un archivo CSV desde almacenamiento local o GCS."""

    def __init__(self) -> None:
        super().__init__()
        # Inicializar métrica para contar filas leídas en el CSV
        self.read_counter = beam.metrics.Metrics.counter("dataflow_metrics", "records_read")

    def process(self, path: str) -> Any:
        from apache_beam.io.filesystems import FileSystems

        try:
            with FileSystems.open(path) as f:
                text_file = io.TextIOWrapper(f, encoding="utf-8-sig")
                reader = csv.DictReader(text_file)
                for row in reader:
                    self.read_counter.inc()
                    yield dict(row)
        except Exception as e:
            logger.error(f"Error leyendo archivo CSV {path}: {str(e)}")
            raise e


class ReadJsonlDoFn(beam.DoFn):
    """DoFn para leer un archivo JSONL desde almacenamiento local o GCS."""

    def __init__(self) -> None:
        super().__init__()
        # Inicializar métricas para contar líneas leídas y rechazos en parseo
        self.read_counter = beam.metrics.Metrics.counter("dataflow_metrics", "records_read")
        self.rejected_counter = beam.metrics.Metrics.counter("dataflow_metrics", "records_rejected")

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
                        self.read_counter.inc()
                        try:
                            yield json.loads(line)
                        except Exception as e:
                            self.rejected_counter.inc()
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

    def __init__(self) -> None:
        super().__init__()
        # Inicializar métricas para registros válidos y rechazados
        self.valid_counter = beam.metrics.Metrics.counter("dataflow_metrics", "records_valid")
        self.rejected_counter = beam.metrics.Metrics.counter("dataflow_metrics", "records_rejected")

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
                    self.rejected_counter.inc()
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
                        self.rejected_counter.inc()
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

                self.valid_counter.inc()
                yield res

        except Exception as e:
            logger.warning(f"Fallo al transformar registro en {source_dataset}: {str(e)}")
            self.rejected_counter.inc()
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
    parser.add_argument(
        "--summary-output-path",
        required=False,
        default=None,
        help="Ruta local o GCS para escribir el resumen de procesamiento en formato JSON.",
    )

    args, pipeline_args = parser.parse_known_args(argv)
    return args, pipeline_args


def expand_env_placeholders(value: str | None) -> str | None:
    """Expand ${VAR} placeholders in Cloud Run job args before validation."""
    if value is None:
        return None

    def replace(match: re.Match[str]) -> str:
        env_name = match.group(1)
        env_value = os.getenv(env_name)
        if env_value is None:
            raise ValueError(f"Variable de entorno requerida no definida: {env_name}")
        return env_value

    return ENV_PLACEHOLDER_PATTERN.sub(replace, value)


def resolve_runtime_arguments(args: argparse.Namespace) -> argparse.Namespace:
    """Resolve Cloud Run environment-driven arguments used by launcher jobs."""
    env_defaults = {
        "source_dataset": "SOURCE_DATASET",
        "extraction_date": "BRONZE_EXTRACTION_DATE",
        "input_path": "INPUT_PATH",
        "output_table": "OUTPUT_TABLE",
        "pipeline_run_id": "PIPELINE_RUN_ID",
    }

    for field, env_name in env_defaults.items():
        current_value = getattr(args, field, None)
        if current_value is None or str(current_value).strip() == "":
            env_value = os.getenv(env_name)
            if env_value:
                setattr(args, field, env_value)

    expandable_fields = [
        "source_dataset",
        "extraction_date",
        "input_path",
        "output_table",
        "pipeline_run_id",
        "summary_output_path",
    ]
    for field in expandable_fields:
        value = getattr(args, field, None)
        if isinstance(value, str):
            setattr(args, field, expand_env_placeholders(value))

    return args


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

    if not args.dry_run:
        if not args.output_table:
            raise ValueError(
                "El argumento --output-table es requerido cuando --dry-run no esta activo."
            )
        if not args.temp_location or not str(args.temp_location).strip():
            raise ValueError(
                "El argumento --temp-location es requerido cuando --dry-run no está activo y se escribe en BigQuery."
            )

    if args.output_table:
        build_bigquery_write_config(
            args.output_table,
            write_disposition=args.write_disposition,
            create_disposition=args.create_disposition,
            custom_gcs_temp_location=args.temp_location,
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
    if args.temp_location:
        options_dict["temp_location"] = args.temp_location

    project_id = args.project
    if not project_id and args.output_table:
        try:
            project_id = args.output_table.split(":")[0]
        except Exception:
            pass

    if project_id:
        options_dict["project"] = project_id

    if args.runner == "DataflowRunner":
        options_dict.update({
            "region": args.region,
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
) -> dict[str, Any] | None:
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
    transformed = transform_bronze_record(
        record,
        source_system=source_system,
        source_dataset=source_dataset,
        extraction_date=extraction_date,
        pipeline_run_id=pipeline_run_id,
        ingestion_timestamp=ingestion_timestamp,
    )
    return [
        transformed
        for transformed in [transformed]
        if transformed is not None
    ]


def run(argv: list[str] | None = None) -> None:
    """
    Función principal de ejecución del pipeline.
    """
    # Registrar marca de tiempo de inicio
    started_at = datetime.now(timezone.utc)

    # Intentar parsear argumentos de entrada de forma segura
    try:
        args, pipeline_args = parse_arguments(argv)
        args = resolve_runtime_arguments(args)
        # Asegurar que el ID de proyecto esté en el entorno para DirectRunner
        project_id = args.project
        if not project_id and args.output_table:
            try:
                if ":" in args.output_table:
                    project_id = args.output_table.split(":")[0]
            except Exception:
                pass
        if project_id:
            os.environ["GOOGLE_CLOUD_PROJECT"] = project_id
    except Exception as e:
        # En caso de fallo grave de parseo, registrar estado FAILED
        summary = {
            "source_system": None,
            "source_dataset": None,
            "extraction_date": None,
            "pipeline_run_id": f"run_{int(started_at.timestamp())}",
            "input_format": None,
            "dry_run": True,
            "dlq_enabled": False,
            "records_read": 0,
            "records_valid": 0,
            "records_rejected": 0,
            "rejection_rate": 0.0,
            "started_at": started_at.isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "duration_seconds": 0.0,
            "status": "FAILED",
            "error_message": f"Error parseando argumentos: {str(e)}",
        }
        logger.error(f"Fallo al parsear argumentos: {str(e)}")
        logger.info(f"Resumen de procesamiento:\n{json.dumps(summary, indent=4, ensure_ascii=False)}")
        raise e

    pipeline_run_id = args.pipeline_run_id or f"run_{int(started_at.timestamp())}"
    ingestion_timestamp = started_at.isoformat()

    # Construir ruta dlq de forma determinista para el resumen
    dlq_path = None
    if args.source_system and args.source_dataset and args.extraction_date:
        from pipelines.common.dlq import build_dlq_path
        try:
            dlq_path = build_dlq_path(
                output_root=args.dlq_output_root or "tmp/dlq",
                source_system=args.source_system,
                source_dataset=args.source_dataset,
                extraction_date=args.extraction_date,
            )
        except Exception:
            pass

    status = "COMPLETED"
    error_message = None
    read_count = 0
    valid_count = 0
    rejected_count = 0

    try:
        # Validar argumentos de entrada
        validate_arguments(args)

        pipeline_options = build_pipeline_options(args, pipeline_args)

        log_event(
            logger,
            "INFO",
            "Iniciando pipeline Bronze a Silver",
            source_system=args.source_system,
            source_dataset=args.source_dataset,
            runner=args.runner,
            dry_run=args.dry_run,
        )

        p = beam.Pipeline(options=pipeline_options)

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
                custom_gcs_temp_location=args.temp_location,
            )
            log_event(
                logger,
                "INFO",
                "Escribiendo registros Silver en BigQuery",
                output_table=bq_config.output_table,
                write_disposition=bq_config.write_disposition,
                create_disposition=bq_config.create_disposition,
                custom_gcs_temp_location=bq_config.custom_gcs_temp_location,
            )
            (
                transformed
                | "Write Silver Records to BigQuery" >> build_bigquery_write_transform(
                    bq_config
                )
            )

        # 4. Escritura al DLQ si no está deshabilitado
        if not args.disable_dlq:
            from pipelines.common.dlq import serialize_rejected_record
            
            # Serializar y escribir
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

        # Ejecutar el pipeline de forma síncrona
        result = p.run()
        result.wait_until_finish()

        # Consultar las métricas de Beam
        try:
            metrics = result.metrics().query(beam.metrics.MetricsFilter().with_namespace("dataflow_metrics"))
            
            def get_counter_value(counter) -> int:
                committed = counter.committed
                attempted = counter.attempted
                vals = [v for v in (committed, attempted) if v is not None]
                return max(vals) if vals else 0

            for counter in metrics["counters"]:
                name = counter.key.metric.name
                val = get_counter_value(counter)
                if name == "records_read":
                    read_count = val
                elif name == "records_valid":
                    valid_count = val
                elif name == "records_rejected":
                    rejected_count = val
        except Exception as me:
            logger.warning(f"No se pudieron extraer las métricas de Beam: {str(me)}")

        # Determinar el estado final según los registros rechazados
        if rejected_count > 0:
            status = "COMPLETED_WITH_REJECTIONS"

    except Exception as e:
        status = "FAILED"
        error_message = str(e)
        logger.error(f"El pipeline falló de forma fatal: {error_message}")
        raise e
    finally:
        # Registrar fin y calcular duración
        finished_at = datetime.now(timezone.utc)
        duration_seconds = (finished_at - started_at).total_seconds()

        rejection_rate = 0.0
        if read_count > 0:
            rejection_rate = float(rejected_count) / read_count

        # Construir estructura del resumen de procesamiento
        summary = {
            "source_system": getattr(args, "source_system", None),
            "source_dataset": getattr(args, "source_dataset", None),
            "extraction_date": getattr(args, "extraction_date", None),
            "pipeline_run_id": pipeline_run_id,
            "input_format": getattr(args, "input_format", None),
            "input_path": getattr(args, "input_path", None),
            "output_table": getattr(args, "output_table", None),
            "dry_run": getattr(args, "dry_run", True),
            "dlq_enabled": not getattr(args, "disable_dlq", False),
            "dlq_output_path": dlq_path if not getattr(args, "disable_dlq", False) else None,
            "records_read": read_count,
            "records_valid": valid_count,
            "records_rejected": rejected_count,
            "rejection_rate": round(rejection_rate, 6),
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "duration_seconds": round(duration_seconds, 2),
            "status": status,
        }

        if error_message:
            summary["error_message"] = error_message

        # Persistir o registrar el resumen
        summary_path = getattr(args, "summary_output_path", None)
        if summary_path:
            if not summary_path.startswith("gs://"):
                from pathlib import Path
                try:
                    Path(summary_path).parent.mkdir(parents=True, exist_ok=True)
                except Exception as pe:
                    logger.warning(f"No se pudo crear el directorio para el resumen: {str(pe)}")
            
            try:
                from apache_beam.io.filesystems import FileSystems
                with FileSystems.create(summary_path) as f:
                    f.write(json.dumps(summary, indent=4, ensure_ascii=False).encode("utf-8"))
                logger.info(f"Resumen de procesamiento escrito en {summary_path}")
            except Exception as se:
                logger.error(f"No se pudo escribir el resumen de procesamiento en {summary_path}: {str(se)}")
        else:
            logger.info(f"Resumen de procesamiento:\n{json.dumps(summary, indent=4, ensure_ascii=False)}")


if __name__ == "__main__":
    run()
