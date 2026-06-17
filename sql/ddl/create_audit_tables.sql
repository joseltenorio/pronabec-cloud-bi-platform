-- ============================================================================
-- Project Cloud BI Platform
-- BigQuery Audit table definitions
-- ============================================================================
--
-- Este script define las tablas de auditoría operacional del proyecto.
--
-- Decisión de diseño:
-- - Las tablas Audit permiten registrar ejecuciones del pipeline, extracciones,
--   resultados de calidad y ejecuciones de Dataflow.
-- - Estas tablas serán pobladas posteriormente por utilidades Python, Cloud Run
--   Jobs, Dataflow y Cloud Composer.
-- - Audit no reemplaza Cloud Logging ni Cloud Monitoring; los complementa con
--   registros consultables desde SQL y Power BI si se requiere.
--
-- Reemplazar antes de ejecutar:
-- - your-gcp-project-id
-- ============================================================================

-- ============================================================================
-- Tabla: audit.pipeline_runs
-- Propósito:
-- Registrar la ejecución general de un pipeline batch.
-- ============================================================================

CREATE OR REPLACE TABLE `your-gcp-project-id.audit.pipeline_runs`
(
  run_id STRING OPTIONS(description = "Identificador único de la ejecución del pipeline."),
  pipeline_name STRING OPTIONS(description = "Nombre lógico del pipeline."),
  environment STRING OPTIONS(description = "Ambiente de ejecución: dev, test o prod."),
  execution_date DATE OPTIONS(description = "Fecha lógica de ejecución del pipeline."),
  status STRING OPTIONS(description = "Estado final o actual de la ejecución: STARTED, SUCCESS, FAILED, WARNING."),
  trigger_type STRING OPTIONS(description = "Origen de la ejecución: manual, scheduled, ci_cd o retry."),
  orchestrator STRING OPTIONS(description = "Orquestador utilizado, por ejemplo Cloud Composer, local o GitHub Actions."),

  started_at TIMESTAMP OPTIONS(description = "Timestamp de inicio de la ejecución."),
  finished_at TIMESTAMP OPTIONS(description = "Timestamp de fin de la ejecución."),
  duration_seconds INT64 OPTIONS(description = "Duración total de la ejecución en segundos."),

  total_tasks INT64 OPTIONS(description = "Cantidad total de tareas esperadas."),
  successful_tasks INT64 OPTIONS(description = "Cantidad de tareas exitosas."),
  failed_tasks INT64 OPTIONS(description = "Cantidad de tareas fallidas."),

  error_message STRING OPTIONS(description = "Mensaje de error principal, si aplica."),
  metadata JSON OPTIONS(description = "Metadatos adicionales de ejecución en formato JSON."),

  created_at TIMESTAMP OPTIONS(description = "Timestamp de creación del registro de auditoría.")
)
PARTITION BY execution_date
CLUSTER BY pipeline_name, status
OPTIONS (
  description = "Auditoría de ejecuciones generales del pipeline batch."
);

-- ============================================================================
-- Tabla: audit.extraction_runs
-- Propósito:
-- Registrar cada extracción desde fuentes públicas hacia Cloud Storage Bronze.
-- ============================================================================

CREATE OR REPLACE TABLE `your-gcp-project-id.audit.extraction_runs`
(
  extraction_run_id STRING OPTIONS(description = "Identificador único de la ejecución de extracción."),
  run_id STRING OPTIONS(description = "Identificador de la ejecución general del pipeline."),
  pipeline_name STRING OPTIONS(description = "Nombre lógico del pipeline."),
  environment STRING OPTIONS(description = "Ambiente de ejecución."),
  source_name STRING OPTIONS(description = "Fuente de datos, por ejemplo PRONABEC Datos Abiertos o MEF Consulta Amigable."),
  source_dataset STRING OPTIONS(description = "Dataset o recurso extraído."),
  extraction_date DATE OPTIONS(description = "Fecha lógica de extracción."),

  target_bucket STRING OPTIONS(description = "Bucket de Cloud Storage destino."),
  target_path STRING OPTIONS(description = "Ruta destino del archivo en Cloud Storage."),
  output_format STRING OPTIONS(description = "Formato generado: JSON, JSONL, CSV u otro."),

  records_read INT64 OPTIONS(description = "Cantidad de registros leídos desde la fuente."),
  records_written INT64 OPTIONS(description = "Cantidad de registros escritos en Bronze."),
  pages_read INT64 OPTIONS(description = "Cantidad de páginas leídas, si la fuente es paginada."),
  bytes_written INT64 OPTIONS(description = "Cantidad aproximada de bytes escritos."),

  status STRING OPTIONS(description = "Estado de la extracción: STARTED, SUCCESS, FAILED, WARNING."),
  error_message STRING OPTIONS(description = "Mensaje de error, si aplica."),

  started_at TIMESTAMP OPTIONS(description = "Timestamp de inicio de extracción."),
  finished_at TIMESTAMP OPTIONS(description = "Timestamp de fin de extracción."),
  duration_seconds INT64 OPTIONS(description = "Duración de extracción en segundos."),

  metadata JSON OPTIONS(description = "Metadatos adicionales de extracción en formato JSON."),
  created_at TIMESTAMP OPTIONS(description = "Timestamp de creación del registro de auditoría.")
)
PARTITION BY extraction_date
CLUSTER BY source_name, source_dataset, status
OPTIONS (
  description = "Auditoría de extracciones desde fuentes públicas hacia Cloud Storage Bronze."
);

-- ============================================================================
-- Tabla: audit.dataflow_runs
-- Propósito:
-- Registrar ejecuciones de Dataflow/Apache Beam para Bronze -> Silver.
-- ============================================================================

CREATE OR REPLACE TABLE `your-gcp-project-id.audit.dataflow_runs`
(
  dataflow_run_id STRING OPTIONS(description = "Identificador lógico interno de ejecución Dataflow."),
  run_id STRING OPTIONS(description = "Identificador de la ejecución general del pipeline."),
  pipeline_name STRING OPTIONS(description = "Nombre lógico del pipeline."),
  environment STRING OPTIONS(description = "Ambiente de ejecución."),

  dataflow_job_id STRING OPTIONS(description = "Identificador del job de Dataflow en Google Cloud."),
  dataflow_job_name STRING OPTIONS(description = "Nombre del job de Dataflow."),
  runner STRING OPTIONS(description = "Runner utilizado: DirectRunner o DataflowRunner."),
  region STRING OPTIONS(description = "Región de ejecución de Dataflow."),

  source_dataset STRING OPTIONS(description = "Dataset fuente procesado."),
  target_table STRING OPTIONS(description = "Tabla destino en BigQuery Silver."),
  extraction_date DATE OPTIONS(description = "Fecha lógica de extracción procesada."),

  records_read INT64 OPTIONS(description = "Cantidad de registros leídos desde Bronze."),
  records_valid INT64 OPTIONS(description = "Cantidad de registros válidos."),
  records_invalid INT64 OPTIONS(description = "Cantidad de registros inválidos."),
  records_written INT64 OPTIONS(description = "Cantidad de registros escritos en Silver."),
  rejected_records_path STRING OPTIONS(description = "Ruta DLQ con registros rechazados, si aplica."),

  status STRING OPTIONS(description = "Estado del job: STARTED, SUCCESS, FAILED, WARNING."),
  error_message STRING OPTIONS(description = "Mensaje de error, si aplica."),

  started_at TIMESTAMP OPTIONS(description = "Timestamp de inicio del job."),
  finished_at TIMESTAMP OPTIONS(description = "Timestamp de fin del job."),
  duration_seconds INT64 OPTIONS(description = "Duración del job en segundos."),

  metadata JSON OPTIONS(description = "Metadatos adicionales del job en formato JSON."),
  created_at TIMESTAMP OPTIONS(description = "Timestamp de creación del registro de auditoría.")
)
PARTITION BY extraction_date
CLUSTER BY source_dataset, status, dataflow_job_name
OPTIONS (
  description = "Auditoría de ejecuciones Dataflow para transformación Bronze a Silver."
);

-- ============================================================================
-- Tabla: audit.data_quality_results
-- Propósito:
-- Registrar resultados de validaciones de calidad de datos.
-- ============================================================================

CREATE OR REPLACE TABLE `your-gcp-project-id.audit.data_quality_results`
(
  quality_run_id STRING OPTIONS(description = "Identificador único de la corrida de calidad."),
  pipeline_run_id STRING OPTIONS(description = "Identificador de la ejecución general del pipeline."),
  execution_timestamp TIMESTAMP OPTIONS(description = "Timestamp de ejecución del check."),
  check_id STRING OPTIONS(description = "Identificador único de la regla de calidad."),
  layer STRING OPTIONS(description = "Capa de datos: bronze, silver, gold."),
  table_name STRING OPTIONS(description = "Tabla evaluada."),
  severity STRING OPTIONS(description = "Severidad del check: INFO, WARNING, ERROR."),
  passed BOOL OPTIONS(description = "Indica si el check es exitoso (true) o fallido (false)."),
  failed_rows INT64 OPTIONS(description = "Cantidad de registros que fallan la regla."),
  details STRING OPTIONS(description = "Mensaje detallado con la justificación del resultado."),
  query_file STRING OPTIONS(description = "Archivo SQL de donde proviene el check."),
  source_system STRING OPTIONS(description = "Sistema origen de los datos."),
  source_dataset STRING OPTIONS(description = "Dataset origen de los datos.")
)
PARTITION BY DATE(execution_timestamp)
CLUSTER BY layer, table_name, severity
OPTIONS (
  description = "Resultados detallados de validaciones de calidad de datos."
);