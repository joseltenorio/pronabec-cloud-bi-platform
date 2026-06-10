-- ============================================================================
-- Project Cloud BI Platform
-- BigQuery Bronze external tables
-- ============================================================================
--
-- Este script define tablas externas sobre archivos almacenados en Cloud Storage
-- dentro de la capa Bronze.
--
-- Decisión de diseño:
-- - PRONABEC se consulta desde archivos JSONL normalizados estructuralmente
--   generados a partir de la respuesta JSON paginada rows[].cell.
-- - PRONABEC conserva adicionalmente data_raw.json para trazabilidad, pero la
--   tabla externa principal se define sobre data.jsonl.
-- - MEF se define inicialmente sobre data.csv porque el scraper controlado
--   genera una salida tabular.
-- - La capa Bronze prioriza trazabilidad y lectura inicial. La conversión fuerte
--   de tipos se realizará en Silver.
--
-- Reemplazar antes de ejecutar:
-- - your-gcp-project-id
-- - project-cloud-bi-platform-lake
-- ============================================================================

-- ============================================================================
-- PRONABEC: notas_becarios
-- Fuente Bronze:
-- gs://<bucket>/bronze/pronabec/notas_becarios/extraction_date=YYYY-MM-DD/data.jsonl
-- ============================================================================

CREATE OR REPLACE EXTERNAL TABLE `your-gcp-project-id.bronze.pronabec_notas_becarios_raw`
(
  source_row_id STRING,
  nro_fila STRING,
  codigo_becario STRING,
  semestre STRING,
  ciclo STRING,
  nota_promedio STRING,
  fecha_carga STRING
)
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://project-cloud-bi-platform-lake/bronze/pronabec/notas_becarios/extraction_date=*/data.jsonl'],
  ignore_unknown_values = TRUE,
  max_bad_records = 0
);

-- ============================================================================
-- PRONABEC: perdida_becas
-- ============================================================================

CREATE OR REPLACE EXTERNAL TABLE `your-gcp-project-id.bronze.pronabec_perdida_becas_raw`
(
  source_row_id STRING,
  nro_fila STRING,
  convocatoria STRING,
  departamento STRING,
  motivo_perdida STRING,
  tipo_resolucion STRING,
  fecha_resolucion STRING,
  fecha_inicio_beca STRING,
  tipo_ies STRING,
  institucion STRING,
  sede STRING,
  carrera STRING,
  sexo STRING,
  fecha_carga STRING
)
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://project-cloud-bi-platform-lake/bronze/pronabec/perdida_becas/extraction_date=*/data.jsonl'],
  ignore_unknown_values = TRUE,
  max_bad_records = 0
);

-- ============================================================================
-- PRONABEC: convocatorias
-- ============================================================================

CREATE OR REPLACE EXTERNAL TABLE `your-gcp-project-id.bronze.pronabec_convocatorias_raw`
(
  source_row_id STRING,
  nro_fila STRING,
  id_convocatoria STRING,
  codigo_convocatoria STRING,
  nombre_convocatoria STRING,
  nombre_programa STRING,
  modalidad STRING,
  vacantes STRING,
  etapas STRING,
  fecha_fin_convocatoria STRING,
  fecha_inicio_postulacion STRING,
  fecha_fin_postulacion STRING,
  fecha_inicio_evaluacion STRING,
  fecha_fin_evaluacion STRING,
  fecha_inicio_vigencia STRING,
  fecha_fin_vigencia STRING,
  edad_minima STRING,
  edad_maxima STRING,
  resolucion STRING,
  fecha_carga STRING
)
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://project-cloud-bi-platform-lake/bronze/pronabec/convocatorias/extraction_date=*/data.jsonl'],
  ignore_unknown_values = TRUE,
  max_bad_records = 0
);

-- ============================================================================
-- PRONABEC: concepto_pago
-- ============================================================================

CREATE OR REPLACE EXTERNAL TABLE `your-gcp-project-id.bronze.pronabec_concepto_pago_raw`
(
  source_row_id STRING,
  nro_fila STRING,
  tipo_subvencion STRING,
  concepto STRING,
  subconcepto STRING,
  modalidad STRING,
  estado STRING,
  aplica_descuento STRING,
  fecha_carga STRING
)
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://project-cloud-bi-platform-lake/bronze/pronabec/concepto_pago/extraction_date=*/data.jsonl'],
  ignore_unknown_values = TRUE,
  max_bad_records = 0
);

-- ============================================================================
-- PRONABEC: becarios_provincia
-- ============================================================================

CREATE OR REPLACE EXTERNAL TABLE `your-gcp-project-id.bronze.pronabec_becarios_provincia_raw`
(
  source_row_id STRING,
  nro_fila STRING,
  departamento STRING,
  provincia STRING,
  b18_n STRING,
  b18_pct STRING,
  permanencia_n STRING,
  permanencia_pct STRING,
  bicentenario_n STRING,
  bicentenario_pct STRING,
  especial_n STRING,
  especial_pct STRING,
  ffaa_n STRING,
  ffaa_pct STRING,
  vraem_n STRING,
  vraem_pct STRING,
  repec_n STRING,
  repec_pct STRING,
  internacional_n STRING,
  internacional_pct STRING,
  otros_n STRING,
  otros_pct STRING,
  fecha_carga STRING
)
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://project-cloud-bi-platform-lake/bronze/pronabec/becarios_provincia/extraction_date=*/data.jsonl'],
  ignore_unknown_values = TRUE,
  max_bad_records = 0
);

-- ============================================================================
-- PRONABEC: ubigeo_postulacion
-- ============================================================================

CREATE OR REPLACE EXTERNAL TABLE `your-gcp-project-id.bronze.pronabec_ubigeo_postulacion_raw`
(
  source_row_id STRING,
  nro_fila STRING,
  codigo_ubigeo STRING,
  departamento STRING,
  provincia STRING,
  distrito STRING,
  fecha_carga STRING
)
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://project-cloud-bi-platform-lake/bronze/pronabec/ubigeo_postulacion/extraction_date=*/data.jsonl'],
  ignore_unknown_values = TRUE,
  max_bad_records = 0
);

-- ============================================================================
-- PRONABEC: periodos_academicos
-- ============================================================================

CREATE OR REPLACE EXTERNAL TABLE `your-gcp-project-id.bronze.pronabec_periodos_academicos_raw`
(
  source_row_id STRING,
  nro_fila STRING,
  anio STRING,
  mes_numero STRING,
  periodo_completo STRING,
  mes_nombre STRING,
  fecha_carga STRING
)
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://project-cloud-bi-platform-lake/bronze/pronabec/periodos_academicos/extraction_date=*/data.jsonl'],
  ignore_unknown_values = TRUE,
  max_bad_records = 0
);

-- ============================================================================
-- MEF: presupuesto
-- Fuente Bronze:
-- gs://<bucket>/bronze/mef/presupuesto/extraction_date=YYYY-MM-DD/data.csv
-- ============================================================================

CREATE OR REPLACE EXTERNAL TABLE `your-gcp-project-id.bronze.mef_presupuesto_raw`
(
  ano STRING,
  ejecutora_nombre STRING,
  pia STRING,
  pim STRING,
  certificacion STRING,
  compromiso_anual STRING,
  compromiso_mensual STRING,
  devengado STRING,
  girado STRING,
  avance_porcentaje STRING
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://project-cloud-bi-platform-lake/bronze/mef/presupuesto/extraction_date=*/data.csv'],
  skip_leading_rows = 1,
  field_delimiter = ',',
  allow_quoted_newlines = TRUE,
  allow_jagged_rows = TRUE,
  max_bad_records = 0
);