-- ============================================================================
-- Project Cloud BI Platform
-- BigQuery Silver normalized tables
-- ============================================================================
--
-- Este script define las tablas Silver del proyecto.
--
-- Decisión de diseño:
-- - Bronze conserva datos crudos o normalizados estructuralmente.
-- - Silver almacena datos limpios, tipados, normalizados y listos para consumo
--   por Gold, calidad de datos, Power BI y BigQuery ML.
-- - Las tablas Silver serán pobladas posteriormente por Dataflow/Apache Beam.
-- - Todas las tablas incorporan campos de auditoría técnica para trazabilidad.
--
-- Reemplazar antes de ejecutar:
-- - your-gcp-project-id
-- ============================================================================

-- ============================================================================
-- PRONABEC: notas_becarios
-- ============================================================================

CREATE OR REPLACE TABLE `your-gcp-project-id.silver.notas_becarios`
(
  source_row_id STRING OPTIONS(description = "Identificador técnico de la fila en la fuente."),
  nro_fila INT64 OPTIONS(description = "Número de fila reportado por la fuente."),
  codigo_becario STRING OPTIONS(description = "Código del becario en la fuente."),
  semestre STRING OPTIONS(description = "Semestre académico registrado."),
  ciclo STRING OPTIONS(description = "Ciclo académico del becario."),
  nota_promedio NUMERIC OPTIONS(description = "Nota promedio normalizada a número decimal."),
  fecha_carga TIMESTAMP OPTIONS(description = "Fecha y hora de carga reportada por la fuente."),

  source_system STRING OPTIONS(description = "Sistema fuente del registro."),
  source_dataset STRING OPTIONS(description = "Dataset fuente del registro."),
  extraction_date DATE OPTIONS(description = "Fecha lógica de extracción del pipeline."),
  ingestion_timestamp TIMESTAMP OPTIONS(description = "Timestamp de escritura en Silver."),
  pipeline_run_id STRING OPTIONS(description = "Identificador de ejecución del pipeline.")
)
PARTITION BY extraction_date
CLUSTER BY codigo_becario, semestre
OPTIONS (
  description = "Notas promedio de becarios normalizadas para análisis académico y riesgo."
);

-- ============================================================================
-- PRONABEC: perdida_becas
-- ============================================================================

CREATE OR REPLACE TABLE `your-gcp-project-id.silver.perdida_becas`
(
  source_row_id STRING OPTIONS(description = "Identificador técnico de la fila en la fuente."),
  nro_fila INT64 OPTIONS(description = "Número de fila reportado por la fuente."),
  convocatoria STRING OPTIONS(description = "Convocatoria asociada al registro."),
  departamento STRING OPTIONS(description = "Departamento, región o valor territorial reportado."),
  motivo_perdida STRING OPTIONS(description = "Motivo de pérdida de beca."),
  tipo_resolucion STRING OPTIONS(description = "Tipo de resolución administrativa, si existe."),
  fecha_resolucion DATE OPTIONS(description = "Fecha de resolución, si está disponible."),
  fecha_inicio_beca DATE OPTIONS(description = "Fecha de inicio de beca, si está disponible."),
  tipo_ies STRING OPTIONS(description = "Tipo de institución educativa superior."),
  institucion STRING OPTIONS(description = "Institución educativa superior."),
  sede STRING OPTIONS(description = "Sede educativa."),
  carrera STRING OPTIONS(description = "Carrera del becario."),
  sexo STRING OPTIONS(description = "Sexo reportado en la fuente."),
  fecha_carga TIMESTAMP OPTIONS(description = "Fecha y hora de carga reportada por la fuente."),

  source_system STRING OPTIONS(description = "Sistema fuente del registro."),
  source_dataset STRING OPTIONS(description = "Dataset fuente del registro."),
  extraction_date DATE OPTIONS(description = "Fecha lógica de extracción del pipeline."),
  ingestion_timestamp TIMESTAMP OPTIONS(description = "Timestamp de escritura en Silver."),
  pipeline_run_id STRING OPTIONS(description = "Identificador de ejecución del pipeline.")
)
PARTITION BY extraction_date
CLUSTER BY departamento, convocatoria
OPTIONS (
  description = "Registros normalizados de pérdida de becas para análisis de deserción o pérdida."
);

-- ============================================================================
-- PRONABEC: convocatorias
-- ============================================================================

CREATE OR REPLACE TABLE `your-gcp-project-id.silver.convocatorias`
(
  source_row_id STRING OPTIONS(description = "Identificador técnico de la fila en la fuente."),
  nro_fila INT64 OPTIONS(description = "Número de fila reportado por la fuente."),
  id_convocatoria INT64 OPTIONS(description = "Identificador de convocatoria."),
  codigo_convocatoria STRING OPTIONS(description = "Código de convocatoria."),
  nombre_convocatoria STRING OPTIONS(description = "Nombre de la convocatoria."),
  nombre_programa STRING OPTIONS(description = "Nombre del programa asociado."),
  modalidad STRING OPTIONS(description = "Modalidad reportada por la fuente."),
  vacantes INT64 OPTIONS(description = "Cantidad de vacantes."),
  etapas INT64 OPTIONS(description = "Cantidad de etapas."),
  fecha_fin_convocatoria DATE OPTIONS(description = "Fecha de fin de convocatoria."),
  fecha_inicio_postulacion DATE OPTIONS(description = "Fecha de inicio de postulación."),
  fecha_fin_postulacion DATE OPTIONS(description = "Fecha de fin de postulación."),
  fecha_inicio_evaluacion DATE OPTIONS(description = "Fecha de inicio de evaluación."),
  fecha_fin_evaluacion DATE OPTIONS(description = "Fecha de fin de evaluación."),
  fecha_inicio_vigencia DATE OPTIONS(description = "Fecha de inicio de vigencia."),
  fecha_fin_vigencia DATE OPTIONS(description = "Fecha de fin de vigencia."),
  edad_minima INT64 OPTIONS(description = "Edad mínima permitida."),
  edad_maxima INT64 OPTIONS(description = "Edad máxima permitida."),
  resolucion STRING OPTIONS(description = "Resolución asociada a la convocatoria."),
  fecha_carga TIMESTAMP OPTIONS(description = "Fecha y hora de carga reportada por la fuente."),

  source_system STRING OPTIONS(description = "Sistema fuente del registro."),
  source_dataset STRING OPTIONS(description = "Dataset fuente del registro."),
  extraction_date DATE OPTIONS(description = "Fecha lógica de extracción del pipeline."),
  ingestion_timestamp TIMESTAMP OPTIONS(description = "Timestamp de escritura en Silver."),
  pipeline_run_id STRING OPTIONS(description = "Identificador de ejecución del pipeline.")
)
PARTITION BY extraction_date
CLUSTER BY nombre_programa, modalidad
OPTIONS (
  description = "Convocatorias normalizadas con fechas, vacantes, etapas y modalidad."
);

-- ============================================================================
-- PRONABEC: concepto_pago
-- ============================================================================

CREATE OR REPLACE TABLE `your-gcp-project-id.silver.concepto_pago`
(
  source_row_id STRING OPTIONS(description = "Identificador técnico de la fila en la fuente."),
  nro_fila INT64 OPTIONS(description = "Número de fila reportado por la fuente."),
  tipo_subvencion STRING OPTIONS(description = "Tipo de subvención."),
  concepto STRING OPTIONS(description = "Concepto de pago."),
  subconcepto STRING OPTIONS(description = "Subconcepto de pago."),
  modalidad STRING OPTIONS(description = "Modalidad asociada."),
  estado STRING OPTIONS(description = "Estado del concepto."),
  aplica_descuento STRING OPTIONS(description = "Indicador de descuento reportado por la fuente."),
  fecha_carga TIMESTAMP OPTIONS(description = "Fecha y hora de carga reportada por la fuente."),

  source_system STRING OPTIONS(description = "Sistema fuente del registro."),
  source_dataset STRING OPTIONS(description = "Dataset fuente del registro."),
  extraction_date DATE OPTIONS(description = "Fecha lógica de extracción del pipeline."),
  ingestion_timestamp TIMESTAMP OPTIONS(description = "Timestamp de escritura en Silver."),
  pipeline_run_id STRING OPTIONS(description = "Identificador de ejecución del pipeline.")
)
PARTITION BY extraction_date
CLUSTER BY tipo_subvencion, estado
OPTIONS (
  description = "Conceptos y subconceptos de pago normalizados para análisis financiero de beneficios."
);

-- ============================================================================
-- PRONABEC: becarios_provincia
-- ============================================================================

CREATE OR REPLACE TABLE `your-gcp-project-id.silver.becarios_provincia`
(
  source_row_id STRING OPTIONS(description = "Identificador técnico de la fila en la fuente."),
  nro_fila INT64 OPTIONS(description = "Número de fila reportado por la fuente."),
  departamento STRING OPTIONS(description = "Departamento."),
  provincia STRING OPTIONS(description = "Provincia."),
  b18_n INT64 OPTIONS(description = "Cantidad de becarios Beca 18."),
  b18_pct NUMERIC OPTIONS(description = "Porcentaje de becarios Beca 18."),
  permanencia_n INT64 OPTIONS(description = "Cantidad de becarios Permanencia."),
  permanencia_pct NUMERIC OPTIONS(description = "Porcentaje de becarios Permanencia."),
  bicentenario_n INT64 OPTIONS(description = "Cantidad de becarios Bicentenario."),
  bicentenario_pct NUMERIC OPTIONS(description = "Porcentaje de becarios Bicentenario."),
  especial_n INT64 OPTIONS(description = "Cantidad de becarios de becas especiales."),
  especial_pct NUMERIC OPTIONS(description = "Porcentaje de becas especiales."),
  ffaa_n INT64 OPTIONS(description = "Cantidad de becarios FF.AA."),
  ffaa_pct NUMERIC OPTIONS(description = "Porcentaje FF.AA."),
  vraem_n INT64 OPTIONS(description = "Cantidad de becarios VRAEM."),
  vraem_pct NUMERIC OPTIONS(description = "Porcentaje VRAEM."),
  repec_n INT64 OPTIONS(description = "Cantidad de becarios REPEC."),
  repec_pct NUMERIC OPTIONS(description = "Porcentaje REPEC."),
  internacional_n INT64 OPTIONS(description = "Cantidad de becarios Internacional."),
  internacional_pct NUMERIC OPTIONS(description = "Porcentaje Internacional."),
  otros_n INT64 OPTIONS(description = "Cantidad de becarios de otros programas."),
  otros_pct NUMERIC OPTIONS(description = "Porcentaje de otros programas."),
  fecha_carga TIMESTAMP OPTIONS(description = "Fecha y hora de carga reportada por la fuente."),

  source_system STRING OPTIONS(description = "Sistema fuente del registro."),
  source_dataset STRING OPTIONS(description = "Dataset fuente del registro."),
  extraction_date DATE OPTIONS(description = "Fecha lógica de extracción del pipeline."),
  ingestion_timestamp TIMESTAMP OPTIONS(description = "Timestamp de escritura en Silver."),
  pipeline_run_id STRING OPTIONS(description = "Identificador de ejecución del pipeline.")
)
PARTITION BY extraction_date
CLUSTER BY departamento, provincia
OPTIONS (
  description = "Distribución territorial de becarios por provincia y tipo de beca."
);

-- ============================================================================
-- PRONABEC: ubigeo_postulacion
-- ============================================================================

CREATE OR REPLACE TABLE `your-gcp-project-id.silver.ubigeo_postulacion`
(
  source_row_id STRING OPTIONS(description = "Identificador técnico de la fila en la fuente."),
  nro_fila INT64 OPTIONS(description = "Número de fila reportado por la fuente."),
  codigo_ubigeo STRING OPTIONS(description = "Código UBIGEO conservado como texto."),
  departamento STRING OPTIONS(description = "Departamento."),
  provincia STRING OPTIONS(description = "Provincia."),
  distrito STRING OPTIONS(description = "Distrito."),
  fecha_carga TIMESTAMP OPTIONS(description = "Fecha y hora de carga reportada por la fuente."),

  source_system STRING OPTIONS(description = "Sistema fuente del registro."),
  source_dataset STRING OPTIONS(description = "Dataset fuente del registro."),
  extraction_date DATE OPTIONS(description = "Fecha lógica de extracción del pipeline."),
  ingestion_timestamp TIMESTAMP OPTIONS(description = "Timestamp de escritura en Silver."),
  pipeline_run_id STRING OPTIONS(description = "Identificador de ejecución del pipeline.")
)
PARTITION BY extraction_date
CLUSTER BY departamento, provincia
OPTIONS (
  description = "Catálogo territorial de postulación a becas con UBIGEO conservado como texto."
);

-- ============================================================================
-- PRONABEC: periodos_academicos
-- ============================================================================

CREATE OR REPLACE TABLE `your-gcp-project-id.silver.periodos_academicos`
(
  source_row_id STRING OPTIONS(description = "Identificador técnico de la fila en la fuente."),
  nro_fila INT64 OPTIONS(description = "Número de fila reportado por la fuente."),
  anio INT64 OPTIONS(description = "Año académico."),
  mes_numero INT64 OPTIONS(description = "Número de mes."),
  periodo_completo STRING OPTIONS(description = "Etiqueta completa del periodo académico."),
  mes_nombre STRING OPTIONS(description = "Nombre del mes."),
  fecha_carga TIMESTAMP OPTIONS(description = "Fecha y hora de carga reportada por la fuente."),

  source_system STRING OPTIONS(description = "Sistema fuente del registro."),
  source_dataset STRING OPTIONS(description = "Dataset fuente del registro."),
  extraction_date DATE OPTIONS(description = "Fecha lógica de extracción del pipeline."),
  ingestion_timestamp TIMESTAMP OPTIONS(description = "Timestamp de escritura en Silver."),
  pipeline_run_id STRING OPTIONS(description = "Identificador de ejecución del pipeline.")
)
PARTITION BY extraction_date
CLUSTER BY anio, mes_numero
OPTIONS (
  description = "Periodos académicos normalizados para análisis temporal."
);

-- ============================================================================
-- MEF: presupuesto_mef
-- ============================================================================

CREATE OR REPLACE TABLE `your-gcp-project-id.silver.presupuesto_mef`
(
  ano INT64 OPTIONS(description = "Año fiscal consultado."),
  ejecutora_nombre STRING OPTIONS(description = "Nombre de la entidad o unidad ejecutora."),
  pia NUMERIC OPTIONS(description = "Presupuesto Institucional de Apertura."),
  pim NUMERIC OPTIONS(description = "Presupuesto Institucional Modificado."),
  certificacion NUMERIC OPTIONS(description = "Monto certificado."),
  compromiso_anual NUMERIC OPTIONS(description = "Compromiso anual."),
  compromiso_mensual NUMERIC OPTIONS(description = "Compromiso mensual."),
  devengado NUMERIC OPTIONS(description = "Monto devengado."),
  girado NUMERIC OPTIONS(description = "Monto girado."),
  avance_porcentaje NUMERIC OPTIONS(description = "Porcentaje de avance presupuestal."),
  saldo_no_ejecutado NUMERIC OPTIONS(description = "Diferencia entre PIM y devengado."),
  fecha_carga TIMESTAMP OPTIONS(description = "Fecha y hora de carga del registro."),

  source_system STRING OPTIONS(description = "Sistema fuente del registro."),
  source_dataset STRING OPTIONS(description = "Dataset fuente del registro."),
  extraction_date DATE OPTIONS(description = "Fecha lógica de extracción del pipeline."),
  ingestion_timestamp TIMESTAMP OPTIONS(description = "Timestamp de escritura en Silver."),
  pipeline_run_id STRING OPTIONS(description = "Identificador de ejecución del pipeline.")
)
PARTITION BY extraction_date
CLUSTER BY ano, ejecutora_nombre
OPTIONS (
  description = "Presupuesto MEF normalizado para análisis de ejecución presupuestal de PRONABEC."
);