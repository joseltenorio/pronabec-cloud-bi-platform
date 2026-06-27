-- ============================================================================
-- Project Cloud BI Platform
-- Consultas de Calidad de Datos sobre la capa Silver de BigQuery
--
-- Regla de división: Las consultas están separadas por punto y coma (;) para
-- permitir que el runner Python las divida e interprete de forma homogénea.
--
-- Shape homogéneo devuelto por cada consulta:
-- - check_id (STRING): Identificador único del check de calidad.
-- - layer (STRING): Capa de datos (silver, gold).
-- - table_name (STRING): Tabla evaluada.
-- - severity (STRING): Severidad del fallo (ERROR, WARNING, INFO).
-- - failed_rows (INT64): Cantidad de registros que fallan la regla.
-- - passed (BOOL): Indica si el check es exitoso (TRUE) o falló (FALSE).
-- - details (STRING): Mensaje detallado del resultado.
-- ============================================================================

-- Check: silver_pronabec_convocatorias_not_empty
-- Tipo: Tabla no vacía
SELECT
  'silver_pronabec_convocatorias_not_empty' AS check_id,
  'silver' AS layer,
  'pronabec_convocatorias' AS table_name,
  'ERROR' AS severity,
  IF(cnt = 0, 1, 0) AS failed_rows,
  (cnt > 0) AS passed,
  IF(cnt = 0, 'La tabla de convocatorias está vacía', 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS cnt FROM `{project_id}.{silver_dataset}.pronabec_convocatorias`
);

-- Check: silver_pronabec_convocatorias_critical_nulls
-- Tipo: Campos críticos no nulos
SELECT
  'silver_pronabec_convocatorias_critical_nulls' AS check_id,
  'silver' AS layer,
  'pronabec_convocatorias' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con campos críticos nulos'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{silver_dataset}.pronabec_convocatorias`
  WHERE source_system IS NULL OR source_dataset IS NULL OR extraction_date IS NULL OR pipeline_run_id IS NULL OR id_convocatoria IS NULL
);

-- Check: silver_pronabec_ubigeo_postulacion_not_empty
-- Tipo: Tabla no vacía
SELECT
  'silver_pronabec_ubigeo_postulacion_not_empty' AS check_id,
  'silver' AS layer,
  'pronabec_ubigeo_postulacion' AS table_name,
  'ERROR' AS severity,
  IF(cnt = 0, 1, 0) AS failed_rows,
  (cnt > 0) AS passed,
  IF(cnt = 0, 'La tabla de ubigeo postulación está vacía', 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS cnt FROM `{project_id}.{silver_dataset}.pronabec_ubigeo_postulacion`
);

-- Check: silver_pronabec_ubigeo_postulacion_format
-- Tipo: Formato de ubigeo (STRING) con longitud razonable
SELECT
  'silver_pronabec_ubigeo_postulacion_format' AS check_id,
  'silver' AS layer,
  'pronabec_ubigeo_postulacion' AS table_name,
  'WARNING' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con códigos de ubigeo nulos o vacíos'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{silver_dataset}.pronabec_ubigeo_postulacion`
  WHERE codigo_ubigeo IS NULL OR LENGTH(TRIM(codigo_ubigeo)) = 0
);

-- Check: silver_pronabec_becarios_pais_estudio_not_empty
-- Tipo: Tabla no vacía
SELECT
  'silver_pronabec_becarios_pais_estudio_not_empty' AS check_id,
  'silver' AS layer,
  'pronabec_becarios_pais_estudio' AS table_name,
  'ERROR' AS severity,
  IF(cnt = 0, 1, 0) AS failed_rows,
  (cnt > 0) AS passed,
  IF(cnt = 0, 'La tabla de becarios por país de estudio está vacía', 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS cnt FROM `{project_id}.{silver_dataset}.pronabec_becarios_pais_estudio`
);

-- Check: silver_pronabec_becarios_pais_estudio_nulls
-- Tipo: Campos críticos no nulos
SELECT
  'silver_pronabec_becarios_pais_estudio_nulls' AS check_id,
  'silver' AS layer,
  'pronabec_becarios_pais_estudio' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con campos críticos nulos'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{silver_dataset}.pronabec_becarios_pais_estudio`
  WHERE source_system IS NULL OR source_dataset IS NULL OR extraction_date IS NULL OR pais_estudio IS NULL
);

-- Check: silver_pronabec_colegios_elegibles_not_empty
-- Tipo: Tabla no vacía
SELECT
  'silver_pronabec_colegios_elegibles_not_empty' AS check_id,
  'silver' AS layer,
  'pronabec_colegios_elegibles' AS table_name,
  'ERROR' AS severity,
  IF(cnt = 0, 1, 0) AS failed_rows,
  (cnt > 0) AS passed,
  IF(cnt = 0, 'La tabla de colegios elegibles está vacía', 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS cnt FROM `{project_id}.{silver_dataset}.pronabec_colegios_elegibles`
);

-- Check: silver_pronabec_colegios_elegibles_nulls
-- Tipo: Campos críticos no nulos
SELECT
  'silver_pronabec_colegios_elegibles_nulls' AS check_id,
  'silver' AS layer,
  'pronabec_colegios_elegibles' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' colegios con código modular o de anexo nulos'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{silver_dataset}.pronabec_colegios_elegibles`
  WHERE codigo_modular IS NULL OR anexo IS NULL
);

-- Check: silver_pronabec_report_beca18_universitarios_carrera_anual_not_empty
-- Tipo: Tabla no vacía
SELECT
  'silver_pronabec_report_beca18_universitarios_carrera_anual_not_empty' AS check_id,
  'silver' AS layer,
  'pronabec_report_beca18_universitarios_carrera_anual' AS table_name,
  'ERROR' AS severity,
  IF(cnt = 0, 1, 0) AS failed_rows,
  (cnt > 0) AS passed,
  IF(cnt = 0, 'La tabla de reporte de becarios universitarios por carrera está vacía', 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS cnt FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_universitarios_carrera_anual`
);

-- Check: silver_pronabec_report_beca18_universitarios_carrera_anual_duplicates
-- Tipo: Duplicados por clave natural
SELECT
  'silver_pronabec_report_beca18_universitarios_carrera_anual_duplicates' AS check_id,
  'silver' AS layer,
  'pronabec_report_beca18_universitarios_carrera_anual' AS table_name,
  'ERROR' AS severity,
  dup_cnt AS failed_rows,
  (dup_cnt = 0) AS passed,
  IF(dup_cnt > 0, CONCAT('Se encontraron ', CAST(dup_cnt AS STRING), ' llaves duplicadas para carrera, año y fecha de extracción'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS dup_cnt
  FROM (
    SELECT carrera_estudio, ano_convocatoria, extraction_date
    FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_universitarios_carrera_anual`
    GROUP BY carrera_estudio, ano_convocatoria, extraction_date
    HAVING COUNT(*) > 1
  )
);

-- Check: silver_pronabec_report_beca18_universitarios_carrera_anual_canonical_consistency
-- Tipo: Consistencia de campos canónicos
SELECT
  'silver_pronabec_report_beca18_universitarios_carrera_anual_canonical_consistency' AS check_id,
  'silver' AS layer,
  'pronabec_report_beca18_universitarios_carrera_anual' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con carrera_estudio_canonical_match_method pero carrera_estudio_canonical nulo'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_universitarios_carrera_anual`
  WHERE carrera_estudio_canonical_match_method IS NOT NULL AND carrera_estudio_canonical IS NULL
);

-- Check: silver_pronabec_report_beca18_universitarios_carrera_anual_ranges
-- Tipo: Rango de valores (no negativos)
SELECT
  'silver_pronabec_report_beca18_universitarios_carrera_anual_ranges' AS check_id,
  'silver' AS layer,
  'pronabec_report_beca18_universitarios_carrera_anual' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con cantidad_becarios negativa'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_universitarios_carrera_anual`
  WHERE cantidad_becarios < 0
);

-- Check: silver_pronabec_report_beca18_universitarios_universidad_anual_not_empty
-- Tipo: Tabla no vacía
SELECT
  'silver_pronabec_report_beca18_universitarios_universidad_anual_not_empty' AS check_id,
  'silver' AS layer,
  'pronabec_report_beca18_universitarios_universidad_anual' AS table_name,
  'ERROR' AS severity,
  IF(cnt = 0, 1, 0) AS failed_rows,
  (cnt > 0) AS passed,
  IF(cnt = 0, 'La tabla de reporte de becarios universitarios por universidad está vacía', 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS cnt FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_universitarios_universidad_anual`
);

-- Check: silver_pronabec_report_beca18_universitarios_universidad_anual_duplicates
-- Tipo: Duplicados por clave natural
SELECT
  'silver_pronabec_report_beca18_universitarios_universidad_anual_duplicates' AS check_id,
  'silver' AS layer,
  'pronabec_report_beca18_universitarios_universidad_anual' AS table_name,
  'ERROR' AS severity,
  dup_cnt AS failed_rows,
  (dup_cnt = 0) AS passed,
  IF(dup_cnt > 0, CONCAT('Se encontraron ', CAST(dup_cnt AS STRING), ' llaves duplicadas para universidad, año y fecha de extracción'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS dup_cnt
  FROM (
    SELECT universidad, ano_convocatoria, extraction_date
    FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_universitarios_universidad_anual`
    GROUP BY universidad, ano_convocatoria, extraction_date
    HAVING COUNT(*) > 1
  )
);

-- Check: silver_pronabec_report_beca18_universitarios_universidad_anual_canonical_consistency
-- Tipo: Consistencia de campos canónicos
SELECT
  'silver_pronabec_report_beca18_universitarios_universidad_anual_canonical_consistency' AS check_id,
  'silver' AS layer,
  'pronabec_report_beca18_universitarios_universidad_anual' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con universidad_canonical_match_method pero universidad_canonical nulo'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_universitarios_universidad_anual`
  WHERE universidad_canonical_match_method IS NOT NULL AND universidad_canonical IS NULL
);

-- Check: silver_pronabec_report_beca18_universitarios_universidad_anual_ranges
-- Tipo: Rango de valores (no negativos)
SELECT
  'silver_pronabec_report_beca18_universitarios_universidad_anual_ranges' AS check_id,
  'silver' AS layer,
  'pronabec_report_beca18_universitarios_universidad_anual' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con cantidad_becarios negativa'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_universitarios_universidad_anual`
  WHERE cantidad_becarios < 0
);

-- Check: silver_pronabec_report_beca18_becas_otorgadas_modalidad_anual_not_empty
-- Tipo: Tabla no vacía (PES 2025)
SELECT
  'silver_pronabec_report_beca18_becas_otorgadas_modalidad_anual_not_empty' AS check_id,
  'silver' AS layer,
  'pronabec_report_beca18_becas_otorgadas_modalidad_anual' AS table_name,
  'ERROR' AS severity,
  IF(cnt = 0, 1, 0) AS failed_rows,
  (cnt > 0) AS passed,
  IF(cnt = 0, 'La tabla de becas otorgadas por modalidad (PES 2025) está vacía', 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS cnt FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_becas_otorgadas_modalidad_anual`
);

-- Check: silver_pronabec_report_beca18_becas_otorgadas_modalidad_anual_ranges
-- Tipo: Rango de valores no negativos (PES 2025)
SELECT
  'silver_pronabec_report_beca18_becas_otorgadas_modalidad_anual_ranges' AS check_id,
  'silver' AS layer,
  'pronabec_report_beca18_becas_otorgadas_modalidad_anual' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con becas_otorgadas negativas en PES 2025'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_becas_otorgadas_modalidad_anual`
  WHERE becas_otorgadas < 0
);

-- Check: silver_presupuesto_mef_not_empty
-- Tipo: Tabla no vacía
SELECT
  'silver_presupuesto_mef_not_empty' AS check_id,
  'silver' AS layer,
  'presupuesto_mef' AS table_name,
  'ERROR' AS severity,
  IF(cnt = 0, 1, 0) AS failed_rows,
  (cnt > 0) AS passed,
  IF(cnt = 0, 'La tabla de presupuesto MEF general está vacía', 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS cnt FROM `{project_id}.{silver_dataset}.presupuesto_mef`
);

-- Check: silver_presupuesto_mef_nulls
-- Tipo: Campos críticos no nulos
SELECT
  'silver_presupuesto_mef_nulls' AS check_id,
  'silver' AS layer,
  'presupuesto_mef' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros de presupuesto MEF con campos críticos nulos'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{silver_dataset}.presupuesto_mef`
  WHERE ano IS NULL OR source_system IS NULL OR source_dataset IS NULL OR extraction_date IS NULL
);

-- Check: silver_presupuesto_mef_ano_range
-- Tipo: Rango de año esperado
SELECT
  'silver_presupuesto_mef_ano_range' AS check_id,
  'silver' AS layer,
  'presupuesto_mef' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con año fiscal fuera de rango (2000-2050)'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{silver_dataset}.presupuesto_mef`
  WHERE ano < 2000 OR ano > 2050
);

-- Check: silver_presupuesto_mef_temporal_not_empty
-- Tipo: Tabla no vacía
SELECT
  'silver_presupuesto_mef_temporal_not_empty' AS check_id,
  'silver' AS layer,
  'presupuesto_mef_temporal' AS table_name,
  'ERROR' AS severity,
  IF(cnt = 0, 1, 0) AS failed_rows,
  (cnt > 0) AS passed,
  IF(cnt = 0, 'La tabla de presupuesto MEF temporal está vacía', 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS cnt FROM `{project_id}.{silver_dataset}.presupuesto_mef_temporal`
);

-- Check: silver_presupuesto_mef_temporal_period_consistency
-- Tipo: Consistencia en periodos temporales (mensuales/trimestrales)
SELECT
  'silver_presupuesto_mef_temporal_period_consistency' AS check_id,
  'silver' AS layer,
  'presupuesto_mef_temporal' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros temporales MEF con trimestre o mes inconsistente con periodo_tipo'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{silver_dataset}.presupuesto_mef_temporal`
  WHERE (periodo_tipo = 'MENSUAL' AND (mes_numero IS NULL OR mes_numero < 1 OR mes_numero > 12))
     OR (periodo_tipo = 'TRIMESTRAL' AND (trimestre IS NULL OR trimestre < 1 OR trimestre > 4))
);

-- Check: silver_presupuesto_mef_producto_not_empty
-- Tipo: Tabla no vacía
SELECT
  'silver_presupuesto_mef_producto_not_empty' AS check_id,
  'silver' AS layer,
  'presupuesto_mef_producto' AS table_name,
  'ERROR' AS severity,
  IF(cnt = 0, 1, 0) AS failed_rows,
  (cnt > 0) AS passed,
  IF(cnt = 0, 'La tabla de presupuesto MEF por producto está vacía', 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS cnt FROM `{project_id}.{silver_dataset}.presupuesto_mef_producto`
);

-- Check: silver_presupuesto_mef_producto_temporal_not_empty
-- Tipo: Tabla no vacía
SELECT
  'silver_presupuesto_mef_producto_temporal_not_empty' AS check_id,
  'silver' AS layer,
  'presupuesto_mef_producto_temporal' AS table_name,
  'ERROR' AS severity,
  IF(cnt = 0, 1, 0) AS failed_rows,
  (cnt > 0) AS passed,
  IF(cnt = 0, 'La tabla de presupuesto MEF producto temporal está vacía', 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS cnt FROM `{project_id}.{silver_dataset}.presupuesto_mef_producto_temporal`
);

-- Check: silver_presupuesto_mef_actividad_not_empty
-- Tipo: Tabla no vacía
SELECT
  'silver_presupuesto_mef_actividad_not_empty' AS check_id,
  'silver' AS layer,
  'presupuesto_mef_actividad' AS table_name,
  'ERROR' AS severity,
  IF(cnt = 0, 1, 0) AS failed_rows,
  (cnt > 0) AS passed,
  IF(cnt = 0, 'La tabla de presupuesto MEF actividad está vacía', 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS cnt FROM `{project_id}.{silver_dataset}.presupuesto_mef_actividad`
);

-- Check: silver_presupuesto_mef_actividad_temporal_not_empty
-- Tipo: Tabla no vacía
SELECT
  'silver_presupuesto_mef_actividad_temporal_not_empty' AS check_id,
  'silver' AS layer,
  'presupuesto_mef_actividad_temporal' AS table_name,
  'ERROR' AS severity,
  IF(cnt = 0, 1, 0) AS failed_rows,
  (cnt > 0) AS passed,
  IF(cnt = 0, 'La tabla de presupuesto MEF actividad temporal está vacía', 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS cnt FROM `{project_id}.{silver_dataset}.presupuesto_mef_actividad_temporal`
);

-- Check: silver_presupuesto_mef_generica_not_empty
-- Tipo: Tabla no vacía
SELECT
  'silver_presupuesto_mef_generica_not_empty' AS check_id,
  'silver' AS layer,
  'presupuesto_mef_generica' AS table_name,
  'ERROR' AS severity,
  IF(cnt = 0, 1, 0) AS failed_rows,
  (cnt > 0) AS passed,
  IF(cnt = 0, 'La tabla de presupuesto MEF genérica está vacía', 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS cnt FROM `{project_id}.{silver_dataset}.presupuesto_mef_generica`
);

-- Check: silver_presupuesto_mef_generica_temporal_not_empty
-- Tipo: Tabla no vacía
SELECT
  'silver_presupuesto_mef_generica_temporal_not_empty' AS check_id,
  'silver' AS layer,
  'presupuesto_mef_generica_temporal' AS table_name,
  'ERROR' AS severity,
  IF(cnt = 0, 1, 0) AS failed_rows,
  (cnt > 0) AS passed,
  IF(cnt = 0, 'La tabla de presupuesto MEF genérica temporal está vacía', 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS cnt FROM `{project_id}.{silver_dataset}.presupuesto_mef_generica_temporal`
);

-- Check: silver_presupuesto_mef_hierarchy_not_empty
-- Tipo: Tabla no vacía
SELECT
  'silver_presupuesto_mef_hierarchy_not_empty' AS check_id,
  'silver' AS layer,
  'presupuesto_mef_hierarchy' AS table_name,
  'ERROR' AS severity,
  IF(cnt = 0, 1, 0) AS failed_rows,
  (cnt > 0) AS passed,
  IF(cnt = 0, 'La tabla de jerarquía MEF está vacía', 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS cnt FROM `{project_id}.{silver_dataset}.presupuesto_mef_hierarchy`
);

-- Check: silver_presupuesto_mef_hierarchy_nulls
-- Tipo: Campos críticos no nulos en jerarquía
SELECT
  'silver_presupuesto_mef_hierarchy_nulls' AS check_id,
  'silver' AS layer,
  'presupuesto_mef_hierarchy' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con nivel o nombre de entidad nulos en jerarquía'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{silver_dataset}.presupuesto_mef_hierarchy`
  WHERE nivel_jerarquia IS NULL OR nombre_entidad IS NULL
);

-- Check: silver_pronabec_beca18_becarios_provincia_2016_not_empty
-- Tipo: Tabla no vacía
SELECT
  'silver_pronabec_beca18_becarios_provincia_2016_not_empty' AS check_id,
  'silver' AS layer,
  'pronabec_beca18_becarios_provincia_2016' AS table_name,
  'ERROR' AS severity,
  IF(cnt = 0, 1, 0) AS failed_rows,
  (cnt > 0) AS passed,
  IF(cnt = 0, 'La tabla de becarios por provincia está vacía', 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS cnt FROM `{project_id}.{silver_dataset}.pronabec_beca18_becarios_provincia_2016`
);

-- Check: silver_pronabec_beca18_becarios_provincia_2016_invalid_rows
-- Tipo: Filtrado de totales, nulos obligatorios y metadatos
SELECT
  'silver_pronabec_beca18_becarios_provincia_2016_invalid_rows' AS check_id,
  'silver' AS layer,
  'pronabec_beca18_becarios_provincia_2016' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con totales, nulos o metadatos inválidos'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{silver_dataset}.pronabec_beca18_becarios_provincia_2016`
  WHERE UPPER(TRIM(provincia)) = 'TOTAL'
     OR UPPER(TRIM(provincia)) LIKE 'TOTAL%'
     OR region IS NULL
     OR TRIM(region) = ''
     OR provincia IS NULL
     OR TRIM(provincia) = ''
     OR extraction_date IS NULL
     OR pipeline_run_id IS NULL
     OR TRIM(pipeline_run_id) = ''
);

-- Check: silver_pronabec_beca18_becarios_provincia_2016_negative_counts
-- Tipo: Conteos negativos no permitidos
SELECT
  'silver_pronabec_beca18_becarios_provincia_2016_negative_counts' AS check_id,
  'silver' AS layer,
  'pronabec_beca18_becarios_provincia_2016' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con conteos negativos de becarios'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{silver_dataset}.pronabec_beca18_becarios_provincia_2016`
  WHERE becarios_b18_count < 0
);

-- Check: silver_presupuesto_mef_negative_values
-- Tipo: Rango de montos no negativos en presupuesto general
SELECT
  'silver_presupuesto_mef_negative_values' AS check_id,
  'silver' AS layer,
  'presupuesto_mef' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con montos o avances negativos en presupuesto_mef'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{silver_dataset}.presupuesto_mef`
  WHERE pia < 0 OR pim < 0 OR devengado < 0 OR avance_porcentaje < 0
);

-- Check: silver_presupuesto_mef_producto_negative_values
-- Tipo: Rango de montos no negativos en producto
SELECT
  'silver_presupuesto_mef_producto_negative_values' AS check_id,
  'silver' AS layer,
  'presupuesto_mef_producto' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con montos o avances negativos en presupuesto_mef_producto'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{silver_dataset}.presupuesto_mef_producto`
  WHERE pia < 0 OR pim < 0 OR devengado < 0 OR avance_porcentaje < 0
);

-- Check: silver_presupuesto_mef_actividad_negative_values
-- Tipo: Rango de montos no negativos en actividad
SELECT
  'silver_presupuesto_mef_actividad_negative_values' AS check_id,
  'silver' AS layer,
  'presupuesto_mef_actividad' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con montos o avances negativos en presupuesto_mef_actividad'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{silver_dataset}.presupuesto_mef_actividad`
  WHERE pia < 0 OR pim < 0 OR devengado < 0 OR avance_porcentaje < 0
);

-- Check: silver_presupuesto_mef_generica_negative_values
-- Tipo: Rango de montos no negativos en genérica
SELECT
  'silver_presupuesto_mef_generica_negative_values' AS check_id,
  'silver' AS layer,
  'presupuesto_mef_generica' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con montos o avances negativos en presupuesto_mef_generica'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{silver_dataset}.presupuesto_mef_generica`
  WHERE pia < 0 OR pim < 0 OR devengado < 0 OR avance_porcentaje < 0
);

-- Check: silver_presupuesto_mef_hierarchy_negative_values
-- Tipo: Rango de montos no negativos en jerarquía
SELECT
  'silver_presupuesto_mef_hierarchy_negative_values' AS check_id,
  'silver' AS layer,
  'presupuesto_mef_hierarchy' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con montos o avances negativos en presupuesto_mef_hierarchy'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{silver_dataset}.presupuesto_mef_hierarchy`
  WHERE pia < 0 OR pim < 0 OR devengado < 0 OR avance_porcentaje < 0
);

-- Check: silver_pronabec_report_beca18_autoidentificacion_etnica_modalidad_2025_valid
-- Tipo: Validación de contenido y metadatos
SELECT
  'silver_pronabec_report_beca18_autoidentificacion_etnica_modalidad_2025_valid' AS check_id,
  'silver' AS layer,
  'pronabec_report_beca18_autoidentificacion_etnica_modalidad_2025' AS table_name,
  'ERROR' AS severity,
  IF(total_cnt = 0, 1, failed_cnt) AS failed_rows,
  (total_cnt > 0 AND failed_cnt = 0) AS passed,
  IF(total_cnt = 0, 'La tabla pronabec_report_beca18_autoidentificacion_etnica_modalidad_2025 está vacía',
     IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con metadatos inválidos'), 'Validación exitosa')) AS details
FROM (
  SELECT 
    COUNT(*) AS total_cnt,
    COUNTIF(extraction_date IS NULL OR pipeline_run_id IS NULL OR TRIM(pipeline_run_id) = '') AS failed_cnt
  FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_autoidentificacion_etnica_modalidad_2025`
);

-- Check: silver_pronabec_report_beca18_colegio_gestion_2025_valid
-- Tipo: Validación de contenido y metadatos
SELECT
  'silver_pronabec_report_beca18_colegio_gestion_2025_valid' AS check_id,
  'silver' AS layer,
  'pronabec_report_beca18_colegio_gestion_2025' AS table_name,
  'ERROR' AS severity,
  IF(total_cnt = 0, 1, failed_cnt) AS failed_rows,
  (total_cnt > 0 AND failed_cnt = 0) AS passed,
  IF(total_cnt = 0, 'La tabla pronabec_report_beca18_colegio_gestion_2025 está vacía',
     IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con metadatos inválidos'), 'Validación exitosa')) AS details
FROM (
  SELECT 
    COUNT(*) AS total_cnt,
    COUNTIF(extraction_date IS NULL OR pipeline_run_id IS NULL OR TRIM(pipeline_run_id) = '') AS failed_cnt
  FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_colegio_gestion_2025`
);

-- Check: silver_pronabec_report_beca18_enp_promedio_caracteristica_2025_valid
-- Tipo: Validación de contenido y metadatos
SELECT
  'silver_pronabec_report_beca18_enp_promedio_caracteristica_2025_valid' AS check_id,
  'silver' AS layer,
  'pronabec_report_beca18_enp_promedio_caracteristica_2025' AS table_name,
  'ERROR' AS severity,
  IF(total_cnt = 0, 1, failed_cnt) AS failed_rows,
  (total_cnt > 0 AND failed_cnt = 0) AS passed,
  IF(total_cnt = 0, 'La tabla pronabec_report_beca18_enp_promedio_caracteristica_2025 está vacía',
     IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con metadatos inválidos'), 'Validación exitosa')) AS details
FROM (
  SELECT 
    COUNT(*) AS total_cnt,
    COUNTIF(extraction_date IS NULL OR pipeline_run_id IS NULL OR TRIM(pipeline_run_id) = '') AS failed_cnt
  FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_enp_promedio_caracteristica_2025`
);

-- Check: silver_pronabec_report_beca18_enp_promedio_region_2025_valid
-- Tipo: Validación de contenido y metadatos
SELECT
  'silver_pronabec_report_beca18_enp_promedio_region_2025_valid' AS check_id,
  'silver' AS layer,
  'pronabec_report_beca18_enp_promedio_region_2025' AS table_name,
  'ERROR' AS severity,
  IF(total_cnt = 0, 1, failed_cnt) AS failed_rows,
  (total_cnt > 0 AND failed_cnt = 0) AS passed,
  IF(total_cnt = 0, 'La tabla pronabec_report_beca18_enp_promedio_region_2025 está vacía',
     IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con metadatos inválidos'), 'Validación exitosa')) AS details
FROM (
  SELECT 
    COUNT(*) AS total_cnt,
    COUNTIF(extraction_date IS NULL OR pipeline_run_id IS NULL OR TRIM(pipeline_run_id) = '') AS failed_cnt
  FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_enp_promedio_region_2025`
);

-- Check: silver_pronabec_report_beca18_lengua_materna_modalidad_2025_valid
-- Tipo: Validación de contenido y metadatos
SELECT
  'silver_pronabec_report_beca18_lengua_materna_modalidad_2025_valid' AS check_id,
  'silver' AS layer,
  'pronabec_report_beca18_lengua_materna_modalidad_2025' AS table_name,
  'ERROR' AS severity,
  IF(total_cnt = 0, 1, failed_cnt) AS failed_rows,
  (total_cnt > 0 AND failed_cnt = 0) AS passed,
  IF(total_cnt = 0, 'La tabla pronabec_report_beca18_lengua_materna_modalidad_2025 está vacía',
     IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con metadatos inválidos'), 'Validación exitosa')) AS details
FROM (
  SELECT 
    COUNT(*) AS total_cnt,
    COUNTIF(extraction_date IS NULL OR pipeline_run_id IS NULL OR TRIM(pipeline_run_id) = '') AS failed_cnt
  FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_lengua_materna_modalidad_2025`
);

-- Check: silver_pronabec_report_beca18_migracion_region_acumulada_valid
-- Tipo: Validación de contenido y metadatos
SELECT
  'silver_pronabec_report_beca18_migracion_region_acumulada_valid' AS check_id,
  'silver' AS layer,
  'pronabec_report_beca18_migracion_region_acumulada' AS table_name,
  'ERROR' AS severity,
  IF(total_cnt = 0, 1, failed_cnt) AS failed_rows,
  (total_cnt > 0 AND failed_cnt = 0) AS passed,
  IF(total_cnt = 0, 'La tabla pronabec_report_beca18_migracion_region_acumulada está vacía',
     IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con metadatos inválidos'), 'Validación exitosa')) AS details
FROM (
  SELECT 
    COUNT(*) AS total_cnt,
    COUNTIF(extraction_date IS NULL OR pipeline_run_id IS NULL OR TRIM(pipeline_run_id) = '') AS failed_cnt
  FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_migracion_region_acumulada`
);

-- Check: silver_pronabec_report_beca18_migracion_region_anual_valid
-- Tipo: Validación de contenido y metadatos
SELECT
  'silver_pronabec_report_beca18_migracion_region_anual_valid' AS check_id,
  'silver' AS layer,
  'pronabec_report_beca18_migracion_region_anual' AS table_name,
  'ERROR' AS severity,
  IF(total_cnt = 0, 1, failed_cnt) AS failed_rows,
  (total_cnt > 0 AND failed_cnt = 0) AS passed,
  IF(total_cnt = 0, 'La tabla pronabec_report_beca18_migracion_region_anual está vacía',
     IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con metadatos inválidos'), 'Validación exitosa')) AS details
FROM (
  SELECT 
    COUNT(*) AS total_cnt,
    COUNTIF(extraction_date IS NULL OR pipeline_run_id IS NULL OR TRIM(pipeline_run_id) = '') AS failed_cnt
  FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_migracion_region_anual`
);

-- Check: silver_pronabec_report_beca18_no_continuaria_sin_beca_caracteristica_2025_valid
-- Tipo: Validación de contenido y metadatos
SELECT
  'silver_pronabec_report_beca18_no_continuaria_sin_beca_caracteristica_2025_valid' AS check_id,
  'silver' AS layer,
  'pronabec_report_beca18_no_continuaria_sin_beca_caracteristica_2025' AS table_name,
  'ERROR' AS severity,
  IF(total_cnt = 0, 1, failed_cnt) AS failed_rows,
  (total_cnt > 0 AND failed_cnt = 0) AS passed,
  IF(total_cnt = 0, 'La tabla pronabec_report_beca18_no_continuaria_sin_beca_caracteristica_2025 está vacía',
     IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con metadatos inválidos'), 'Validación exitosa')) AS details
FROM (
  SELECT 
    COUNT(*) AS total_cnt,
    COUNTIF(extraction_date IS NULL OR pipeline_run_id IS NULL OR TRIM(pipeline_run_id) = '') AS failed_cnt
  FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_no_continuaria_sin_beca_caracteristica_2025`
);

-- Check: silver_pronabec_report_beca18_padres_nivel_educativo_2025_valid
-- Tipo: Validación de contenido y metadatos
SELECT
  'silver_pronabec_report_beca18_padres_nivel_educativo_2025_valid' AS check_id,
  'silver' AS layer,
  'pronabec_report_beca18_padres_nivel_educativo_2025' AS table_name,
  'ERROR' AS severity,
  IF(total_cnt = 0, 1, failed_cnt) AS failed_rows,
  (total_cnt > 0 AND failed_cnt = 0) AS passed,
  IF(total_cnt = 0, 'La tabla pronabec_report_beca18_padres_nivel_educativo_2025 está vacía',
     IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con metadatos inválidos'), 'Validación exitosa')) AS details
FROM (
  SELECT 
    COUNT(*) AS total_cnt,
    COUNTIF(extraction_date IS NULL OR pipeline_run_id IS NULL OR TRIM(pipeline_run_id) = '') AS failed_cnt
  FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_padres_nivel_educativo_2025`
);

-- Check: silver_pronabec_report_beca18_periodo_ingreso_ies_genero_2025_valid
-- Tipo: Validación de contenido y metadatos
SELECT
  'silver_pronabec_report_beca18_periodo_ingreso_ies_genero_2025_valid' AS check_id,
  'silver' AS layer,
  'pronabec_report_beca18_periodo_ingreso_ies_genero_2025' AS table_name,
  'ERROR' AS severity,
  IF(total_cnt = 0, 1, failed_cnt) AS failed_rows,
  (total_cnt > 0 AND failed_cnt = 0) AS passed,
  IF(total_cnt = 0, 'La tabla pronabec_report_beca18_periodo_ingreso_ies_genero_2025 está vacía',
     IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con metadatos inválidos'), 'Validación exitosa')) AS details
FROM (
  SELECT 
    COUNT(*) AS total_cnt,
    COUNTIF(extraction_date IS NULL OR pipeline_run_id IS NULL OR TRIM(pipeline_run_id) = '') AS failed_cnt
  FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_periodo_ingreso_ies_genero_2025`
);

-- Check: silver_pronabec_report_beca18_preparacion_ies_meses_caracteristica_2025_valid
-- Tipo: Validación de contenido y metadatos
SELECT
  'silver_pronabec_report_beca18_preparacion_ies_meses_caracteristica_2025_valid' AS check_id,
  'silver' AS layer,
  'pronabec_report_beca18_preparacion_ies_meses_caracteristica_2025' AS table_name,
  'ERROR' AS severity,
  IF(total_cnt = 0, 1, failed_cnt) AS failed_rows,
  (total_cnt > 0 AND failed_cnt = 0) AS passed,
  IF(total_cnt = 0, 'La tabla pronabec_report_beca18_preparacion_ies_meses_caracteristica_2025 está vacía',
     IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con metadatos inválidos'), 'Validación exitosa')) AS details
FROM (
  SELECT 
    COUNT(*) AS total_cnt,
    COUNTIF(extraction_date IS NULL OR pipeline_run_id IS NULL OR TRIM(pipeline_run_id) = '') AS failed_cnt
  FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_preparacion_ies_meses_caracteristica_2025`
);

-- Check: silver_pronabec_report_beca18_preparacion_ies_tipo_2025_valid
-- Tipo: Validación de contenido y metadatos
SELECT
  'silver_pronabec_report_beca18_preparacion_ies_tipo_2025_valid' AS check_id,
  'silver' AS layer,
  'pronabec_report_beca18_preparacion_ies_tipo_2025' AS table_name,
  'ERROR' AS severity,
  IF(total_cnt = 0, 1, failed_cnt) AS failed_rows,
  (total_cnt > 0 AND failed_cnt = 0) AS passed,
  IF(total_cnt = 0, 'La tabla pronabec_report_beca18_preparacion_ies_tipo_2025 está vacía',
     IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con metadatos inválidos'), 'Validación exitosa')) AS details
FROM (
  SELECT 
    COUNT(*) AS total_cnt,
    COUNTIF(extraction_date IS NULL OR pipeline_run_id IS NULL OR TRIM(pipeline_run_id) = '') AS failed_cnt
  FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_preparacion_ies_tipo_2025`
);

-- Check: silver_pronabec_report_beca18_primera_generacion_region_valid
-- Tipo: Validación de contenido y metadatos
SELECT
  'silver_pronabec_report_beca18_primera_generacion_region_valid' AS check_id,
  'silver' AS layer,
  'pronabec_report_beca18_primera_generacion_region' AS table_name,
  'ERROR' AS severity,
  IF(total_cnt = 0, 1, failed_cnt) AS failed_rows,
  (total_cnt > 0 AND failed_cnt = 0) AS passed,
  IF(total_cnt = 0, 'La tabla pronabec_report_beca18_primera_generacion_region está vacía',
     IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con metadatos inválidos'), 'Validación exitosa')) AS details
FROM (
  SELECT 
    COUNT(*) AS total_cnt,
    COUNTIF(extraction_date IS NULL OR pipeline_run_id IS NULL OR TRIM(pipeline_run_id) = '') AS failed_cnt
  FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_primera_generacion_region`
);

-- Check: silver_pronabec_report_beca18_razones_eleccion_carrera_gestion_ies_2025_valid
-- Tipo: Validación de contenido y metadatos
SELECT
  'silver_pronabec_report_beca18_razones_eleccion_carrera_gestion_ies_2025_valid' AS check_id,
  'silver' AS layer,
  'pronabec_report_beca18_razones_eleccion_carrera_gestion_ies_2025' AS table_name,
  'ERROR' AS severity,
  IF(total_cnt = 0, 1, failed_cnt) AS failed_rows,
  (total_cnt > 0 AND failed_cnt = 0) AS passed,
  IF(total_cnt = 0, 'La tabla pronabec_report_beca18_razones_eleccion_carrera_gestion_ies_2025 está vacía',
     IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con metadatos inválidos'), 'Validación exitosa')) AS details
FROM (
  SELECT 
    COUNT(*) AS total_cnt,
    COUNTIF(extraction_date IS NULL OR pipeline_run_id IS NULL OR TRIM(pipeline_run_id) = '') AS failed_cnt
  FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_razones_eleccion_carrera_gestion_ies_2025`
);

-- Check: silver_pronabec_report_beca18_razones_eleccion_carrera_sexo_2025_valid
-- Tipo: Validación de contenido y metadatos
SELECT
  'silver_pronabec_report_beca18_razones_eleccion_carrera_sexo_2025_valid' AS check_id,
  'silver' AS layer,
  'pronabec_report_beca18_razones_eleccion_carrera_sexo_2025' AS table_name,
  'ERROR' AS severity,
  IF(total_cnt = 0, 1, failed_cnt) AS failed_rows,
  (total_cnt > 0 AND failed_cnt = 0) AS passed,
  IF(total_cnt = 0, 'La tabla pronabec_report_beca18_razones_eleccion_carrera_sexo_2025 está vacía',
     IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con metadatos inválidos'), 'Validación exitosa')) AS details
FROM (
  SELECT 
    COUNT(*) AS total_cnt,
    COUNTIF(extraction_date IS NULL OR pipeline_run_id IS NULL OR TRIM(pipeline_run_id) = '') AS failed_cnt
  FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_razones_eleccion_carrera_sexo_2025`
);

-- Check: silver_pronabec_report_beca18_razones_eleccion_ies_gestion_2025_valid
-- Tipo: Validación de contenido y metadatos
SELECT
  'silver_pronabec_report_beca18_razones_eleccion_ies_gestion_2025_valid' AS check_id,
  'silver' AS layer,
  'pronabec_report_beca18_razones_eleccion_ies_gestion_2025' AS table_name,
  'ERROR' AS severity,
  IF(total_cnt = 0, 1, failed_cnt) AS failed_rows,
  (total_cnt > 0 AND failed_cnt = 0) AS passed,
  IF(total_cnt = 0, 'La tabla pronabec_report_beca18_razones_eleccion_ies_gestion_2025 está vacía',
     IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con metadatos inválidos'), 'Validación exitosa')) AS details
FROM (
  SELECT 
    COUNT(*) AS total_cnt,
    COUNTIF(extraction_date IS NULL OR pipeline_run_id IS NULL OR TRIM(pipeline_run_id) = '') AS failed_cnt
  FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_razones_eleccion_ies_gestion_2025`
);

-- Check: silver_pronabec_report_beca18_region_postulacion_2025_valid
-- Tipo: Validación de contenido y metadatos
SELECT
  'silver_pronabec_report_beca18_region_postulacion_2025_valid' AS check_id,
  'silver' AS layer,
  'pronabec_report_beca18_region_postulacion_2025' AS table_name,
  'ERROR' AS severity,
  IF(total_cnt = 0, 1, failed_cnt) AS failed_rows,
  (total_cnt > 0 AND failed_cnt = 0) AS passed,
  IF(total_cnt = 0, 'La tabla pronabec_report_beca18_region_postulacion_2025 está vacía',
     IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con metadatos inválidos'), 'Validación exitosa')) AS details
FROM (
  SELECT 
    COUNT(*) AS total_cnt,
    COUNTIF(extraction_date IS NULL OR pipeline_run_id IS NULL OR TRIM(pipeline_run_id) = '') AS failed_cnt
  FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_region_postulacion_2025`
);

-- Check: silver_pronabec_report_beca18_region_postulacion_acumulada_valid
-- Tipo: Validación de contenido y metadatos
SELECT
  'silver_pronabec_report_beca18_region_postulacion_acumulada_valid' AS check_id,
  'silver' AS layer,
  'pronabec_report_beca18_region_postulacion_acumulada' AS table_name,
  'ERROR' AS severity,
  IF(total_cnt = 0, 1, failed_cnt) AS failed_rows,
  (total_cnt > 0 AND failed_cnt = 0) AS passed,
  IF(total_cnt = 0, 'La tabla pronabec_report_beca18_region_postulacion_acumulada está vacía',
     IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con metadatos inválidos'), 'Validación exitosa')) AS details
FROM (
  SELECT 
    COUNT(*) AS total_cnt,
    COUNTIF(extraction_date IS NULL OR pipeline_run_id IS NULL OR TRIM(pipeline_run_id) = '') AS failed_cnt
  FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_region_postulacion_acumulada`
);

-- Check: silver_pronabec_report_beca18_region_postulacion_anual_valid
-- Tipo: Validación de contenido y metadatos
SELECT
  'silver_pronabec_report_beca18_region_postulacion_anual_valid' AS check_id,
  'silver' AS layer,
  'pronabec_report_beca18_region_postulacion_anual' AS table_name,
  'ERROR' AS severity,
  IF(total_cnt = 0, 1, failed_cnt) AS failed_rows,
  (total_cnt > 0 AND failed_cnt = 0) AS passed,
  IF(total_cnt = 0, 'La tabla pronabec_report_beca18_region_postulacion_anual está vacía',
     IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con metadatos inválidos'), 'Validación exitosa')) AS details
FROM (
  SELECT 
    COUNT(*) AS total_cnt,
    COUNTIF(extraction_date IS NULL OR pipeline_run_id IS NULL OR TRIM(pipeline_run_id) = '') AS failed_cnt
  FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_region_postulacion_anual`
);

-- Check: silver_pronabec_report_beca18_sexo_anual_valid
-- Tipo: Validación de contenido y metadatos
SELECT
  'silver_pronabec_report_beca18_sexo_anual_valid' AS check_id,
  'silver' AS layer,
  'pronabec_report_beca18_sexo_anual' AS table_name,
  'ERROR' AS severity,
  IF(total_cnt = 0, 1, failed_cnt) AS failed_rows,
  (total_cnt > 0 AND failed_cnt = 0) AS passed,
  IF(total_cnt = 0, 'La tabla pronabec_report_beca18_sexo_anual está vacía',
     IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con metadatos inválidos'), 'Validación exitosa')) AS details
FROM (
  SELECT 
    COUNT(*) AS total_cnt,
    COUNTIF(extraction_date IS NULL OR pipeline_run_id IS NULL OR TRIM(pipeline_run_id) = '') AS failed_cnt
  FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_sexo_anual`
);

-- Check: silver_pronabec_colegios_elegibles_fields_format
-- Tipo: Campos obligatorios no vacíos
SELECT
  'silver_pronabec_colegios_elegibles_fields_format' AS check_id,
  'silver' AS layer,
  'pronabec_colegios_elegibles' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' colegios con institución educativa o tipo de gestión inválidos'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{silver_dataset}.pronabec_colegios_elegibles`
  WHERE institucion_educativa IS NULL OR TRIM(institucion_educativa) = ''
     OR tipo_gestion_colegio IS NULL OR TRIM(tipo_gestion_colegio) = ''
);

-- Check: silver_pronabec_ubigeo_postulacion_fields_nulls
-- Tipo: Campos de ubicación obligatorios no vacíos
SELECT
  'silver_pronabec_ubigeo_postulacion_fields_nulls' AS check_id,
  'silver' AS layer,
  'pronabec_ubigeo_postulacion' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros de ubigeo con región, provincia o distrito nulos o vacíos'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{silver_dataset}.pronabec_ubigeo_postulacion`
  WHERE region IS NULL OR TRIM(region) = ''
     OR provincia IS NULL OR TRIM(provincia) = ''
     OR distrito IS NULL OR TRIM(distrito) = ''
);

-- Check: silver_pronabec_becarios_pais_estudio_fields_nulls
-- Tipo: Campos obligatorios de estudios no vacíos
SELECT
  'silver_pronabec_becarios_pais_estudio_fields_nulls' AS check_id,
  'silver' AS layer,
  'pronabec_becarios_pais_estudio' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros de becarios con institución nula o vacía'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{silver_dataset}.pronabec_becarios_pais_estudio`
  WHERE institucion IS NULL OR TRIM(institucion) = ''
);
