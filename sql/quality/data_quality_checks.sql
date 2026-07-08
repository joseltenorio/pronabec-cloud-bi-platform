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
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' colegios con campos críticos nulos o vacíos'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{silver_dataset}.pronabec_colegios_elegibles`
  WHERE (
    ugel IS NULL OR TRIM(ugel) = ''
    OR institucion_educativa IS NULL OR TRIM(institucion_educativa) = ''
    OR tipo_gestion_colegio IS NULL OR TRIM(tipo_gestion_colegio) = ''
  )
  AND NOT REGEXP_CONTAINS(
    UPPER(TRIM(COALESCE(institucion_educativa, ''))),
    r'ESTUDIOS EN EL EXTRANJERO|CONVALIDADOS POR MINEDU'
  )
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
  'WARNING' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' colegios con completitud territorial o descriptiva parcial'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{silver_dataset}.pronabec_colegios_elegibles`
  WHERE distrito IS NULL OR TRIM(distrito) = ''
     OR nivel_modalidad IS NULL OR TRIM(nivel_modalidad) = ''
     OR forma_atencion IS NULL OR TRIM(forma_atencion) = ''
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
     OR distrito IS NULL OR TRIM(distrito) = ''
     OR (
       (provincia IS NULL OR TRIM(provincia) = '')
       AND UPPER(TRIM(region)) NOT IN ('CHILE', 'COLOMBIA', 'MEXICO')
     )
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

-- Check: silver_inei_population_youth_region_required_ranges
-- Tipo: Campos obligatorios y rangos regionales
SELECT
  'silver_inei_population_youth_region_required_ranges' AS check_id,
  'silver' AS layer,
  'inei_population_youth_region' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros INEI población con campos obligatorios nulos o poblaciones negativas'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{silver_dataset}.inei_population_youth_region`
  WHERE anio IS NULL
     OR region IS NULL OR TRIM(region) = ''
     OR poblacion_15_24 < 0
     OR poblacion_15_29 < 0
);

-- Check: silver_inei_population_youth_region_duplicates
-- Tipo: Duplicados por año y región
SELECT
  'silver_inei_population_youth_region_duplicates' AS check_id,
  'silver' AS layer,
  'inei_population_youth_region' AS table_name,
  'ERROR' AS severity,
  dup_cnt AS failed_rows,
  (dup_cnt = 0) AS passed,
  IF(dup_cnt > 0, CONCAT('Se encontraron ', CAST(dup_cnt AS STRING), ' llaves duplicadas INEI población por año y región'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS dup_cnt
  FROM (
    SELECT anio, region
    FROM `{project_id}.{silver_dataset}.inei_population_youth_region`
    GROUP BY anio, region
    HAVING COUNT(*) > 1
  )
);

-- Check: silver_inei_demographic_indicators_region_required_ranges
-- Tipo: Campos obligatorios y rangos demográficos
SELECT
  'silver_inei_demographic_indicators_region_required_ranges' AS check_id,
  'silver' AS layer,
  'inei_demographic_indicators_region' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros INEI demográficos con campos obligatorios nulos o esperanza de vida inválida'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{silver_dataset}.inei_demographic_indicators_region`
  WHERE anio IS NULL
     OR region IS NULL OR TRIM(region) = ''
     OR esperanza_vida_nacer <= 0
);

-- Check: silver_inei_demographic_indicators_region_duplicates
-- Tipo: Duplicados por año y región
SELECT
  'silver_inei_demographic_indicators_region_duplicates' AS check_id,
  'silver' AS layer,
  'inei_demographic_indicators_region' AS table_name,
  'ERROR' AS severity,
  dup_cnt AS failed_rows,
  (dup_cnt = 0) AS passed,
  IF(dup_cnt > 0, CONCAT('Se encontraron ', CAST(dup_cnt AS STRING), ' llaves duplicadas INEI demográficas por año y región'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS dup_cnt
  FROM (
    SELECT anio, region
    FROM `{project_id}.{silver_dataset}.inei_demographic_indicators_region`
    GROUP BY anio, region
    HAVING COUNT(*) > 1
  )
);

-- Check: silver_inei_pobreza_departamental_required_ranges
-- Tipo: Campos obligatorios y rangos de pobreza
SELECT
  'silver_inei_pobreza_departamental_required_ranges' AS check_id,
  'silver' AS layer,
  'inei_pobreza_departamental' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros INEI pobreza con campos obligatorios nulos o porcentaje fuera de rango'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{silver_dataset}.inei_pobreza_departamental`
  WHERE anio IS NULL
     OR region IS NULL OR TRIM(region) = ''
     OR pobreza_monetaria_pct NOT BETWEEN 0 AND 100
);

-- Check: silver_inei_pobreza_departamental_duplicates
-- Tipo: Duplicados por año y región
SELECT
  'silver_inei_pobreza_departamental_duplicates' AS check_id,
  'silver' AS layer,
  'inei_pobreza_departamental' AS table_name,
  'ERROR' AS severity,
  dup_cnt AS failed_rows,
  (dup_cnt = 0) AS passed,
  IF(dup_cnt > 0, CONCAT('Se encontraron ', CAST(dup_cnt AS STRING), ' llaves duplicadas INEI pobreza por año y región'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS dup_cnt
  FROM (
    SELECT anio, region
    FROM `{project_id}.{silver_dataset}.inei_pobreza_departamental`
    GROUP BY anio, region
    HAVING COUNT(*) > 1
  )
);

-- Check: silver_inei_internet_acceso_region_required_ranges
-- Tipo: Campos obligatorios y rangos de acceso a internet
SELECT
  'silver_inei_internet_acceso_region_required_ranges' AS check_id,
  'silver' AS layer,
  'inei_internet_acceso_region' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros INEI internet con campos obligatorios nulos o porcentaje fuera de rango'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{silver_dataset}.inei_internet_acceso_region`
  WHERE anio IS NULL
     OR region IS NULL OR TRIM(region) = ''
     OR internet_acceso_pct NOT BETWEEN 0 AND 100
);

-- Check: silver_inei_internet_acceso_region_duplicates
-- Tipo: Duplicados por año y región
SELECT
  'silver_inei_internet_acceso_region_duplicates' AS check_id,
  'silver' AS layer,
  'inei_internet_acceso_region' AS table_name,
  'ERROR' AS severity,
  dup_cnt AS failed_rows,
  (dup_cnt = 0) AS passed,
  IF(dup_cnt > 0, CONCAT('Se encontraron ', CAST(dup_cnt AS STRING), ' llaves duplicadas INEI internet por año y región'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS dup_cnt
  FROM (
    SELECT anio, region
    FROM `{project_id}.{silver_dataset}.inei_internet_acceso_region`
    GROUP BY anio, region
    HAVING COUNT(*) > 1
  )
);

-- ============================================================================
-- ML regional context foundation checks
-- ============================================================================

-- Check: ml_region_context_features_not_empty
-- Tipo: Tabla/vista no vacía
SELECT
  'ml_region_context_features_not_empty' AS check_id,
  'ml' AS layer,
  'region_context_features' AS table_name,
  'ERROR' AS severity,
  IF(cnt = 0, 1, 0) AS failed_rows,
  (cnt > 0) AS passed,
  IF(cnt = 0, 'La vista ML de contexto regional está vacía', 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS cnt FROM `{project_id}.{ml_dataset}.region_context_features`
);

-- Check: ml_region_context_features_unique_grain
-- Tipo: Unicidad por grano canónico
SELECT
  'ml_region_context_features_unique_grain' AS check_id,
  'ml' AS layer,
  'region_context_features' AS table_name,
  'ERROR' AS severity,
  dup_cnt AS failed_rows,
  (dup_cnt = 0) AS passed,
  IF(dup_cnt > 0, CONCAT('Se encontraron ', CAST(dup_cnt AS STRING), ' combinaciones duplicadas de anio y region_canonical'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS dup_cnt
  FROM (
    SELECT anio, region_canonical
    FROM `{project_id}.{ml_dataset}.region_context_features`
    GROUP BY anio, region_canonical
    HAVING COUNT(*) > 1
  )
);

-- Check: ml_region_context_features_year_range
-- Tipo: Rango de años esperado
SELECT
  'ml_region_context_features_year_range' AS check_id,
  'ml' AS layer,
  'region_context_features' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con anio fuera de rango 2012-2025'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{ml_dataset}.region_context_features`
  WHERE anio < 2012 OR anio > 2025
);

-- Check: ml_region_context_features_percentage_ranges
-- Tipo: Rangos porcentuales válidos
SELECT
  'ml_region_context_features_percentage_ranges' AS check_id,
  'ml' AS layer,
  'region_context_features' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con porcentajes fuera de rango'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{ml_dataset}.region_context_features`
  WHERE (pobreza_monetaria_pct IS NOT NULL AND (pobreza_monetaria_pct < 0 OR pobreza_monetaria_pct > 100))
     OR (internet_acceso_pct IS NOT NULL AND (internet_acceso_pct < 0 OR internet_acceso_pct > 100))
     OR (brecha_digital_pct IS NOT NULL AND (brecha_digital_pct < 0 OR brecha_digital_pct > 100))
     OR (ruralidad_educativa_pct IS NOT NULL AND (ruralidad_educativa_pct < 0 OR ruralidad_educativa_pct > 100))
);

-- Check: ml_region_context_features_nonnegative_counts
-- Tipo: Conteos no negativos
SELECT
  'ml_region_context_features_nonnegative_counts' AS check_id,
  'ml' AS layer,
  'region_context_features' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con conteos negativos'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{ml_dataset}.region_context_features`
  WHERE (matricula_5to_secundaria IS NOT NULL AND matricula_5to_secundaria < 0)
     OR (matricula_5to_publica IS NOT NULL AND matricula_5to_publica < 0)
     OR (matricula_5to_privada IS NOT NULL AND matricula_5to_privada < 0)
     OR (matricula_5to_urbana IS NOT NULL AND matricula_5to_urbana < 0)
     OR (matricula_5to_rural IS NOT NULL AND matricula_5to_rural < 0)
     OR (poblacion_total IS NOT NULL AND poblacion_total < 0)
     OR (poblacion_15_24 IS NOT NULL AND poblacion_15_24 < 0)
     OR (poblacion_15_29 IS NOT NULL AND poblacion_15_29 < 0)
);

-- Check: ml_region_context_features_completeness_score_range
-- Tipo: Score de completitud en rango
SELECT
  'ml_region_context_features_completeness_score_range' AS check_id,
  'ml' AS layer,
  'region_context_features' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con feature_completeness_score fuera de rango'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{ml_dataset}.region_context_features`
  WHERE feature_completeness_score IS NOT NULL
    AND (feature_completeness_score < 0 OR feature_completeness_score > 1)
);

-- Check: ml_region_context_features_feature_quality_flag_allowed_values
-- Tipo: Bandera de calidad controlada
SELECT
  'ml_region_context_features_feature_quality_flag_allowed_values' AS check_id,
  'ml' AS layer,
  'region_context_features' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con feature_quality_flag fuera del conjunto permitido'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{ml_dataset}.region_context_features`
  WHERE feature_quality_flag IS NOT NULL
    AND feature_quality_flag NOT IN ('complete', 'usable', 'partial', 'insufficient')
);

-- Check: ml_region_context_features_source_priority_allowed_values
-- Tipo: Prioridad de fuente controlada
SELECT
  'ml_region_context_features_source_priority_allowed_values' AS check_id,
  'ml' AS layer,
  'region_context_features' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con source_priority fuera del conjunto permitido'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{ml_dataset}.region_context_features`
  WHERE source_priority IS NOT NULL
    AND source_priority NOT IN ('official', 'manual_public_source', 'synthetic_demo', 'mixed')
);

-- Check: ml_region_context_features_critical_regions_present
-- Tipo: Cobertura de regiones críticas
SELECT
  'ml_region_context_features_critical_regions_present' AS check_id,
  'ml' AS layer,
  'region_context_features' AS table_name,
  'ERROR' AS severity,
  missing_cnt AS failed_rows,
  (missing_cnt = 0) AS passed,
  IF(missing_cnt > 0, CONCAT('Faltan ', CAST(missing_cnt AS STRING), ' regiones críticas en la base regional ML'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS missing_cnt
  FROM (
    SELECT region_name
    FROM UNNEST(['LIMA', 'CALLAO', 'CUSCO', 'PUNO', 'LORETO', 'CAJAMARCA', 'AYACUCHO']) AS region_name
    EXCEPT DISTINCT
    SELECT DISTINCT region_canonical
FROM `{project_id}.{ml_dataset}.region_context_features`
  )
);

-- Check: ml_region_context_features_no_legacy_region_variants
-- Tipo: Ausencia de variantes regionales obsoletas en la salida final
SELECT
  'ml_region_context_features_no_legacy_region_variants' AS check_id,
  'ml' AS layer,
  'region_context_features' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' regiones finales con variantes no canónicas'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
FROM `{project_id}.{ml_dataset}.region_context_features`
  WHERE region IN ('LIMA METROPOLITANA', 'LIMA PROVINCIAS', 'PROV. CONST. DEL CALLAO', 'PROVINCIA CONSTITUCIONAL DEL CALLAO')
     OR region_canonical IN ('LIMA METROPOLITANA', 'LIMA PROVINCIAS', 'PROV. CONST. DEL CALLAO', 'PROVINCIA CONSTITUCIONAL DEL CALLAO')
);

-- Check: ml_region_context_features_synthetic_metadata_consistency
-- Tipo: Consistencia de metadata sintética
SELECT
  'ml_region_context_features_synthetic_metadata_consistency' AS check_id,
  'ml' AS layer,
  'region_context_features' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con metadata sintética inconsistente'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
FROM `{project_id}.{ml_dataset}.region_context_features`
  WHERE (has_synthetic_values = TRUE AND (synthetic_fields IS NULL OR TRIM(synthetic_fields) = ''))
     OR (has_synthetic_values = FALSE AND synthetic_fields IS NOT NULL AND TRIM(synthetic_fields) <> '')
);

-- =============================================================================
-- ML regional priority score checks
-- =============================================================================

-- Check: ml_region_priority_scores_not_empty
-- Tipo: Tabla/vista no vacía
SELECT
  'ml_region_priority_scores_not_empty' AS check_id,
  'ml' AS layer,
  'region_priority_scores' AS table_name,
  'ERROR' AS severity,
  IF(cnt = 0, 1, 0) AS failed_rows,
  (cnt > 0) AS passed,
  IF(cnt = 0, 'La vista ML de score regional está vacía', 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS cnt FROM `{project_id}.{ml_dataset}.region_priority_scores`
);

-- Check: ml_region_priority_scores_unique_grain
-- Tipo: Unicidad por grano canónico
SELECT
  'ml_region_priority_scores_unique_grain' AS check_id,
  'ml' AS layer,
  'region_priority_scores' AS table_name,
  'ERROR' AS severity,
  dup_cnt AS failed_rows,
  (dup_cnt = 0) AS passed,
  IF(dup_cnt > 0, CONCAT('Se encontraron ', CAST(dup_cnt AS STRING), ' combinaciones duplicadas de anio y region_canonical'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS dup_cnt
  FROM (
    SELECT anio, region_canonical
    FROM `{project_id}.{ml_dataset}.region_priority_scores`
    GROUP BY anio, region_canonical
    HAVING COUNT(*) > 1
  )
);

-- Check: ml_region_priority_scores_priority_score_range
-- Tipo: Score principal en rango
SELECT
  'ml_region_priority_scores_priority_score_range' AS check_id,
  'ml' AS layer,
  'region_priority_scores' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con priority_score fuera de rango'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{ml_dataset}.region_priority_scores`
  WHERE priority_score IS NOT NULL
    AND (priority_score < 0 OR priority_score > 1)
);

-- Check: ml_region_priority_scores_component_ranges
-- Tipo: Componentes normalizados en rango
SELECT
  'ml_region_priority_scores_component_ranges' AS check_id,
  'ml' AS layer,
  'region_priority_scores' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con componentes normalizados fuera de rango'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{ml_dataset}.region_priority_scores`
  WHERE (pobreza_score IS NOT NULL AND (pobreza_score < 0 OR pobreza_score > 1))
     OR (demanda_educativa_score IS NOT NULL AND (demanda_educativa_score < 0 OR demanda_educativa_score > 1))
     OR (poblacion_joven_score IS NOT NULL AND (poblacion_joven_score < 0 OR poblacion_joven_score > 1))
     OR (ruralidad_score IS NOT NULL AND (ruralidad_score < 0 OR ruralidad_score > 1))
     OR (brecha_digital_score IS NOT NULL AND (brecha_digital_score < 0 OR brecha_digital_score > 1))
);

-- Check: ml_region_priority_scores_tier_allowed_values
-- Tipo: Tiers controlados
SELECT
  'ml_region_priority_scores_tier_allowed_values' AS check_id,
  'ml' AS layer,
  'region_priority_scores' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con priority_tier fuera del conjunto permitido'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{ml_dataset}.region_priority_scores`
  WHERE priority_tier IS NOT NULL
    AND priority_tier NOT IN ('Muy alta', 'Alta', 'Media', 'Baja', 'Insuficiente')
);

-- Check: ml_region_priority_scores_rank_positive
-- Tipo: Ranking positivo cuando hay score
SELECT
  'ml_region_priority_scores_rank_positive' AS check_id,
  'ml' AS layer,
  'region_priority_scores' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con priority_rank inválido'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{ml_dataset}.region_priority_scores`
  WHERE priority_score IS NOT NULL
    AND (priority_rank IS NULL OR priority_rank < 1)
);

-- Check: ml_region_priority_scores_version_constant
-- Tipo: Versionado estable del score
SELECT
  'ml_region_priority_scores_version_constant' AS check_id,
  'ml' AS layer,
  'region_priority_scores' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con score_version distinto de regional_context_v1'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{ml_dataset}.region_priority_scores`
  WHERE score_version IS NOT NULL
    AND score_version <> 'regional_context_v1'
);

-- Check: ml_region_priority_scores_no_legacy_region_variants
-- Tipo: Ausencia de variantes regionales obsoletas
SELECT
  'ml_region_priority_scores_no_legacy_region_variants' AS check_id,
  'ml' AS layer,
  'region_priority_scores' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' regiones finales con variantes no canónicas'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{ml_dataset}.region_priority_scores`
  WHERE region IN ('LIMA METROPOLITANA', 'LIMA PROVINCIAS', 'PROV. CONST. DEL CALLAO', 'PROVINCIA CONSTITUCIONAL DEL CALLAO')
     OR region_canonical IN ('LIMA METROPOLITANA', 'LIMA PROVINCIAS', 'PROV. CONST. DEL CALLAO', 'PROVINCIA CONSTITUCIONAL DEL CALLAO')
);

-- Check: ml_region_priority_scores_critical_regions_present
-- Tipo: Cobertura de regiones críticas
SELECT
  'ml_region_priority_scores_critical_regions_present' AS check_id,
  'ml' AS layer,
  'region_priority_scores' AS table_name,
  'ERROR' AS severity,
  missing_cnt AS failed_rows,
  (missing_cnt = 0) AS passed,
  IF(missing_cnt > 0, CONCAT('Faltan ', CAST(missing_cnt AS STRING), ' regiones críticas en la prioridad regional ML'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS missing_cnt
  FROM (
    SELECT region_name
    FROM UNNEST(['LIMA', 'CALLAO', 'CUSCO', 'PUNO', 'LORETO', 'CAJAMARCA', 'AYACUCHO']) AS region_name
    EXCEPT DISTINCT
    SELECT DISTINCT region_canonical
    FROM `{project_id}.{ml_dataset}.region_priority_scores`
  )
);

-- =============================================================================
-- Gold predictive region priority score checks
-- =============================================================================

-- Check: gold_predictive_region_priority_scores_not_empty
-- Tipo: Vista no vacía
SELECT
  'gold_predictive_region_priority_scores_not_empty' AS check_id,
  'gold' AS layer,
  'vw_predictive_region_priority_scores' AS table_name,
  'ERROR' AS severity,
  IF(cnt = 0, 1, 0) AS failed_rows,
  (cnt > 0) AS passed,
  IF(cnt = 0, 'La vista Gold predictiva de score regional está vacía', 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS cnt FROM `{project_id}.{gold_dataset}.vw_predictive_region_priority_scores`
);

-- Check: gold_predictive_region_priority_scores_unique_grain
-- Tipo: Unicidad por grano canónico
SELECT
  'gold_predictive_region_priority_scores_unique_grain' AS check_id,
  'gold' AS layer,
  'vw_predictive_region_priority_scores' AS table_name,
  'ERROR' AS severity,
  dup_cnt AS failed_rows,
  (dup_cnt = 0) AS passed,
  IF(dup_cnt > 0, CONCAT('Se encontraron ', CAST(dup_cnt AS STRING), ' combinaciones duplicadas de anio y region_canonical'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS dup_cnt
  FROM (
    SELECT anio, region_canonical
    FROM `{project_id}.{gold_dataset}.vw_predictive_region_priority_scores`
    GROUP BY anio, region_canonical
    HAVING COUNT(*) > 1
  )
);

-- Check: gold_predictive_region_priority_scores_priority_score_range
-- Tipo: Score principal en rango
SELECT
  'gold_predictive_region_priority_scores_priority_score_range' AS check_id,
  'gold' AS layer,
  'vw_predictive_region_priority_scores' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con priority_score fuera de rango'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{gold_dataset}.vw_predictive_region_priority_scores`
  WHERE priority_score IS NOT NULL
    AND (priority_score < 0 OR priority_score > 1)
);

-- Check: gold_predictive_region_priority_scores_priority_score_pct_range
-- Tipo: Score porcentual en rango
SELECT
  'gold_predictive_region_priority_scores_priority_score_pct_range' AS check_id,
  'gold' AS layer,
  'vw_predictive_region_priority_scores' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con priority_score_pct fuera de rango'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{gold_dataset}.vw_predictive_region_priority_scores`
  WHERE priority_score_pct IS NOT NULL
    AND (priority_score_pct < 0 OR priority_score_pct > 100)
);

-- Check: gold_predictive_region_priority_scores_rank_positive
-- Tipo: Ranking positivo cuando hay score
SELECT
  'gold_predictive_region_priority_scores_rank_positive' AS check_id,
  'gold' AS layer,
  'vw_predictive_region_priority_scores' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con priority_rank inválido'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{gold_dataset}.vw_predictive_region_priority_scores`
  WHERE priority_score IS NOT NULL
    AND (priority_rank IS NULL OR priority_rank < 1)
);

-- =============================================================================
-- ML regional coverage feature checks
-- =============================================================================

-- Check: ml_region_coverage_features_not_empty
SELECT
  'ml_region_coverage_features_not_empty' AS check_id,
  'ml' AS layer,
  'region_coverage_features' AS table_name,
  'ERROR' AS severity,
  IF(cnt = 0, 1, 0) AS failed_rows,
  (cnt > 0) AS passed,
  IF(cnt = 0, 'La vista ML de cobertura regional está vacía', 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS cnt FROM `{project_id}.{ml_dataset}.region_coverage_features`
);

-- Check: ml_region_coverage_features_unique_grain
SELECT
  'ml_region_coverage_features_unique_grain' AS check_id,
  'ml' AS layer,
  'region_coverage_features' AS table_name,
  'ERROR' AS severity,
  dup_cnt AS failed_rows,
  (dup_cnt = 0) AS passed,
  IF(dup_cnt > 0, CONCAT('Se encontraron ', CAST(dup_cnt AS STRING), ' combinaciones duplicadas de anio y region_canonical'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS dup_cnt
  FROM (
    SELECT anio, region_canonical
    FROM `{project_id}.{ml_dataset}.region_coverage_features`
    GROUP BY anio, region_canonical
    HAVING COUNT(*) > 1
  )
);

-- Check: ml_region_coverage_features_nonnegative_rates
SELECT
  'ml_region_coverage_features_nonnegative_rates' AS check_id,
  'ml' AS layer,
  'region_coverage_features' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' tasas de cobertura negativas'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{ml_dataset}.region_coverage_features`
  WHERE (becas_por_1000_jovenes IS NOT NULL AND becas_por_1000_jovenes < 0)
     OR (becas_por_1000_matriculados_5to IS NOT NULL AND becas_por_1000_matriculados_5to < 0)
);

-- Check: ml_region_coverage_features_score_ranges
SELECT
  'ml_region_coverage_features_score_ranges' AS check_id,
  'ml' AS layer,
  'region_coverage_features' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' scores de cobertura fuera de rango'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{ml_dataset}.region_coverage_features`
  WHERE (coverage_gap_score IS NOT NULL AND (coverage_gap_score < 0 OR coverage_gap_score > 1))
     OR (primera_generacion_score IS NOT NULL AND (primera_generacion_score < 0 OR primera_generacion_score > 1))
);

-- Check: ml_region_coverage_features_flag_allowed_values
SELECT
  'ml_region_coverage_features_flag_allowed_values' AS check_id,
  'ml' AS layer,
  'region_coverage_features' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con coverage_data_quality_flag fuera del conjunto permitido'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{ml_dataset}.region_coverage_features`
  WHERE coverage_data_quality_flag IS NOT NULL
    AND coverage_data_quality_flag NOT IN ('complete', 'usable', 'partial', 'insufficient')
);

-- Check: ml_region_coverage_features_source_method_allowed_values
SELECT
  'ml_region_coverage_features_source_method_allowed_values' AS check_id,
  'ml' AS layer,
  'region_coverage_features' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con coverage_source_method fuera del conjunto permitido'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{ml_dataset}.region_coverage_features`
  WHERE coverage_source_method IS NOT NULL
    AND coverage_source_method NOT IN (
      'reported_regional_count',
      'estimated_from_regional_share',
      'first_generation_snapshot_only',
      'unavailable',
      'mixed'
    )
);

-- Check: ml_region_coverage_features_no_legacy_region_variants
SELECT
  'ml_region_coverage_features_no_legacy_region_variants' AS check_id,
  'ml' AS layer,
  'region_coverage_features' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' regiones legacy en la cobertura regional'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{ml_dataset}.region_coverage_features`
  WHERE region IN ('LIMA METROPOLITANA', 'LIMA PROVINCIAS', 'PROV. CONST. DEL CALLAO', 'PROVINCIA CONSTITUCIONAL DEL CALLAO')
     OR region_canonical IN ('LIMA METROPOLITANA', 'LIMA PROVINCIAS', 'PROV. CONST. DEL CALLAO', 'PROVINCIA CONSTITUCIONAL DEL CALLAO')
);

-- =============================================================================
-- ML regional priority score v2 checks
-- =============================================================================

-- Check: ml_region_priority_scores_v2_not_empty
SELECT
  'ml_region_priority_scores_v2_not_empty' AS check_id,
  'ml' AS layer,
  'region_priority_scores_v2' AS table_name,
  'ERROR' AS severity,
  IF(cnt = 0, 1, 0) AS failed_rows,
  (cnt > 0) AS passed,
  IF(cnt = 0, 'La vista ML de score regional v2 está vacía', 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS cnt FROM `{project_id}.{ml_dataset}.region_priority_scores_v2`
);

-- Check: ml_region_priority_scores_v2_unique_grain
SELECT
  'ml_region_priority_scores_v2_unique_grain' AS check_id,
  'ml' AS layer,
  'region_priority_scores_v2' AS table_name,
  'ERROR' AS severity,
  dup_cnt AS failed_rows,
  (dup_cnt = 0) AS passed,
  IF(dup_cnt > 0, CONCAT('Se encontraron ', CAST(dup_cnt AS STRING), ' combinaciones duplicadas de anio y region_canonical'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS dup_cnt
  FROM (
    SELECT anio, region_canonical
    FROM `{project_id}.{ml_dataset}.region_priority_scores_v2`
    GROUP BY anio, region_canonical
    HAVING COUNT(*) > 1
  )
);

-- Check: ml_region_priority_scores_v2_priority_score_range
SELECT
  'ml_region_priority_scores_v2_priority_score_range' AS check_id,
  'ml' AS layer,
  'region_priority_scores_v2' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con priority_score_v2 fuera de rango'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{ml_dataset}.region_priority_scores_v2`
  WHERE priority_score_v2 IS NOT NULL
    AND (priority_score_v2 < 0 OR priority_score_v2 > 1)
);

-- Check: ml_region_priority_scores_v2_component_ranges
SELECT
  'ml_region_priority_scores_v2_component_ranges' AS check_id,
  'ml' AS layer,
  'region_priority_scores_v2' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' componentes v2 fuera de rango'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{ml_dataset}.region_priority_scores_v2`
  WHERE (context_priority_score IS NOT NULL AND (context_priority_score < 0 OR context_priority_score > 1))
     OR (coverage_gap_score IS NOT NULL AND (coverage_gap_score < 0 OR coverage_gap_score > 1))
     OR (primera_generacion_score IS NOT NULL AND (primera_generacion_score < 0 OR primera_generacion_score > 1))
);

-- Check: ml_region_priority_scores_v2_tier_allowed_values
SELECT
  'ml_region_priority_scores_v2_tier_allowed_values' AS check_id,
  'ml' AS layer,
  'region_priority_scores_v2' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con priority_tier_v2 fuera del conjunto permitido'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{ml_dataset}.region_priority_scores_v2`
  WHERE priority_tier_v2 IS NOT NULL
    AND priority_tier_v2 NOT IN ('Muy alta', 'Alta', 'Media', 'Baja', 'Insuficiente')
);

-- Check: ml_region_priority_scores_v2_rank_positive
SELECT
  'ml_region_priority_scores_v2_rank_positive' AS check_id,
  'ml' AS layer,
  'region_priority_scores_v2' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con priority_rank_v2 inválido'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{ml_dataset}.region_priority_scores_v2`
  WHERE priority_score_v2 IS NOT NULL
    AND (priority_rank_v2 IS NULL OR priority_rank_v2 < 1)
);

-- Check: ml_region_priority_scores_v2_version_constant
SELECT
  'ml_region_priority_scores_v2_version_constant' AS check_id,
  'ml' AS layer,
  'region_priority_scores_v2' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con score_version distinto de regional_context_coverage_v2'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{ml_dataset}.region_priority_scores_v2`
  WHERE score_version IS NOT NULL
    AND score_version <> 'regional_context_coverage_v2'
);

-- =============================================================================
-- Gold regional priority score v2 checks
-- =============================================================================

-- Check: gold_predictive_region_priority_scores_v2_not_empty
SELECT
  'gold_predictive_region_priority_scores_v2_not_empty' AS check_id,
  'gold' AS layer,
  'vw_predictive_region_priority_scores_v2' AS table_name,
  'ERROR' AS severity,
  IF(cnt = 0, 1, 0) AS failed_rows,
  (cnt > 0) AS passed,
  IF(cnt = 0, 'La vista Gold predictiva de score regional v2 está vacía', 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS cnt FROM `{project_id}.{gold_dataset}.vw_predictive_region_priority_scores_v2`
);

-- Check: gold_predictive_region_priority_scores_v2_unique_grain
SELECT
  'gold_predictive_region_priority_scores_v2_unique_grain' AS check_id,
  'gold' AS layer,
  'vw_predictive_region_priority_scores_v2' AS table_name,
  'ERROR' AS severity,
  dup_cnt AS failed_rows,
  (dup_cnt = 0) AS passed,
  IF(dup_cnt > 0, CONCAT('Se encontraron ', CAST(dup_cnt AS STRING), ' combinaciones duplicadas de anio y region_canonical'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS dup_cnt
  FROM (
    SELECT anio, region_canonical
    FROM `{project_id}.{gold_dataset}.vw_predictive_region_priority_scores_v2`
    GROUP BY anio, region_canonical
    HAVING COUNT(*) > 1
  )
);

-- Check: gold_predictive_region_priority_scores_v2_priority_score_range
SELECT
  'gold_predictive_region_priority_scores_v2_priority_score_range' AS check_id,
  'gold' AS layer,
  'vw_predictive_region_priority_scores_v2' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con priority_score_v2 fuera de rango'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{gold_dataset}.vw_predictive_region_priority_scores_v2`
  WHERE priority_score_v2 IS NOT NULL
    AND (priority_score_v2 < 0 OR priority_score_v2 > 1)
);

-- Check: gold_predictive_region_priority_scores_v2_priority_score_pct_range
SELECT
  'gold_predictive_region_priority_scores_v2_priority_score_pct_range' AS check_id,
  'gold' AS layer,
  'vw_predictive_region_priority_scores_v2' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con priority_score_v2_pct fuera de rango'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{gold_dataset}.vw_predictive_region_priority_scores_v2`
  WHERE priority_score_v2_pct IS NOT NULL
    AND (priority_score_v2_pct < 0 OR priority_score_v2_pct > 100)
);

-- Check: gold_predictive_region_priority_scores_v2_rank_positive
SELECT
  'gold_predictive_region_priority_scores_v2_rank_positive' AS check_id,
  'gold' AS layer,
  'vw_predictive_region_priority_scores_v2' AS table_name,
  'ERROR' AS severity,
  failed_cnt AS failed_rows,
  (failed_cnt = 0) AS passed,
  IF(failed_cnt > 0, CONCAT('Se encontraron ', CAST(failed_cnt AS STRING), ' registros con priority_rank_v2 inválido'), 'Validación exitosa') AS details
FROM (
  SELECT COUNT(*) AS failed_cnt
  FROM `{project_id}.{gold_dataset}.vw_predictive_region_priority_scores_v2`
  WHERE priority_score_v2 IS NOT NULL
    AND (priority_rank_v2 IS NULL OR priority_rank_v2 < 1)
);
