-- =============================================================================
-- Project Cloud BI Platform
-- BigQuery Gold analytical views
-- =============================================================================
--
-- Este script define las vistas analíticas de la capa Gold del proyecto.
--
-- Decisiones de diseño:
-- 1. Gold consume exclusivamente de tablas Silver ({project_id}.{silver_dataset}),
--    nunca directamente de Bronze, ni de archivos crudos locales o en GCS.
-- 2. El formateo, casteos fuertes, unpivot y canonización ya fueron resueltos en Silver.
-- 3. Las vistas Gold exponen agregaciones seguras, unificaciones lógicas (UNION ALL) y
--    KPIs clave optimizados para el consumo directo en Power BI.
-- 4. Se utilizan placeholders ({project_id}, {silver_dataset}, {gold_dataset})
--    para la creación dinámica de las vistas.
--
-- =============================================================================

-- =============================================================================
-- Gold - Resumen ejecutivo
-- =============================================================================

CREATE OR REPLACE VIEW `{project_id}.{gold_dataset}.vw_pronabec_resumen_ejecutivo` AS
WITH mef_stats AS (
  SELECT
    SUM(pia) AS pia_total,
    SUM(pim) AS pim_total,
    SUM(devengado) AS devengado_total,
    SAFE_MULTIPLY(SAFE_DIVIDE(SUM(devengado), NULLIF(SUM(pim), 0)), 100) AS avance_presupuestal_pct
  FROM `{project_id}.{silver_dataset}.presupuesto_mef`
),
becas_stats AS (
  SELECT
    SUM(becas_otorgadas) AS total_becas_otorgadas,
    COUNT(DISTINCT modalidad) AS modalidades_atendidas
  FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_becas_otorgadas_modalidad_anual`
),
convocatorias_stats AS (
  SELECT
    COUNT(DISTINCT id_convocatoria) AS convocatorias_registradas,
    SUM(vacantes) AS vacantes_registradas
  FROM `{project_id}.{silver_dataset}.pronabec_convocatorias`
)
SELECT
  CURRENT_DATE() AS fecha_consulta,
  mef_stats.pia_total,
  mef_stats.pim_total,
  mef_stats.devengado_total,
  mef_stats.avance_presupuestal_pct,
  becas_stats.total_becas_otorgadas,
  becas_stats.modalidades_atendidas,
  convocatorias_stats.convocatorias_registradas,
  convocatorias_stats.vacantes_registradas
FROM mef_stats
CROSS JOIN becas_stats
CROSS JOIN convocatorias_stats;


-- =============================================================================
-- Gold - Beca 18 evolución y cobertura
-- =============================================================================

CREATE OR REPLACE VIEW `{project_id}.{gold_dataset}.vw_beca18_becas_otorgadas_anual` AS
SELECT
  modalidad,
  ano_convocatoria,
  becas_otorgadas,
  source_document_file,
  source_page
FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_becas_otorgadas_modalidad_anual`;


-- =============================================================================
-- Gold - Beca 18 cobertura territorial
-- =============================================================================

CREATE OR REPLACE VIEW `{project_id}.{gold_dataset}.vw_beca18_cobertura_territorial_2016` AS
SELECT
  region,
  provincia,
  becarios_b18_count,
  source_snapshot_date,
  extraction_date,
  pipeline_run_id,
  RANK() OVER (ORDER BY becarios_b18_count DESC) AS ranking_nacional_provincia,
  SUM(becarios_b18_count) OVER (PARTITION BY region) AS total_region_b18,
  SAFE_DIVIDE(
    becarios_b18_count,
    SUM(becarios_b18_count) OVER (PARTITION BY region)
  ) AS participacion_provincia_region
FROM `{project_id}.{silver_dataset}.pronabec_beca18_becarios_provincia_2016`
WHERE region IS NOT NULL
  AND TRIM(region) != ''
  AND provincia IS NOT NULL
  AND TRIM(provincia) != ''
  AND UPPER(TRIM(provincia)) NOT LIKE 'TOTAL%';


-- =============================================================================
-- Gold - Beca 18 universitarios
-- =============================================================================

CREATE OR REPLACE VIEW `{project_id}.{gold_dataset}.vw_beca18_universitarios_carrera_anual` AS
SELECT
  carrera_estudio,
  carrera_estudio_canonical,
  COALESCE(carrera_estudio_canonical, carrera_estudio) AS carrera_estudio_final,
  carrera_estudio_canonical_match_method,
  carrera_estudio_canonical_review_required,
  ano_convocatoria,
  cantidad_becarios,
  es_anio_preliminar,
  source_publication_url,
  source_table
FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_universitarios_carrera_anual`;


CREATE OR REPLACE VIEW `{project_id}.{gold_dataset}.vw_beca18_universitarios_universidad_anual` AS
SELECT
  universidad,
  universidad_canonical,
  COALESCE(universidad_canonical, universidad) AS universidad_final,
  universidad_canonical_match_method,
  universidad_canonical_review_required,
  ano_convocatoria,
  cantidad_becarios,
  es_anio_preliminar,
  source_publication_url,
  source_table
FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_universitarios_universidad_anual`;


-- =============================================================================
-- Gold - Perfil social PES 2025
-- =============================================================================

CREATE OR REPLACE VIEW `{project_id}.{gold_dataset}.vw_beca18_perfil_social_indicadores` AS
SELECT
  'Perfil Étnico' AS indicator_group,
  'Autoidentificación Étnica' AS indicator_name,
  modalidad AS category,
  autoidentificacion_etnica AS subcategory,
  CAST(ano_encuesta AS STRING) AS period,
  CAST(NULL AS INT64) AS value_count,
  porcentaje_becarios AS value_percentage,
  source_dataset,
  source_document_file
FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_autoidentificacion_etnica_modalidad_2025`

UNION ALL

SELECT
  'Perfil Lingüístico' AS indicator_group,
  'Lengua Materna' AS indicator_name,
  modalidad AS category,
  lengua_materna AS subcategory,
  CAST(ano_encuesta AS STRING) AS period,
  CAST(NULL AS INT64) AS value_count,
  porcentaje_becarios AS value_percentage,
  source_dataset,
  source_document_file
FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_lengua_materna_modalidad_2025`

UNION ALL

SELECT
  'Entorno Educativo' AS indicator_group,
  'Colegio de Procedencia' AS indicator_name,
  'Tipo de Gestión' AS category,
  tipo_gestion_colegio AS subcategory,
  CAST(ano_encuesta AS STRING) AS period,
  CAST(NULL AS INT64) AS value_count,
  porcentaje_becarios AS value_percentage,
  source_dataset,
  source_document_file
FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_colegio_gestion_2025`

UNION ALL

SELECT
  'Entorno Familiar' AS indicator_group,
  'Nivel Educativo de los Padres' AS indicator_name,
  'Nivel Educativo' AS category,
  nivel_educativo_padres AS subcategory,
  CAST(ano_encuesta AS STRING) AS period,
  CAST(NULL AS INT64) AS value_count,
  porcentaje_becarios AS value_percentage,
  source_dataset,
  source_document_file
FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_padres_nivel_educativo_2025`

UNION ALL

SELECT
  'Movilidad Social' AS indicator_group,
  'Primera Generación Universitaria' AS indicator_name,
  region AS category,
  'Primera Generación' AS subcategory,
  periodo AS period,
  total_becarios_primera_generacion AS value_count,
  ratio_primera_generacion AS value_percentage,
  source_dataset,
  source_document_file
FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_primera_generacion_region`

UNION ALL

SELECT
  'Impacto Social' AS indicator_group,
  'No Continuaría Sin Beca' AS indicator_name,
  grupo_caracteristica AS category,
  caracteristica AS subcategory,
  CAST(ano_encuesta AS STRING) AS period,
  CAST(NULL AS INT64) AS value_count,
  porcentaje_becarios AS value_percentage,
  source_dataset,
  source_document_file
FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_no_continuaria_sin_beca_caracteristica_2025`

UNION ALL

SELECT
  'Entorno Educativo' AS indicator_group,
  'Tipo de Preparación IES' AS indicator_name,
  'Tipo de Preparación' AS category,
  tipo_preparacion AS subcategory,
  CAST(ano_encuesta AS STRING) AS period,
  CAST(NULL AS INT64) AS value_count,
  porcentaje_becarios AS value_percentage,
  source_dataset,
  source_document_file
FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_preparacion_ies_tipo_2025`

UNION ALL

SELECT
  'Entorno Educativo' AS indicator_group,
  'Meses de Preparación IES' AS indicator_name,
  grupo_caracteristica AS category,
  caracteristica AS subcategory,
  CAST(ano_encuesta AS STRING) AS period,
  CAST(NULL AS INT64) AS value_count,
  promedio_meses_preparacion AS value_percentage,
  source_dataset,
  source_document_file
FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_preparacion_ies_meses_caracteristica_2025`

UNION ALL

SELECT
  'Perfil Demográfico' AS indicator_group,
  'Distribución por Sexo' AS indicator_name,
  'Sexo' AS category,
  sexo AS subcategory,
  CAST(ano_convocatoria AS STRING) AS period,
  CAST(NULL AS INT64) AS value_count,
  porcentaje_becarios AS value_percentage,
  source_dataset,
  source_document_file
FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_sexo_anual`

UNION ALL

SELECT
  'Rendimiento ENP' AS indicator_group,
  'Puntaje ENP por Característica' AS indicator_name,
  grupo_caracteristica AS category,
  caracteristica AS subcategory,
  CAST(ano_encuesta AS STRING) AS period,
  CAST(NULL AS INT64) AS value_count,
  puntaje_promedio_enp AS value_percentage,
  source_dataset,
  source_document_file
FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_enp_promedio_caracteristica_2025`

UNION ALL

SELECT
  'Rendimiento ENP' AS indicator_group,
  'Puntaje ENP por Región' AS indicator_name,
  'Región' AS category,
  region AS subcategory,
  CAST(ano_encuesta AS STRING) AS period,
  CAST(NULL AS INT64) AS value_count,
  puntaje_promedio_enp AS value_percentage,
  source_dataset,
  source_document_file
FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_enp_promedio_region_2025`

UNION ALL

SELECT
  'Entorno Educativo' AS indicator_group,
  'Periodo de Ingreso a la Educación Superior' AS indicator_name,
  sexo AS category,
  periodo_ingreso_ies AS subcategory,
  CAST(ano_encuesta AS STRING) AS period,
  CAST(NULL AS INT64) AS value_count,
  porcentaje_becarios AS value_percentage,
  source_dataset,
  source_document_file
FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_periodo_ingreso_ies_genero_2025`;


CREATE OR REPLACE VIEW `{project_id}.{gold_dataset}.vw_beca18_region_postulacion` AS
SELECT
  'anual' AS tipo_registro,
  grupo_region AS region,
  CAST(ano_convocatoria AS STRING) AS periodo,
  porcentaje_becarios,
  source_dataset,
  source_document_file
FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_region_postulacion_anual`

UNION ALL

SELECT
  'acumulado' AS tipo_registro,
  region,
  periodo,
  porcentaje_acumulado AS porcentaje_becarios,
  source_dataset,
  source_document_file
FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_region_postulacion_acumulada`

UNION ALL

SELECT
  'encuesta_2025' AS tipo_registro,
  region,
  CAST(ano_encuesta AS STRING) AS periodo,
  porcentaje_becarios,
  source_dataset,
  source_document_file
FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_region_postulacion_2025`;


CREATE OR REPLACE VIEW `{project_id}.{gold_dataset}.vw_beca18_migracion_region` AS
SELECT
  'anual' AS tipo_registro,
  'Nacional' AS region,
  CAST(ano_convocatoria AS STRING) AS periodo,
  porcentaje_migracion_region AS tasa_migracion,
  source_dataset,
  source_document_file
FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_migracion_region_anual`

UNION ALL

SELECT
  'acumulado' AS tipo_registro,
  region,
  periodo,
  tasa_migracion_acumulada AS tasa_migracion,
  source_dataset,
  source_document_file
FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_migracion_region_acumulada`;


-- =============================================================================
-- Gold - Trayectoria y elección
-- =============================================================================

CREATE OR REPLACE VIEW `{project_id}.{gold_dataset}.vw_beca18_trayectoria_eleccion` AS
SELECT
  'Elección de Carrera por Gestión IES' AS dimension_analisis,
  razon_eleccion_carrera AS motivo,
  gestion_ies AS segmento,
  porcentaje_becarios,
  source_dataset,
  source_document_file
FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_razones_eleccion_carrera_gestion_ies_2025`

UNION ALL

SELECT
  'Elección de Carrera por Sexo' AS dimension_analisis,
  razon_eleccion_carrera AS motivo,
  sexo AS segmento,
  porcentaje_becarios,
  source_dataset,
  source_document_file
FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_razones_eleccion_carrera_sexo_2025`

UNION ALL

SELECT
  'Elección de IES por Gestión Escolar de Origen' AS dimension_analisis,
  razon_eleccion_ies AS motivo,
  gestion_ies AS segmento,
  porcentaje_becarios,
  source_dataset,
  source_document_file
FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_razones_eleccion_ies_gestion_2025`;


-- =============================================================================
-- Gold - Presupuesto MEF
-- =============================================================================

CREATE OR REPLACE VIEW `{project_id}.{gold_dataset}.vw_mef_presupuesto_ejecucion_anual` AS
SELECT
  ano,
  codigo_entidad,
  nombre_entidad,
  pia,
  pim,
  devengado,
  avance_porcentaje,
  SAFE_MULTIPLY(SAFE_DIVIDE(devengado, NULLIF(pim, 0)), 100) AS avance_calculado_pct,
  extraction_date,
  ingestion_timestamp
FROM `{project_id}.{silver_dataset}.presupuesto_mef`;


CREATE OR REPLACE VIEW `{project_id}.{gold_dataset}.vw_mef_presupuesto_ejecucion_temporal` AS
SELECT
  ano,
  periodo_tipo,
  periodo_valor,
  trimestre,
  mes_numero,
  mes_nombre,
  devengado,
  extraction_date,
  ingestion_timestamp
FROM `{project_id}.{silver_dataset}.presupuesto_mef_temporal`;


CREATE OR REPLACE VIEW `{project_id}.{gold_dataset}.vw_mef_presupuesto_producto` AS
SELECT
  ano,
  codigo_producto,
  producto,
  pia,
  pim,
  devengado,
  avance_porcentaje,
  SAFE_MULTIPLY(SAFE_DIVIDE(devengado, NULLIF(pim, 0)), 100) AS avance_calculado_pct
FROM `{project_id}.{silver_dataset}.presupuesto_mef_producto`;


CREATE OR REPLACE VIEW `{project_id}.{gold_dataset}.vw_mef_presupuesto_actividad` AS
SELECT
  ano,
  codigo_producto,
  producto,
  codigo_actividad,
  actividad,
  pia,
  pim,
  devengado,
  avance_porcentaje,
  SAFE_MULTIPLY(SAFE_DIVIDE(devengado, NULLIF(pim, 0)), 100) AS avance_calculado_pct
FROM `{project_id}.{silver_dataset}.presupuesto_mef_actividad`;


CREATE OR REPLACE VIEW `{project_id}.{gold_dataset}.vw_mef_presupuesto_generica` AS
SELECT
  ano,
  codigo_generica,
  generica,
  pia,
  pim,
  devengado,
  avance_porcentaje,
  SAFE_MULTIPLY(SAFE_DIVIDE(devengado, NULLIF(pim, 0)), 100) AS avance_calculado_pct
FROM `{project_id}.{silver_dataset}.presupuesto_mef_generica`;


-- =============================================================================
-- Gold - Becas vs presupuesto
-- =============================================================================

CREATE OR REPLACE VIEW `{project_id}.{gold_dataset}.vw_pronabec_becas_vs_presupuesto_anual` AS
WITH becas_anuales AS (
  SELECT
    ano_convocatoria AS ano,
    SUM(becas_otorgadas) AS becas_otorgadas_total
  FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_becas_otorgadas_modalidad_anual`
  GROUP BY ano_convocatoria
),
mef_anual AS (
  SELECT
    ano,
    SUM(pia) AS pia_total,
    SUM(pim) AS pim_total,
    SUM(devengado) AS devengado_total
  FROM `{project_id}.{silver_dataset}.presupuesto_mef`
  GROUP BY ano
)
SELECT
  COALESCE(becas_anuales.ano, mef_anual.ano) AS ano,
  becas_anuales.becas_otorgadas_total,
  mef_anual.pia_total,
  mef_anual.pim_total,
  mef_anual.devengado_total,
  SAFE_MULTIPLY(SAFE_DIVIDE(mef_anual.devengado_total, NULLIF(mef_anual.pim_total, 0)), 100) AS avance_presupuestal_pct,
  SAFE_DIVIDE(mef_anual.devengado_total, NULLIF(becas_anuales.becas_otorgadas_total, 0)) AS devengado_por_beca,
  SAFE_DIVIDE(mef_anual.pim_total, NULLIF(becas_anuales.becas_otorgadas_total, 0)) AS pim_por_beca
FROM becas_anuales
FULL OUTER JOIN mef_anual ON becas_anuales.ano = mef_anual.ano;


-- =============================================================================
-- Gold - Resumen analítico Beca 18
-- =============================================================================

CREATE OR REPLACE VIEW `{project_id}.{gold_dataset}.vw_pronabec_beca18_resumen_analitico` AS
WITH becas_otorgadas AS (
  SELECT
    ano_convocatoria AS ano,
    SUM(becas_otorgadas) AS total_becas_otorgadas
  FROM `{project_id}.{silver_dataset}.pronabec_report_beca18_becas_otorgadas_modalidad_anual`
  GROUP BY ano_convocatoria
),
presupuesto_ejecutado AS (
  SELECT
    ano,
    SUM(pia) AS total_pia,
    SUM(pim) AS total_pim,
    SUM(devengado) AS total_devengado
  FROM `{project_id}.{silver_dataset}.presupuesto_mef`
  GROUP BY ano
)
SELECT
  COALESCE(b.ano, p.ano) AS ano,
  b.total_becas_otorgadas,
  p.total_pia,
  p.total_pim,
  p.total_devengado,
  SAFE_MULTIPLY(SAFE_DIVIDE(p.total_devengado, NULLIF(p.total_pim, 0)), 100) AS avance_presupuestal_pct,
  SAFE_DIVIDE(p.total_devengado, NULLIF(b.total_becas_otorgadas, 0)) AS costo_promedio_por_beca
FROM becas_otorgadas b
FULL OUTER JOIN presupuesto_ejecutado p ON b.ano = p.ano;