-- ============================================================================
-- Project Cloud BI Platform
-- BigQuery Gold analytics views
-- ============================================================================
--
-- Este script define las primeras vistas Gold del proyecto.
--
-- Decisión de diseño:
-- - Las vistas Gold consumen tablas Silver, no archivos Bronze.
-- - Gold expone KPIs, agregaciones y estructuras listas para Power BI.
-- - La limpieza fuerte, casteos y normalización pertenecen a Silver.
-- - Estas vistas pueden evolucionar luego hacia marts físicos si el volumen
--   o el rendimiento lo requieren.
--
-- Reemplazar antes de ejecutar:
-- - your-gcp-project-id
-- ============================================================================

-- ============================================================================
-- Vista: gold.vw_resumen_ejecutivo
-- Propósito:
-- Vista de KPIs generales para una página ejecutiva en Power BI.
-- ============================================================================

CREATE OR REPLACE VIEW `your-gcp-project-id.gold.vw_resumen_ejecutivo` AS
WITH presupuesto AS (
  SELECT
    SUM(pia) AS pia_total,
    SUM(pim) AS pim_total,
    SUM(devengado) AS devengado_total,
    SUM(girado) AS girado_total,
    SUM(saldo_no_ejecutado) AS saldo_no_ejecutado_total,
    SAFE_MULTIPLY(SAFE_DIVIDE(SUM(devengado), NULLIF(SUM(pim), 0)), 100) AS avance_presupuestal_pct
  FROM `your-gcp-project-id.silver.presupuesto_mef`
),
becarios AS (
  SELECT
    SUM(
      COALESCE(b18_n, 0)
      + COALESCE(permanencia_n, 0)
      + COALESCE(bicentenario_n, 0)
      + COALESCE(especial_n, 0)
      + COALESCE(ffaa_n, 0)
      + COALESCE(vraem_n, 0)
      + COALESCE(repec_n, 0)
      + COALESCE(internacional_n, 0)
      + COALESCE(otros_n, 0)
    ) AS becarios_reportados
  FROM `your-gcp-project-id.silver.becarios_provincia`
  WHERE UPPER(TRIM(COALESCE(provincia, ''))) <> 'TOTAL'
),
notas AS (
  SELECT
    AVG(nota_promedio) AS nota_promedio_general,
    COUNT(*) AS registros_notas
  FROM `your-gcp-project-id.silver.notas_becarios`
),
perdidas AS (
  SELECT
    COUNT(*) AS registros_perdida_beca
  FROM `your-gcp-project-id.silver.perdida_becas`
),
convocatorias AS (
  SELECT
    COUNT(DISTINCT id_convocatoria) AS convocatorias_registradas,
    SUM(vacantes) AS vacantes_registradas
  FROM `your-gcp-project-id.silver.convocatorias`
)
SELECT
  CURRENT_DATE() AS fecha_consulta,
  presupuesto.pia_total,
  presupuesto.pim_total,
  presupuesto.devengado_total,
  presupuesto.girado_total,
  presupuesto.saldo_no_ejecutado_total,
  presupuesto.avance_presupuestal_pct,
  becarios.becarios_reportados,
  notas.nota_promedio_general,
  notas.registros_notas,
  perdidas.registros_perdida_beca,
  convocatorias.convocatorias_registradas,
  convocatorias.vacantes_registradas
FROM presupuesto
CROSS JOIN becarios
CROSS JOIN notas
CROSS JOIN perdidas
CROSS JOIN convocatorias;

-- ============================================================================
-- Vista: gold.vw_presupuesto_mef_anual
-- Propósito:
-- Evolución anual del presupuesto y ejecución financiera.
-- ============================================================================

CREATE OR REPLACE VIEW `your-gcp-project-id.gold.vw_presupuesto_mef_anual` AS
SELECT
  ano,
  ejecutora_nombre,
  SUM(pia) AS pia_total,
  SUM(pim) AS pim_total,
  SUM(certificacion) AS certificacion_total,
  SUM(compromiso_anual) AS compromiso_anual_total,
  SUM(compromiso_mensual) AS compromiso_mensual_total,
  SUM(devengado) AS devengado_total,
  SUM(girado) AS girado_total,
  SUM(saldo_no_ejecutado) AS saldo_no_ejecutado_total,
  SAFE_MULTIPLY(SAFE_DIVIDE(SUM(devengado), NULLIF(SUM(pim), 0)), 100) AS tasa_ejecucion_pct,
  MAX(extraction_date) AS ultima_fecha_extraccion,
  MAX(ingestion_timestamp) AS ultima_ingesta
FROM `your-gcp-project-id.silver.presupuesto_mef`
GROUP BY
  ano,
  ejecutora_nombre;

-- ============================================================================
-- Vista: gold.vw_becarios_por_departamento
-- Propósito:
-- Cobertura territorial agregada por departamento.
-- ============================================================================

CREATE OR REPLACE VIEW `your-gcp-project-id.gold.vw_becarios_por_departamento` AS
SELECT
  departamento,
  SUM(COALESCE(b18_n, 0)) AS beca18_total,
  SUM(COALESCE(permanencia_n, 0)) AS permanencia_total,
  SUM(COALESCE(bicentenario_n, 0)) AS bicentenario_total,
  SUM(COALESCE(especial_n, 0)) AS especial_total,
  SUM(COALESCE(ffaa_n, 0)) AS ffaa_total,
  SUM(COALESCE(vraem_n, 0)) AS vraem_total,
  SUM(COALESCE(repec_n, 0)) AS repec_total,
  SUM(COALESCE(internacional_n, 0)) AS internacional_total,
  SUM(COALESCE(otros_n, 0)) AS otros_total,
  SUM(
    COALESCE(b18_n, 0)
    + COALESCE(permanencia_n, 0)
    + COALESCE(bicentenario_n, 0)
    + COALESCE(especial_n, 0)
    + COALESCE(ffaa_n, 0)
    + COALESCE(vraem_n, 0)
    + COALESCE(repec_n, 0)
    + COALESCE(internacional_n, 0)
    + COALESCE(otros_n, 0)
  ) AS becarios_total,
  COUNT(DISTINCT provincia) AS provincias_reportadas,
  MAX(extraction_date) AS ultima_fecha_extraccion,
  MAX(ingestion_timestamp) AS ultima_ingesta
FROM `your-gcp-project-id.silver.becarios_provincia`
WHERE UPPER(TRIM(COALESCE(provincia, ''))) <> 'TOTAL'
GROUP BY
  departamento;

-- ============================================================================
-- Vista: gold.vw_notas_por_semestre
-- Propósito:
-- Rendimiento académico agregado por semestre y ciclo.
-- ============================================================================

CREATE OR REPLACE VIEW `your-gcp-project-id.gold.vw_notas_por_semestre` AS
SELECT
  semestre,
  ciclo,
  COUNT(DISTINCT codigo_becario) AS becarios_con_nota,
  COUNT(*) AS registros_notas,
  AVG(nota_promedio) AS nota_promedio,
  MIN(nota_promedio) AS nota_minima,
  MAX(nota_promedio) AS nota_maxima,
  COUNTIF(nota_promedio < 11) AS registros_en_riesgo_academico,
  SAFE_MULTIPLY(SAFE_DIVIDE(COUNTIF(nota_promedio < 11), COUNT(*)), 100) AS tasa_riesgo_academico_pct,
  MAX(extraction_date) AS ultima_fecha_extraccion,
  MAX(ingestion_timestamp) AS ultima_ingesta
FROM `your-gcp-project-id.silver.notas_becarios`
GROUP BY
  semestre,
  ciclo;

-- ============================================================================
-- Vista: gold.vw_desercion_por_convocatoria
-- Propósito:
-- Análisis de pérdida de beca por convocatoria, motivo y territorio.
-- ============================================================================

CREATE OR REPLACE VIEW `your-gcp-project-id.gold.vw_desercion_por_convocatoria` AS
SELECT
  convocatoria,
  departamento,
  motivo_perdida,
  tipo_ies,
  sexo,
  COUNT(*) AS total_perdidas,
  COUNT(DISTINCT institucion) AS instituciones_reportadas,
  COUNT(DISTINCT carrera) AS carreras_reportadas,
  MAX(fecha_resolucion) AS ultima_fecha_resolucion,
  MAX(extraction_date) AS ultima_fecha_extraccion,
  MAX(ingestion_timestamp) AS ultima_ingesta
FROM `your-gcp-project-id.silver.perdida_becas`
GROUP BY
  convocatoria,
  departamento,
  motivo_perdida,
  tipo_ies,
  sexo;

-- ============================================================================
-- Vista: gold.vw_presupuesto_vs_becas
-- Propósito:
-- Relacionar presupuesto anual con cobertura territorial reportada.
--
-- Nota:
-- Esta vista ofrece una aproximación ejecutiva. La relación exacta entre
-- presupuesto y beneficiarios dependerá de reglas de negocio adicionales,
-- granularidad temporal compatible y definición final de cobertura.
-- ============================================================================

CREATE OR REPLACE VIEW `your-gcp-project-id.gold.vw_presupuesto_vs_becas` AS
WITH presupuesto_anual AS (
  SELECT
    ano,
    SUM(pim) AS pim_total,
    SUM(devengado) AS devengado_total,
    SUM(saldo_no_ejecutado) AS saldo_no_ejecutado_total,
    SAFE_MULTIPLY(SAFE_DIVIDE(SUM(devengado), NULLIF(SUM(pim), 0)), 100) AS tasa_ejecucion_pct
  FROM `your-gcp-project-id.silver.presupuesto_mef`
  GROUP BY
    ano
),
cobertura AS (
  SELECT
    SUM(
      COALESCE(b18_n, 0)
      + COALESCE(permanencia_n, 0)
      + COALESCE(bicentenario_n, 0)
      + COALESCE(especial_n, 0)
      + COALESCE(ffaa_n, 0)
      + COALESCE(vraem_n, 0)
      + COALESCE(repec_n, 0)
      + COALESCE(internacional_n, 0)
      + COALESCE(otros_n, 0)
    ) AS becarios_reportados
  FROM `your-gcp-project-id.silver.becarios_provincia`
  WHERE UPPER(TRIM(COALESCE(provincia, ''))) <> 'TOTAL'
)
SELECT
  presupuesto_anual.ano,
  presupuesto_anual.pim_total,
  presupuesto_anual.devengado_total,
  presupuesto_anual.saldo_no_ejecutado_total,
  presupuesto_anual.tasa_ejecucion_pct,
  cobertura.becarios_reportados,
  SAFE_DIVIDE(presupuesto_anual.devengado_total, NULLIF(cobertura.becarios_reportados, 0)) AS devengado_por_becario_referencial,
  SAFE_DIVIDE(presupuesto_anual.pim_total, NULLIF(cobertura.becarios_reportados, 0)) AS pim_por_becario_referencial
FROM presupuesto_anual
CROSS JOIN cobertura;