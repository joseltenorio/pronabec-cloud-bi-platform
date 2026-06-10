-- ============================================================================
-- Project Cloud BI Platform
-- BigQuery dataset creation script
-- ============================================================================

-- Este script crea los datasets base del proyecto en BigQuery.
-- Los nombres siguen la arquitectura Medallion y separan datos crudos,
-- datos limpios, datos analíticos, auditoría y Machine Learning.
--
-- Reemplazar `your-gcp-project-id` por el ID real del proyecto GCP antes
-- de ejecutar este script en BigQuery o mediante bq CLI.

CREATE SCHEMA IF NOT EXISTS `your-gcp-project-id.bronze`
OPTIONS (
  location = "US",
  description = "Dataset para datos crudos, tablas externas o staging raw provenientes de Cloud Storage."
);

CREATE SCHEMA IF NOT EXISTS `your-gcp-project-id.silver`
OPTIONS (
  location = "US",
  description = "Dataset para datos limpios, normalizados y validados del pipeline analítico."
);

CREATE SCHEMA IF NOT EXISTS `your-gcp-project-id.gold`
OPTIONS (
  location = "US",
  description = "Dataset para vistas y marts analíticos listos para consumo desde Power BI."
);

CREATE SCHEMA IF NOT EXISTS `your-gcp-project-id.audit`
OPTIONS (
  location = "US",
  description = "Dataset para auditoría de ejecuciones, extracciones y resultados de calidad de datos."
);

CREATE SCHEMA IF NOT EXISTS `your-gcp-project-id.ml`
OPTIONS (
  location = "US",
  description = "Dataset para modelos, evaluaciones y predicciones implementadas con BigQuery ML."
);