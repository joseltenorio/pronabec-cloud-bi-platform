# PRONABEC Cloud BI Platform

Plataforma de ingeniería de datos en Google Cloud para analítica de PRONABEC, orientada a integrar datos públicos de becas y presupuesto en una plataforma batch basada en arquitectura Medallion, BigQuery y Power BI.

## Descripción general

PRONABEC Cloud Data Platform es un proyecto de Data Engineering y Business Intelligence orientado al procesamiento batch de información pública relacionada con programas de becas, rendimiento académico, cobertura territorial y ejecución presupuestal.

La plataforma sigue una arquitectura cloud-native sobre Google Cloud. Los datos crudos se ingieren desde fuentes públicas, se almacenan en Cloud Storage, se transforman mediante procesamiento batch, se modelan en BigQuery y se consumen mediante dashboards ejecutivos en Power BI.

## Contexto de negocio

El proyecto se enfoca en datos públicos relacionados con programas de becas de PRONABEC y ejecución presupuestal del Ministerio de Economía y Finanzas. Su propósito es apoyar preguntas analíticas como:

- ¿Cómo ha evolucionado el presupuesto público asignado a PRONABEC a lo largo del tiempo?
- ¿Cuál es la relación entre la ejecución presupuestal y la cobertura de becas?
- ¿Qué regiones concentran la mayor cantidad de beneficiarios?
- ¿Cómo se comportan el rendimiento académico y la pérdida de becas por periodos, programas y territorios?
- ¿Qué indicadores pueden apoyar la toma de decisiones ejecutiva para la planificación de becas?

## Arquitectura Google Cloud planificada

El proyecto está diseñado como una plataforma batch de datos utilizando los siguientes servicios:

- **Cloud Storage** como data lake para archivos Bronze e intermedios.
- **Cloud Run Jobs** para ejecutar trabajos de extracción de datos en contenedores.
- **Dataflow** para la transformación batch desde Bronze hacia Silver.
- **BigQuery** como data warehouse analítico para las capas Silver y Gold.
- **Cloud Composer** para la orquestación de workflows con Apache Airflow.
- **Cloud Logging** y **Cloud Monitoring** para observabilidad.
- **Secret Manager** para la gestión segura de configuración y credenciales.
- **Power BI** como capa de reporting y visualización ejecutiva.

## Arquitectura Medallion

La plataforma sigue una arquitectura Medallion:

### Bronze

Datos crudos extraídos desde los sistemas fuente y almacenados en Cloud Storage sin transformaciones de negocio. Esta capa conserva la estructura original de los datos y permite trazabilidad sobre cada extracción.

### Silver

Datos limpios, estandarizados y validados en BigQuery. Esta capa incluye conversión de tipos, normalización de textos, formateo de fechas, validaciones de calidad y manejo de registros rechazados.

### Gold

Tablas y vistas analíticas listas para negocio en BigQuery. Esta capa está optimizada para consumo desde Power BI y análisis ejecutivo.

## Fuentes de datos

El proyecto considera las siguientes fuentes públicas:

- Endpoints públicos de datos de PRONABEC.
- Información presupuestal pública del MEF.
- Datos geográficos de referencia para análisis territorial.

## Capa de reporting

Los dashboards de Power BI consumirán datasets curados desde BigQuery Gold. Las páginas de reporte planificadas son:

1. Resumen Ejecutivo.
2. Ejecución Presupuestal.
3. Rendimiento Académico y Pérdida de Beca.
4. Cobertura Territorial.

## Estado del proyecto

Este repositorio se encuentra en desarrollo activo. La etapa actual define la arquitectura cloud, el alcance analítico y la estructura base del repositorio antes de implementar los pipelines de ingesta, transformación y modelado.

## Stack tecnológico

- Python
- Google Cloud Storage
- Google BigQuery
- Google Cloud Dataflow
- Google Cloud Composer
- Cloud Run Jobs
- Cloud Logging
- Cloud Monitoring
- Secret Manager
- Power BI
- GitHub
