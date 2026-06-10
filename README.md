# PRONABEC Cloud BI Platform

Plataforma de datos en Google Cloud para integrar, transformar y analizar información pública relacionada con PRONABEC y presupuesto del MEF, aplicando arquitectura Medallion, procesamiento batch, BigQuery como data warehouse y Power BI como capa de visualización ejecutiva.

## Descripción general

Este proyecto implementa una solución de Data Engineering y Business Intelligence orientada al análisis de becas, rendimiento académico, cobertura territorial y ejecución presupuestal.

La plataforma integra datos públicos de PRONABEC y del Ministerio de Economía y Finanzas, los almacena en una arquitectura por capas, los transforma mediante servicios cloud y los expone en modelos analíticos listos para reportes en Power BI.

## Objetivo

Diseñar una plataforma batch en Google Cloud que permita centralizar datos públicos, transformarlos en información confiable y facilitar el análisis ejecutivo sobre programas de becas, presupuesto, deserción, rendimiento académico y cobertura territorial.

## Arquitectura propuesta

La solución sigue una arquitectura Medallion:

- Bronze: almacenamiento de datos crudos en Cloud Storage.
- Silver: datos limpios, normalizados y estructurados en BigQuery.
- Gold: vistas y modelos analíticos optimizados para Power BI.
- ML: capa opcional de BigQuery ML para análisis predictivo.

## Servicios principales

- Google Cloud Storage para el data lake.
- Dataflow para procesamiento batch.
- BigQuery para almacenamiento analítico.
- Cloud Composer para orquestación de workflows.
- Cloud Run Jobs para tareas de ingesta.
- Cloud Logging y Cloud Monitoring para observabilidad.
- Power BI para visualización ejecutiva.
- GitHub para versionamiento del código y documentación.

## Fuentes de datos

- API pública de datos abiertos de PRONABEC.
- Consulta presupuestal del MEF.
- Catálogos auxiliares de ubicación, becas, periodos y entidades educativas.

## Capas de datos

### Bronze

Contiene los datos crudos extraídos desde las fuentes originales. Esta capa conserva los archivos con mínima transformación y permite trazabilidad sobre cada extracción.

### Silver

Contiene datos depurados, tipados y normalizados. En esta capa se corrigen nombres de columnas, formatos de fecha, montos, categorías de beca y campos geográficos.

### Gold

Contiene vistas, tablas agregadas y modelos analíticos preparados para consumo en Power BI. Esta capa responde preguntas de negocio sobre presupuesto, becarios, deserción, rendimiento y cobertura territorial.

## Alcance inicial

El alcance del proyecto incluye:

- Ingesta batch de datos públicos.
- Almacenamiento de datos crudos en Cloud Storage.
- Transformación con Dataflow.
- Modelado analítico en BigQuery.
- Validaciones de calidad de datos.
- Orquestación con Cloud Composer.
- Reportes ejecutivos en Power BI.
- Observabilidad con Cloud Logging y Cloud Monitoring.
- Modelo predictivo opcional con BigQuery ML.

## Estado del proyecto

Estructura inicial del repositorio.
