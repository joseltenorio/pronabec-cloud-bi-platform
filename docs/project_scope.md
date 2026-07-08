# Alcance del Proyecto

## Objetivo

El objetivo de este proyecto es diseñar e implementar una plataforma batch de datos cloud-native en Google Cloud para analítica de PRONABEC. La plataforma integrará datos públicos de becas y presupuesto, los transformará en datasets analíticos curados y expondrá los resultados mediante dashboards en Power BI.

## Dentro del alcance

El proyecto incluye los siguientes componentes:

### Ingesta de datos

- Extraer datos públicos de PRONABEC desde endpoints disponibles.
- Extraer información presupuestal del MEF relacionada con PRONABEC.
- Almacenar datos crudos en Cloud Storage usando una estructura Bronze.
- Capturar metadatos de extracción para trazabilidad.

### Procesamiento de datos

- Transformar datos crudos Bronze en datasets limpios Silver.
- Usar Dataflow para transformación batch.
- Validar esquemas, columnas obligatorias y tipos de datos.
- Separar registros inválidos en una ubicación dead-letter.

### Data warehouse

- Crear datasets de BigQuery para Bronze, Silver, Gold, Audit y ML.
- Crear tablas normalizadas en Silver.
- Crear vistas y marts analíticos en Gold para reporting.
- Almacenar resultados de auditoría y validaciones de calidad.

### Calidad de datos

- Validar que las tablas no estén vacías.
- Detectar registros duplicados.
- Validar campos críticos nulos.
- Validar rangos numéricos.
- Validar notas académicas dentro del rango esperado.
- Validar montos presupuestales.

### Orquestación

- Usar Cloud Composer para orquestar extracción, transformación, validaciones de calidad y generación de Gold.
- Definir dependencias entre tareas, reintentos y parámetros de ejecución.

### Observabilidad

- Usar Cloud Logging para logs estructurados del pipeline.
- Usar Cloud Monitoring para estado de jobs, fallos y visibilidad operativa.
- Documentar alertas recomendadas.

### Reporting

- Conectar Power BI a datasets Gold de BigQuery.
- Construir dashboards ejecutivos sobre becas, presupuesto, rendimiento académico y cobertura territorial.

### Machine Learning

- Implementar una base regional predictiva en el dataset `ml` a partir de fuentes Silver.
- Estandarizar contexto territorial con INEI + MINEDU.
- Preparar features para scoring, simulación y clustering territorial.
- Dejar la capa lista para BigQuery ML y futuros modelos regionales.

## Fuera del alcance inicial

Los siguientes elementos no forman parte del alcance inicial:

- Ingesta en tiempo real.
- Arquitectura event-driven con Pub/Sub.
- Almacenamiento en Bigtable.
- Endpoints de predicción online con Vertex AI.
- Despliegue en Kubernetes o GKE.
- Aplicación web transaccional.
- Reporting final basado manualmente en archivos CSV.
- Predicción individual de estudiantes.
- Modelos supervisados de estudiantes sintéticos.
- KMeans, ARIMA_PLUS y regresiones en esta rama.

## Modo de procesamiento

El proyecto utiliza procesamiento batch. Esta decisión se basa en la naturaleza de las fuentes de datos, que se actualizan periódicamente y se usan principalmente para reporting analítico, no para decisiones operativas en tiempo real.

## Entregables esperados

Los entregables esperados son:

- Repositorio GitHub documentado.
- Jobs Python de ingesta preparados para la nube.
- Pipelines batch de transformación con Dataflow.
- Datasets, tablas y vistas en BigQuery.
- Validaciones de calidad y tablas de auditoría.
- DAG de Cloud Composer.
- Dashboard Power BI conectado a BigQuery.
- Evidencia de ejecución en Google Cloud.
- Modelo BigQuery ML y salidas de predicción.

## Estado de Avance y Entregables

A la fecha actual, la **Fase Cloud de Ingeniería de Datos está 100% completada y validada**:
- Extracciones y Staging local/cloud finalizados y operativos.
- Pipelines Dataflow/Apache Beam finalizados.
- Capa de modelado BigQuery Silver y vistas Gold desplegadas.
- Orquestación en Composer (Airflow) programada semanalmente.

Los siguientes entregables quedan como **Trabajo Pendiente para Fases Posteriores**:
- Conexión e implementación final de dashboards ejecutivos en **Power BI**.
- Implementación de algoritmos predictivos y entrenamiento en BigQuery ML.
