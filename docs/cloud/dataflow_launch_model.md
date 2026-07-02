# Modelo de lanzamiento cloud de Dataflow

## Propósito

El modelo de lanzamiento cloud de Dataflow define cómo PRONABEC Cloud BI Platform ejecuta transformaciones Bronze a Silver sin acoplar la lógica de procesamiento al entorno de Cloud Composer.

La transformación distribuida se mantiene en Apache Beam/Dataflow. Cloud Run Jobs actúa como lanzador batch del pipeline DataflowRunner, y Cloud Composer coordina la ejecución de los jobs registrados.

## Principio de separación

El diseño mantiene una separación explícita de responsabilidades:

```text
Cloud Composer
  Coordina dependencias, parámetros y orden de ejecución.

Cloud Run Jobs
  Ejecuta comandos batch empaquetados en la imagen del proyecto.

Dataflow
  Procesa la transformación distribuida Bronze a Silver.

BigQuery
  Almacena las tablas Silver resultantes y las capas analíticas asociadas.
```

Composer no ejecuta directamente el código Python del pipeline ni mantiene dependencias del repositorio. Composer dispara Cloud Run Jobs registrados. Los jobs lanzadores ejecutan el comando Python empaquetado en la imagen Docker del proyecto y envían la ejecución a DataflowRunner.

## Pipeline ejecutado

El pipeline de transformación se encuentra en:

```text
pipelines/dataflow_bronze_to_silver.py
```

Este pipeline soporta ejecución local controlada y ejecución en DataflowRunner. En ejecución cloud, recibe parámetros de fuente, dataset, ruta Bronze, tabla Silver destino, rutas temporales, ruta DLQ y ruta de summary.

## Jobs lanzadores

La plataforma registra Cloud Run Jobs para lanzar transformaciones Dataflow de las familias principales del modelo de datos.

### Familia PRONABEC API

Los jobs PRONABEC API procesan datasets seleccionados desde Bronze PRONABEC hacia sus tablas Silver correspondientes.

```text
dataflow-pronabec-convocatorias-job
dataflow-pronabec-ubigeo-postulacion-job
dataflow-pronabec-becarios-pais-estudio-job
dataflow-pronabec-colegios-habiles-job
dataflow-pronabec-becarios-provincia-job
```

Mapeo operativo:

```text
convocatorias -> silver.pronabec_convocatorias
ubigeo_postulacion -> silver.pronabec_ubigeo_postulacion
becarios_pais_estudio -> silver.pronabec_becarios_pais_estudio
colegios_habiles -> silver.pronabec_colegios_elegibles
becarios_provincia -> silver.pronabec_beca18_becarios_provincia_2016
```

Entrada Bronze esperada:

```text
bronze/pronabec/<dataset>/extraction_date=<fecha>/data.jsonl
```

### Familia MEF Presupuesto

Los jobs MEF procesan las 9 rebanadas seleccionadas hacia Silver.

```text
dataflow-mef-presupuesto-job
dataflow-mef-presupuesto-temporal-job
dataflow-mef-producto-job
dataflow-mef-producto-temporal-job
dataflow-mef-actividad-job
dataflow-mef-actividad-temporal-job
dataflow-mef-generica-job
dataflow-mef-generica-temporal-job
dataflow-mef-hierarchy-job
```

Mapeo operativo:

```text
presupuesto -> silver.presupuesto_mef
presupuesto_temporal -> silver.presupuesto_mef_temporal
presupuesto_producto -> silver.presupuesto_mef_producto
presupuesto_producto_temporal -> silver.presupuesto_mef_producto_temporal
presupuesto_actividad -> silver.presupuesto_mef_actividad
presupuesto_actividad_temporal -> silver.presupuesto_mef_actividad_temporal
presupuesto_generica -> silver.presupuesto_mef_generica
presupuesto_generica_temporal -> silver.presupuesto_mef_generica_temporal
presupuesto_hierarchy -> silver.presupuesto_mef_hierarchy
```

Entrada Bronze esperada:

```text
bronze/mef/<slice>/extraction_date=<fecha>/year=*/data.csv
```

### Familia PRONABEC Reports

Los reportes documentales PRONABEC se transforman mediante un único Cloud Run Job parametrizable:

```text
dataflow-pronabec-report-job
```

Este job no representa un único dataset fijo. Composer lo ejecuta una vez por cada reporte seleccionado y sobreescribe las variables operativas de la ejecución:

```text
SOURCE_DATASET
INPUT_PATH
OUTPUT_TABLE
```

Entrada Bronze esperada:

```text
bronze/pronabec_reports/<dataset>/extraction_date=<fecha>/data.csv
```

Salida Silver esperada:

```text
silver.pronabec_<dataset>
```

Ejemplo:

```text
SOURCE_DATASET=report_beca18_universitarios_universidad_anual
OUTPUT_TABLE=<project>:silver.pronabec_report_beca18_universitarios_universidad_anual
```

## Parámetros cloud

Los jobs lanzadores reciben configuración operativa desde variables de entorno y argumentos de ejecución.

Variables principales:

```text
GCP_PROJECT_ID
GCS_BUCKET
BQ_SILVER_DATASET
DATAFLOW_TEMP_LOCATION
DATAFLOW_STAGING_LOCATION
DATAFLOW_SERVICE_ACCOUNT
BRONZE_EXTRACTION_DATE
PIPELINE_RUN_ID
SOURCE_DATASET
INPUT_PATH
OUTPUT_TABLE
```

Argumentos principales:

```text
--runner DataflowRunner
--project
--region
--temp-location
--staging-location
--service-account-email
--source-system
--source-dataset
--input-path
--input-format
--output-table
--dlq-output-root
--summary-output-path
```

`DATAFLOW_SERVICE_ACCOUNT` define la service account worker usada por Dataflow. Los Cloud Run Jobs actuan como launchers y pueden usar una service account distinta; esa service account launcher necesita `roles/iam.serviceAccountUser` sobre `DATAFLOW_SERVICE_ACCOUNT`. Los workers Dataflow deben usar una service account dedicada con permisos Dataflow, GCS y BigQuery, y no la Compute default service account.

## Rutas operativas

El modelo utiliza rutas estandarizadas para entrada Bronze, registros rechazados y resumen de procesamiento.

```text
gs://<bucket>/bronze/...
gs://<bucket>/dlq/...
gs://<bucket>/audit/processing_summary/...
```

La fecha lógica de extracción se propaga mediante `BRONZE_EXTRACTION_DATE`. El identificador de corrida se propaga mediante `PIPELINE_RUN_ID`.

## Relación con DLQ y summary

El pipeline Dataflow separa registros válidos y rechazados. Los registros válidos se escriben en BigQuery Silver. Los registros rechazados se conservan en DLQ. El summary registra conteos y estado operativo de la ejecución.

Esta separación permite rastrear el resultado de cada transformación sin detener el procesamiento completo por registros individuales problemáticos.

## Relación con Composer

Composer ejecuta los Cloud Run Jobs lanzadores mediante comandos `gcloud run jobs execute`. Cada ejecución puede recibir overrides operativos de fecha, identificador de corrida y parámetros de dataset.

El DAG mantiene el orden entre extracción, transformación y calidad, pero no ejecuta directamente transformaciones Beam dentro del entorno Airflow.

## Alcance operativo y selección de datasets

El modelo actual soporta la orquestación de transformaciones distribuidas de Apache Beam/Dataflow para tres familias principales de datos:

1. PRONABEC API seleccionada.
2. MEF Presupuesto seleccionado.
3. PRONABEC Reports seleccionados.

### PRONABEC API Selected Silver

Se despliegan transformaciones para:

```text
pronabec_convocatorias
pronabec_ubigeo_postulacion
pronabec_becarios_pais_estudio
pronabec_colegios_elegibles
pronabec_beca18_becarios_provincia_2016
```

### MEF Selected Silver

Se despliegan transformaciones para:

```text
presupuesto_mef
presupuesto_mef_temporal
presupuesto_mef_producto
presupuesto_mef_producto_temporal
presupuesto_mef_actividad
presupuesto_mef_actividad_temporal
presupuesto_mef_generica
presupuesto_mef_generica_temporal
presupuesto_mef_hierarchy
```

### PRONABEC Reports Selected Silver

Se procesan 23 reportes documentales mapeados como `pronabec_report_*`. Para optimizar recursos y simplificar el despliegue cloud, no se despliegan 23 jobs de Cloud Run independientes. En su lugar, se utiliza un único job común parametrizable:

```text
dataflow-pronabec-report-job
```

## Exclusiones

Los siguientes datasets se conservan para trazabilidad en Bronze, pero no se promueven a Silver ni Gold en esta versión del pipeline:

```text
convocatorias_carrera_sede
presupuesto_departamento
presupuesto_fuente
presupuesto_rubro
```

`convocatorias_carrera_sede` no cuenta con lanzador Dataflow, no se transforma a Silver y no se referencia desde vistas Gold.
