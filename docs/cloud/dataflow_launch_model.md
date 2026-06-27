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

La plataforma registra Cloud Run Jobs específicos para lanzar transformaciones Dataflow representativas de las familias principales del modelo de datos.

### `dataflow-pronabec-convocatorias-job`

Transforma el dataset `convocatorias` desde Bronze PRONABEC hacia la tabla Silver `pronabec_convocatorias`.

Familia de fuente:

```text
pronabec
```

Entrada Bronze:

```text
bronze/pronabec/convocatorias/extraction_date=<fecha>/data.jsonl
```

Salida Silver:

```text
silver.pronabec_convocatorias
```

### `dataflow-mef-presupuesto-job`

Transforma el slice presupuestal base del MEF desde Bronze hacia la tabla Silver `presupuesto_mef`.

Familia de fuente:

```text
mef
```

Entrada Bronze:

```text
bronze/mef/presupuesto/extraction_date=<fecha>/year=*/data.csv
```

Salida Silver:

```text
silver.presupuesto_mef
```

### `dataflow-report-universitarios-job`

Transforma el reporte oficial Beca 18 universitarios por universidad desde Bronze PRONABEC reports hacia la tabla Silver correspondiente.

Familia de fuente:

```text
pronabec_reports
```

Entrada Bronze:

```text
bronze/pronabec_reports/report_beca18_universitarios_universidad_anual/extraction_date=<fecha>/data.csv
```

Salida Silver:

```text
silver.pronabec_report_beca18_universitarios_universidad_anual
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
BRONZE_EXTRACTION_DATE
PIPELINE_RUN_ID
```

Argumentos principales:

```text
--runner DataflowRunner
--project
--region
--temp-location
--staging-location
--source-system
--source-dataset
--input-path
--input-format
--output-table
--dlq-output-root
--summary-output-path
```

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

Composer ejecuta los Cloud Run Jobs lanzadores mediante comandos `gcloud run jobs execute`. Cada ejecución puede recibir overrides operativos de fecha e identificador de corrida.

El DAG mantiene el orden entre extracción, transformación y calidad, pero no ejecuta directamente transformaciones Beam dentro del entorno Airflow.

## Alcance Operativo y Selección de Datasets

El modelo de lanzamiento actual soporta la orquestación de transformaciones distribuidas de Apache Beam/Dataflow de las tres familias de datos principales del proyecto, aplicando filtros explícitos sobre los conjuntos seleccionados:

### 1. Familia PRONABEC API (Selected Silver)
Se despliegan transformaciones para las siguientes tablas:
- `pronabec_convocatorias`
- `pronabec_ubigeo_postulacion`
- `pronabec_becarios_pais_estudio`
- `pronabec_colegios_elegibles`
- `pronabec_beca18_becarios_provincia_2016` (esta tabla transforma los beneficiarios filtrando registros totales a nivel regional y provincial).

**Exclusión Crítica**:
- **convocatorias_carrera_sede**: Se conserva en Bronze para trazabilidad, pero no se promueve a Silver ni Gold en esta versión del pipeline. Está fuera del alcance del procesamiento Dataflow y no cuenta con lanzador Dataflow.

### 2. Familia MEF Presupuesto (Selected Silver)
Se procesan mediante Apache Beam las 9 rebanadas (slices) seleccionadas para la capa Silver:
- `presupuesto_mef`
- `presupuesto_mef_temporal`
- `presupuesto_mef_producto`
- `presupuesto_mef_producto_temporal`
- `presupuesto_mef_actividad`
- `presupuesto_mef_actividad_temporal`
- `presupuesto_mef_generica`
- `presupuesto_mef_generica_temporal`
- `presupuesto_mef_hierarchy`

**Exclusiones**:
- Los datasets `presupuesto_departamento`, `presupuesto_fuente` y `presupuesto_rubro` son clasificados como **Bronze-only** y no se envían a Dataflow para procesamiento a Silver.

### 3. Familia PRONABEC Reports (Selected Silver)
Se procesan en total los 23 reportes documentales mapeados (`pronabec_report_*`). Para optimizar recursos y simplificar el despliegue cloud, **no se despliegan 23 jobs de Cloud Run independientes**. En su lugar, se utiliza un único job común parametrizable:
- **Job Lanzador**: `dataflow-pronabec-report-job`
- **Parámetros Dinámicos (Overrides)**: En cada ejecución, se redefinen los argumentos a través de variables de entorno:
  - `SOURCE_DATASET`: Nombre interno de la subfuente.
  - `INPUT_PATH`: Ruta origen en GCS Bronze (`gs://<bucket>/bronze/pronabec_reports/...`).
  - `OUTPUT_TABLE`: Tabla analítica destino en BigQuery Silver (`{project_id}.{silver_dataset}.pronabec_report_...`).
