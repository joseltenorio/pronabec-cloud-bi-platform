# Arquitectura Cloud

## Objetivo de arquitectura

El objetivo de esta arquitectura es implementar una plataforma batch de datos cloud-native en Google Cloud para analítica de PRONABEC. La plataforma integra fuentes públicas, almacena datos crudos en un data lake controlado, transforma los datasets en estructuras analíticas y expone información curada mediante BigQuery y Power BI.

## Flujo de alto nivel

```text
Fuentes de datos públicas
    |
    |-- API pública de PRONABEC
    |-- Datos presupuestales del MEF
    |
    v
Cloud Run Jobs
    |
    v
Cloud Storage - Bronze
    |
    v
Dataflow Batch Pipelines
    |
    v
BigQuery - Silver
    |
    v
BigQuery - Gold
    |
    v
Power BI Dashboards
```

## Componentes principales

### GitHub

GitHub funciona como la fuente principal de versionamiento del proyecto. Almacena los pipelines Python, scripts SQL, DAGs de Airflow, plantillas de configuración, documentación técnica y evidencias del proyecto.

### Cloud Run Jobs

Cloud Run Jobs ejecuta trabajos de extracción en contenedores. Estos jobs son responsables de descargar información desde fuentes públicas y escribir los resultados crudos en Cloud Storage.

Trabajos de extracción planificados:

- Extracción desde la API pública de PRONABEC.
- Extracción de información presupuestal del MEF.

### Cloud Storage

Cloud Storage actúa como data lake del proyecto. Almacena archivos crudos e intermedios mediante una estructura particionada por fuente, dataset y fecha de extracción.

Estructura planificada:

```text
gs://<bucket-name>/bronze/pronabec/<dataset>/extraction_date=YYYY-MM-DD/
gs://<bucket-name>/bronze/mef/presupuesto/extraction_date=YYYY-MM-DD/
gs://<bucket-name>/dlq/<dataset>/extraction_date=YYYY-MM-DD/
```

### Dataflow

Dataflow ejecuta los pipelines batch de transformación. Lee archivos Bronze desde Cloud Storage, valida y transforma registros, escribe datos limpios en BigQuery Silver y envía registros inválidos a una ubicación dead-letter.

Responsabilidades principales:

- Parsear archivos crudos.
- Validar esquemas.
- Normalizar campos de texto.
- Convertir columnas numéricas y fechas.
- Remover duplicados.
- Separar registros válidos e inválidos.
- Cargar datos limpios en BigQuery.

### BigQuery

BigQuery es el data warehouse analítico del proyecto.

Datasets planificados:

```text
bronze
silver
gold
audit
ml
```

La capa Silver almacena tablas normalizadas. La capa Gold expone vistas y marts analíticos listos para Power BI.

### Cloud Composer

Cloud Composer orquesta el workflow batch mediante Apache Airflow. Define dependencias entre tareas de extracción, transformación, validación de calidad y generación de modelos Gold.

Flujo de DAG planificado:

```text
start
  |
extract_pronabec_api_to_gcs
  |
scrape_mef_budget_to_gcs
  |
run_dataflow_bronze_to_silver
  |
run_bigquery_gold_sql
  |
run_data_quality_checks
  |
end
```

### Cloud Logging y Cloud Monitoring

Cloud Logging almacena logs estructurados producidos por los jobs de extracción, pipelines Dataflow y tareas de orquestación.

Cloud Monitoring permite observar fallos de jobs, duración de ejecuciones, registros procesados, métricas operativas y condiciones de alerta.

### Power BI

Power BI se conecta a los datasets Gold de BigQuery para construir dashboards ejecutivos. La capa de reporting no consume archivos CSV locales; consume vistas analíticas curadas desde BigQuery.

## Decisiones arquitectónicas

### Procesamiento batch

El proyecto utiliza procesamiento batch porque las fuentes de datos se actualizan de forma periódica y no requieren ingesta en tiempo real.

### BigQuery como data warehouse analítico

BigQuery se utiliza como repositorio analítico central porque el proyecto está orientado a BI, análisis SQL y consumo mediante dashboards.

### Dataflow como motor de transformación

Dataflow se selecciona como motor principal de transformación porque es un servicio administrado de Google Cloud diseñado para procesamiento de datos escalable. En este proyecto se utilizará únicamente en modo batch.

### Exclusión de Bigtable

Bigtable no se utiliza porque el proyecto no requiere acceso NoSQL de baja latencia ni cargas de trabajo de clave-valor de alto rendimiento. El patrón de acceso analítico está mejor cubierto por BigQuery.

### Exclusión inicial de Vertex AI

Vertex AI no forma parte del alcance inicial. El primer componente de Machine Learning se implementará con BigQuery ML porque los datos ya se encuentran en BigQuery y el modelo planificado es tabular.

## Arquitectura objetivo

```text
GitHub
  |
  v
Cloud Build o GitHub Actions
  |
  v
Artifact Registry
  |
  v
Cloud Run Jobs
  |
  v
Cloud Storage Bronze
  |
  v
Dataflow Batch
  |
  v
BigQuery Silver
  |
  v
BigQuery Gold
  |
  v
Power BI
```
