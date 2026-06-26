# Modelo de orquestación con Cloud Composer

## Propósito

Cloud Composer representa la capa de orquestación batch de PRONABEC Cloud BI Platform. Su función es coordinar la ejecución de procesos operativos relacionados con extracción, validación y control técnico de la plataforma de datos.

Composer no contiene lógica de negocio ni transformaciones pesadas. La lógica de procesamiento permanece en los módulos Python del repositorio, en Cloud Run Jobs, en Dataflow y en BigQuery. El DAG actúa como coordinador de dependencias, parámetros y orden de ejecución.

## DAG principal

El repositorio define el DAG:

```text
pronabec_medallion_batch
```

Este DAG coordina procesos batch asociados a la plataforma Medallion. Orquesta Cloud Run Jobs responsables de extracción PRONABEC, extracción MEF, staging de reportes documentales PRONABEC desde Landing hacia Bronze, transformaciones Dataflow y ejecución de controles de calidad.

## Responsabilidades del DAG

El DAG mantiene las siguientes responsabilidades:

- iniciar la ejecución batch del flujo;
- ejecutar el job de extracción PRONABEC;
- ejecutar el job de extracción MEF;
- ejecutar staging de reportes documentales PRONABEC desde `landing/pronabec_reports/` hacia Bronze;
- ejecutar controles de calidad;
- propagar parámetros operativos como fecha de extracción e identificador de ejecución;
- controlar reintentos y concurrencia;
- evitar ejecuciones simultáneas del mismo flujo.

## Tareas principales

### `run_pronabec_extract`

Ejecuta el Cloud Run Job asociado a la extracción PRONABEC. Este job conserva datos fuente en la capa Bronze y utiliza la configuración versionada del repositorio.

### `run_mef_extract`

Ejecuta el Cloud Run Job asociado a la extracción MEF. Este job procesa información presupuestal pública y conserva la salida en la zona Bronze bajo las reglas del modelo de datos crudo.

### `stage_pronabec_reports_pes_2025` y `stage_pronabec_reports_universitarios`

Ejecutan el Cloud Run Job parametrizable de staging de reportes PRONABEC. Cada tarea cambia `SOURCE_SUBSET` y usa la misma `EXTRACTION_DATE` del DAG para leer desde Landing y escribir Bronze.

### `run_quality_checks`

Ejecuta el Cloud Run Job de calidad. Este job evalúa reglas SQL sobre BigQuery y registra resultados estructurados en la capa Audit.

## Parámetros operativos

El DAG acepta parámetros de ejecución para controlar su comportamiento sin modificar código fuente.

```text
extraction_date
run_pronabec
run_mef
run_pronabec_reports_staging
run_dataflow_reports
run_quality
```

`extraction_date` representa la fecha lógica asociada a la ejecución. La misma fecha se propaga a PRONABEC API, MEF y PRONABEC reports para alinear particiones Bronze. Los parámetros booleanos permiten habilitar o deshabilitar familias de tareas dentro de una corrida controlada.

## Variables Airflow

El DAG utiliza variables Airflow para desacoplar configuración de entorno respecto al código versionado.

```text
gcp_project_id
gcp_region
pronabec_extract_job_name
mef_extract_job_name
pronabec_reports_stage_job_name
pronabec_reports_landing_prefix
pronabec_reports_bronze_prefix
quality_checks_job_name
```

Estas variables identifican el proyecto, región y nombres de Cloud Run Jobs registrados en el entorno cloud.

## Modelo de ejecución

Composer ejecuta comandos `gcloud run jobs execute` para disparar procesos batch ya registrados. Cada ejecución espera la finalización del job antes de avanzar a la siguiente dependencia.

Este diseño mantiene a Composer como orquestador y evita duplicar lógica de extracción o validación dentro del DAG.

El orden operativo para reportes documentales garantiza que Dataflow de reportes no inicie antes de completar el staging:

```text
start
  -> extract_pronabec
  -> extract_mef
  -> stage_pronabec_reports_pes_2025
  -> stage_pronabec_reports_universitarios
  -> dataflow_pronabec
  -> dataflow_mef
  -> dataflow_reports
  -> quality_checks
  -> end
```

## Reintentos y concurrencia

El DAG define reintentos controlados, pausa entre reintentos y límite de concurrencia mediante `max_active_runs=1`. Esta configuración evita ejecuciones superpuestas del mismo flujo y reduce el riesgo de generar salidas inconsistentes para una misma fecha lógica de extracción.

## Separación de responsabilidades

Cloud Composer coordina el flujo. Cloud Run Jobs ejecuta procesos batch discretos. Dataflow mantiene la responsabilidad de transformación distribuida Bronze a Silver. BigQuery conserva las capas Silver, Gold y Audit.

La separación por servicio reduce acoplamiento operativo y mantiene la arquitectura alineada con responsabilidades cloud claras.

## Carga del DAG

El repositorio incluye un script de publicación que resuelve el bucket de DAGs del entorno Composer y copia el archivo versionado del DAG. El script no modifica la definición del DAG ni genera código derivado.

El DAG versionado se mantiene en:

```text
dags/pronabec_medallion_batch_dag.py
```

## Alcance actual

El modelo de orquestación actual coordina Cloud Run Jobs de extracción y calidad. La transformación distribuida Bronze a Silver se mantiene como responsabilidad de Dataflow y se integra como componente separado dentro de la arquitectura Medallion.
