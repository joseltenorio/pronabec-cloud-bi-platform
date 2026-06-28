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
También publica y valida las vistas Gold como parte del mismo ciclo operativo.

## Responsabilidades del DAG

El DAG mantiene las siguientes responsabilidades:

- iniciar la ejecución batch del flujo;
- ejecutar el job de extracción PRONABEC;
- ejecutar el job de extracción MEF;
- ejecutar staging de reportes documentales PRONABEC desde `landing/pronabec_reports/` hacia Bronze;
- validar que Bronze esté completo mediante manifests antes de promover datos a Silver;
- publicar vistas Gold analíticas;
- validar contratos Gold antes de calidad;
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

### `validate_bronze_manifests`

Ejecuta el Cloud Run Job de validación de manifests Bronze. Esta tarea actúa como compuerta operativa antes de las transformaciones Bronze a Silver. Si falta un `manifest.json`, falta un marcador `_SUCCESS` o la fecha lógica no coincide, el flujo se detiene y no se lanzan jobs de Dataflow.

### `run_quality_checks`

Ejecuta el Cloud Run Job de calidad. Este job evalúa reglas SQL sobre BigQuery y registra resultados estructurados en la capa Audit.

### `publish_gold_views`

Ejecuta el Cloud Run Job que publica las vistas Gold mediante `CREATE OR REPLACE VIEW` sobre BigQuery. El DAG no embebe SQL ni llama `bq` directamente.

### `validate_gold_contracts`

Ejecuta el Cloud Run Job que verifica las vistas Gold publicadas antes de permitir que el flujo avance a calidad.

## Parámetros operativos

El DAG acepta parámetros de ejecución para controlar su comportamiento sin modificar código fuente.

```text
extraction_date
run_pronabec
run_mef
run_pronabec_reports_staging
run_bronze_manifest_validation
run_dataflow_pronabec
run_dataflow_mef
run_dataflow_reports
run_gold_publish
run_gold_validation
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
bronze_manifest_validation_job_name
gold_publish_job_name
gold_validate_job_name
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
  -> stage_pronabec_reports_beca18_universitarios_2012_2026
  -> validate_bronze_manifests
  -> dataflow_pronabec
  -> dataflow_mef
  -> dataflow_reports
  -> publish_gold_views
  -> validate_gold_contracts
  -> quality_checks
  -> end
```

El Cloud Run Job de staging recibe `SOURCE_SUBSET` por tarea:

```text
stage_pronabec_reports_pes_2025 -> SOURCE_SUBSET=pes_2025
stage_pronabec_reports_universitarios -> SOURCE_SUBSET=beca18_universitarios_2012_2026
```

Ambas tareas usan `BRONZE_EXTRACTION_DATE={{ dag_run.conf.get('extraction_date') or ds }}`, el mismo valor lógico usado por las extracciones PRONABEC y MEF.

La tarea `validate_bronze_manifests` se ubica después de las extracciones y del staging de reportes, y antes de cualquier transformación Dataflow. Esta compuerta evita que Silver, Gold o Power BI consuman particiones Bronze incompletas.

## Reintentos y concurrencia

El DAG define reintentos controlados, pausa entre reintentos y límite de concurrencia mediante `max_active_runs=1`. Esta configuración evita ejecuciones superpuestas del mismo flujo y reduce el riesgo de generar salidas inconsistentes para una misma fecha lógica de extracción.

## Separación de responsabilidades

Cloud Composer coordina el flujo. Cloud Run Jobs ejecuta procesos batch discretos. Dataflow mantiene la responsabilidad de transformación distribuida Bronze a Silver. BigQuery conserva las capas Silver, Gold y Audit.

La separación por servicio reduce acoplamiento operativo y mantiene la arquitectura alineada con responsabilidades cloud claras.

## Operación con control de costos

Cloud Composer es un orquestador persistente y puede representar el mayor costo recurrente del entorno. Para desarrollo, depuración y pruebas por componente, la plataforma puede operar sin Composer activo, ejecutando Cloud Run Jobs y lanzadores Dataflow manualmente.

El criterio operativo recomendado es:

```text
desarrollo y validación por componentes -> sin Composer activo
prueba E2E orquestada y evidencia final -> Composer activo temporalmente
cierre de pruebas -> eliminación del entorno Composer
```

Eliminar Composer no elimina Cloud Run Jobs, imágenes de Artifact Registry, datasets de BigQuery, buckets de Cloud Storage ni scripts versionados del repositorio. Solo elimina el entorno Airflow, sus variables, historial interno y DAGs cargados en ese environment.

## Carga del DAG

El repositorio incluye un script de publicación que resuelve el bucket de DAGs del entorno Composer y copia el archivo versionado del DAG. El script no modifica la definición del DAG ni genera código derivado.

Para que el DAG pueda importar sus helpers y resolver configuración declarativa en Composer, el mismo script sincroniza también `config/` y `pipelines/` dentro del prefijo de DAGs del entorno.

El DAG versionado se mantiene en:

```text
dags/pronabec_medallion_batch_dag.py
```

## Programación y Planificación

El DAG principal `pronabec_medallion_batch` está programado para ejecutarse semanalmente:

- **Expresión Cron**: `0 5 * * 6` (todos los sábados a las 05:00 UTC o la zona horaria por defecto del entorno Composer).
- **catchup=False**: Evita ejecuciones históricas acumuladas automáticas (backfills) al activar o desplegar el DAG.

## Exclusiones del Flujo Orchestrado

- **convocatorias_carrera_sede**: Se conserva en Bronze para trazabilidad, pero no se promueve a Silver ni Gold en esta versión del pipeline. Está excluido de las tareas de Dataflow y calidad ejecutadas por Composer.
- **presupuesto_departamento, presupuesto_fuente, presupuesto_rubro**: Son datasets de MEF clasificados como Bronze-only y están excluidos del flujo de transformación a Silver.

## Orquestación de Reportes

Los reportes documentales de PRONABEC se transforman utilizando un único job parametrizado en Cloud Run/Dataflow llamado `dataflow-pronabec-report-job`. Composer sobreescribe las siguientes variables de entorno para cada una de las tareas del reporte:

- `SOURCE_DATASET`: Especifica el subconjunto o tabla de reporte.
- `INPUT_PATH`: Ruta origen en GCS Bronze.
- `OUTPUT_TABLE`: Tabla de salida en BigQuery Silver.
