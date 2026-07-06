# Estrategia de observabilidad cloud

## Propósito

La estrategia de observabilidad cloud de PRONABEC Cloud BI Platform define cómo la plataforma registra eventos operativos, métricas técnicas y señales de ejecución para los procesos batch desplegados sobre Google Cloud.

La observabilidad se concentra en tres objetivos:

- trazabilidad de ejecuciones batch;
- diagnóstico operativo de errores;
- visibilidad sobre calidad, rechazos y estado de procesamiento.

## Alcance

La observabilidad aplica a los componentes operativos de la arquitectura Medallion:

```text
Cloud Run Jobs
Dataflow
Cloud Composer
BigQuery Audit
Cloud Logging
Cloud Monitoring
```

Los logs estructurados se emiten desde procesos Python empaquetados en la imagen batch del proyecto. Cloud Logging centraliza los eventos producidos por Cloud Run Jobs, Dataflow y Composer. BigQuery Audit conserva resultados persistentes de calidad y ejecuciones.

## Eventos estructurados

El proyecto utiliza eventos JSON para registrar hitos operativos relevantes. Los eventos contienen campos estándar para facilitar búsqueda, filtros, métricas y correlación entre servicios.

Eventos principales:

```text
pipeline_started
pipeline_completed
pipeline_failed
pipeline_metric
```

### `pipeline_started`

Representa el inicio lógico de una ejecución batch. Permite identificar cuándo comenzó un proceso, qué fuente procesa y qué identificador de corrida lo agrupa.

### `pipeline_completed`

Representa la finalización exitosa de una ejecución batch. Incluye conteos, salidas generadas, tabla destino, tasa de rechazo y duración cuando la información está disponible.

### `pipeline_failed`

Representa una falla controlada del proceso. Incluye código de error, mensaje técnico y contexto operativo.

### `pipeline_metric`

Representa una métrica puntual emitida durante la ejecución. Se usa para publicar señales como registros leídos, válidos, rechazados, tasa de rechazo o conteos de salida.

## Campos estándar

Los eventos estructurados usan campos comunes para mantener consistencia entre componentes:

```text
timestamp
level
logger
message
event_type
pipeline_name
pipeline_run_id
status
source_system
source_dataset
extraction_date
records_read
records_valid
records_rejected
rejection_rate
output_path
output_table
duration_seconds
error_code
error_message
metric_name
metric_value
```

No todos los eventos requieren todos los campos. Los campos vacíos no se emiten para evitar ruido operativo.

## Correlación de ejecuciones

`pipeline_run_id` permite relacionar eventos emitidos por distintas etapas del flujo. Composer propaga este identificador hacia Cloud Run Jobs y los jobs lo transmiten a procesos de extracción, transformación y calidad.

`extraction_date` representa la fecha lógica del lote procesado. Este campo permite separar la fecha de ejecución técnica respecto a la partición de datos procesada.

## Cloud Run Jobs

Cloud Run Jobs emite logs estructurados desde los procesos Python del proyecto. Cada job mantiene contexto sobre el módulo ejecutado, fuente, dataset, bucket, tabla destino y resultado de ejecución.

Los jobs relevantes para observabilidad son:

```text
pronabec-discovery-job
pronabec-build-plan-job
pronabec-run-plan-job
pronabec-finalize-dataset-job
mef-extract-job
pronabec-stage-reports-job
dataflow-pronabec-convocatorias-job
dataflow-pronabec-ubigeo-postulacion-job
dataflow-pronabec-becarios-pais-estudio-job
dataflow-pronabec-colegios-habiles-job
dataflow-pronabec-becarios-provincia-job
dataflow-mef-presupuesto-job
dataflow-mef-presupuesto-temporal-job
dataflow-mef-producto-job
dataflow-mef-producto-temporal-job
dataflow-mef-actividad-job
dataflow-mef-actividad-temporal-job
dataflow-mef-generica-job
dataflow-mef-generica-temporal-job
dataflow-mef-hierarchy-job
dataflow-pronabec-report-job
quality-checks-job
```

Para PRONABEC reports, la observabilidad se concentra en `dataflow-pronabec-report-job`. Composer ejecuta este job parametrizable una vez por cada reporte seleccionado, propagando `SOURCE_DATASET`, `INPUT_PATH`, `OUTPUT_TABLE`, `BRONZE_EXTRACTION_DATE` y `PIPELINE_RUN_ID` para diferenciar cada corrida.

## Dataflow

Dataflow conserva logs y métricas propias del servicio administrado. Además, el pipeline Bronze a Silver genera summaries de procesamiento y eventos estructurados para registrar conteos operativos, registros rechazados y rutas de salida.

La combinación de Cloud Logging, summary JSON y DLQ permite rastrear una ejecución desde entrada Bronze hasta escritura Silver.

## Cloud Composer

Composer registra la orquestación de dependencias y las llamadas a Cloud Run v2 REST API usadas para lanzar jobs. Su responsabilidad observacional es mostrar orden de ejecución, estados de tareas, reintentos y fallos de coordinación.

En el DAG principal, las ramas Bronze independientes (`pronabec_api_bronze`, `mef_bronze` y `pronabec_reports_bronze`) pueden verse ejecutando en paralelo. `validate_bronze_manifests` debe quedar como barrera antes de Silver. Luego las ramas Silver/Dataflow (`pronabec_api_silver`, `mef_silver` y `pronabec_reports_silver`) tambien pueden ejecutarse en paralelo, y `publish_gold_views` solo debe arrancar cuando esas tres ramas concluyen.

El paralelismo visible en Composer corresponde a launchers Cloud Run/Dataflow. La cantidad de workers dentro de cada Dataflow job se observa y ajusta desde Dataflow, no desde el grafo de tareas Composer.

Las tasks Cloud Run del DAG no dependen de `gcloud run jobs execute --wait` ni de `subprocess`. El operador usa `AuthorizedSession`, lanza el job con Cloud Run v2 REST API y hace polling del long-running operation. En logs de Composer, `operation_name` y, cuando aparece, `execution_name` son las llaves para correlacionar:

```text
Cloud Run operation: <operation-name>
Cloud Run operation=<operation-name> job=<job-name> elapsed=<seconds> done=<true|false>
Cloud Run execution: <execution-name>
```

Si Composer marca una task como failed, primero revise si la operation devolvio `error.code`, `error.message` o si hubo timeout de polling. Si Cloud Run fue exitoso pero Airflow fallo, conserve como evidencia el task log completo, el `operation_name`, el `execution_name` si fue emitido y los logs de Cloud Run/Dataflow asociados.

Composer no concentra la lógica de procesamiento ni reemplaza los logs emitidos por Cloud Run Jobs y Dataflow.

## BigQuery Audit

BigQuery Audit conserva resultados persistentes de calidad y ejecución. A diferencia de Cloud Logging, su propósito es analítico e histórico. Esta capa permite consultar resultados por fecha, dataset, regla de calidad, estado y severidad.

Cloud Logging responde a diagnóstico operativo. BigQuery Audit responde a trazabilidad histórica y análisis de calidad.

## Severidad

El modelo de observabilidad usa niveles estándar de logging:

```text
INFO
WARNING
ERROR
```

`INFO` registra eventos esperados del flujo. `WARNING` representa condiciones no bloqueantes que requieren revisión. `ERROR` representa fallas de ejecución, errores de calidad bloqueantes o interrupciones operativas.

## Señales monitoreables

Las principales señales operativas del pipeline son:

```text
fallo de Cloud Run Job
fallo de tarea Composer
fallo de Dataflow Job
tasa elevada de registros rechazados
ejecución sin registros válidos
fallo de reglas críticas de calidad
ausencia de summary de procesamiento
ausencia de resultados Audit
```

Estas señales permiten construir alertas sobre Cloud Monitoring y consultas operativas en Cloud Logging.

## Relación con DLQ y Quality

DLQ conserva registros rechazados con detalle de error por fila. Quality registra validaciones sobre BigQuery. Observabilidad conecta ambas capas mediante eventos y métricas estructuradas.

Un aumento en `records_rejected` o `rejection_rate` debe correlacionarse con rutas DLQ y con resultados de calidad persistidos en Audit.

## Control de datos sensibles

Los logs estructurados no deben incluir credenciales, tokens, llaves privadas, archivos `.env`, contenido completo de datasets ni secretos de configuración.

Los eventos deben registrar metadata operativa y conteos, no datos sensibles de negocio ni payloads completos de fuentes.
