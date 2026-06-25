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
pronabec-extract-job
mef-extract-job
dataflow-pronabec-convocatorias-job
dataflow-mef-presupuesto-job
dataflow-report-universitarios-job
quality-checks-job
```

## Dataflow

Dataflow conserva logs y métricas propias del servicio administrado. Además, el pipeline Bronze a Silver genera summaries de procesamiento y eventos estructurados para registrar conteos operativos, registros rechazados y rutas de salida.

La combinación de Cloud Logging, summary JSON y DLQ permite rastrear una ejecución desde entrada Bronze hasta escritura Silver.

## Cloud Composer

Composer registra la orquestación de dependencias y la ejecución de comandos `gcloud run jobs execute`. Su responsabilidad observacional es mostrar orden de ejecución, estados de tareas, reintentos y fallos de coordinación.

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
