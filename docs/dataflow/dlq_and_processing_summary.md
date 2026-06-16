# Comportamiento Operativo de Dead Letter Queue (DLQ) y Resumen de Procesamiento (Processing Summary)

Este documento detalla la implementación y el comportamiento operativo del mecanismo de Dead Letter Queue (DLQ) y del resumen de procesamiento (Processing Summary) dentro del pipeline de transición Bronze -> Silver (`dataflow_bronze_to_silver.py`).

---

## 1. Objetivo del Dead Letter Queue (DLQ)

El objetivo principal de la cola de descarte (DLQ) es **evitar la pérdida silenciosa de registros problemáticos** y **garantizar la resiliencia del pipeline**. En lugar de abortar la ejecución completa ante un error puntual en una sola fila (por ejemplo, por fallas de parseo en JSON o por incoherencias tipológicas en columnas numéricas de un CSV), el pipeline:
- Aísla la fila fallida.
- Adjunta metadatos técnicos y el mensaje exacto del error.
- Enruta el registro rechazado a una ruta persistente independiente (DLQ).
- Continúa el procesamiento de las filas válidas del lote.

---

## 2. Fuentes de Datos Cubiertas

El DLQ y el resumen de procesamiento cubren de forma homogénea las tres fuentes del proyecto:

1. **`pronabec`**: API e ingesta JSONL de Datos Abiertos de PRONABEC.
2. **`mef`**: Archivos CSV de presupuesto público extraídos del scraping de Consulta Amigable.
3. **`pronabec_reports`**: Familia de fuentes manuales documentales estructuradas en formato CSV.
   > [!IMPORTANT]
   > `pronabec_reports` es una familia documental/manual amplia. No se limita a los dos datasets de Beca 18 universitarios, sino que incluye también los 21 archivos CSV del Panorama de Estudios Sociales (**`pes_2025`**). Todos están integrados en las especificaciones Silver y son compatibles con el flujo DLQ y resumen.

---

## 3. Estructura del Registro Rechazado (Rejected Record)

Cada registro descartado se serializa en una línea JSON con la siguiente estructura:

```json
{
  "source_system": "pronabec_reports",
  "source_dataset": "report_beca18_becas_otorgadas_modalidad_anual",
  "extraction_date": "2026-06-15",
  "ingestion_timestamp": "2026-06-16T22:00:00Z",
  "pipeline_run_id": "manual-run-123",
  "processing_stage": "transform",
  "error_code": "TRANSFORM_ERROR",
  "error_message": "ValueError: El valor 'abc' no es numérico para la columna 'becas_otorgadas'",
  "failed_field": "becas_otorgadas",
  "failed_value": "abc",
  "raw_record": {
    "modalidad": "ORDINARIA",
    "2026 (*)": "abc",
    "Total": "abc",
    "source_document_file": "pes.pdf",
    "source_document_title": "Estudios",
    "source_page": "2",
    "source_figure": "Figura A",
    "extraction_method": "camelot"
  },
  "partial_record": null,
  "exception_type": "ValueError"
}
```

### Campos Clave:
* **`raw_record`**: Conserva el registro de entrada tal como vino de Bronze (sin mutaciones).
* **`processing_stage`**: Fase en la que ocurrió el fallo (`parse`, `transform`, `validation`).
* **`error_code`**: Código estandarizado de error (`PARSE_ERROR`, `TRANSFORM_ERROR`, `SCHEMA_MISMATCH`, `INVALID_RECORD`, `UNKNOWN_ERROR`).

---

## 4. Rutas de DLQ Determinísticas

Las rutas se construyen de forma determinística en base al sistema, dataset y fecha de extracción:

* **Ruta local (para dry-run y pruebas locales)**:
  `tmp/dlq/<source_system>/<source_dataset>/extraction_date=YYYY-MM-DD/rejected_records.jsonl`
* **Ruta de Cloud Storage (producción)**:
  `gs://<bucket>/dlq/<source_system>/<source_dataset>/extraction_date=YYYY-MM-DD/rejected_records.jsonl`

---

## 5. Resumen de Procesamiento (Processing Summary)

Al final de cada ejecución (exitosas o fallidas), se genera un archivo de resumen con metadatos y métricas detalladas:

```json
{
    "source_system": "pronabec_reports",
    "source_dataset": "report_beca18_universitarios_carrera_anual",
    "extraction_date": "2026-06-15",
    "pipeline_run_id": "manual-summary-check",
    "input_format": "csv",
    "input_path": "tmp/bronze/pronabec_reports/report_beca18_universitarios_carrera_anual/extraction_date=2026-06-15/data.csv",
    "output_table": "test-project:silver.pronabec_report_beca18_universitarios_carrera_anual",
    "dry_run": true,
    "dlq_enabled": true,
    "dlq_output_path": "tmp/dlq/pronabec_reports/report_beca18_universitarios_carrera_anual/extraction_date=2026-06-15/rejected_records.jsonl",
    "records_read": 673,
    "records_valid": 672,
    "records_rejected": 1,
    "rejection_rate": 0.001486,
    "started_at": "2026-06-16T22:00:00.000000+00:00",
    "finished_at": "2026-06-16T22:00:08.420000+00:00",
    "duration_seconds": 8.42,
    "status": "COMPLETED_WITH_REJECTIONS"
}
```

### Explicación de Métricas:
* **`records_read`**: Total de registros o líneas procesadas desde Bronze.
* **`records_valid`**: Registros que pasaron con éxito a Silver (para BigQuery o logueo).
* **`records_rejected`**: Registros derivados al DLQ.
* **`rejection_rate`**: Proporción de descarte, calculada como `records_rejected / records_read`. Si `records_read = 0`, automáticamente vale `0.0` para evitar errores de división por cero.

---

## 6. Estados Operativos del Pipeline

El pipeline reporta tres estados al finalizar:

1. **`COMPLETED`**: Todos los registros fueron procesados exitosamente y no hubo ningún descarte (`records_rejected = 0`).
2. **`COMPLETED_WITH_REJECTIONS`**: El pipeline finalizó correctamente y procesó registros válidos, pero algunos registros fallaron y se desviaron de forma segura al DLQ. 
   > [!NOTE]
   > Este estado no indica un fallo total ni aborta la orquestación. Significa que los datos válidos ya están en Silver y los descartados están aislados para análisis/corrección.
3. **`FAILED`**: Ocurrió un error fatal que impidió la ejecución (problemas de configuración, ruta de origen inexistente, parámetros CLI incorrectos, etc.). En este caso, el resumen incluye el campo `error_message`.

---

## 7. Comportamiento según el Modo de Ejecución

### Modo Dry-Run (`--dry-run`)
* No realiza escrituras físicas en BigQuery.
* Envía los registros válidos al flujo de log estructurado.
* Si está habilitado el DLQ (`--disable-dlq` inactivo), escribe los descartes localmente en `tmp/dlq/`.
* Si se proporciona la opción `--summary-output-path`, escribe el JSON localmente; de lo contrario, lo escribe como un string formateado en los logs.
* Sirve para pruebas rápidas de integración local y depuración.

### Modo No Dry-Run (Producción / Integración)
* Escribe los registros válidos en las tablas Silver de BigQuery (usando la configuración configurada de `WriteToBigQuery`).
* Escribe los registros descartados en la ruta DLQ parametrizada (local o Cloud Storage `gs://`).
* Registra el resumen de ejecución en logs o escribe el JSON en la ruta indicada por `--summary-output-path`.
* *Las pruebas automáticas utilizan mocks para evitar el contacto real con GCP.*

---

## 8. Relaciones y Limitaciones del Sistema

* **Relación con Calidad de Datos (Quality SQL)**: El DLQ y el resumen manejan excepciones a nivel de fila y fallas sintácticas o tipológicas básicas. No reemplazan las validaciones lógicas de calidad de datos (duplicados por claves de negocio, consistencias cruzadas, etc.), las cuales se implementarán en bloques posteriores sobre la base de BigQuery.
* **Relación con BigQuery Audit**: En este bloque, el resumen de procesamiento se escribe a archivos JSON o logs, quedando listo para ser persistido en tablas estructuradas de auditoría en la base de datos de auditoría más adelante.
* **Relación con Gold**: La tabla analítica final (capa Gold) debe consumir datos de Silver asegurando que los registros procesados estén libres de sesgos. El análisis de registros en DLQ debe realizarse antes de consolidar reportes de negocio para garantizar la veracidad de los indicadores agregados.
* **Reglas para PES 2025**: Dado que PES 2025 abarca 21 archivos CSV con estructuras variables y propensas a errores tipográficos de origen (como guiones, celdas de total general o decimales mal formados), el DLQ jugará un rol fundamental al aislar aquellas filas que tengan anomalías antes de la fase de Gold analítico.

---

## 9. Comandos de Ejemplo para Ejecución Local

Para probar el enrutamiento de descarte y la generación del resumen en tu ambiente local:

```powershell
.venv\Scripts\python -m pipelines.dataflow_bronze_to_silver `
  --source-system pronabec_reports `
  --source-dataset report_beca18_universitarios_carrera_anual `
  --input-format csv `
  --input-path "tmp\bronze\pronabec_reports\report_beca18_universitarios_carrera_anual\extraction_date=2026-06-15\data.csv" `
  --extraction-date 2026-06-15 `
  --pipeline-run-id manual-summary-check `
  --runner DirectRunner `
  --dry-run `
  --dlq-output-root tmp\dlq `
  --summary-output-path tmp\audit\dataflow_summary\summary.json
```

---

## 10. Qué NO hace el componente en este Bloque
* No establece comunicación real con APIs o servicios de Google Cloud (GCS, BigQuery, Dataflow Runner) durante la ejecución de los tests.
* No persiste los eventos de auditoría en BigQuery Audit.
* No calcula métricas agregadas de negocio de calidad de datos (Quality SQL).
* No crea ni actualiza vistas en Gold.
* No ejecuta la validación física estricta de esquemas a nivel de motor de datos (se hace vía validación de schema en memoria contra los archivos JSON locales).
