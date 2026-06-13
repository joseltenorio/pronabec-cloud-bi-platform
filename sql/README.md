# SQL Scripts

Esta carpeta contiene scripts SQL versionados para crear y mantener estructuras analíticas de BigQuery en Project Cloud BI Platform.

## Estrategia de DDL generado - B2

Los JSON schemas versionados en `config/schemas/bronze/` y `config/schemas/silver/` son la fuente de verdad para las estructuras Bronze y Silver.

Los DDL de BigQuery para Bronze y Silver se generan bajo demanda en:

```text
build/generated/sql/
```

Estos archivos generados no se versionan y no deben editarse manualmente.

Archivos generados:

```text
build/generated/sql/create_bronze_external_tables.sql
build/generated/sql/create_silver_tables.sql
```

## Configuración requerida

El generador requiere conocer explícitamente:

```text
GCP_PROJECT_ID
GCS_BUCKET_NAME
```

La configuración puede entregarse de dos formas.

### Opción 1: variables de entorno o archivo `.env`

Archivo `.env` local:

```env
GCP_PROJECT_ID=<gcp-project-id>
GCS_BUCKET_NAME=<gcs-bucket-name>
```

Generación:

```bash
python tools/generate_bigquery_ddl.py
```

### Opción 2: argumentos CLI

```bash
python tools/generate_bigquery_ddl.py \
  --project-id <gcp-project-id> \
  --bucket <gcs-bucket-name>
```

Los argumentos CLI tienen prioridad sobre las variables de entorno.

Si no se proporciona `GCP_PROJECT_ID` o `GCS_BUCKET_NAME`, el generador falla y no produce DDL.

## Soporte YAML futuro

El soporte para leer configuración desde un YAML real no versionado, por ejemplo `config/gcp.local.yaml` o `config/gcp.dev.yaml`, queda como mejora futura.

La prioridad esperada será:

```text
CLI > variables de entorno/.env > YAML
```

`config/gcp.example.yaml` se mantiene como plantilla documentada y no debe reemplazar la configuración real del ambiente.

## Uso en CI/CD

En CI/CD, el workflow debe inyectar `GCP_PROJECT_ID` y `GCS_BUCKET_NAME` como variables o secretos del ambiente.

El flujo esperado es:

```text
1. Ejecutar tests.
2. Generar DDL Bronze/Silver temporalmente.
3. Ejecutar los DDL contra BigQuery.
4. No commitear archivos generados.
```

Cloud Run no genera DDL. Cloud Run ejecuta jobs de extracción.

## SQL versionado

Los siguientes scripts permanecen versionados porque no se derivan directamente de los JSON schemas Bronze/Silver:

```text
sql/ddl/create_datasets.sql
sql/ddl/create_gold_views.sql
sql/ddl/create_audit_tables.sql
```

## Para agregar un dataset PRONABEC

1. Agregar la fuente en `config/endpoints.yaml`.
2. Crear `config/schemas/bronze/<dataset>_schema.json`.
3. Crear `config/schemas/silver/<dataset>_schema.json` solo si el dataset pasa a Silver.
4. Ejecutar el generador de DDL.
5. Ejecutar tests.

## Estructura planificada

```text
sql/
|-- ddl/
|   |-- create_datasets.sql
|   |-- create_gold_views.sql
|   `-- create_audit_tables.sql
|-- quality/
|   `-- data_quality_checks.sql
|-- gold/
|   |-- resumen_ejecutivo.sql
|   |-- rendimiento_becarios.sql
|   `-- presupuesto_mef.sql
`-- ml/
    |-- train_dropout_risk_model.sql
    |-- evaluate_dropout_risk_model.sql
    `-- predict_dropout_risk.sql
```

## Convención

Los scripts deben organizarse por propósito:

- `ddl/`: creación de datasets, tablas Audit y vistas Gold versionadas.
- `quality/`: validaciones de calidad de datos.
- `gold/`: marts y vistas analíticas.
- `ml/`: modelos y predicciones con BigQuery ML.
