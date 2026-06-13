# SQL Scripts

Esta carpeta contiene scripts SQL versionados para crear y mantener estructuras analiticas de BigQuery en Project Cloud BI Platform.

## Generated DDL strategy - B2

Los JSON schemas versionados en `config/schemas/bronze/` y `config/schemas/silver/` son la fuente de verdad para Bronze y Silver.

Los DDL BigQuery Bronze/Silver se generan bajo demanda en `build/generated/sql/`. Esos archivos generados no se commitean y no deben usarse como fuente de verdad permanente.

Para generar localmente:

```bash
python tools/generate_bigquery_ddl.py
```

Para generar con valores reales:

```bash
python tools/generate_bigquery_ddl.py --project-id "$GCP_PROJECT_ID" --bucket "$GCS_BUCKET_NAME"
```

En el futuro, CI/CD generara los DDL en un workspace temporal y ejecutara `bq query` contra BigQuery.

Cloud Run no genera DDL. Cloud Run ejecuta jobs de extraccion.

Para agregar un dataset:

1. Agregarlo a `config/endpoints.yaml` si es PRONABEC.
2. Crear `config/schemas/bronze/<dataset>_schema.json`.
3. Crear `config/schemas/silver/<dataset>_schema.json` solo si pasa a Silver.
4. Ejecutar `python tools/generate_bigquery_ddl.py`.
5. Ejecutar tests.
6. No commitear DDL generado.

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

## Convencion

Los scripts deben estar organizados por proposito:

- ddl/: creacion de datasets, tablas Audit y vistas Gold versionadas.
- quality/: validaciones de calidad de datos.
- gold/: marts y vistas analiticas.
- ml/: modelos y predicciones con BigQuery ML.
