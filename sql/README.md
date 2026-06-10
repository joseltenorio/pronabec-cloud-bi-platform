# SQL Scripts

Esta carpeta contiene los scripts SQL utilizados para crear y mantener las estructuras analíticas de BigQuery en Project Cloud BI Platform.

## Estructura planificada

```text
sql/
├── ddl/
│   ├── create_datasets.sql
│   ├── create_bronze_external_tables.sql
│   ├── create_silver_tables.sql
│   └── create_gold_views.sql
├── quality/
│   └── data_quality_checks.sql
├── gold/
│   ├── resumen_ejecutivo.sql
│   ├── rendimiento_becarios.sql
│   └── presupuesto_mef.sql
└── ml/
    ├── train_dropout_risk_model.sql
    ├── evaluate_dropout_risk_model.sql
    └── predict_dropout_risk.sql
```

## Convención

Los scripts deben estar organizados por proposito:

- ddl/: creación de datasets, tablas y vistas.
- quality/: validaciones de calidad de datos.
- gold/: marts y vistas analíticas.
- ml/: modelos y predicciones con BigQuery ML.
