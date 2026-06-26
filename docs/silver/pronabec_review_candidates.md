# Datasets PRONABEC candidatos para Silver

Este documento registra candidatos revisados para la capa Silver de PRONABEC API. En el alcance vigente no quedan datasets PRONABEC API pendientes de decisión como candidatos Silver.

## Decisiones cerradas

### pronabec_beca18_becarios_provincia_2016

- **Dataset origen**: `becarios_provincia` en Bronze.
- **Decisión**: aprobado para Silver como snapshot histórico 2016 de Beca 18 a nivel provincial.
- **Regla clave**: Silver conserva solo detalle provincial. Los totales regionales y nacionales se conservan en Bronze.
- **Contrato final**: no incluye `aggregation_scope`; ese campo ya no es necesario porque Silver no conserva agregados.

### convocatorias_carrera_sede

- **Dataset origen**: `convocatorias_carrera_sede` en Bronze.
- **Decisión**: `BRONZE_ONLY`.
- **Motivo**: se conserva para trazabilidad y revisión histórica, pero no será usado directamente en Gold/Power BI en esta versión.
- **Restricción**: no debe existir schema Silver, transform Silver, job Dataflow Silver ni vista Gold dependiente de este dataset.

## Metadata

Las tablas aprobadas para Silver incluyen la metadata estándar de la plataforma:

- `source_system` (`STRING`): sistema origen, por ejemplo `PRONABEC`.
- `source_dataset` (`STRING`): nombre del dataset de origen.
- `extraction_date` (`DATE`): fecha lógica de extracción.
- `ingestion_timestamp` (`TIMESTAMP`): timestamp físico de escritura en Silver.
- `pipeline_run_id` (`STRING`): identificador único de ejecución del pipeline.
