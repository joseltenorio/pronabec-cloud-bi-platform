# Modelo de despliegue BigQuery

## Propósito

BigQuery representa la capa analítica principal de PRONABEC Cloud BI Platform. En esta plataforma, BigQuery administra datasets, tablas externas Bronze, tablas Silver, tablas Audit y vistas Gold.

El modelo de despliegue BigQuery sigue una estrategia mixta. Los contratos Bronze y Silver se generan desde schemas JSON versionados, mientras que los objetos Audit, Gold y Quality se mantienen como SQL manual versionado por contener lógica operativa, auditoría, validaciones y reglas analíticas explícitas.

## Estrategia de generación DDL

El proyecto utiliza una estrategia basada en contratos declarativos para las capas Bronze y Silver. Los schemas JSON ubicados en `config/schemas/bronze/` y `config/schemas/silver/` actúan como fuente de verdad para generar DDL BigQuery.

Los archivos generados se escriben en:

```text
build/generated/sql/create_bronze_external_tables.sql
build/generated/sql/create_silver_tables.sql
```

Estos archivos son artefactos temporales de despliegue. No forman parte del código fuente versionado.

## SQL versionado

El repositorio mantiene SQL manual versionado para objetos que contienen lógica explícita del modelo analítico u operativo:

```text
sql/ddl/create_datasets.sql
sql/ddl/create_audit_tables.sql
sql/ddl/create_gold_views.sql
sql/quality/data_quality_checks.sql
```

Los datasets, tablas Audit, vistas Gold y reglas de calidad no se derivan mecánicamente de schemas JSON. Por ese motivo se mantienen como SQL controlado en el repositorio.

## Renderizado de SQL parametrizado

Parte del SQL manual utiliza placeholders para evitar fijar valores de proyecto, datasets o entornos dentro de los archivos versionados.

Los placeholders contemplados por el modelo de renderizado son:

```text
your-gcp-project-id
{project_id}
{bronze_dataset}
{silver_dataset}
{gold_dataset}
{audit_dataset}
{ml_dataset}
```

El renderizado genera versiones ejecutables de los SQL manuales dentro de:

```text
build/generated/sql/
```

Los archivos renderizados siguen la convención:

```text
create_datasets.rendered.sql
create_audit_tables.rendered.sql
create_gold_views.rendered.sql
create_dim_region_mapping.rendered.sql
create_region_context_features.rendered.sql
create_region_priority_scores.rendered.sql
create_region_coverage_features.rendered.sql
create_region_priority_scores_v2.rendered.sql
data_quality_checks.rendered.sql
```

Estos archivos también son artefactos temporales. No se versionan como fuente principal del modelo.

## Capas BigQuery

### Bronze

La capa Bronze en BigQuery se materializa mediante tablas externas o estructuras de lectura sobre archivos almacenados en Cloud Storage. Esta capa conserva el dato crudo y mantiene trazabilidad con la fuente original.

Las tablas Bronze se generan desde schemas JSON Bronze y apuntan a rutas del data lake organizadas por sistema fuente, dataset y fecha de extracción.

### Silver

La capa Silver contiene tablas tipadas, normalizadas y validadas. Sus contratos se generan desde schemas JSON Silver. Las cargas hacia Silver se realizan desde el pipeline Bronze a Silver implementado con Apache Beam/Dataflow.

Silver no replica automáticamente todos los datasets Bronze. Solo incluye datasets aprobados para transformación analítica, calidad, auditoría o consumo posterior.

### Audit

La capa Audit registra información operativa del pipeline y resultados de calidad. Sus tablas se definen mediante SQL manual versionado porque responden a una estructura operativa estable, no a contratos derivados de fuentes externas.

### Gold

La capa Gold contiene vistas analíticas construidas sobre el modelo Silver vigente. Estas vistas consolidan indicadores ejecutivos, estructuras presupuestales y reportes oficiales transformados.

Gold se mantiene como SQL manual versionado para controlar reglas de negocio, granularidad, joins, agregaciones y prevención de doble conteo.

## Herramientas de despliegue

El repositorio separa herramientas Python portables y wrappers PowerShell para ejecución local desde Windows.

### `tools/generate_bigquery_ddl.py`

Genera DDL temporal para Bronze y Silver a partir de schemas JSON. Esta herramienta escribe los resultados en `build/generated/sql/`.

### `scripts/generate_bigquery_ddl.ps1`

Wrapper PowerShell para ejecutar la generación de DDL Bronze/Silver desde un entorno local Windows. Este script invoca `tools/generate_bigquery_ddl.py`.

### `tools/render_sql_templates.py`

Renderiza SQL manual versionado reemplazando placeholders por valores reales de proyecto y datasets. Esta herramienta produce SQL ejecutable dentro de `build/generated/sql/`.

### `scripts/render_sql_templates.ps1`

Wrapper PowerShell para ejecutar el renderizado de SQL manual desde un entorno local Windows. Este script invoca `tools/render_sql_templates.py`.

### `scripts/deploy_bigquery_sql.ps1`

Script de despliegue SQL sobre BigQuery. Coordina la ejecución de datasets, DDL generado Bronze/Silver, SQL renderizado para Audit y Gold, y objetos requeridos por calidad de datos.

Este script pertenece al provisioning inicial o a cambios estructurales. La publicación semanal de vistas Gold ya no depende de este wrapper: Composer dispara `pipelines.publish_gold_views` y `pipelines.validate_gold` como jobs de runtime idempotentes.

## Separación de responsabilidades

Cloud Run Jobs no genera DDL BigQuery. Cloud Run Jobs ejecuta procesos batch de extracción o validación. La generación y despliegue de objetos BigQuery pertenece a herramientas de despliegue y automatización.

Dataflow no crea tablas Silver. Dataflow escribe hacia tablas ya existentes, manteniendo una configuración conservadora para evitar creación accidental de estructuras analíticas fuera de contrato.

El SQL manual versionado no se ejecuta directamente cuando contiene placeholders. Primero se renderiza hacia `build/generated/sql/` y luego se ejecuta la versión renderizada.

## Configuración requerida

La generación y renderización de SQL utiliza valores de configuración provenientes de parámetros explícitos o variables de entorno.

Variables relevantes:

```text
GCP_PROJECT_ID
GCS_BUCKET_NAME
GCS_BUCKET
BRONZE_EXTRACTION_DATE
BQ_BRONZE_DATASET
BQ_SILVER_DATASET
BQ_GOLD_DATASET
BQ_AUDIT_DATASET
BQ_ML_DATASET
BQ_LOCATION
```

`BRONZE_EXTRACTION_DATE` permite generar tablas externas Bronze apuntando a una fecha de extracción específica cuando se requiere limitar la lectura a una partición física concreta.

## Control de artefactos

Los DDL generados y SQL renderizados se mantienen fuera del control de versiones. El repositorio conserva los contratos de entrada, las herramientas de generación, los SQL fuente y la lógica manual de despliegue, pero no los archivos derivados.

Esta separación evita inconsistencias entre schemas JSON y SQL generado, reduce ruido en commits y mantiene los contratos Bronze/Silver como fuente de verdad.

## Relación con calidad y auditoría

El despliegue BigQuery prepara los objetos requeridos por las reglas de calidad y auditoría. Las tablas Audit reciben resultados de ejecución y validaciones, mientras que Gold depende de Silver para exponer vistas analíticas.

Los SQL de la capa `ml` se renderizan y despliegan con el flujo estándar de BigQuery, después de que las tablas Silver estén disponibles. `ml.region_context_features` depende de `ml.dim_region_mapping` como fuente única de normalización regional. `ml.region_priority_scores` se ejecuta después de `ml.region_context_features`. Luego `ml.region_coverage_features` combina el contexto con PRONABEC y alimenta `ml.region_priority_scores_v2`. La vista Gold v2 consume ese score ya calculado, sin recalcularlo.

La consistencia entre Silver, Gold y Audit permite que el pipeline conserve trazabilidad técnica desde la transformación hasta la validación posterior.
