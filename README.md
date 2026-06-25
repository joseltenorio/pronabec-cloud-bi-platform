# PRONABEC Cloud BI Platform

Plataforma batch de ingeniería de datos y analítica en Google Cloud para integrar, transformar, validar y modelar información pública relacionada con PRONABEC y ejecución presupuestal del MEF bajo una arquitectura Medallion.

El repositorio implementa componentes de extracción, staging, transformación Bronze a Silver, control de calidad, auditoría técnica y vistas analíticas Gold orientadas al análisis ejecutivo.

## Descripción general

PRONABEC Cloud BI Platform es un proyecto de Data Engineering y Business Intelligence enfocado en el procesamiento batch de datos públicos asociados a programas de becas, cobertura territorial, reportes oficiales de PRONABEC y presupuesto público del Ministerio de Economía y Finanzas.

La plataforma organiza los datos en capas Medallion, conservando trazabilidad desde los archivos crudos hasta las vistas analíticas. El procesamiento se estructura con Python, Apache Beam/Dataflow, Cloud Storage y BigQuery, separando responsabilidades entre ingesta, transformación, validación, auditoría y consumo analítico.

## Contexto analítico

El proyecto integra información pública para responder preguntas relacionadas con:

- evolución del presupuesto asignado a PRONABEC;
- relación entre presupuesto público y cobertura de becas;
- distribución territorial de postulaciones y beneficiarios;
- características sociales y educativas observadas en reportes oficiales;
- evolución anual de becarios universitarios por institución y carrera;
- consistencia, completitud y trazabilidad de datos usados en análisis ejecutivo.

## Arquitectura de datos

La arquitectura sigue un enfoque batch cloud-native basado en capas Medallion.

```text
Fuentes públicas y reportes oficiales
        |
        v
Cloud Storage / Bronze
        |
        v
Dataflow / Apache Beam
        |
        v
BigQuery Silver
        |
        v
BigQuery Gold
        |
        v
Auditoría, calidad y consumo analítico
```

## Capas Medallion

### Bronze

La capa Bronze conserva los datos crudos extraídos desde las fuentes públicas o preparados desde reportes oficiales tabulados. Los schemas Bronze mantienen los campos en formato conservador, priorizando trazabilidad, reproducibilidad y preservación del dato original.

Los datos Bronze consideran:

- endpoints públicos de PRONABEC;
- información presupuestal del MEF;
- reportes oficiales PRONABEC PES 2025;
- reportes Beca 18 universitarios 2012-2026;
- metadata de extracción y staging.

### Silver

La capa Silver contiene datos limpios, tipados y normalizados en BigQuery. Las transformaciones aplican reglas específicas por familia de fuente, evitando promover automáticamente todo Bronze a Silver.

La capa Silver incluye:

- transformación de datasets PRONABEC seleccionados;
- transformación de slices presupuestales MEF;
- transformación de reportes oficiales PRONABEC en estructuras analíticas;
- normalización técnica de texto;
- conversión de tipos;
- manejo explícito de registros rechazados;
- canonización controlada de valores PRONABEC mediante mappings versionados.

La canonización conserva el valor original y agrega columnas paralelas para el valor canónico, método de coincidencia y bandera de revisión. No se reemplaza información original de Silver.

### Gold

La capa Gold contiene vistas analíticas en BigQuery construidas sobre el modelo Silver vigente. Estas vistas consolidan indicadores ejecutivos, reportes PRONABEC, evolución presupuestal y cruces analíticos entre becas y presupuesto.

La capa Gold incluye vistas para:

- resumen ejecutivo PRONABEC;
- presupuesto MEF anual y temporal;
- presupuesto por producto, actividad y genérica;
- indicadores sociales y educativos derivados de PES 2025;
- región de postulación y migración regional;
- trayectoria y elección educativa;
- becarios universitarios por universidad y carrera;
- análisis entre becas otorgadas y presupuesto público.

## Fuentes de datos

El proyecto trabaja con tres familias principales de datos.

### PRONABEC Datos Abiertos

Corresponde a fuentes públicas estructuradas de PRONABEC. Las respuestas se procesan conservando identificadores de origen y columnas esperadas definidas en configuración versionada.

Datasets PRONABEC considerados en el modelo incluyen convocatorias, ubigeo de postulación, colegios elegibles y becarios por país de estudio, entre otros datasets conservados en Bronze.

### MEF Consulta Amigable

Corresponde a información presupuestal pública del Ministerio de Economía y Finanzas. El procesamiento MEF separa slices presupuestales anuales, temporales y jerárquicos.

El modelo evita mezclar granularidades incompatibles y no usa estructuras jerárquicas como hechos sumables para prevenir dobles conteos financieros.

### Reportes oficiales PRONABEC

Corresponde a reportes documentales tabulados y stageados como fuentes Bronze controladas. Esta familia se procesa de forma diferenciada respecto a la API pública de PRONABEC.

Incluye reportes PES 2025 y Beca 18 universitarios 2012-2026, transformados hacia estructuras Silver y Gold aptas para análisis comparativo y ejecutivo.

## Procesamiento Bronze a Silver

El pipeline `pipelines/dataflow_bronze_to_silver.py` implementa el procesamiento batch desde Bronze hacia Silver mediante Apache Beam.

El pipeline soporta:

- lectura de archivos CSV y JSONL;
- ejecución local en modo `dry-run`;
- ejecución configurable con runner de Dataflow;
- transformación por sistema fuente y dataset;
- separación de registros válidos y rechazados;
- escritura configurada hacia BigQuery;
- generación de salidas DLQ;
- generación de resumen de procesamiento.

En modo `dry-run`, el pipeline valida lectura y transformación sin contactar recursos reales de GCP. En modo no `dry-run`, la escritura a BigQuery requiere una tabla destino explícita y usa disposiciones conservadoras para evitar creación accidental de tablas.

## Registros rechazados y DLQ

El proyecto implementa una estrategia de Dead Letter Queue para registros que no pueden procesarse correctamente durante la transformación.

Cada registro rechazado conserva:

- registro original;
- sistema fuente;
- dataset fuente;
- fecha de extracción;
- identificador de ejecución;
- etapa de procesamiento;
- código de error;
- mensaje de error;
- campo y valor asociado al fallo, cuando aplica.

Esta estrategia evita pérdida silenciosa de datos y permite que una fila problemática no bloquee el procesamiento completo de un dataset.

## Calidad de datos y auditoría

El proyecto incluye reglas de calidad sobre BigQuery y un runner Python para ejecutar validaciones y persistir resultados de auditoría.

Las validaciones cubren aspectos como:

- duplicados;
- nulos en campos obligatorios;
- consistencia de llaves;
- coherencia de campos de negocio;
- validaciones específicas sobre reportes, universidades, carreras y presupuesto.

La capa Audit registra resultados estructurados de calidad y ejecuciones del pipeline. Esto permite trazabilidad técnica sobre la salud de los datos procesados y facilita el análisis de fallos, advertencias y métricas de ejecución.

## SQL y modelo analítico

El repositorio mantiene SQL versionado para:

- creación de datasets BigQuery;
- tablas de auditoría;
- vistas Gold;
- reglas de calidad.

Los DDL de Bronze y Silver se generan desde schemas JSON versionados. Los archivos generados se consideran artefactos temporales y no forman parte del código fuente versionado.

Esta separación mantiene a los schemas JSON como fuente de verdad para contratos Bronze/Silver, mientras que Gold, Audit y Quality permanecen como SQL explícito debido a su lógica analítica y operativa.

## Estructura del repositorio

```text
pronabec-cloud-bi-platform/
├── config/
│   ├── endpoints.yaml
│   ├── pipeline.yaml
│   ├── gcp.example.yaml
│   ├── reference/
│   └── schemas/
│       ├── bronze/
│       └── silver/
├── dags/
├── docs/
├── evidence/
├── pipelines/
│   ├── common/
│   ├── transforms/
│   ├── dataflow_bronze_to_silver.py
│   ├── extract_pronabec.py
│   ├── quality_checks.py
│   └── scrape_mef_budget.py
├── powerbi/
├── scripts/
├── sql/
│   ├── ddl/
│   └── quality/
├── tests/
├── tools/
├── Dockerfile
├── requirements.txt
├── requirements-dev.txt
└── README.md
```

## Componentes principales

### `config/`

Contiene configuración funcional, endpoints, parámetros de pipeline, mappings de referencia y contratos de datos para Bronze y Silver.

### `pipelines/`

Contiene los procesos principales de extracción, scraping, transformación, calidad y utilidades comunes para ejecución batch.

### `pipelines/transforms/`

Contiene transformaciones específicas por familia de fuente: PRONABEC, reportes PRONABEC y MEF.

### `pipelines/common/`

Contiene componentes reutilizables para configuración, logging, validación, BigQuery, GCS, auditoría, DLQ, canonización y normalización de texto.

### `sql/`

Contiene SQL versionado para datasets, auditoría, vistas Gold y controles de calidad.

### `tools/`

Contiene utilidades de soporte para generación de DDL, profiling, exploración de fuentes y staging de reportes manuales.

### `tests/`

Contiene pruebas unitarias y de validación para schemas, transformaciones, generación de SQL, Dataflow, DLQ, métricas, calidad y vistas Gold.

## Estado técnico actual

El repositorio contiene una implementación local avanzada del core de la plataforma de datos:

- schemas Bronze y Silver versionados;
- extracción PRONABEC;
- scraping MEF;
- staging de reportes PRONABEC;
- transformaciones Silver para datasets seleccionados;
- canonización PRONABEC mediante mappings explícitos;
- pipeline Beam Bronze a Silver;
- configuración de escritura a BigQuery;
- DLQ para registros rechazados;
- resumen de métricas de procesamiento;
- reglas SQL de calidad;
- runner de calidad con persistencia en Audit;
- vistas Gold analíticas;
- pruebas automatizadas para componentes críticos.

La plataforma está diseñada para ejecución batch. No implementa streaming ni procesamiento en tiempo real, ya que las fuentes consideradas no requieren baja latencia.

## Stack tecnológico

- Python
- Apache Beam
- Google Cloud Storage
- Google BigQuery
- Google Cloud Dataflow
- Cloud Run Jobs
- Cloud Composer / Apache Airflow
- Cloud Logging
- Cloud Monitoring
- Power BI
- Pytest
- Ruff
- SQL
- Docker
- GitHub

## Gestión de datos y seguridad

El repositorio no versiona datos reales, credenciales, archivos `.env`, llaves privadas, outputs temporales, logs locales ni artefactos generados.

Los archivos locales de datos y evidencias operativas se mantienen fuera del control de versiones mediante reglas de `.gitignore` y `.dockerignore`.

## Convenciones del proyecto

El proyecto mantiene una separación explícita entre:

- contratos de datos versionados;
- DDL generado temporalmente;
- SQL analítico versionado;
- datos reales no versionados;
- documentación técnica;
- evidencias de ejecución;
- pruebas automatizadas.

Esta separación permite mantener trazabilidad, reducir riesgo de exponer información local y sostener una evolución incremental de la plataforma.
