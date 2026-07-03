# Modelo de configuración cloud

## Propósito

El modelo de configuración cloud centraliza los parámetros técnicos utilizados por PRONABEC Cloud BI Platform para ejecutar procesos batch, organizar recursos de Google Cloud y mantener consistencia entre extracción, transformación, calidad, auditoría y modelado analítico.

La configuración del proyecto se declara mediante plantillas versionadas y variables de entorno. Los valores reales de entorno, credenciales y secretos no forman parte del repositorio.

## Principios de configuración

La configuración del repositorio sigue los siguientes principios:

- separación entre configuración versionada y valores sensibles;
- uso de plantillas reutilizables para entornos cloud;
- ausencia de credenciales reales en el código fuente;
- consistencia de nombres entre Cloud Storage, BigQuery, Dataflow, Cloud Run Jobs y Composer;
- definición explícita de rutas operativas para Bronze, DLQ, calidad, auditoría y evidencias;
- parametrización de datasets BigQuery por capa Medallion;
- separación entre configuración técnica y lógica de transformación.

## Archivos de configuración

### `.env.example`

Define una plantilla de variables de entorno utilizadas por scripts, procesos batch y componentes de ejecución. Este archivo contiene nombres esperados, valores de ejemplo y convenciones de entorno, sin incluir secretos ni valores productivos reales.

Las variables se organizan por dominio:

- proyecto Google Cloud;
- Cloud Storage;
- BigQuery;
- Artifact Registry;
- Dataflow;
- Cloud Run Jobs;
- Composer;
- calidad;
- auditoría;
- extracción PRONABEC;
- extracción MEF;
- logging.

### `.env`

Representa el archivo local de variables reales para entornos de desarrollo o Cloud Shell. Este archivo no se versiona y no forma parte de los artefactos del repositorio.

El archivo `.env` contiene valores concretos del proyecto, como el identificador del proyecto GCP, bucket real de Cloud Storage, datasets BigQuery, rutas operativas en GCS y cuentas de servicio.

### `config/gcp.example.yaml`

Define una plantilla estructurada de recursos cloud del proyecto. Este archivo describe la relación entre proyecto, ubicaciones, service accounts, Artifact Registry, Cloud Run Jobs, Cloud Storage, BigQuery, Dataflow, Composer, calidad, logging y métricas operativas.

### `config/pipeline.yaml`

Contiene parámetros funcionales del pipeline de datos. Este archivo agrupa convenciones de procesamiento, rutas lógicas, formato de datos y parámetros de ejecución asociados a las capas Bronze, Silver, DLQ y auditoría.

### `config/endpoints.yaml`

Contiene la definición de fuentes públicas estructuradas, endpoints y columnas esperadas para procesos de extracción. Este archivo evita acoplar directamente la lógica de extracción a nombres de columnas o rutas externas.

## Variables principales

### Proyecto Google Cloud

Las variables `GCP_PROJECT_ID`, `GCP_PROJECT_NAME`, `GCP_PROJECT_NUMBER`, `GCP_REGION`, `GCP_ZONE`, `BQ_LOCATION` y `GCS_LOCATION` identifican el proyecto, ubicación regional y ubicación multirregional de recursos analíticos.

### Cloud Storage

`GCS_BUCKET_NAME` define el bucket principal de la plataforma. Las rutas operativas se declaran mediante prefijos y ubicaciones explícitas:

- `GCS_BRONZE_PREFIX`;
- `GCS_DLQ_PREFIX`;
- `GCS_QUALITY_PREFIX`;
- `GCS_AUDIT_PREFIX`;
- `GCS_EVIDENCE_PREFIX`;
- `DLQ_LOCATION`;
- `QUALITY_LOCATION`;
- `AUDIT_LOCATION`;
- `EVIDENCE_LOCATION`.

### BigQuery

Los datasets BigQuery se declaran por capa y responsabilidad:

- `BQ_BRONZE_DATASET`;
- `BQ_SILVER_DATASET`;
- `BQ_GOLD_DATASET`;
- `BQ_AUDIT_DATASET`;
- `BQ_ML_DATASET`.

Esta convención mantiene la separación Medallion y evita referencias rígidas a nombres de datasets dentro del código.

### Dataflow

Las variables Dataflow definen runner, rutas temporales, staging, service account y capacidad inicial de workers:

- `DATAFLOW_RUNNER`;
- `DATAFLOW_TEMP_LOCATION`;
- `DATAFLOW_STAGING_LOCATION`;
- `DATAFLOW_SERVICE_ACCOUNT`;
- `DATAFLOW_SDK_CONTAINER_IMAGE`;
- `DATAFLOW_WORKER_IMAGE_NAME`;
- `DATAFLOW_WORKER_IMAGE_TAG`;
- `DATAFLOW_WORKER_MACHINE_TYPE`;
- `DATAFLOW_MAX_NUM_WORKERS`.

### Cloud Run Jobs

Cloud Run Jobs utiliza variables para región, cuenta de servicio e imagen de ejecución:

- `CLOUD_RUN_REGION`;
- `CLOUD_RUN_SERVICE_ACCOUNT`;
- `CLOUD_RUN_IMAGE`.

La imagen se publica en Artifact Registry y se reutiliza para ejecutar procesos batch específicos.

### Artifact Registry

Artifact Registry almacena la imagen launcher de Cloud Run y la imagen worker de Dataflow. Las variables asociadas son:

- `ARTIFACT_REGISTRY_LOCATION`;
- `ARTIFACT_REGISTRY_REPOSITORY`;
- `ARTIFACT_IMAGE_NAME`;
- `ARTIFACT_IMAGE_TAG`;
- `DATAFLOW_WORKER_IMAGE_NAME`;
- `DATAFLOW_WORKER_IMAGE_TAG`.

### Composer

Composer utiliza variables para identificar el entorno de orquestación y su cuenta de servicio:

- `COMPOSER_ENVIRONMENT_NAME`;
- `COMPOSER_LOCATION`;
- `COMPOSER_SERVICE_ACCOUNT`.

### Calidad

La configuración de calidad declara el archivo SQL de reglas y el comportamiento operativo del runner:

- `QUALITY_CHECKS_FILE`;
- `QUALITY_FAIL_ON_ERROR`;
- `QUALITY_DRY_RUN`.

## Relación con componentes del repositorio

### Extractores

Los procesos de extracción utilizan configuración versionada para identificar fuentes, formatos de salida, rutas de almacenamiento y parámetros de ejecución. Esto permite mantener separada la lógica de extracción respecto a valores específicos de entorno.

### Dataflow

El pipeline Bronze a Silver utiliza parametros de runner, region, rutas temporales, staging, tabla destino, DLQ, resumen de procesamiento y `DATAFLOW_SDK_CONTAINER_IMAGE`. La configuracion cloud permite mantener consistencia entre ejecucion local controlada y ejecucion sobre infraestructura administrada.

En DataflowRunner, `DATAFLOW_SDK_CONTAINER_IMAGE` es obligatorio. La imagen worker se construye con `Dockerfile.dataflow`, instala `requirements-dataflow-worker.txt` e instala el paquete `pipelines` mediante `pip install .` y `pyproject.toml`.

Los contratos de dependencias son:

- `requirements.txt`: runtime de imagen launcher Cloud Run y jobs batch generales;
- `requirements-dataflow-worker.txt`: dependencias instaladas durante build de la imagen worker Dataflow;
- `requirements-dev.txt`: desarrollo local, tests, lint y exploracion.

### BigQuery

Los datasets BigQuery se organizan según la arquitectura Medallion:

- `bronze`;
- `silver`;
- `gold`;
- `audit`;
- `ml`.

Los nombres se declaran como parámetros para evitar referencias rígidas en scripts y SQL parametrizado.

### Cloud Storage

Cloud Storage funciona como data lake y espacio operativo para el procesamiento batch. La configuración define rutas lógicas para datos Bronze, registros rechazados, resultados de calidad, auditoría y evidencias de ejecución.

### Cloud Run Jobs

Cloud Run Jobs utiliza la imagen launcher versionada del proyecto y ejecuta comandos Python especificos por responsabilidad batch. Los jobs `dataflow-*` son launchers y pasan `--sdk-container-image` para que los workers usen la imagen dedicada de Dataflow. El modelo de configuracion mantiene nombres de jobs, imagen, region y service account separados de la logica de negocio.

### Composer

Composer representa la capa de orquestación batch. Su configuración conserva el nombre del entorno, ubicación y service account asociados a la ejecución coordinada de jobs y pipelines.

### Calidad y auditoría

La configuración define el archivo SQL de reglas de calidad, comportamiento ante errores y datasets de auditoría. Esto permite que los resultados de calidad se registren de forma estructurada sin modificar el código de validación.

## Seguridad de configuración

El repositorio no versiona:

- archivos `.env` reales;
- credenciales de Google Cloud;
- llaves privadas;
- archivos de service account;
- secretos;
- datasets locales;
- logs;
- outputs generados;
- artefactos temporales.

Las plantillas incluidas en el repositorio documentan nombres, estructura y convenciones, pero no contienen credenciales ni información sensible de ejecución.

## Convención de nombres

El proyecto utiliza nombres explícitos por dominio para facilitar trazabilidad operativa:

- variables `GCP_*` para configuración general de Google Cloud;
- variables `GCS_*` para bucket y prefijos del data lake;
- variables `BQ_*` para datasets BigQuery;
- variables `DATAFLOW_*` para ejecución Beam/Dataflow;
- variables `CLOUD_RUN_*` para jobs batch;
- variables `COMPOSER_*` para orquestación;
- variables `QUALITY_*` para reglas de calidad;
- variables `ARTIFACT_*` para imagen y repositorio de contenedores.

## Modelo de separación

La configuración cloud no contiene lógica de transformación ni reglas de negocio. Su función es declarar parámetros técnicos y convenciones de ejecución. La lógica de procesamiento permanece en `pipelines/`, las reglas SQL permanecen en `sql/` y los contratos de datos permanecen en `config/schemas/`.

Esta separación reduce acoplamiento, mejora trazabilidad y mantiene el repositorio preparado para ejecutar los mismos componentes bajo distintos entornos sin modificar código fuente.
