# Cloud Run Jobs

## Propósito

Cloud Run Jobs representa la capa de ejecución serverless para procesos batch de PRONABEC Cloud BI Platform. Su responsabilidad es ejecutar componentes Python versionados en el repositorio, empaquetados dentro de una imagen Docker común, para soportar procesos de extracción, staging, validación y control operativo de la plataforma de datos.

Dentro de la arquitectura Medallion del proyecto, Cloud Run Jobs se ubica antes de la capa Bronze y actúa como punto de ejecución para procesos que no requieren mantener un servicio HTTP activo. Esta decisión mantiene el procesamiento alineado con la naturaleza batch de las fuentes PRONABEC, MEF y reportes oficiales tabulados.

## Alcance operativo

La imagen Docker del proyecto contiene los módulos necesarios para ejecutar procesos Python relacionados con:

- extracción de fuentes públicas PRONABEC;
- extracción y scraping controlado de información presupuestal MEF;
- staging de reportes documentales PRONABEC;
- ejecución de controles de calidad;
- uso de configuración versionada del proyecto;
- acceso a contratos, schemas y SQL versionado requerido por la plataforma.

La imagen no incluye datos reales, credenciales, archivos temporales, logs locales ni salidas generadas durante ejecuciones previas.

## Imagen de ejecución

La imagen se basa en `python:3.11-slim` y define `/app` como directorio de trabajo. Las dependencias se instalan desde `requirements.txt` y el contenedor copia únicamente los componentes necesarios para ejecutar procesos de datos:

```text
config/
pipelines/
tools/
sql/
```

La imagen utiliza `python` como punto de entrada. Esta convención permite ejecutar módulos del repositorio como comandos de job, manteniendo una única imagen reutilizable para distintos procesos batch.

## Publicación de imagen

La imagen de ejecución batch se publica en Artifact Registry. Esta imagen contiene el runtime Python del proyecto, sus dependencias, configuración versionada, pipelines, herramientas y SQL necesario para ejecutar procesos batch de la plataforma.

La convención de publicación utilizada por el proyecto es:

```text
<region>-docker.pkg.dev/<project_id>/<repository>/<image_name>:<tag>
```

Para el entorno cloud del proyecto, la imagen se publica bajo una estructura equivalente a:

```text
us-central1-docker.pkg.dev/pronabec-cloud-bi-platform/project-cloud-bi/pronabec-cloud-bi-platform:latest
```

El repositorio incluye un script de publicación que construye la imagen Docker localmente y la registra en Artifact Registry. La imagen publicada se utiliza como base común para los Cloud Run Jobs de extracción y validación.

## Componentes incluidos

### `config/`

Contiene configuración funcional del proyecto, endpoints, parámetros de pipeline, configuración de referencia y schemas de las capas Bronze y Silver. Estos archivos definen contratos y parámetros técnicos usados por extractores, generadores de DDL, validadores y transformaciones.

### `pipelines/`

Contiene los procesos principales de datos:

- extracción PRONABEC;
- scraping MEF;
- transformación Bronze a Silver mediante Apache Beam/Dataflow;
- ejecución de reglas de calidad;
- utilidades comunes para configuración, logging, validación, BigQuery, GCS, auditoría, DLQ y normalización de texto.

### `tools/`

Contiene herramientas auxiliares para generación de DDL, renderizado de SQL, profiling, staging de reportes manuales y exploración controlada de fuentes.

### `sql/`

Contiene SQL versionado para datasets, vistas Gold, tablas Audit y reglas de calidad. Los DDL generados temporalmente desde schemas no forman parte de la imagen como artefactos preconstruidos.

## Jobs registrados

La plataforma define Cloud Run Jobs separados por responsabilidad operativa.

### `pronabec-extract-job`

Ejecuta el proceso batch de extracción PRONABEC. Su responsabilidad es obtener datos públicos configurados en el repositorio y conservarlos como datos Bronze.

El job utiliza la lógica de extracción versionada en:

```text
pipelines/extract_pronabec.py
```

### `mef-extract-job`

Ejecuta el proceso batch de extracción MEF. Su responsabilidad es obtener información presupuestal pública y conservarla en la zona Bronze bajo las reglas de preservación del dato crudo.

El job utiliza la lógica de extracción versionada en:

```text
pipelines/scrape_mef_budget.py
```

### `quality-checks-job`

Ejecuta controles de calidad sobre BigQuery. Su responsabilidad es evaluar reglas SQL y registrar resultados estructurados en la capa Audit.

El job utiliza la lógica de validación versionada en:

```text
pipelines/quality_checks.py
```

## Responsabilidades por tipo de job

### Extracción PRONABEC

Los jobs de extracción PRONABEC ejecutan la lógica asociada a fuentes públicas estructuradas de PRONABEC. Los datos obtenidos se conservan en formato Bronze, manteniendo trazabilidad hacia la fuente original y respetando los contratos definidos en `config/schemas/bronze`.

### Extracción MEF

Los jobs MEF ejecutan scraping tabular controlado sobre información presupuestal. La salida se conserva en Bronze con campos en formato crudo, evitando reinterpretaciones prematuras de datos financieros.

### Staging de reportes PRONABEC

Los procesos de staging de reportes PRONABEC preparan archivos tabulados derivados de fuentes documentales oficiales. La lógica conserva metadata documental y separa esta familia de fuentes de la API pública PRONABEC.

### Calidad de datos

Los jobs de calidad ejecutan reglas SQL y registran resultados estructurados en la capa Audit. Esta responsabilidad se mantiene separada de la extracción y de la transformación para conservar trazabilidad operativa.

## Modelo de ejecución

Los jobs utilizan una imagen común y comandos diferenciados mediante argumentos de ejecución. La imagen define `python` como punto de entrada, por lo que cada job declara el módulo Python correspondiente como argumento.

```text
python -m pipelines.extract_pronabec
python -m pipelines.scrape_mef_budget
python -m pipelines.quality_checks
```

Esta convención reduce duplicación de imágenes, mantiene consistencia entre procesos batch y evita crear contenedores distintos para cada responsabilidad operativa.

## Variables operativas

Los jobs reciben variables de entorno para identificar proyecto, bucket, datasets BigQuery y configuración operativa.

Variables principales:

```text
GCP_PROJECT_ID
GCS_BUCKET
BQ_BRONZE_DATASET
BQ_SILVER_DATASET
BQ_GOLD_DATASET
BQ_AUDIT_DATASET
STRUCTURED_LOGGING
LOG_LEVEL
```

Estas variables permiten ejecutar los mismos módulos Python bajo un entorno cloud sin modificar el código fuente.

## Service account

Los Cloud Run Jobs se ejecutan con una service account dedicada. Esta identidad concentra permisos operativos para interactuar con Cloud Storage, BigQuery, Logging y otros servicios requeridos por la plataforma batch.

La separación por service account evita usar credenciales locales dentro de la imagen y permite controlar permisos desde IAM.

## Relación con la arquitectura Medallion

Cloud Run Jobs participa principalmente en la preparación y control de la entrada de datos hacia Bronze. La transformación Bronze a Silver pertenece a Dataflow/Apache Beam. Las vistas Gold se administran mediante SQL versionado en BigQuery. La auditoría se conserva en tablas Audit y se alimenta desde procesos de calidad y ejecución.

```text
Cloud Run Jobs
    |
    v
Cloud Storage Bronze
    |
    v
BigQuery Bronze / Dataflow
    |
    v
BigQuery Silver
    |
    v
BigQuery Gold / Audit
```

## Separación frente a Dataflow y Composer

Cloud Run Jobs no reemplaza a Dataflow ni a Composer. Cloud Run Jobs ejecuta procesos batch discretos. Dataflow procesa transformaciones distribuidas Bronze a Silver. Composer organiza dependencias operativas entre jobs, transformaciones y validaciones.

BigQuery concentra almacenamiento analítico, vistas Gold y resultados de auditoría. Esta separación mantiene el diseño de la plataforma alineado con responsabilidades claras por servicio.

## Seguridad y control de artefactos

La imagen excluye archivos sensibles y artefactos locales mediante `.dockerignore`. No se empaquetan:

- variables de entorno reales;
- credenciales;
- llaves privadas;
- datasets locales;
- logs;
- salidas temporales;
- evidencias visuales;
- archivos generados por herramientas de empaquetado;
- entornos virtuales.

Esta separación permite que la imagen sea portable sin exponer información local o credenciales del entorno de desarrollo.

## Convención de despliegue

El despliegue de Cloud Run Jobs se apoya en dos responsabilidades separadas:

```text
scripts/build_and_push_image.ps1
scripts/deploy_cloud_run_jobs.ps1
```

El primer script publica la imagen batch en Artifact Registry. El segundo registra o actualiza los Cloud Run Jobs que utilizan esa imagen.

Esta separación evita mezclar construcción de imagen con definición de jobs, mantiene commits más trazables y permite actualizar jobs sin reconstruir necesariamente la imagen.
