# Cloud Run Jobs

## Propósito

Cloud Run Jobs representa la capa de ejecución serverless para procesos batch del proyecto Project Cloud BI Platform. Su responsabilidad principal es ejecutar componentes Python versionados en el repositorio, empaquetados dentro de una imagen Docker común, para soportar procesos de extracción, staging, validación y control operativo de la plataforma de datos.

Dentro de la arquitectura Medallion del proyecto, Cloud Run Jobs se ubica antes de la capa Bronze y actúa como punto de ejecución para procesos que no requieren mantener un servicio HTTP activo. Esta decisión mantiene el procesamiento alineado con la naturaleza batch de las fuentes PRONABEC y MEF.

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

La imagen utiliza `python` como punto de entrada. Esto permite ejecutar módulos y scripts del repositorio como comandos de job, manteniendo una única imagen reutilizable para distintos procesos batch.

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

Contiene herramientas auxiliares para generación de DDL, profiling, staging de reportes manuales y exploración controlada de fuentes.

### `sql/`

Contiene SQL versionado para datasets, vistas Gold, tablas Audit y reglas de calidad. Los DDL generados temporalmente desde schemas no forman parte de la imagen como artefactos preconstruidos.

## Responsabilidades por tipo de job

### Extracción PRONABEC

Los jobs de extracción PRONABEC ejecutan la lógica asociada a fuentes públicas estructuradas de PRONABEC. Los datos obtenidos se conservan en formato Bronze, manteniendo trazabilidad hacia la fuente original y respetando los contratos definidos en `config/schemas/bronze`.

### Extracción MEF

Los jobs MEF ejecutan scraping tabular controlado sobre información presupuestal. La salida se conserva en Bronze con campos en formato crudo, evitando reinterpretaciones prematuras de datos financieros.

### Staging de reportes PRONABEC

Los jobs de staging de reportes PRONABEC preparan archivos tabulados derivados de fuentes documentales oficiales. La lógica conserva metadata documental y separa esta familia de fuentes de la API pública PRONABEC.

### Calidad de datos

Los jobs de calidad ejecutan reglas SQL y registran resultados estructurados en la capa Audit. Esta responsabilidad se mantiene separada de la extracción y de la transformación para conservar trazabilidad operativa.

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

## Separación de responsabilidades

Cloud Run Jobs no reemplaza a Dataflow ni a Composer. Cloud Run Jobs ejecuta procesos batch discretos. Dataflow procesa transformaciones distribuidas Bronze a Silver. Composer organiza dependencias operativas entre jobs, transformaciones y validaciones. BigQuery concentra almacenamiento analítico, vistas Gold y resultados de auditoría.

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

## Convención de ejecución

La imagen define `python` como punto de entrada. Cada Cloud Run Job declara el módulo o script específico que ejecuta. Esta convención permite mantener una sola imagen para múltiples responsabilidades batch, reduciendo duplicación de builds y manteniendo consistencia entre procesos.
