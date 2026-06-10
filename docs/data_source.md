# Fuentes de datos

## Propósito

Este documento describe las fuentes de datos consideradas para el proyecto. El objetivo es identificar el origen, uso analítico, método de ingesta, frecuencia esperada y limitaciones de cada fuente antes de implementar los pipelines de extracción y transformación.

## Resumen de fuentes

| Fuente                     | Tipo                          | Uso principal                                                             | Método de ingesta                            | Capa destino inicial    |
| -------------------------- | ----------------------------- | ------------------------------------------------------------------------- | -------------------------------------------- | ----------------------- |
| PRONABEC Datos Abiertos    | API pública                   | Becas, convocatorias, notas, pérdida de becas, ubigeo y conceptos de pago | Extracción batch con Python                  | Bronze en Cloud Storage |
| MEF Consulta Amigable      | Portal público                | Presupuesto asignado y ejecución presupuestal                             | Extracción batch mediante scraper controlado | Bronze en Cloud Storage |
| Datos geográficos / ubigeo | Dataset público de referencia | Análisis territorial y normalización geográfica                           | Extracción batch o carga controlada          | Silver en BigQuery      |

## Fuente 1: PRONABEC Datos Abiertos

### Descripción

La fuente de datos abiertos de PRONABEC proporciona información pública relacionada con programas de becas, convocatorias, becarios, notas académicas, pérdida de becas, conceptos de pago y ubicación geográfica de postulantes.

### Justificación de uso

Esta fuente es relevante porque permite analizar el comportamiento de los programas de becas desde una perspectiva académica, territorial y operativa. Además, al tratarse de una fuente pública estructurada, es adecuada para un pipeline batch automatizable.

### Datasets esperados

Los datasets principales considerados son:

| Dataset             | Descripción                                                     | Uso analítico                            |
| ------------------- | --------------------------------------------------------------- | ---------------------------------------- |
| notas_becarios      | Notas promedio de becarios por semestre académico               | Rendimiento académico y riesgo académico |
| perdida_becas       | Registros de becarios que perdieron la beca                     | Análisis de deserción o pérdida de beca  |
| convocatorias       | Información de convocatorias, programas, modalidades y vacantes | Análisis histórico de oferta de becas    |
| concepto_pago       | Conceptos y subconceptos de subvención                          | Análisis de beneficios y pagos           |
| becarios_provincia  | Cantidad de becarios por provincia y tipo de beca               | Cobertura territorial                    |
| ubigeo_postulacion  | Ubicación geográfica de postulantes                             | Normalización territorial                |
| periodos_academicos | Periodos académicos de becarios                                 | Dimensión temporal académica             |

### Método de ingesta

La ingesta se realizará mediante scripts Python ejecutados como Cloud Run Jobs. Los datos serán descargados en modo batch y almacenados en Cloud Storage dentro de la capa Bronze.

Ruta esperada:

```text
gs://<bucket>/bronze/pronabec/<dataset>/extraction_date=YYYY-MM-DD/data.csv
```

### Frecuencia esperada

La frecuencia recomendada es diaria o semanal, dependiendo de la disponibilidad y actualización real de la fuente.

### Consideraciones

- La estructura de los endpoints puede cambiar.
- Algunos campos pueden venir vacíos o con formatos inconsistentes.
- La capa Bronze debe conservar los datos crudos para trazabilidad.
- La limpieza y estandarización se realizará en la capa Silver.

## Fuente 2: MEF Consulta Amigable

### Descripción

La Consulta Amigable del Ministerio de Economía y Finanzas permite consultar información presupuestal pública. Para este proyecto se considera la información relacionada con PRONABEC o con la unidad ejecutora correspondiente a becas y crédito educativo.

### Justificación de uso

Esta fuente permite incorporar el componente financiero al análisis. Al cruzar presupuesto con cobertura, becarios, convocatorias y rendimiento académico, el proyecto puede responder preguntas sobre ejecución presupuestal, evolución del presupuesto y relación entre recursos públicos y alcance de los programas de becas.

### Campos esperados

| Campo              | Descripción                             |
| ------------------ | --------------------------------------- |
| ano                | Año fiscal consultado                   |
| ejecutora_nombre   | Nombre de la entidad o unidad ejecutora |
| pia                | Presupuesto Institucional de Apertura   |
| pim                | Presupuesto Institucional Modificado    |
| certificacion      | Monto certificado                       |
| compromiso_anual   | Compromiso anual                        |
| compromiso_mensual | Compromiso mensual                      |
| devengado          | Monto devengado                         |
| girado             | Monto girado                            |
| avance_porcentaje  | Porcentaje de avance presupuestal       |

### Método de ingesta

La ingesta se realizará mediante un scraper batch controlado en Python. El resultado se almacenará en Cloud Storage dentro de la capa Bronze.

Ruta esperada:

```text
gs://<bucket>/bronze/mef/presupuesto/extraction_date=YYYY-MM-DD/data.csv
```

### Frecuencia esperada

La frecuencia recomendada es semanal o mensual, debido a que el análisis presupuestal no requiere actualización en tiempo real.

### Consideraciones

- El scraping puede ser más frágil que una API.
- Si el portal cambia su estructura, el extractor puede requerir mantenimiento.
- Se deben registrar logs por año consultado.
- Se deben validar montos numéricos y años fiscales.

## Fuente 3: Datos geográficos y ubigeo

### Descripción

Los datos geográficos permiten normalizar departamentos, provincias y distritos para análisis territorial. Esta fuente puede provenir de datos abiertos o de catálogos internos derivados de PRONABEC.

### Justificación de uso

El análisis territorial es una parte central del proyecto. Permite identificar concentración de becarios, distribución por regiones y cobertura geográfica de los programas.

### Uso esperado

- Normalizar nombres de departamentos.
- Relacionar postulantes y becarios con ubicación geográfica.
- Crear dimensiones territoriales para BigQuery y Power BI.
- Permitir visualizaciones por departamento, provincia y distrito.

## Criterios generales de ingesta

Todas las fuentes deben cumplir estos principios:

1. La extracción debe ser reproducible.
2. Los datos crudos deben almacenarse sin transformaciones de negocio en Bronze.
3. Cada ejecución debe registrar fecha, fuente, dataset, cantidad de registros y estado.
4. Los errores deben quedar registrados en logs.
5. Las transformaciones deben ejecutarse después de preservar la fuente original.
6. Los datos inválidos deben separarse en una zona de registros rechazados o DLQ.

## Relación con la arquitectura Medallion

| Capa   | Rol                                                     |
| ------ | ------------------------------------------------------- |
| Bronze | Almacena datos crudos extraídos desde PRONABEC y MEF    |
| Silver | Contiene datos limpios, normalizados y validados        |
| Gold   | Contiene vistas y marts analíticos listos para Power BI |
