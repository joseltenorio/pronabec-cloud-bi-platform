# Fuentes de datos

## Propósito

Este documento describe las fuentes de datos consideradas para Project Cloud BI Platform. El objetivo es identificar el origen, uso analítico, método de ingesta, frecuencia esperada, formato de almacenamiento y principales limitaciones antes de implementar los pipelines productivos de extracción, transformación y carga.

## Resumen de fuentes

| Fuente                     | Tipo de fuente                                      | Uso principal                                                             | Método de ingesta                            | Capa destino inicial    |
| -------------------------- | --------------------------------------------------- | ------------------------------------------------------------------------- | -------------------------------------------- | ----------------------- |
| PRONABEC Datos Abiertos    | Portal público con endpoints JSON paginados         | Becas, convocatorias, notas, pérdida de becas, ubigeo y conceptos de pago | Extracción batch con Python                  | Bronze en Cloud Storage |
| MEF Consulta Amigable      | Portal público                                      | Presupuesto asignado y ejecución presupuestal                             | Extracción batch mediante scraper controlado | Bronze en Cloud Storage |
| Datos geográficos / ubigeo | Dataset público de referencia o derivado de fuentes | Análisis territorial y normalización geográfica                           | Extracción batch o carga controlada          | Silver en BigQuery      |

---

## Fuente 1: PRONABEC Datos Abiertos

### Descripción

La fuente de datos abiertos de PRONABEC proporciona información pública relacionada con programas de becas, convocatorias, becarios, notas académicas, pérdida de becas, conceptos de pago y ubicación geográfica de postulantes.

Durante la etapa de exploración se confirmó que las rutas visibles del portal devuelven páginas HTML para consulta web. Los datos tabulares se obtienen mediante endpoints JSON paginados usados por la propia interfaz del portal.

### Patrón técnico identificado

Las páginas visuales siguen este patrón:

```text
https://datosabiertos.pronabec.gob.pe/Dataset/<Dataset>
```

Los datos se obtienen desde endpoints con el patrón:

```text
https://datosabiertos.pronabec.gob.pe/Dataset/Listar<Dataset>
```

La respuesta del endpoint de datos usa una estructura compatible con jqGrid:

```text
total
page
records
rows
```

Cada elemento de `rows` contiene:

```text
id
cell
```

El arreglo `cell` contiene los valores reales de cada fila. Durante la extracción, esos valores se mapearán a columnas usando la configuración `expected_columns` definida en `config/endpoints.yaml`.

### Justificación de uso

Esta fuente es relevante porque permite analizar el comportamiento de los programas de becas desde una perspectiva académica, territorial, presupuestal indirecta y operativa. Además, al tratarse de una fuente pública estructurada y paginada, es adecuada para un pipeline batch automatizable en Google Cloud.

### Datasets considerados

| Dataset | Endpoint de datos esperado | Descripción | Uso analítico |
| :--- | :--- | :--- | :--- |
| `notas_becarios` | `ListarNotasDeBecarios` | Notas promedio de becarios por semestre académico | Rendimiento académico y riesgo académico |
| `perdida_becas` | `ListarPerdidaDeBecas` | Registros de becarios que perdieron la beca | Análisis de deserción o pérdida de beca |
| `convocatorias` | `ListarConvocatorias` | Información de convocatorias, programas, modalidades y vacantes | Análisis histórico de oferta de becas |
| `concepto_pago` | `ListarConceptoDePago` | Conceptos y subconceptos de subvención | Análisis de beneficios y pagos |
| `becarios_provincia` | `ListarBeca18BecariosPorProvincia` | Cantidad de becarios por provincia y tipo de beca | Cobertura territorial |
| `ubigeo_postulacion` | `ListarUbigeoDePostulacionABecas` | Ubicación geográfica de postulantes | Normalización territorial |
| `periodos_academicos` | `ListarPeriodosAcademicosDeBecarios` | Periodos académicos de becarios | Dimensión temporal académica |
| `colegios_habiles` | `ListarColegiosHabiles` | Colegios habilitados para la postulación | Análisis de procedencia y oferta educativa |
| `becarios_pais_estudio` | `ListarBecariosPorPaisDeEstudio` | Distribución de becarios por país de estudio | Análisis de internacionalización y convenios |
| `convocatorias_carrera_sede` | `ListarConvocatoriaPorCarreraSede` | Convocatorias desagregadas por carrera e institución/sede | Oferta académica de carreras y especialidades |
| `nota_postulante_region` | `ListarNotaPromedioDelPostulantePorRegion` | Nota promedio de postulantes por región y modalidad | Rendimiento y admisión por origen territorial |

### Volumen identificado en exploración

La exploración inicial permitió identificar los siguientes volúmenes aproximados reportados por los endpoints:

| Dataset | Registros reportados | Páginas estimadas con `rows=100` |
| :--- | :---: | :---: |
| `convocatorias_carrera_sede` | 395487 | 3955 |
| `notas_becarios` | 103224 | 1033 |
| `becarios_pais_estudio` | 87501 | 876 |
| `colegios_habiles` | 71605 | 717 |
| `ubigeo_postulacion` | 1695 | 17 |
| `concepto_pago` | 819 | 9 |
| `convocatorias` | 402 | 5 |
| `becarios_provincia` | 221 | 3 |
| `periodos_academicos` | 216 | 3 |
| `perdida_becas` | 141 | 2 |
| `nota_postulante_region` | 61 | 1 |

Estos valores deben considerarse referenciales, ya que pueden cambiar si PRONABEC actualiza sus datos públicos.

### Método de ingesta

La ingesta se realizará mediante scripts Python ejecutados como Cloud Run Jobs. Los jobs consultarán los endpoints `Listar<Dataset>` en modo batch, recorrerán la paginación disponible y almacenarán los resultados en Cloud Storage dentro de la capa Bronze.

El extractor deberá:

1. Leer la configuración desde `config/endpoints.yaml`.
2. Construir el endpoint de datos con el patrón `Listar<Dataset>`.
3. Consultar la página inicial para obtener `total`, `records` y `rows`.
4. Recorrer las páginas desde `page=1` hasta `page=total`.
5. Guardar la respuesta original para trazabilidad.
6. Convertir `rows[].cell` en registros tabulares.
7. Guardar una salida normalizada estructuralmente en formato JSONL.
8. Registrar metadatos de extracción.

### Ruta esperada en Bronze

Para PRONABEC se conservarán dos salidas en Bronze:

```text
gs://<bucket>/bronze/pronabec/<dataset>/extraction_date=YYYY-MM-DD/data_raw.json
gs://<bucket>/bronze/pronabec/<dataset>/extraction_date=YYYY-MM-DD/data.jsonl
```

`data_raw.json` conservará la respuesta original de la fuente, incluyendo metadatos como `total`, `page`, `records` y `rows`.

`data.jsonl` conservará los registros normalizados estructuralmente, convirtiendo `rows[].cell` en columnas tabulares. Esta normalización no aplicará todavía reglas de negocio; solo convertirá la estructura técnica de la fuente para facilitar la lectura posterior desde Dataflow y BigQuery.

### Frecuencia esperada

La frecuencia recomendada es diaria o semanal, dependiendo de la disponibilidad y actualización real de la fuente. Para un proyecto de portafolio, una frecuencia semanal es suficiente y reduce costos de ejecución.

### Consideraciones de calidad detectadas

A partir del profiling inicial se identificaron los siguientes puntos:

- `notas_becarios` es el dataset de mayor volumen y será clave para análisis académico y BigQuery ML.
- `nota_promedio` viene con coma decimal, por ejemplo `16,43`, por lo que debe convertirse a `NUMERIC` en Silver.
- `perdida_becas` contiene campos con alta nulidad, como `tipo_resolucion` y `fecha_resolucion`; no deben tratarse como obligatorios.
- `departamento` en `perdida_becas` puede contener valores internacionales como países, por lo que no siempre debe validarse contra un catálogo territorial peruano.
- `convocatorias` contiene múltiples campos de fecha y campos numéricos como `vacantes`, `etapas`, `edad_minima` y `edad_maxima`.
- `becarios_provincia` contiene cantidades y porcentajes por tipo de beca; los porcentajes pueden venir con coma decimal.
- `ubigeo_postulacion` contiene códigos ubigeo que deben conservarse como texto para no perder ceros a la izquierda.
- `periodos_academicos` contiene una estructura útil para construir una dimensión temporal académica.
- En `convocatorias_carrera_sede`, varias columnas administrativas (como `resolucion`, `region`, `web`, `representante`, `telefono` y `email`) pueden venir nulas y deben conservarse como tales en la transformación.
- En `nota_postulante_region`, `nota_promedio` viene representado como texto con coma decimal (ej. `14,500000`), conservándose como STRING en Bronze, requiriendo conversión controlada antes de su uso numérico en Silver.
- Campos de contacto e identificadores administrativos como `telefono` (en `colegios_habiles` y `convocatorias_carrera_sede`) y `ruc` (en `convocatorias_carrera_sede`) deben conservarse como STRING para evitar la pérdida de ceros iniciales, guiones, nulos o caracteres especiales.
- La columna `fecha_carga` en todos los nuevos datasets se conserva como STRING en Bronze para mantener la fidelidad de la respuesta cruda de la fuente.
- Posibles espacios en blanco iniciales o finales (espacios sobrantes) en campos textuales de varios endpoints requerirán un proceso de limpieza (trimming) durante la transformación.

### Limitaciones

- La estructura de los endpoints puede cambiar.
- Los endpoints públicos no deben interpretarse como acceso directo a la base de datos interna de PRONABEC.
- Algunos campos pueden venir vacíos o con formatos inconsistentes.
- Algunos valores numéricos usan coma decimal.
- Algunos campos de fecha pueden venir como `YYYY-MM`, `DD/MM/YYYY` o fecha/hora.
- La capa Bronze debe conservar trazabilidad antes de aplicar reglas de negocio.
- La limpieza, tipado y estandarización se realizará en la capa Silver.

---

## Fuente 2: MEF Consulta Amigable

### Descripción

La Consulta Amigable del Ministerio de Economía y Finanzas permite consultar información presupuestal pública. Para este proyecto se considera la información relacionada con PRONABEC o con la unidad ejecutora correspondiente a becas y crédito educativo.

### Justificación de uso

Esta fuente permite incorporar el componente financiero al análisis. Al cruzar presupuesto con cobertura, becarios, convocatorias y rendimiento académico, el proyecto puede responder preguntas sobre ejecución presupuestal, evolución del presupuesto y relación entre recursos públicos y alcance de los programas de becas.

### Campos esperados

| Campo                | Descripción                             |
| -------------------- | --------------------------------------- |
| `ano`                | Año fiscal consultado                   |
| `ejecutora_nombre`   | Nombre de la entidad o unidad ejecutora |
| `pia`                | Presupuesto Institucional de Apertura   |
| `pim`                | Presupuesto Institucional Modificado    |
| `certificacion`      | Monto certificado                       |
| `compromiso_anual`   | Compromiso anual                        |
| `compromiso_mensual` | Compromiso mensual                      |
| `devengado`          | Monto devengado                         |
| `girado`             | Monto girado                            |
| `avance_porcentaje`  | Porcentaje de avance presupuestal       |

### Método de ingesta

La ingesta se realizará mediante un scraper batch controlado en Python. El resultado se almacenará en Cloud Storage dentro de la capa Bronze.

A diferencia de PRONABEC, MEF no se tratará inicialmente como un endpoint JSON tabular. El scraper será responsable de producir una salida tabular controlada.

### Ruta esperada en Bronze

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
- Los montos deben convertirse a tipos numéricos en Silver.
- El porcentaje de avance debe normalizarse antes de ser usado en métricas Gold.

---

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
- Conservar códigos ubigeo como texto para no perder ceros iniciales.

### Consideraciones

El dataset `ubigeo_postulacion` de PRONABEC puede actuar como una base inicial para análisis territorial. Sin embargo, en Silver será necesario validar si se requiere complementar con una fuente oficial de ubigeo para asegurar consistencia de departamentos, provincias y distritos.

---

## Criterios generales de ingesta

Todas las fuentes deben cumplir estos principios:

1. La extracción debe ser reproducible.
2. Los datos crudos deben almacenarse sin transformaciones de negocio en Bronze.
3. Cada ejecución debe registrar fecha, fuente, dataset, cantidad de registros y estado.
4. Los errores deben quedar registrados en logs.
5. Las transformaciones deben ejecutarse después de preservar la fuente original.
6. Los datos inválidos deben separarse en una zona de registros rechazados o DLQ.
7. La capa Bronze debe priorizar trazabilidad.
8. La capa Silver debe aplicar limpieza, tipado, normalización y validaciones.
9. La capa Gold debe exponer estructuras analíticas simples para Power BI.

---

## Relación con la arquitectura Medallion

| Capa   | Rol                                                                 |
| ------ | ------------------------------------------------------------------- |
| Bronze | Almacena datos crudos y normalizados estructuralmente desde fuentes |
| Silver | Contiene datos limpios, tipados, normalizados y validados           |
| Gold   | Contiene vistas y marts analíticos listos para Power BI             |

## Decisión preliminar

La decisión preliminar de almacenamiento queda así:

```text
PRONABEC Bronze raw:
data_raw.json

PRONABEC Bronze normalizado estructural:
data.jsonl

MEF Bronze:
data.csv

Silver:
tablas BigQuery tipadas y normalizadas

Gold:
vistas o marts BigQuery orientados a Power BI
```
