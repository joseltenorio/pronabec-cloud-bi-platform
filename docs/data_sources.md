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

La Consulta Amigable del Ministerio de Economía y Finanzas (MEF) permite consultar información presupuestal pública. Para este proyecto, se extrae la información correspondiente al presupuesto anual y ejecución financiera de la Unidad Ejecutora de PRONABEC.

* **Fuente**: MEF Consulta Amigable - Navegador
* **URL base**: `https://apps5.mineco.gob.pe/transparencia/Navegador/`

### Justificación de uso

Esta fuente permite incorporar la perspectiva financiera a la plataforma. Al cruzar presupuesto con cobertura, becarios, convocatorias y rendimiento académico, el proyecto puede responder preguntas sobre ejecución presupuestal, evolución del presupuesto y la relación entre recursos asignados y alcance territorial/operativo de los programas.

### Diferencias de Configuración e Ingesta con PRONABEC

La fuente del MEF difiere significativamente del patrón de extracción de PRONABEC:

* **PRONABEC**:
  * Usa endpoints homogéneos en formato JSON del portal: `/Dataset/Listar<Dataset>`.
  * Cada dataset se registra en [endpoints.yaml](file:///c:/Users/Windows%2011/Desktop/Proyectos/pronabec-cloud-bi-platform/config/endpoints.yaml) especificando `name`, `path`, `enabled` y `expected_columns`.
  * El extractor común mapea el arreglo `rows[].cell` usando `expected_columns` de forma automática.
* **MEF**:
  * No expone endpoints JSON tabulares homogéneos.
  * Consiste en una aplicación web clásica basada en formularios ASP.NET con estado de vista (`__VIEWSTATE`, `__EVENTVALIDATION`), navegación jerárquica y dinámicas del lado del servidor.
  * El extractor realiza un raspado web (scraping) controlado simulando navegación HTTP mediante peticiones GET y POST consecutivas utilizando `requests.Session` y `BeautifulSoup`. No requiere ni usa Selenium.
  * Por ello, toda la lógica de navegación vive de forma dura y estructurada en [scrape_mef_budget.py](file:///c:/Users/Windows%2011/Desktop/Proyectos/pronabec-cloud-bi-platform/pipelines/scrape_mef_budget.py).
  * La URL base de Consulta Amigable se maneja como constante del extractor.
  * El archivo [endpoints.yaml](file:///c:/Users/Windows%2011/Desktop/Proyectos/pronabec-cloud-bi-platform/config/endpoints.yaml) conserva principalmente la definición de las columnas esperadas del dataset `presupuesto_mef` para fines de validación técnica, pero no gobierna el flujo de la navegación.

### Flujo de Navegación del Scraper

El scraper navega de forma programática por la jerarquía del portal Consulta Amigable usando la siguiente ruta de selección:

1. **Año fiscal**: Determinado por el año consultado (ej. 2026, o rango indicado por `--start-year` y `--end-year`).
2. **Tipo de registro**: Actividades/Proyectos (`ActProy`).
3. **Nivel de Gobierno**: GOBIERNO NACIONAL.
4. **Sector**: EDUCACION.
5. **Pliego**: `010: M. DE EDUCACION`.
6. **Unidad Ejecutora**: `117-1438: PROGRAMA NACIONAL DE BECAS Y CREDITO EDUCATIVO`.
   *(Nota: Se observó y confirmó en pruebas reales del portal que el código ejecutor de PRONABEC es `117-1438`, y no `117-1483` como se estimó inicialmente).*

Como mecanismo de resiliencia ante cambios menores de texto en la tabla de la página final, el scraper implementa búsquedas alternativas por palabras clave (fallbacks) en el siguiente orden:
1. `["117-1438", "PROGRAMA NACIONAL DE BECAS", "CREDITO EDUCATIVO"]`
2. `["BECAS", "CREDITO"]`
3. `["PRONABEC"]`

### Campos Extraídos y Mapeo a Bronze

El portal MEF expone columnas presupuestales tradicionales en su tabla final. El extractor captura estas columnas y las normaliza al contrato de la capa Bronze definido en [presupuesto_mef_schema.json](file:///c:/Users/Windows%2011/Desktop/Proyectos/pronabec-cloud-bi-platform/config/schemas/bronze/presupuesto_mef_schema.json):

| Columna en Portal MEF | Columna en Contrato Bronze | Descripción |
| :--- | :--- | :--- |
| (Parámetro) | `ano` | Año fiscal consultado |
| Nombre de Fila Ejecutora | `ejecutora_nombre` | Nombre o código de la Unidad Ejecutora |
| PIA | `pia` | Presupuesto Institucional de Apertura |
| PIM | `pim` | Presupuesto Institucional Modificado |
| Certificación | `certificacion` | Monto certificado acumulado |
| Compromiso Anual | `compromiso_anual` | Compromiso anual acumulado |
| Atención de Compromiso Mensual | `compromiso_mensual` | Compromiso mensual acumulado |
| Devengado | `devengado` | Monto devengado acumulado |
| Girado | `girado` | Monto girado acumulado |
| Avance % | `avance_porcentaje` | Porcentaje de avance de la ejecución presupuestal |

### Método de Ingesta y Parámetros

El método principal de ingesta es mediante el modo de raspado real, habilitado con el flag `--consulta-amigable`. El extractor de MEF se ejecuta mediante:

```bash
python -m pipelines.scrape_mef_budget --consulta-amigable [argumentos]
```

### Método de Ingesta y Parámetros

El método principal de ingesta es mediante el modo de raspado real, habilitado con el flag `--consulta-amigable`. El extractor de MEF se ejecuta mediante:

```bash
python -m pipelines.scrape_mef_budget --consulta-amigable [argumentos]
```

#### Parámetros del Scraper:
* `--consulta-amigable`: Activa el modo de scraping interactivo directo contra el portal web de Consulta Amigable.
* `--start-year` / `--end-year`: Especifican el rango de años fiscales a consultar.
* `--extraction-date`: Registra de manera lógica la fecha de la foto de los datos (formato `YYYY-MM-DD`).
* `--dry-run`: Habilita la ejecución local sin interacción de escritura contra Google Cloud Storage.
* `--output-dir`: Especifica la ruta base local del sistema de archivos para guardar resultados cuando se usa `--dry-run`.
* `--include-hierarchy`: Indica si se debe extraer la jerarquía presupuestal superior.
* `--include-spending-breakdowns`: Indica si se debe extraer los desgloses de gasto presupuestal.
* `--breakdown-slices`: Especifica la lista de slices a extraer (valores válidos: `producto`, `generica`, `fuente`, `rubro`, `departamento`, `temporal`).

### Salidas y Rutas en Bronze

A partir de la incorporación de las desagregaciones presupuestales ampliadas, MEF cuenta con las siguientes salidas en formato CSV en la capa Bronze (guardadas dinámicamente bajo `gs://<bucket>/bronze/mef/<slice_name>/extraction_date=YYYY-MM-DD/year=YYYY/data.csv`):

#### 1. Presupuesto Base (`presupuesto`)
* **Rol**: Fila anual de la Unidad Ejecutora PRONABEC.
* **Características**: Contiene la ejecución total consolidada anual de la institución. Incluye `ejecutora_codigo` ("117-1438") y `ejecutora_nombre` ("PROGRAMA NACIONAL DE BECAS Y CREDITO EDUCATIVO").
* **Ruta**: `bronze/mef/presupuesto/extraction_date=YYYY-MM-DD/year=YYYY/data.csv`

#### 2. Corte Temporal General (`presupuesto_temporal`)
* **Rol**: Temporalidad general de ejecución presupuestal de PRONABEC.
* **Características**: Representa la temporalidad mensual, trimestral o anual de ejecución institucional usando `periodo_tipo`, `periodo_valor`, `trimestre`, `mes_numero` y `mes_nombre`.
* **Ruta**: `bronze/mef/presupuesto_temporal/extraction_date=YYYY-MM-DD/year=YYYY/data.csv`

#### 3. Producto Anual (`presupuesto_producto`)
* **Rol**: Distribución anual del presupuesto por Producto/Proyecto de gasto.
* **Características**: Contiene `codigo_producto` y `producto_proyecto`.
* **Ruta**: `bronze/mef/presupuesto_producto/extraction_date=YYYY-MM-DD/year=YYYY/data.csv`

#### 4. Producto Temporal (`presupuesto_producto_temporal`)
* **Rol**: Temporalidad de ejecución por Producto/Proyecto.
* **Características**: Contiene `codigo_producto`, `producto` y campos de temporalidad (`periodo_tipo`, `periodo_valor`, `trimestre`, `mes_numero`, `mes_nombre`).
* **Ruta**: `bronze/mef/presupuesto_producto_temporal/extraction_date=YYYY-MM-DD/year=YYYY/data.csv`

#### 5. Actividad Anual (`presupuesto_actividad`)
* **Rol**: Presupuesto anual por Actividad / Acción de Inversión / Obra.
* **Características**: Vincula cada actividad a su producto padre, conservando `codigo_producto`, `producto`, `codigo_actividad` y `actividad`.
* **Ruta**: `bronze/mef/presupuesto_actividad/extraction_date=YYYY-MM-DD/year=YYYY/data.csv`

#### 6. Actividad Temporal (`presupuesto_actividad_temporal`)
* **Rol**: Temporalidad por actividad dentro de producto.
* **Características**: Contiene la temporalidad para la combinación producto/actividad (`codigo_producto`, `producto`, `codigo_actividad`, `actividad` y campos temporales).
* **Ruta**: `bronze/mef/presupuesto_actividad_temporal/extraction_date=YYYY-MM-DD/year=YYYY/data.csv`

#### 7. Genérica de Gasto Anual (`presupuesto_generica`)
* **Rol**: Clasificación anual por Genérica de Gasto (ej. Bienes y Servicios, Personal, etc.).
* **Características**: Contiene `codigo_generica` y `generica`.
* **Ruta**: `bronze/mef/presupuesto_generica/extraction_date=YYYY-MM-DD/year=YYYY/data.csv`

#### 8. Genérica de Gasto Temporal (`presupuesto_generica_temporal`)
* **Rol**: Temporalidad por genérica de gasto.
* **Características**: Contiene la clasificación por genérica de gasto a nivel mensual, trimestral o anual (`codigo_generica`, `generica` y campos temporales).
* **Ruta**: `bronze/mef/presupuesto_generica_temporal/extraction_date=YYYY-MM-DD/year=YYYY/data.csv`

#### Otros Slices Preservados (`presupuesto_hierarchy`, `presupuesto_fuente`, `presupuesto_rubro`, `presupuesto_departamento`, `presupuesto_categoria` y `presupuesto_subgenerica`)
* Se mantienen en Bronze y son extraídos de forma anual/estructural según corresponda (por ejemplo, `presupuesto_hierarchy` permite comparar con niveles de Gobierno Nacional superiores y `presupuesto_departamento` detalla la distribución geográfica del gasto).

---

### Advertencias Metodológicas y de Calidad

> [!IMPORTANT]
> **Consistencia del Grano**: No se deben sumar granularidades incompatibles en consultas agregadas. Por ejemplo, sumar registros de cortes anuales con registros mensuales/trimestrales duplicará artificialmente la ejecución presupuestal. Asimismo, mezclar los slices de `producto` y `actividad` sin controlar el nivel jerárquico duplicará el presupuesto.

> [!WARNING]
> **Montos y PIA/PIM Vacíos**: El portal de Consulta Amigable no reporta valores acumulados de PIA/PIM para ciertos cortes mensuales intermedios, por lo que estas columnas pueden venir vacías en la capa Bronze. No se deben rellenar artificialmente con cero en Bronze para conservar la fidelidad del reporte original.

> [!NOTE]
> **Montos Negativos**: Los montos negativos reportados en la ejecución (ej. compromiso o devengado negativo) representan rebajas, anulaciones o ajustes oficiales del MEF y deben ser conservados rigurosamente sin alteración.

> [!CAUTION]
> **Región Geográfica**: El slice `presupuesto_departamento` representa la asignación territorial oficial del presupuesto según el clasificador geográfico del MEF y no debe confundirse ni asociarse directamente con la procedencia regional de los becarios de PRONABEC.

---

### Comando de Prueba Local Recomendado

Para realizar una validación local completa que extraiga el presupuesto base, la jerarquía y todas las desagregaciones presupuestales en dry-run:

```powershell
python -m pipelines.scrape_mef_budget `
  --consulta-amigable `
  --extraction-date 2026-06-14 `
  --start-year 2026 `
  --end-year 2026 `
  --include-hierarchy `
  --include-spending-breakdowns `
  --breakdown-slices producto,generica,fuente,rubro,departamento,temporal `
  --dry-run `
  --output-dir tmp
```

#### Comandos de Verificación de Resultados:
```powershell
# 1. Verificar la estructura física generada en tmp
Get-ChildItem -Recurse tmp\bronze\mef

# 2. Previsualizar las primeras líneas de un slice de desglose
Get-Content tmp\bronze\mef\presupuesto_producto\extraction_date=2026-06-14\data.csv -TotalCount 5

# 3. Leer el contenido de metadatos de auditoría
Get-Content tmp\bronze\mef\presupuesto_producto\extraction_date=2026-06-14\extraction_metadata.json -TotalCount 30
```

### Riesgos y Mitigación

* **Fragilidad del Scraping (Riesgo Alto)**: Al depender de un portal web externo que utiliza ASP.NET, cambios en el HTML, variaciones en los IDs de los botones del formulario (ej. `ctl00$CPH1$...`), reestructuraciones de la tabla o alteración del código de Unidad Ejecutora pueden romper la secuencia de peticiones.
* **Mitigación y Plan de Contingencia**:
  * El scraper implementa logs detallados y control de excepciones paso a paso.
  * **Modos de Contingencia**: El script conserva los modos `--source-file` y `--source-url` para ingresar directamente un CSV local ya descargado de forma manual o una tabla HTML estática de contingencia. Estos se mantienen estrictamente como caminos de respaldo y no deben considerarse la vía principal de extracción.

---

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
