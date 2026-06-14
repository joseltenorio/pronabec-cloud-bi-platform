# Exploración y perfilado de fuentes

## Propósito

Este documento describe el proceso de exploración inicial de fuentes para Project Cloud BI Platform.

Antes de implementar extractores productivos, tablas externas Bronze o pipelines Dataflow, se realiza una prueba controlada para conocer el formato real de respuesta de las fuentes públicas.

## Objetivos

La exploración busca responder:

- Qué formato devuelve cada fuente.
- Si la respuesta es JSON, HTML, CSV u otro formato.
- Qué estructura tiene la respuesta.
- Si existe paginación.
- Qué columnas reales aparecen.
- Qué campos coinciden con el diccionario de datos preliminar.
- Qué formato conviene usar para la capa Bronze.
- Qué reglas de calidad iniciales deben aplicarse.

## Hallazgo inicial sobre PRONABEC

Los datasets públicos de PRONABEC exponen una página HTML en el patrón:

```text
https://datosabiertos.pronabec.gob.pe/Dataset/<Dataset>
```

Los datos se obtienen mediante endpoints JSON paginados con el patrón:

```text
https://datosabiertos.pronabec.gob.pe/Dataset/Listar<Dataset>
```

La respuesta usa una estructura compatible con jqGrid:

```text
total
page
records
rows
```

Cada elemento de `rows` contiene un identificador y un arreglo `cell`. El script de exploración convierte ese arreglo `cell` en columnas tabulares usando la configuración `expected_columns` definida en `config/endpoints.yaml`.

## Scripts

### `tools/probe_sources.py`

Explora endpoints públicos, identifica respuestas HTML o JSON, normaliza respuestas jqGrid y guarda muestras locales en `tmp/source_probe/`.

Uso recomendado:

```bash
python tools/probe_sources.py --source pronabec --dataset notas_becarios
python tools/probe_sources.py --source pronabec --dataset perdida_becas
```

### `tools/profile_sample.py`

Perfila una muestra local generada por el script de exploración.

Uso recomendado:

```bash
python tools/profile_sample.py --input tmp/source_probe/<archivo_sample>.jsonl
```

### `tools/profile_pronabec_sources.py`

Ejecuta exploración y perfilado para todos los datasets PRONABEC habilitados.

Uso recomendado:

```bash
python tools/profile_pronabec_sources.py
python tools/profile_pronabec_sources.py --dataset notas_becarios
```

Para guardar la salida completa de ejecución en un archivo local:

```bash
python tools/profile_pronabec_sources.py *> tmp/pronabec_profile_run.txt
```

## Exploración de endpoints adicionales de PRONABEC

Se evaluaron cuatro nuevos endpoints de PRONABEC para su incorporación en la capa Bronze. Todos siguen el patrón técnico jqGrid y se extraen en formato JSON y JSONL.

### 1. Colegios Hábiles (`colegios_habiles`)
- **Endpoint JSON evaluado**: `/Dataset/ListarColegiosHabiles`
- **Estructura de respuesta**: Objeto jqGrid con metadatos de paginación y registros tabulares en `rows[].cell`.
- **Registros reportados**: 71,605
- **Columnas detectadas en `rows[].cell`**: 11 (mapeadas a `nro_fila` más 10 campos de datos).
- **Decisión de aceptación para Bronze**: Aceptado. Se genera salida `data_raw.json` y `data.jsonl`.
- **Observaciones de calidad**: El campo `telefono` puede venir vacío, con formatos no estándar o con caracteres no numéricos, por lo que se conserva como `STRING`.

### 2. Becarios por País de Estudio (`becarios_pais_estudio`)
- **Endpoint JSON evaluado**: `/Dataset/ListarBecariosPorPaisDeEstudio`
- **Estructura de respuesta**: Estructura jqGrid estándar.
- **Registros reportados**: 87,501
- **Columnas detectadas en `rows[].cell`**: 8 (mapeadas a `nro_fila` más 7 campos de datos).
- **Decisión de aceptación para Bronze**: Aceptado.
- **Observaciones de calidad**: Permite el cruce geográfico de la formación internacional del becario. `fecha_carga` se conserva como `STRING`.

### 3. Convocatorias por Carrera y Sede (`convocatorias_carrera_sede`)
- **Endpoint JSON evaluado**: `/Dataset/ListarConvocatoriaPorCarreraSede`
- **Estructura de respuesta**: Estructura jqGrid estándar.
- **Registros reportados**: 395,487
- **Columnas detectadas en `rows[].cell`**: 19 (mapeadas a `nro_fila` más 18 campos de datos).
- **Decisión de aceptación para Bronze**: Aceptado. Es el dataset de mayor volumen identificado para PRONABEC en esta fase.
- **Observaciones de calidad**: Se detectaron valores nulos o vacíos frecuentes en columnas administrativas como `resolucion`, `region`, `web`, `representante`, `telefono` y `email`. El campo `ruc` (Registro Único de Contribuyentes) y `telefono` se conservan como `STRING` en Bronze para asegurar trazabilidad.

### 4. Nota Promedio del Postulante por Región (`nota_postulante_region`)
- **Endpoint JSON evaluado**: `/Dataset/ListarNotaPromedioDelPostulantePorRegion`
- **Estructura de respuesta**: Estructura jqGrid estándar.
- **Registros reportados**: 61
- **Columnas detectadas en `rows[].cell`**: 9 (mapeadas a `nro_fila` más 8 campos de datos).
- **Decisión de aceptación para Bronze**: Aceptado.
- **Observaciones de calidad**: El campo `nota_promedio` se reporta con formato de coma decimal y múltiples posiciones decimales (ej. `14,500000`). Se almacena como `STRING` en la capa Bronze para evitar pérdidas de precisión o fallas en la ingesta cruda. `anio_convocatoria` y `fecha_carga` se mantienen como `STRING`.

## Exploración y hallazgos sobre MEF Consulta Amigable

Para incorporar datos presupuestales, se realizó una inspección y perfilado del portal Consulta Amigable del Ministerio de Economía y Finanzas (MEF) - Navegador de Transparencia Económica.

### Proceso de Inspección
Se verificó que el Navegador web del MEF permite filtrar dinámicamente la información pública por año fiscal y por la clasificación de **Actividades/Proyectos**. A través de la simulación del protocolo HTTP, se observó la jerarquía de navegación requerida para llegar a la información presupuestaria de la institución de becas:
1. **Punto de Partida**: TOTAL (Presupuesto Nacional de la República).
2. **Nivel de Gobierno**: Selección de `E: GOBIERNO NACIONAL`.
3. **Sector**: Selección de `10: EDUCACION`.
4. **Pliego**: Selección de `010: M. DE EDUCACION`.
5. **Unidad Ejecutora**: Búsqueda en la tabla resultante del código correspondiente a PRONABEC.

### Observaciones y Código Técnico de la Ejecutora
Durante la fase de discovery real en el portal Consulta Amigable, se constató que:
* La fila de la Unidad Ejecutora correspondiente a PRONABEC aparece identificada exactamente bajo el texto:
  `117-1438: PROGRAMA NACIONAL DE BECAS Y CREDITO EDUCATIVO`
  *(Nota: Se corrigió una asunción inicial del proyecto donde se pensaba que el código era `117-1483`. Las pruebas y la inspección directa del portal retornaron consistentemente `117-1438`).*
* La fila de datos contiene las siguientes métricas presupuestales en columnas contiguas:
  `PIA` (Presupuesto Institucional de Apertura), `PIM` (Presupuesto Institucional Modificado), `Certificación`, `Compromiso Anual`, `Atención de Compromiso Mensual` (Compromiso Mensual), `Devengado`, `Girado` y `Avance %` (porcentaje de avance de la ejecución).

### Decisiones de Diseño y Normalización
1. **Implementación del Scraper**: Dado que Consulta Amigable es una aplicación ASP.NET clásica que mantiene estado en el servidor y utiliza formularios dinámicos, se descartó el uso de una API JSON sencilla. Se decidió implementar un scraping HTTP controlado basado en `requests.Session` y `BeautifulSoup` dentro de [scrape_mef_budget.py](file:///c:/Users/Windows%2011/Desktop/Proyectos/pronabec-cloud-bi-platform/pipelines/scrape_mef_budget.py).
2. **Resiliencia / Fallbacks**: Como mitigación ante cambios de etiquetas en el portal, se definieron fallbacks que buscan en la tabla las palabras clave `BECAS + CREDITO` o `PRONABEC` si no se encuentra la ejecutora exacta `117-1438`.
3. **Normalización del Contrato**: La salida se normaliza estructuralmente al contrato Bronze `presupuesto_mef` definido en [presupuesto_mef_schema.json](file:///c:/Users/Windows%2011/Desktop/Proyectos/pronabec-cloud-bi-platform/config/schemas/bronze/presupuesto_mef_schema.json), generando un archivo CSV alineado con los nombres estandarizados de columnas.

## Salidas locales

Los archivos generados se almacenan en:

```text
tmp/source_probe/
tmp/source_profile/
```

Estas carpetas no deben subirse al repositorio.

## Decisión preliminar de almacenamiento Bronze

Para PRONABEC se conservarán dos salidas en Bronze:

```text
data_raw.json
data.jsonl
```

`data_raw.json` conserva la respuesta original de la fuente.

`data.jsonl` conserva los registros normalizados estructuralmente, convirtiendo `rows[].cell` en columnas tabulares. Esta normalización no aplica todavía reglas de negocio; solo transforma la estructura técnica para facilitar lectura posterior desde Dataflow y BigQuery.

Para MEF se mantendrá inicialmente una salida tabular:

```text
data.csv
```

## Uso en la arquitectura

Los resultados de esta exploración se usarán para ajustar:

- rutas Bronze en Cloud Storage,
- tablas externas Bronze en BigQuery,
- definición de tablas Silver,
- reglas de calidad,
- diccionario de datos,
- extractores productivos en Cloud Run Jobs.

## Criterios para actualizar el diccionario de datos

Después del profiling, el diccionario de datos debe actualizarse considerando:

- columnas reales detectadas,
- campos con alto porcentaje de nulos,
- campos numéricos detectados,
- campos de fecha detectados,
- valores frecuentes,
- formatos especiales como porcentajes con coma decimal,
- campos que no deben ser obligatorios,
- campos que requieren limpieza en Silver.
