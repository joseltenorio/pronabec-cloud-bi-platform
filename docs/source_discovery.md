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
