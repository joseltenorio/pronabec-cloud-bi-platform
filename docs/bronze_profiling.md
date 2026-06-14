# Flujo local de perfilado de Bronze

Este flujo prepara evidencia para el diseño de Silver mediante la descarga de datos Bronze locales y su perfilado con herramientas reproducibles en Python.

No utiliza notebooks, SQL, BigQuery, Dataflow ni ningún recurso desplegado en GCP. Los archivos generados son artefactos temporales de análisis y no deben ser incluidos en commits.

## Por qué perfilar antes de Silver

Los esquemas Bronze conservan la forma original de la fuente y su trazabilidad. Antes de definir los contratos de Silver, el proyecto necesita evidencia local para decidir:

- qué columnas son útiles para analítica;
- qué columnas pueden eliminarse o son solo metadatos;
- qué columnas deben mantenerse como `STRING`;
- qué columnas son candidatas a `INTEGER`, `NUMERIC`, `DATE` o `TIMESTAMP`;
- qué datasets adicionales de PRONABEC vale la pena promover a Silver;
- qué cortes de MEF son lo suficientemente estables para tiparse en Silver;
- qué advertencias de calidad deben resolverse antes de Dataflow.

Los reportes son insumos para la toma de decisiones. No reemplazan la revisión humana ni modifican esquemas automáticamente.

## Descargar Bronze local

Usa el flujo de extracción local para escribir salidas Bronze temporales bajo `tmp/bronze/`.

El modo Quick es el predeterminado y está pensado para un perfilado rápido. Extrae todos los datasets PRONABEC habilitados con un límite de páginas, y MEF para 2026, salvo que se indique un rango de años personalizado.

```powershell
.\scripts\run_local_full_bronze_extract.ps1 -ExtractionDate 2026-06-14 -StartYear 2012 -EndYear 2026 -OutputDir tmp
```

Para una primera ejecución más rápida, usa explícitamente el modo Quick:

```powershell
.\scripts\run_local_full_bronze_extract.ps1 -ExtractionDate 2026-06-14 -Mode Quick -MaxPages 5 -OutputDir tmp
```

El modo Full descarga los endpoints completos de PRONABEC y MEF para el rango solicitado. Puede tardar varias horas, ya que algunos datasets de PRONABEC tienen miles de páginas.

```powershell
.\scripts\run_local_full_bronze_extract.ps1 -ExtractionDate 2026-06-14 -Mode Full -StartYear 2012 -EndYear 2026 -OutputDir tmp
```

PRONABEC conserva el layout Bronze existente, sin partición por año:

```text
tmp/bronze/pronabec/<dataset>/extraction_date=YYYY-MM-DD/data_raw.json
tmp/bronze/pronabec/<dataset>/extraction_date=YYYY-MM-DD/data.jsonl
tmp/bronze/pronabec/<dataset>/extraction_date=YYYY-MM-DD/extraction_metadata.json
```

MEF conserva el layout Bronze particionado por fecha de extracción y año:

```text
tmp/bronze/mef/<slice>/extraction_date=YYYY-MM-DD/year=YYYY/data.csv
tmp/bronze/mef/<slice>/extraction_date=YYYY-MM-DD/year=YYYY/extraction_metadata.json
```

## Ejecutar el perfilado local

Ejecuta el profiler contra el directorio Bronze local:

```powershell
.venv\Scripts\python tools/profile_bronze_local.py --bronze-dir tmp/bronze --output-dir tmp/profiling/bronze
```

Parámetros opcionales:

```powershell
.venv\Scripts\python tools/profile_bronze_local.py --bronze-dir tmp/bronze --output-dir tmp/profiling/bronze --sample-size 20 --max-distinct-samples 10
```

El profiler solo lee archivos locales. No descarga datos ni llama a GCP.

## Salidas temporales

Las salidas del perfilado se generan en:

```text
tmp/profiling/bronze/
```

Reportes:

- `dataset_summary.csv`
- `column_profile.csv`
- `nulls_report.csv`
- `type_candidates.csv`
- `quality_warnings.csv`
- `silver_candidates.md`
- `profile_summary.json`

No incluir `tmp/bronze/` ni `tmp/profiling/` en commits.

## Cómo usar los reportes

Usa `dataset_summary.csv` para confirmar la cobertura de datasets, conteos de filas, conteos de archivos, fechas de extracción y años MEF detectados.

Usa `column_profile.csv` y `nulls_report.csv` para revisar valores nulos, cadenas vacías, valores de muestra, conteos de valores distintos y longitudes máximas.

Usa `type_candidates.csv` como insumo conservador para recomendaciones de tipos. Las columnas identificadoras, como `codigo_*`, `*_codigo`, `*_id`, `ruc`, `telefono`, `ubigeo` y `source_row_id`, se conservan como candidatas a `STRING`, incluso cuando sus valores parecen numéricos.

Usa `quality_warnings.csv` para revisar problemas como alto porcentaje de nulos, columnas completamente vacías, baja cantidad de valores distintos, identificadores que parecen numéricos, valores decimales con coma y valores con tipos mixtos.

Usa `silver_candidates.md` como checklist legible para revisión humana. Etiquetas como `KEEP_RECOMMENDED`, `DROP_CANDIDATE` y `REVIEW_REQUIRED` son recomendaciones conservadoras, no decisiones finales de esquema.

## Perfilado SQL futuro

El perfilado SQL puede agregarse más adelante, después de desplegar tablas externas Bronze en BigQuery. Eso está intencionalmente fuera del alcance de este flujo local previo a Silver.
