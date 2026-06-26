# Evidencia de Staging y Validación de PRONABEC PES 2025

Este documento detalla el proceso de staging y validación de los 21 conjuntos de datos del **Panorama de Estudios Sociales (PES 2025)** del PRONABEC previo a la transición a la capa Gold final.

## 1. Contexto de PES 2025
El **Panorama de Estudios Sociales (PES 2025)** de PRONABEC es una recopilación estadística oficial e histórica sobre los becarios del programa Beca 18. Esta información pertenece al sistema de origen `pronabec_reports` y, a diferencia de otras fuentes dinámicas de PRONABEC, no proviene de APIs ni endpoints JSON (como jqGrid), sino de la tabulación manual y estructuración controlada de los gráficos y tablas expuestos en el documento PDF oficial: `7219175-panorama-de-estudios-sociales-pronabec.pdf`.

Los archivos fuente originales en formato CSV real viven localmente para desarrollo en:
`data/manual/pronabec_reports/pes_2025/`
(Esta ruta de datos crudos se encuentra excluida del control de versiones).

En cloud productivo, la ruta oficial de entrada es:
`gs://<BUCKET_NAME>/landing/pronabec_reports/pes_2025/*.csv`

Los PDFs oficiales asociados se conservan bajo:
`gs://<BUCKET_NAME>/landing/pronabec_reports/pes_2025/_documents/*.pdf`

El proceso de **Staging** se encarga de estructurar y copiar estos archivos locales hacia la zona temporal local representativa de la capa Bronze:
`tmp/bronze/pronabec_reports/<dataset>/extraction_date=2026-06-15/data.csv`

En producción, un Cloud Run Job stagea los CSV de Landing hacia Bronze. La ingesta posterior consume los archivos particionados en Google Cloud Storage en la ruta:
`gs://<BUCKET_NAME>/bronze/pronabec_reports/<dataset>/extraction_date=YYYY-MM-DD/data.csv`

Cada partición Bronze incluye también:
`gs://<BUCKET_NAME>/bronze/pronabec_reports/<dataset>/extraction_date=YYYY-MM-DD/extraction_metadata.json`

El pipeline periódico debe usar la misma `extraction_date` para PRONABEC API, MEF y reportes PRONABEC, de modo que los dry-runs y ejecuciones cloud comparen una misma fecha lógica.

---

## 2. Proceso de Staging Local

Para preparar la data Bronze local de los 21 archivos CSV se ejecutó la herramienta de staging `stage_pronabec_manual_reports.py` con el subconjunto específico `pes_2025` y en modo estricto.

### Comando de Staging Usado:
```powershell
.venv\Scripts\python tools\stage_pronabec_manual_reports.py `
  --input-dir data\manual\pronabec_reports\pes_2025 `
  --output-dir tmp\bronze\pronabec_reports `
  --extraction-date 2026-06-15 `
  --source-subset pes_2025 `
  --strict
```

### Resultados del Staging:
* **CSV PES 2025 Detectados:** 21 archivos CSV + 1 archivo PDF fuente (el cual fue ignorado correctamente para staging de datos, pero registrado como metadato del origen documental).
* **Datasets Bronze Stageados:** 21 directorios creados exitosamente bajo `tmp/bronze/pronabec_reports/`.
* **Archivos Generados:** Cada dataset contiene su archivo `data.csv` correspondiente al contenido crudo del CSV y su archivo de metadatos de extracción `extraction_metadata.json` con la información de trazabilidad documental, sha256 y cantidad de registros.

---

## 3. Dry-Run del Pipeline Dataflow (Bronze -> Silver)

Una vez stageados los datasets como Bronze local, se ejecutó un pipeline de validación en modo `dry-run` para cada uno de los 21 datasets. El dry-run simula la lectura, transformación y validación estructural de la capa Silver en DirectRunner (Apache Beam en memoria), registrando los outputs rechazados en la ruta local de Dead Letter Queue (DLQ) y persistiendo el resumen operativo (`summary.json`) por dataset.

### Comando Base de Dry-Run Utilizado:
```powershell
.venv\Scripts\python -m pipelines.dataflow_bronze_to_silver `
  --source-system pronabec_reports `
  --source-dataset <dataset> `
  --input-format csv `
  --input-path "tmp\bronze\pronabec_reports\<dataset>\extraction_date=2026-06-15\data.csv" `
  --extraction-date 2026-06-15 `
  --pipeline-run-id "manual-pes-2025-validation" `
  --runner DirectRunner `
  --dry-run `
  --dlq-output-root tmp\dlq `
  --summary-output-path "tmp\audit\dataflow_summary\<dataset>\summary.json"
```

### Comportamiento del DLQ y Processing Summary:
* **Dead Letter Queue (DLQ):** Si alguna fila no cumpliera con las restricciones de tipo de datos (por ejemplo, valores de porcentaje que no pueden parsearse o textos corruptos), el registro es ruteado al DLQ en formato JSONL y el pipeline no se detiene.
* **Processing Summary:** Al final de cada dry-run, el pipeline genera un archivo JSON que resume la ejecución indicando el estado final (`COMPLETED` o `COMPLETED_WITH_REJECTIONS`), total de registros leídos (`records_read`), válidos (`records_valid`) y rechazados (`records_rejected`).
* **Resultados:** Todos los 21 datasets procesaron exitosamente con estado `COMPLETED` y 0 registros rechazados en DLQ. Las diferencias entre registros leídos y válidos se deben a la omisión por diseño de las filas de totales generales y/o columnas de totales que se filtran durante la normalización (Clean & Project).

---

## 4. Tabla de Inventario de Datasets y Evidencia de Validación

A continuación se presenta el estado detallado de la validación sobre los 21 archivos CSV reales de PES 2025:

| Nº | Archivo Origen CSV | Dataset Bronze (Carpeta) | Tabla Silver BigQuery | Estado Staging | Estado Dry-run | Registros Leídos | Registros Válidos | Rechazos DLQ | Observaciones |
|---|---|---|---|---|---|---|---|---|---|
| 1 | `pronabec_report_beca18_autoidentificacion_etnica_modalidad_2025.csv` | `report_beca18_autoidentificacion_etnica_modalidad_2025` | `pronabec_report_beca18_autoidentificacion_etnica_modalidad_2025` | OK | OK | 99 | 88 | 0 | Se omitieron 11 filas de total por diseño. |
| 2 | `pronabec_report_beca18_becas_otorgadas_modalidad_anual.csv` | `report_beca18_becas_otorgadas_modalidad_anual` | `pronabec_report_beca18_becas_otorgadas_modalidad_anual` | OK | OK | 112 | 112 | 0 | Procesado exitosamente como snapshot. |
| 3 | `pronabec_report_beca18_colegio_gestion_2025.csv` | `report_beca18_colegio_gestion_2025` | `pronabec_report_beca18_colegio_gestion_2025` | OK | OK | 2 | 2 | 0 | Procesado exitosamente. |
| 4 | `pronabec_report_beca18_enp_promedio_caracteristica_2025.csv` | `report_beca18_enp_promedio_caracteristica_2025` | `pronabec_report_beca18_enp_promedio_caracteristica_2025` | OK | OK | 8 | 8 | 0 | Procesado exitosamente. |
| 5 | `pronabec_report_beca18_enp_promedio_region_2025.csv` | `report_beca18_enp_promedio_region_2025` | `pronabec_report_beca18_enp_promedio_region_2025` | OK | OK | 24 | 24 | 0 | Procesado exitosamente. |
| 6 | `pronabec_report_beca18_lengua_materna_modalidad_2025.csv` | `report_beca18_lengua_materna_modalidad_2025` | `pronabec_report_beca18_lengua_materna_modalidad_2025` | OK | OK | 45 | 40 | 0 | Se omitieron 5 filas de total por diseño. |
| 7 | `pronabec_report_beca18_migracion_region_acumulada.csv` | `report_beca18_migracion_region_acumulada` | `pronabec_report_beca18_migracion_region_acumulada` | OK | OK | 24 | 24 | 0 | Procesado exitosamente. |
| 8 | `pronabec_report_beca18_migracion_region_anual.csv` | `report_beca18_migracion_region_anual` | `pronabec_report_beca18_migracion_region_anual` | OK | OK | 14 | 14 | 0 | Procesado exitosamente. |
| 9 | `pronabec_report_beca18_no_continuaria_sin_beca_caracteristica_2025.csv` | `report_beca18_no_continuaria_sin_beca_caracteristica_2025` | `pronabec_report_beca18_no_continuaria_sin_beca_caracteristica_2025` | OK | OK | 13 | 12 | 0 | Se omitió 1 fila de total por diseño. |
| 10 | `pronabec_report_beca18_padres_nivel_educativo_2025.csv` | `report_beca18_padres_nivel_educativo_2025` | `pronabec_report_beca18_padres_nivel_educativo_2025` | OK | OK | 12 | 12 | 0 | Procesado exitosamente. |
| 11 | `pronabec_report_beca18_periodo_ingreso_ies_genero_2025.csv` | `report_beca18_periodo_ingreso_ies_genero_2025` | `pronabec_report_beca18_periodo_ingreso_ies_genero_2025` | OK | OK | 6 | 6 | 0 | Procesado exitosamente. |
| 12 | `pronabec_report_beca18_preparacion_ies_meses_caracteristica_2025.csv` | `report_beca18_preparacion_ies_meses_caracteristica_2025` | `pronabec_report_beca18_preparacion_ies_meses_caracteristica_2025` | OK | OK | 5 | 4 | 0 | Se omitió 1 fila de total por diseño. |
| 13 | `pronabec_report_beca18_preparacion_ies_tipo_2025.csv` | `report_beca18_preparacion_ies_tipo_2025` | `pronabec_report_beca18_preparacion_ies_tipo_2025` | OK | OK | 8 | 8 | 0 | Procesado exitosamente. |
| 14 | `pronabec_report_beca18_primera_generacion_region.csv` | `report_beca18_primera_generacion_region` | `pronabec_report_beca18_primera_generacion_region` | OK | OK | 24 | 24 | 0 | Procesado exitosamente. |
| 15 | `pronabec_report_beca18_razones_eleccion_carrera_gestion_ies_2025.csv` | `report_beca18_razones_eleccion_carrera_gestion_ies_2025` | `pronabec_report_beca18_razones_eleccion_carrera_gestion_ies_2025` | OK | OK | 30 | 20 | 0 | Se omitieron 10 filas de total por diseño. |
| 16 | `pronabec_report_beca18_razones_eleccion_carrera_sexo_2025.csv` | `report_beca18_razones_eleccion_carrera_sexo_2025` | `pronabec_report_beca18_razones_eleccion_carrera_sexo_2025` | OK | OK | 30 | 20 | 0 | Se omitieron 10 filas de total por diseño. |
| 17 | `pronabec_report_beca18_razones_eleccion_ies_gestion_2025.csv` | `report_beca18_razones_eleccion_ies_gestion_2025` | `pronabec_report_beca18_razones_eleccion_ies_gestion_2025` | OK | OK | 30 | 20 | 0 | Se omitieron 10 filas de total por diseño. |
| 18 | `pronabec_report_beca18_region_postulacion_2025.csv` | `report_beca18_region_postulacion_2025` | `pronabec_report_beca18_region_postulacion_2025` | OK | OK | 24 | 24 | 0 | Procesado exitosamente. |
| 19 | `pronabec_report_beca18_region_postulacion_acumulada.csv` | `report_beca18_region_postulacion_acumulada` | `pronabec_report_beca18_region_postulacion_acumulada` | OK | OK | 24 | 24 | 0 | Procesado exitosamente. |
| 20 | `pronabec_report_beca18_region_postulacion_anual.csv` | `report_beca18_region_postulacion_anual` | `pronabec_report_beca18_region_postulacion_anual` | OK | OK | 28 | 28 | 0 | Procesado exitosamente. |
| 21 | `pronabec_report_beca18_sexo_anual.csv` | `report_beca18_sexo_anual` | `pronabec_report_beca18_sexo_anual` | OK | OK | 28 | 28 | 0 | Procesado exitosamente. |

---

## 5. Resumen Operativo Local

* **Ubicación de los summaries locales:** `tmp/audit/dataflow_summary/<dataset>/summary.json`
* **Ubicación del DLQ local:** `tmp/dlq/pronabec_reports/<dataset>/` (Vacía al no haber fallos reales de negocio o corrupción de datos).
* **Fixes de mapping aplicados:** Ninguno. La naturaleza declarativa del transformador dinámico y los esquemas predefinidos en Silver/Bronze se alinearon al 100% con la estructura real de los CSVs tabulados.

---

## 6. Relación con Gold final

La validación exhaustiva de los 21 datasets de PES 2025 confirma la viabilidad técnica para proceder a la construcción de la capa Gold final. Los indicadores de perfil demográfico, socioeconómico, geográfico y motivacional dependen críticamente de estas fuentes.

No se debe construir ni actualizar la capa Gold final asumiendo la validez estructural de la data si previamente no se hubiese garantizado que los transforms Bronze -> Silver ejecutan limpiamente sobre DirectRunner. Con esta validación completada, PES 2025 queda catalogado formalmente como origen consistente y listo para ser consumido por las vistas analíticas en Gold.

---

## 7. Pendientes del Siguiente Bloque
Una vez validada localmente la data de origen PES 2025, el siguiente bloque técnico se enfocará en:
1. **Actualización de Vistas Analíticas en Gold:** Incorporar las tablas Silver de PES 2025 y modelar la vista analítica consolidada.
2. **Definición del Dashboard Power BI:** Diseñar las páginas de reporte y el modelo semántico en BigQuery Gold para su consumo final.
