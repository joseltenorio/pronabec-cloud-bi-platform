# Fuente Documental PRONABEC - Beca 18: Cantidad de becarios universitarios

Este documento describe la procedencia, el diseño de modelado y las especificaciones técnicas para la integración de la publicación oficial de PRONABEC sobre becarios universitarios históricos.

## 1. Información de la Fuente Oficial
* **Publicación:** Beca 18: Cantidad de becarios universitarios
* **Institución:** PRONABEC (Programa Nacional de Becas y Crédito Educativo)
* **Fecha de Publicación:** 21 de mayo de 2026
* **Página Oficial:** [PRONABEC Informes y Publicaciones](https://www.gob.pe/institucion/pronabec/informes-publicaciones/8170922-beca-18-cantidad-de-becarios-universitarios)

## 2. Documentos Fuente
La publicación incluye dos documentos PDF descargables:
1. `8170922-beca-18-cantidad-de-becarios-segun-universidad-de-estudio-2012-2026.pdf`
   * **Tabla original:** Beca 18: Cantidad de becarios según universidad de estudio, 2012-2026
2. `8170922-beca-18-cantidad-de-becarios-en-universidades-segun-carrera-de-estudio-2012-2026.pdf`
   * **Tabla original:** Beca 18: Cantidad de becarios en universidades según carrera de estudio, 2012-2026

## 3. Datasets Derivados
Para procesar y catalogar esta información dentro de la plataforma de datos se han definido dos datasets:
* `pronabec_report_beca18_universitarios_universidad_anual`
* `pronabec_report_beca18_universitarios_carrera_anual`

## 4. Flujo de Datos
El ciclo de vida de los datos de este informe sigue la arquitectura Medallion del proyecto:
```text
PDF Oficial PRONABEC 
-> CSV tabulado controlado
-> gs://<bucket>/landing/pronabec_reports/beca18_universitarios_2012_2026/
-> Cloud Run Job de staging
-> gs://<bucket>/bronze/pronabec_reports/<dataset>/extraction_date=YYYY-MM-DD/
-> Dataflow Bronze to Silver
-> BigQuery Silver
-> BigQuery Gold
-> Power BI
```

## 5. Especificaciones de Ubicación

### Ubicación Local Esperada (No versionada)
Los archivos se colocan en el directorio:
`data/manual/pronabec_reports/beca18_universitarios_2012_2026/`

Esta ruta se mantiene para desarrollo local. El flujo productivo usa Cloud Storage Landing.

Contenido esperado:
- Los dos archivos PDF fuentes originales.
- `pronabec_report_beca18_universitarios_universidad_anual.csv` (saneado y tabulado).
- `pronabec_report_beca18_universitarios_carrera_anual.csv` (saneado y tabulado).

### Ubicación en Cloud Storage Landing
* **CSVs tabulados originales:**
  * `gs://<GCS_BUCKET_NAME>/landing/pronabec_reports/beca18_universitarios_2012_2026/*.csv`
* **PDFs de documentación:**
  * `gs://<GCS_BUCKET_NAME>/landing/pronabec_reports/beca18_universitarios_2012_2026/_documents/*.pdf`

### Ubicación en Cloud Storage Bronze
* **CSVs de Datos:**
  * `gs://<GCS_BUCKET_NAME>/bronze/pronabec_reports/report_beca18_universitarios_universidad_anual/extraction_date=YYYY-MM-DD/data.csv`
  * `gs://<GCS_BUCKET_NAME>/bronze/pronabec_reports/report_beca18_universitarios_carrera_anual/extraction_date=YYYY-MM-DD/data.csv`
* **Metadatos de staging:**
  * `gs://<GCS_BUCKET_NAME>/bronze/pronabec_reports/<dataset>/extraction_date=YYYY-MM-DD/extraction_metadata.json`

Bronze conserva el nombre técnico `data.csv` para cada dataset. El dataset se deriva del CSV de Landing removiendo el prefijo `pronabec_` y la extensión `.csv`.

## 6. Naturaleza de los Datos
* **Datos Agregados:** Esta fuente corresponde a tabulados oficiales precalculados agregados por año.
* **No son Microdatos:** No contienen identificadores de estudiantes individuales, por lo que no es posible realizar análisis de nivel individual.
* **Valor de Negocio:** Permiten realizar análisis de series temporales (evolución histórica), rankings de captación universitaria de becarios y mapear preferencias de carreras en universidades específicas.

## 7. Limitaciones y Consideraciones
* **Año Preliminar 2026:** Los datos de 2026 aparecen con un asterisco `2026 (*)` indicando preliminaridad, lo cual se traduce en Silver como `es_anio_preliminar = TRUE`.
* **Ceros y Ausencias:** Los guiones `-` o vacíos de la tabla original representan la ausencia de registros de becarios para esa carrera o universidad en ese año y deben ser convertidos a `0` (o `NULL` si corresponde) durante la transformación Silver.
* **Filas de Total:** La fila `Total general` y columnas `total` se deben conservar en Bronze para cuadre, pero son descartadas en Silver para evitar duplicaciones en agregaciones de bases analíticas.
