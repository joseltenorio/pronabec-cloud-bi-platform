# Panorama de Estudios Sociales del PRONABEC (PES 2025)

Este documento detalla las especificaciones técnicas y metodológicas de la nueva familia de fuentes oficiales agregadas de PRONABEC derivadas del informe oficial de estudios sociales.

## 1. Información de la Fuente
* **Documento original:** `7219175-panorama-de-estudios-sociales-pronabec.pdf`
* **Título oficial:** *Panorama de Estudios Sociales del Pronabec - Volumen I*
* **Institución emisora:** Programa Nacional de Becas y Crédito Educativo (PRONABEC)
* **Año de referencia:** 2025
* **Tipo de fuente:** Informe oficial / Documental (PDF)
* **Método de extracción inicial:** Extracción controlada y tabulación manual de tablas y figuras (`pdf_table_extraction`).

## 2. Naturaleza de los Datos
* Los datos de esta fuente representan información **agregada y consolidada a nivel de población o categorías** publicada formalmente por PRONABEC.
* **No contienen microdatos individuales** ni datos personales (PII) de becarios o postulantes, por lo que no es posible realizar inferencia a nivel individual.
* Son de gran utilidad para análisis histórico de tendencias, participación por género, origen territorial, migración estudiantil, primera generación familiar universitaria, aspectos socioeconómicos (puntajes ENP, lengua materna, autoidentificación étnica) y motivaciones educativas.

## 3. Flujo de Datos en la Plataforma
```text
PDF Oficial PRONABEC (Manual)
  -> Tabulación controlada a CSV
  -> gs://<bucket>/landing/pronabec_reports/pes_2025/
  -> Cloud Run Job de staging
  -> gs://<bucket>/bronze/pronabec_reports/<dataset>/extraction_date=YYYY-MM-DD/
  -> Dataflow Bronze to Silver
  -> BigQuery Silver
  -> BigQuery Gold
  -> Power BI
```

## 4. Rutas de Almacenamiento
### Local (Scratch / No versionado)
* Carpeta de entrada con CSVs y PDF para desarrollo y pruebas:
  `data/manual/pronabec_reports/pes_2025/`

Esta ruta local se mantiene solo para desarrollo. En cloud productivo, la entrada oficial es Landing.

### Google Cloud Storage Landing
* CSVs tabulados originales, conservando nombres reales:
  `gs://<GCS_BUCKET_NAME>/landing/pronabec_reports/pes_2025/*.csv`
* PDFs oficiales asociados:
  `gs://<GCS_BUCKET_NAME>/landing/pronabec_reports/pes_2025/_documents/*.pdf`

### Bronze Local de Pruebas
* Carpeta temporal con estructura de partición por fecha de extracción:
  `tmp/bronze/pronabec_reports/<dataset>/extraction_date=YYYY-MM-DD/data.csv`

### Google Cloud Storage Bronze (Data Lake)
* Datos tabulados en CSV:
  `gs://<GCS_BUCKET_NAME>/bronze/pronabec_reports/<dataset>/extraction_date=YYYY-MM-DD/data.csv`
* Metadatos de staging:
  `gs://<GCS_BUCKET_NAME>/bronze/pronabec_reports/<dataset>/extraction_date=YYYY-MM-DD/extraction_metadata.json`

El nombre del dataset se deriva del nombre del CSV en Landing removiendo el prefijo `pronabec_` y la extensión `.csv`. Por ejemplo, `pronabec_report_beca18_sexo_anual.csv` produce el dataset `report_beca18_sexo_anual`, pero el archivo Bronze se mantiene como `data.csv`.

## 5. Datasets Incluidos
Esta familia de datos agrupa 21 datasets tabulados correspondientes a gráficos, figuras o secciones específicas del reporte:
1. `pronabec_report_beca18_region_postulacion_anual` (Pág. 8, Fig. 3): Distribución regional anual de postulantes.
2. `pronabec_report_beca18_becas_otorgadas_modalidad_anual` (Pág. 7, Fig. 1): Evolución anual de becas otorgadas por modalidad.
3. `pronabec_report_beca18_sexo_anual` (Pág. 7, Fig. 2): Composición de becarios por sexo a lo largo de los años.
4. `pronabec_report_beca18_region_postulacion_acumulada` (Pág. 8, Fig. 4): Postulación acumulada agregada por región.
5. `pronabec_report_beca18_migracion_region_anual` (Pág. 9, Fig. 6): Tasa anual de migración regional de becarios.
6. `pronabec_report_beca18_migracion_region_acumulada` (Pág. 10, Fig. 7): Migración regional acumulada por departamento.
7. `pronabec_report_beca18_colegio_gestion_2025` (Pág. 14, Fig. 1): Distribución de becarios por tipo de gestión escolar.
8. `pronabec_report_beca18_padres_nivel_educativo_2025` (Pág. 14, Fig. 2): Nivel educativo máximo alcanzado por los padres de los becarios.
9. `pronabec_report_beca18_primera_generacion_region` (Pág. 15, Fig. 3): Becarios de primera generación en educación superior por departamento.
10. `pronabec_report_beca18_region_postulacion_2025` (Pág. 16, Fig. 4): Porcentaje de becarios según región de postulación en la encuesta 2025.
11. `pronabec_report_beca18_enp_promedio_caracteristica_2025` (Pág. 19, Fig. 1): Puntaje promedio del Examen Nacional de Preselección (ENP) por características demográficas.
12. `pronabec_report_beca18_enp_promedio_region_2025` (Pág. 20, Fig. 3): Puntaje promedio del ENP por región de origen.
13. `pronabec_report_beca18_lengua_materna_modalidad_2025` (Pág. 22, Fig. 1): Distribución de lengua materna según modalidad de beca.
14. `pronabec_report_beca18_autoidentificacion_etnica_modalidad_2025` (Pág. 22, Fig. 2): Autoidentificación étnica declarada según modalidad de beca.
15. `pronabec_report_beca18_no_continuaria_sin_beca_caracteristica_2025` (Pág. 25, Fig. 1): Porcentaje de becarios que declaran que no continuarían sus estudios si no tuvieran la beca.
16. `pronabec_report_beca18_periodo_ingreso_ies_genero_2025` (Pág. 25, Fig. 2): Periodo de ingreso a la educación superior según género.
17. `pronabec_report_beca18_razones_eleccion_carrera_gestion_ies_2025` (Pág. 28, Fig. 1): Razones declaradas para la elección de carrera según tipo de gestión escolar previa.
18. `pronabec_report_beca18_razones_eleccion_carrera_sexo_2025` (Pág. 28, Fig. 2): Razones declaradas para la elección de carrera según sexo.
19. `pronabec_report_beca18_razones_eleccion_ies_gestion_2025` (Pág. 29, Fig. 3): Razones para elegir la Institución de Educación Superior (IES) según gestión escolar de origen.
20. `pronabec_report_beca18_preparacion_ies_tipo_2025` (Pág. 31, Fig. 1): Tipo de preparación académica reportada antes de la postulación.
21. `pronabec_report_beca18_preparacion_ies_meses_caracteristica_2025` (Pág. 31, Fig. 2): Promedio de meses de preparación reportados según características de los becarios.

## 6. Limitaciones Metodológicas y de Datos
* **Datos consolidados:** Al tratarse de porcentajes o agregaciones precalculadas por PRONABEC en el informe original, los valores numéricos de porcentajes pueden venir redondeados.
* **Tolerancia y Consistencia:** Se debe tener especial cuidado de no sumar directamente los porcentajes a través de diferentes regiones sin sopesar las bases poblacionales si no están disponibles.
* **Extracción:** La fidelidad de los datos depende directamente del proceso manual de tabulación. En el futuro, este proceso puede automatizarse con un parser de PDF sin alterar la estructura Bronze/Silver aquí definida.

## 7. Evidencia de Validación
El proceso completo de staging y la ejecución en DirectRunner para la validación estructural se detalla en [pronabec_pes_2025_validation.md](file:///c:/Users/Windows%2011/Desktop/Proyectos/pronabec-cloud-bi-platform/docs/dataflow/pronabec_pes_2025_validation.md).
