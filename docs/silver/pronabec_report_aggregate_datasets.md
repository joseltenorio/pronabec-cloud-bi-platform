# Modelado de Capa Silver para Reportes Agregados PRONABEC (PES 2025)

Este documento detalla el diseño lógico y físico de la capa Silver para los 21 datasets agregados de la familia PRONABEC Reports (informe oficial PES 2025).

## 1. Objetivo de Silver
La capa Silver transforma los datos de Bronze (tipado laxo y laxamente estructurado en CSVs externos) hacia BigQuery en tablas internas normalizadas y fuertemente tipadas. Los objetivos principales son:
* **Normalización estructural:** Garantizar que todas las columnas sigan convenciones de nombres consistentes y tipos adecuados (ej. `ano_convocatoria` como `INT64`, `porcentaje_becarios` como `NUMERIC`).
* **Metadata de trazabilidad documental:** Conservar columnas que describan el origen en el PDF original (`source_document_file`, `source_page`, etc.) para permitir auditoría rápida.
* **Metadata de auditoría técnica:** Incluir campos estándar (`source_system`, `source_dataset`, `extraction_date`, `ingestion_timestamp`, `pipeline_run_id`) con modo `REQUIRED` para trazabilidad de pipeline.

## 2. Criterios de Diseño
* **Formato Largo (Long Format):** Se favorecen estructuras relacionales de tipo llave-valor o filas desagregadas en lugar de tablas anchas con columnas dinámicas, facilitando las agregaciones posteriores en BigQuery Gold.
* **Tipado Fuerte:**
  * Años, páginas y conteos -> `INT64` / `INTEGER`
  * Porcentajes, ratios, tasas y promedios -> `NUMERIC`
  * Nombres, textos, figuras y métodos -> `STRING`
* **Metadata Técnica:** Al igual que en Silver MEF, los metadatos técnicos del pipeline se establecen como `REQUIRED` para asegurar consistencia del catálogo de datos y evitar nulos inesperados en la auditoría.

## 3. Catálogo y Granularidad de las Tablas (Grano de cada tabla)

A continuación se detalla el diseño de grano, dimensiones y métricas para cada una de las 21 tablas:

### 1. `pronabec_report_beca18_region_postulacion_anual`
* **Grano:** Año de convocatoria + Grupo de región (Ámbito geográfico).
* **Métrica principal:** `porcentaje_becarios` (NUMERIC, avance porcentual).
* **Dimensiones:** `ano_convocatoria` (INT64), `grupo_region` (STRING).
* **Uso:** Analizar cómo evoluciona la distribución geográfica de los postulantes a lo largo de los años.

### 2. `pronabec_report_beca18_becas_otorgadas_modalidad_anual`
* **Grano:** Año de convocatoria + Modalidad de beca.
* **Métrica principal:** `becas_otorgadas` (INT64, conteo de becarios).
* **Dimensiones:** `ano_convocatoria` (INT64), `modalidad` (STRING).
* **Uso:** Evaluar la evolución del número total de becas asignadas a cada modalidad.

### 3. `pronabec_report_beca18_sexo_anual`
* **Grano:** Año de convocatoria + Sexo.
* **Métrica principal:** `porcentaje_becarios` (NUMERIC).
* **Dimensiones:** `ano_convocatoria` (INT64), `sexo` (STRING).
* **Uso:** Estudiar la tendencia de la brecha de género o composición por sexo de los becarios.

### 4. `pronabec_report_beca18_region_postulacion_acumulada`
* **Grano:** Periodo acumulado (ej. 2012-2024) + Región (Departamento).
* **Métrica principal:** `porcentaje_acumulado` (NUMERIC).
* **Dimensiones:** `periodo` (STRING), `region` (STRING).
* **Uso:** Analizar la distribución de procedencia regional de forma acumulada histórica.

### 5. `pronabec_report_beca18_migracion_region_anual`
* **Grano:** Año de convocatoria.
* **Métrica principal:** `porcentaje_migracion_region` (NUMERIC).
* **Dimensiones:** `ano_convocatoria` (INT64).
* **Uso:** Medir el porcentaje total anual de estudiantes que migran fuera de su región para estudiar.

### 6. `pronabec_report_beca18_migracion_region_acumulada`
* **Grano:** Periodo acumulado + Región (Departamento de origen).
* **Métrica principal:** `tasa_migracion_acumulada` (NUMERIC).
* **Dimensiones:** `periodo` (STRING), `region` (STRING).
* **Uso:** Identificar las regiones con mayor salida estudiantil hacia otras provincias.

### 7. `pronabec_report_beca18_colegio_gestion_2025`
* **Grano:** Año de encuesta + Tipo de gestión de colegio.
* **Métrica principal:** `porcentaje_becarios` (NUMERIC).
* **Dimensiones:** `ano_encuesta` (INT64), `tipo_gestion_colegio` (STRING).
* **Uso:** Medir la proporción de procedencia de escuelas públicas vs privadas en la encuesta 2025.

### 8. `pronabec_report_beca18_padres_nivel_educativo_2025`
* **Grano:** Año de encuesta + Nivel educativo máximo de los padres.
* **Métrica principal:** `porcentaje_becarios` (NUMERIC).
* **Dimensiones:** `ano_encuesta` (INT64), `nivel_educativo_padres` (STRING).
* **Uso:** Analizar el perfil del entorno educativo familiar de los becarios.

### 9. `pronabec_report_beca18_primera_generacion_region`
* **Grano:** Periodo acumulado + Región (Departamento).
* **Métrica principal:** `ratio_primera_generacion` (NUMERIC), `total_becarios_primera_generacion` (INT64), `total_becarios_encuestados` (INT64).
* **Dimensiones:** `periodo` (STRING), `region` (STRING).
* **Uso:** Medir la tasa de estudiantes que son la primera generación en acceder a educación superior en su familia por región.

### 10. `pronabec_report_beca18_region_postulacion_2025`
* **Grano:** Año de encuesta + Región.
* **Métrica principal:** `porcentaje_becarios` (NUMERIC).
* **Dimensiones:** `ano_encuesta` (INT64), `region` (STRING).
* **Uso:** Distribución geográfica en la muestra de la encuesta 2025.

### 11. `pronabec_report_beca18_enp_promedio_caracteristica_2025`
* **Grano:** Año de encuesta + Grupo de característica + Característica.
* **Métrica principal:** `puntaje_promedio_enp` (NUMERIC).
* **Dimensiones:** `ano_encuesta` (INT64), `grupo_caracteristica` (STRING), `caracteristica` (STRING).
* **Uso:** Comparar puntajes ENP promedio según variables socioeconómicas y demográficas (ej. pobreza, tipo de colegio).

### 12. `pronabec_report_beca18_enp_promedio_region_2025`
* **Grano:** Año de encuesta + Región.
* **Métrica principal:** `puntaje_promedio_enp` (NUMERIC).
* **Dimensiones:** `ano_encuesta` (INT64), `region` (STRING).
* **Uso:** Analizar el rendimiento académico pre-selección (ENP) promedio por departamento.

### 13. `pronabec_report_beca18_lengua_materna_modalidad_2025`
* **Grano:** Año de encuesta + Lengua materna + Modalidad.
* **Métrica principal:** `porcentaje_becarios` (NUMERIC).
* **Dimensiones:** `ano_encuesta` (INT64), `lengua_materna` (STRING), `modalidad` (STRING).
* **Uso:** Analizar la diversidad lingüística de los becarios según el tipo de beca Beca 18.

### 14. `pronabec_report_beca18_autoidentificacion_etnica_modalidad_2025`
* **Grano:** Año de encuesta + Autoidentificación étnica + Modalidad.
* **Métrica principal:** `porcentaje_becarios` (NUMERIC).
* **Dimensiones:** `ano_encuesta` (INT64), `autoidentificacion_etnica` (STRING), `modalidad` (STRING).
* **Uso:** Analizar la composición de autoidentificación étnica declarada por modalidad de beca.

### 15. `pronabec_report_beca18_no_continuaria_sin_beca_caracteristica_2025`
* **Grano:** Año de encuesta + Grupo de característica + Característica.
* **Métrica principal:** `porcentaje_becarios` (NUMERIC).
* **Dimensiones:** `ano_encuesta` (INT64), `grupo_caracteristica` (STRING), `caracteristica` (STRING).
* **Uso:** Estimar el impacto y dependencia socioeconómica de la beca para la continuidad de estudios superiores.

### 16. `pronabec_report_beca18_periodo_ingreso_ies_genero_2025`
* **Grano:** Año de encuesta + Periodo de ingreso a IES + Sexo.
* **Métrica principal:** `porcentaje_becarios` (NUMERIC).
* **Dimensiones:** `ano_encuesta` (INT64), `periodo_ingreso_ies` (STRING), `sexo` (STRING).
* **Uso:** Analizar la temporalidad del ingreso universitario según género.

### 17. `pronabec_report_beca18_razones_eleccion_carrera_gestion_ies_2025`
* **Grano:** Año de encuesta + Razón de elección de carrera + Gestión de la IES.
* **Métrica principal:** `porcentaje_becarios` (NUMERIC).
* **Dimensiones:** `ano_encuesta` (INT64), `razon_eleccion_carrera` (STRING), `gestion_ies` (STRING).
* **Uso:** Entender qué motiva la elección de carrera dependiendo de si estudian en IES públicas o privadas.

### 18. `pronabec_report_beca18_razones_eleccion_carrera_sexo_2025`
* **Grano:** Año de encuesta + Razón de elección de carrera + Sexo.
* **Métrica principal:** `porcentaje_becarios` (NUMERIC).
* **Dimensiones:** `ano_encuesta` (INT64), `razon_eleccion_carrera` (STRING), `sexo` (STRING).
* **Uso:** Analizar diferencias motivacionales en la elección de carrera según género.

### 19. `pronabec_report_beca18_razones_eleccion_ies_gestion_2025`
* **Grano:** Año de encuesta + Razón de elección de IES + Gestión de IES.
* **Métrica principal:** `porcentaje_becarios` (NUMERIC).
* **Dimensiones:** `ano_encuesta` (INT64), `razon_eleccion_ies` (STRING), `gestion_ies` (STRING).
* **Uso:** Entender motivaciones de elección institucional de acuerdo a la gestión de la IES.

### 20. `pronabec_report_beca18_preparacion_ies_tipo_2025`
* **Grano:** Año de encuesta + Tipo de preparación.
* **Métrica principal:** `porcentaje_becarios` (NUMERIC).
* **Dimensiones:** `ano_encuesta` (INT64), `tipo_preparacion` (STRING).
* **Uso:** Distribución del tipo de academia/preparación previa declarada por los becarios.

### 21. `pronabec_report_beca18_preparacion_ies_meses_caracteristica_2025`
* **Grano:** Año de encuesta + Grupo de característica + Característica.
* **Métrica principal:** `promedio_meses_preparacion` (NUMERIC).
* **Dimensiones:** `ano_encuesta` (INT64), `grupo_caracteristica` (STRING), `caracteristica` (STRING).
* **Uso:** Comparar el tiempo promedio en meses dedicado a la preparación previa según perfiles de becarios.

## Publicación Beca 18: Cantidad de becarios universitarios, 2012-2026

### 22. `pronabec_report_beca18_universitarios_universidad_anual`
* **Grano:** Universidad de estudio + Año de convocatoria.
* **Métrica principal:** `cantidad_becarios` (INT64, conteo de becarios universitarios).
* **Dimensiones:** `universidad` (STRING), `ano_convocatoria` (INT64), `es_anio_preliminar` (BOOL).
* **Uso:** Analizar la evolución histórica y el ranking de universidades receptoras de becarios Beca 18.
* **Decisiones Silver:**
  - Se eliminan la columna `total` y la fila `Total general`.
  - El año 2026 se marca con `es_anio_preliminar = TRUE`.

### 23. `pronabec_report_beca18_universitarios_carrera_anual`
* **Grano:** Carrera de estudio + Año de convocatoria.
* **Métrica principal:** `cantidad_becarios` (INT64, conteo de becarios universitarios).
* **Dimensiones:** `carrera_estudio` (STRING), `ano_convocatoria` (INT64), `es_anio_preliminar` (BOOL).
* **Uso:** Evaluar la tendencia de carreras de estudio seleccionadas por becarios Beca 18 a nivel universitario.
* **Decisiones Silver:**
  - Se eliminan la columna `total` y la fila `Total general`.
  - El año 2026 se marca con `es_anio_preliminar = TRUE`.

## 4. Diferencia con fuentes PRONABEC API
* **Ruta Bronze:**
  * Endpoints API JSON: `bronze/pronabec/<dataset>/` (formato JSONL)
  * Informes Oficiales: `bronze/pronabec_reports/<dataset>/` (formato CSV)
* **Grano:** Las tablas de la API corresponden a microdatos (ej. un registro por cada becario o nota), mientras que las de la familia `pronabec_report_*` representan datos agregados y tabulaciones ya ejecutadas por el equipo de estudios de PRONABEC.

## 5. Buenas Prácticas de Uso
* **No desagregar incorrectamente:** Evitar sumar los porcentajes directamente a través de dimensiones si no se controla el grano de agrupación o si no se cuenta con las bases totales.
* **Metadata de trazabilidad:** Mantener expuestos en las consultas los campos `source_page` o `source_figure` para contrastar rápidamente con el PDF original en caso de dudas sobre las categorías.

## 6. Relación con la capa Gold
Estas tablas Silver se integrarán en Gold para:
1. Comparar perfiles demográficos históricos de becarios.
2. Cruzar variables geográficas acumuladas de postulaciones y migración con los datos presupuestales de MEF para medir correlación de gasto regional vs captación de postulantes/becarios.
3. Visualizar en Power BI reportes sociales sobre equidad, procedencia familiar y distribución étnica-lingüística.
