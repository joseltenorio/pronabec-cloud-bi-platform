# Validación de Despliegue Inicial GCP y Consistencia de Bronze

Este documento detalla la evidencia del estado inicial de la infraestructura en Google Cloud Platform (GCP) y las pruebas de consistencia realizadas sobre los conjuntos de datos en la capa **Bronze**.

> [!WARNING]
> **Alcance del Documento**: Este reporte documenta únicamente el despliegue inicial de la infraestructura y las validaciones de consistencia en la capa Bronze. **No constituye un cierre completo del pipeline cloud**. La carga física de datos en Silver/Gold y la conexión a Power BI se ejecutarán en fases posteriores.

---

## 1. Información General y Fecha de Evidencia
- **Fecha de Evidencia**: 2026-06-17
- **Entorno**: Desarrollo / Inicial

---

## 2. Estado de Infraestructura GCP Inicial
Se ha verificado la creación e inicialización de los siguientes componentes base:

- **GCP Project ID**: `pronabec-cloud-bi-platform`
- **GCP Project Number**: `1030103187284`
- **Región Principal**: `us-central1`
- **Ubicación de BigQuery y GCS**: `US` (Multi-regional)
- **Bucket de Cloud Storage (Data Lake)**: `gs://pronabec-cloud-bi-platform-lake-1030103187284/`
- **Artifact Registry**: `pronabec-containers`
- **Datasets de BigQuery Creados**:
  - `bronze` (Tablas externas)
  - `silver` (Tablas físicas vacías)
  - `gold` (Vistas/Tablas de consumo vacías)
  - `audit` (Tablas de control y auditoría de calidad vacías)
  - `ml` (Modelos y predicciones vacíos)

---

## 3. Cuentas de Servicio y Permisos
Se registraron las siguientes cuentas de servicio para la automatización del pipeline:

1. **Dataflow Runner SA**:
   `pronabec-dataflow-sa@pronabec-cloud-bi-platform.iam.gserviceaccount.com`
2. **Cloud Run Exec SA**:
   `pronabec-cloudrun-sa@pronabec-cloud-bi-platform.iam.gserviceaccount.com`
3. **CI/CD Deployment SA**:
   `pronabec-cicd-sa@pronabec-cloud-bi-platform.iam.gserviceaccount.com`

### APIs Activadas (Resumen)
Se encuentran activas las APIs esenciales para el procesamiento distribuido y analítica:
- Cloud Storage API (`storage.googleapis.com`)
- BigQuery API (`bigquery.googleapis.com`)
- Dataflow API (`dataflow.googleapis.com`)
- Artifact Registry API (`artifactregistry.googleapis.com`)
- Cloud Resource Manager API (`cloudresourcemanager.googleapis.com`)

---

## 4. Estructura Física en Cloud Storage (Bronze)
El layout físico del bucket cuenta con los archivos organizados bajo las siguientes rutas:
- `gs://pronabec-cloud-bi-platform-lake-1030103187284/bronze/pronabec/` (Datos estructurados de APIs)
- `gs://pronabec-cloud-bi-platform-lake-1030103187284/bronze/mef/` (Datos del presupuesto público MEF por año fiscal)
- `gs://pronabec-cloud-bi-platform-lake-1030103187284/bronze/pronabec_reports/` (Reportes manuales en CSV)
- `gs://pronabec-cloud-bi-platform-lake-1030103187284/bronze/pronabec_reports/_documents/` (Archivos fuente en PDF)

### Documentos Oficiales PDF Almacenados en `_documents/`
Se conservan los archivos PDF originales para asegurar la auditabilidad del pipeline:
- `7219175-panorama-de-estudios-sociales-pronabec.pdf`
- `8170922-beca-18-cantidad-de-becarios-segun-universidad-de-estudio-2012-2026.pdf`
- `8170922-beca-18-cantidad-de-becarios-en-universidades-segun-carrera-de-estudio-2012-2026.pdf`

---

## 5. Tablas en BigQuery (Esquemas)

### Capa Bronze (Tablas Externas)
Se crearon las tablas externas sobre Cloud Storage utilizando DDLs corregidos. A continuación, se detallan los conteos de registros observados y reportados por el usuario como evidencia del estado del Data Lake:

| Tabla de BigQuery (Capa Bronze) | Filas Reportadas (Evidencia) |
| :--- | :--- |
| `bronze.pronabec_convocatorias_raw` | 403 |
| `bronze.pronabec_ubigeo_postulacion_raw` | 500 |
| `bronze.pronabec_report_beca18_universitarios_carrera_anual_raw` | 673 |
| `bronze.pronabec_report_beca18_universitarios_universidad_anual_raw` | 96 |
| `bronze.mef_presupuesto_raw` | 10 |
| `bronze.mef_presupuesto_producto_raw` | 31 |
| `bronze.mef_presupuesto_generica_raw` | 35 |
| `bronze.mef_presupuesto_temporal_raw` | 90 |

> [!NOTE]
> Estos conteos representan evidencia inicial observada y reportada en la carga inicial y deberán revalidarse mediante consultas directas a BigQuery una vez se ejecute el pipeline completo de extremo a extremo.

### Capa Silver (Tablas Físicas Vacías)
Se crearon las tablas de base física en la capa Silver para recibir los datos transformados. En este momento se encuentran vacías (conteo = 0):
- `silver.pronabec_convocatorias`: 0 filas
- `silver.pronabec_report_beca18_universitarios_carrera_anual`: 0 filas
- `silver.presupuesto_mef`: 0 filas

---

## 6. Hallazgos Técnicos y Soluciones Aplicadas

### Hallazgo 1: Límite de Wildcards en Tablas Externas BigQuery (MEF)
- **Problema**: BigQuery no soporta múltiples wildcards (`*`) en la URI de origen de una tabla externa (ej. `gs://.../extraction_date=*/year=*/data.csv`). Esto impedía la lectura correcta de los archivos MEF particionados.
- **Solución**: Se actualizó `tools/generate_bigquery_ddl.py` para requerir el argumento `--bronze-extraction-date` (o la variable `BRONZE_EXTRACTION_DATE`). Al generarse el DDL con una fecha fija (ej. `2026-06-17`), la URI resultante tiene un único wildcard para el año fiscal (`year=*`), satisfaciendo las restricciones de BigQuery sin perder la capacidad de consolidar años.

### Hallazgo 2: Falta de Columnas de Metadatos en Reportes Universitarios
- **Problema**: Los archivos CSV originales de becarios universitarios por carrera y universidad no contenían las columnas documentales (`source_document_file`, `source_document_title`, etc.) requeridas por sus esquemas Bronze, lo que generaba errores de mapeo en BigQuery.
- **Solución**: Se actualizó la herramienta de staging `tools/stage_pronabec_manual_reports.py` para normalizar los nombres de las columnas e inyectar por fila los metadatos de trazabilidad documental indicados en la configuración, manteniendo la estructura ancha original.

### Limpieza de Esquemas
- Se eliminaron definitivamente del repositorio los esquemas no aprobados de MEF `presupuesto_mef_categoria` y `presupuesto_mef_subgenerica` para evitar referencias obsoletas, dado que están fuera del alcance actual.

---

## 7. Próximos Pasos (Pendientes)
1. Cargar datos de la fecha de extracción `2026-06-17` a Bronze local/nube según aplique.
2. Validar que las tablas externas de BigQuery leen correctamente las nuevas rutas.
3. Ejecutar el pipeline de transformación Bronze -> Silver (usando carga `WRITE_TRUNCATE` controlada para la primera carga).
4. Ejecutar y validar las pruebas de calidad (Audit real).
5. Desplegar vistas físicas/lógicas en la capa Gold de BigQuery.
6. Diseñar el modelo semántico en Power BI y desarrollar el archivo PBIX (guardando capturas de pantalla de los dashboards únicamente al concluir este paso).
