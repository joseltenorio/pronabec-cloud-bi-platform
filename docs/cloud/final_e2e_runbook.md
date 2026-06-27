# Runbook Operativo: Ejecución Cloud de Extremo a Extremo (E2E)

## 1. Propósito

Este runbook proporciona una guía práctica y detallada para ejecutar de extremo a extremo la plataforma de datos PRONABEC Cloud BI Platform en Google Cloud Platform (GCP) hasta la capa Gold de BigQuery.

> [!IMPORTANT]
> **Power BI queda fuera de este runbook y se abordará en una fase posterior.** Este documento cubre el flujo técnico de extracción, staging, transformación distribuida con Dataflow, control de calidad y materialización de vistas Gold.

---

## 2. Requisitos previos

Para poder ejecutar el pipeline de datos completo, asegúrese de contar con los siguientes elementos configurados en su entorno GCP:

- Proyecto GCP activo y configurado.
- APIs de Google Cloud habilitadas:
  - BigQuery API
  - Cloud Storage API
  - Cloud Run API
  - Dataflow API
  - Cloud Artifact Registry API
  - Cloud Composer API

- Bucket de Cloud Storage (`GCS_BUCKET_NAME`) para data lake (Bronze, DLQ, logs, temporales).
- Datasets BigQuery creados para las capas: Bronze (como tablas externas), Silver, Gold y Audit.
- Repositorio en Artifact Registry para almacenar la imagen Docker del proyecto.
- Entorno de Cloud Composer (Airflow) activo y conectado a la red correspondiente.
- Variables de entorno del proyecto cargadas localmente (por ejemplo, mediante un archivo `~/pronabec_env.sh` en Cloud Shell o PowerShell).
- Los reportes documentales de origen listos y cargados en la ruta de Landing en Cloud Storage.

---

## 3. Variables esperadas

El pipeline y las herramientas de despliegue esperan que las siguientes variables de entorno estén definidas antes de iniciar la ejecución:

```bash
# Variables del entorno GCP
export GCP_PROJECT_ID="tu-proyecto-gcp-id"
export GCP_REGION="us-central1"
export GCS_BUCKET_NAME="tu-bucket-lake-name"
export BQ_LOCATION="US"

# Datasets de BigQuery
export BQ_BRONZE_DATASET="bronze"
export BQ_SILVER_DATASET="silver"
export BQ_GOLD_DATASET="gold"
export BQ_AUDIT_DATASET="audit"

# Rutas operativas de Dataflow
export DATAFLOW_TEMP_LOCATION="gs://tu-bucket-lake-name/temp"
export DATAFLOW_STAGING_LOCATION="gs://tu-bucket-lake-name/staging"

# Repositorio Docker
export ARTIFACT_REGISTRY_REGION="us-central1"
export ARTIFACT_REGISTRY_REPOSITORY="project-cloud-bi"
export ARTIFACT_IMAGE_NAME="pronabec-cloud-bi-platform"
export ARTIFACT_IMAGE_TAG="latest"

# Composer
export COMPOSER_ENVIRONMENT="tu-entorno-composer"
export COMPOSER_LOCATION="us-central1"
```

---

## 4. Preparar entorno

Para comenzar a operar, inicie su terminal (Cloud Shell o terminal local con el SDK de Google Cloud instalado y autenticado) y configure el proyecto activo.

### En Cloud Shell

```bash
source ~/pronabec_env.sh
gcloud config set project $GCP_PROJECT_ID
gcloud config set run/region $GCP_REGION
```

### En PowerShell

```powershell
. .\scripts\load_env.ps1
gcloud config set project $env:GCP_PROJECT_ID
gcloud config set run/region $env:GCP_REGION
```

---

## 5. Construir y publicar imagen Docker

Construya la imagen Docker de procesamiento batch que contiene todos los pipelines de extracción, staging, transformación y calidad de datos, y publíquela en Artifact Registry:

```powershell
.\scripts\build_and_push_image.ps1
```

Este script compila la imagen utilizando las dependencias de `requirements.txt` y la sube al repositorio especificado por las variables de entorno.

---

## 6. Desplegar objetos en BigQuery

Genere los DDL dinámicos y despliegue los datasets, tablas y vistas en BigQuery:

```powershell
# 1. Generar DDL dinámicos para tablas Bronze y Silver a partir de los esquemas JSON
.\scripts\generate_bigquery_ddl.ps1

# 2. Renderizar plantillas de SQL manuales reemplazando placeholders
.\scripts\render_sql_templates.ps1

# 3. Desplegar los recursos y tablas en BigQuery
.\scripts\deploy_bigquery_sql.ps1
```

> [!NOTE]
> Las carpetas `build/generated/sql/` contienen los DDL intermedios renderizados que son ignorados por Git para evitar ruido en el repositorio. Se vuelven a generar dinámicamente en cada despliegue.

---

## 7. Desplegar Cloud Run Jobs

Registre y actualice la configuración de todos los Cloud Run Jobs necesarios para el pipeline:

```powershell
.\scripts\deploy_cloud_run_jobs.ps1
```

Este comando registrará los siguientes Cloud Run Jobs en su región de GCP:

- **`pronabec-extract-job`**: extractor de la API pública PRONABEC.
- **`mef-extract-job`**: extractor presupuestal MEF.
- **`pronabec-stage-reports-job`**: staging para reportes de Landing a Bronze.
- **`quality-checks-job`**: ejecutor de validaciones y calidad de datos sobre BigQuery.
- **Jobs lanzadores de Dataflow**:
  - `dataflow-pronabec-convocatorias-job`
  - `dataflow-pronabec-ubigeo-postulacion-job`
  - `dataflow-pronabec-becarios-pais-estudio-job`
  - `dataflow-pronabec-colegios-habiles-job`
  - `dataflow-pronabec-becarios-provincia-job`
  - `dataflow-mef-presupuesto-job`
  - `dataflow-mef-presupuesto-temporal-job`
  - `dataflow-mef-producto-job`
  - `dataflow-mef-producto-temporal-job`
  - `dataflow-mef-actividad-job`
  - `dataflow-mef-actividad-temporal-job`
  - `dataflow-mef-generica-job`
  - `dataflow-mef-generica-temporal-job`
  - `dataflow-mef-hierarchy-job`
  - **`dataflow-pronabec-report-job`**: job parametrizable único para los 23 reportes documentales.

---

## 8. Subir DAG a Composer

Copie el DAG de Airflow al bucket de DAGs asociado a su entorno de Composer:

```powershell
.\scripts\upload_composer_dag.ps1
```

El archivo del DAG se cargará en el bucket de Cloud Composer y estará disponible en la consola de Airflow en pocos minutos.

---

## 9. Preparar Landing de reportes

Antes de ejecutar el pipeline, los reportes documentales deben subirse en formato CSV, con sus nombres y carpetas correctas, a las rutas correspondientes del bucket de Cloud Storage:

```text
gs://<bucket-lake-name>/landing/pronabec_reports/pes_2025/
gs://<bucket-lake-name>/landing/pronabec_reports/beca18_universitarios_2012_2026/
```

El proceso de staging `pronabec-stage-reports-job` leerá estos archivos de Landing, adjuntará metadatos técnicos y creará las estructuras correspondientes en la capa Bronze.

---

## 10. Ejecutar DAG manualmente

Aunque el DAG `pronabec_medallion_batch` está programado para ejecutarse de forma semanal (sábados a las 05:00), puede lanzar una corrida manual:

1. Abra la interfaz web de Airflow en Google Cloud Composer.
2. Busque el DAG `pronabec_medallion_batch`.
3. Presione el botón **Trigger DAG** o use **Trigger DAG w/ config** para enviar parámetros como una `extraction_date` personalizada.

---

## 11. Validar Bronze en Cloud Storage

Una vez completadas las extracciones y tareas de staging, valide que los archivos crudos se encuentren correctamente escritos en las siguientes rutas estructuradas:

- **API PRONABEC**: `gs://<bucket-lake-name>/bronze/pronabec/<dataset>/extraction_date=YYYY-MM-DD/`
- **MEF**: `gs://<bucket-lake-name>/bronze/mef/<slice>/extraction_date=YYYY-MM-DD/year=YYYY/`
- **Reports**: `gs://<bucket-lake-name>/bronze/pronabec_reports/<dataset>/extraction_date=YYYY-MM-DD/`

---

## 12. Validar Silver en BigQuery

Confirme que las tablas de la capa Silver en BigQuery se hayan actualizado correctamente con los datos limpios y tipados:

- **PRONABEC API Silver**:
  - `pronabec_convocatorias`
  - `pronabec_ubigeo_postulacion`
  - `pronabec_becarios_pais_estudio`
  - `pronabec_colegios_elegibles`
  - `pronabec_beca18_becarios_provincia_2016`

- **MEF Silver**:
  - `presupuesto_mef`
  - `presupuesto_mef_temporal`
  - `presupuesto_mef_producto`
  - `presupuesto_mef_producto_temporal`
  - `presupuesto_mef_actividad`
  - `presupuesto_mef_actividad_temporal`
  - `presupuesto_mef_generica`
  - `presupuesto_mef_generica_temporal`
  - `presupuesto_mef_hierarchy`

- **PRONABEC Reports Silver**:
  - Tablas con el prefijo `pronabec_report_*` (en total 23 tablas de reportes).

---

## 13. Validar Gold en BigQuery

Las vistas analíticas en la capa Gold deben estar disponibles y listas para el consumo:

- `vw_pronabec_resumen_ejecutivo`
- `vw_beca18_becas_otorgadas_anual`
- `vw_beca18_cobertura_territorial_2016`
- `vw_beca18_universitarios_carrera_anual`
- `vw_beca18_universitarios_universidad_anual`
- `vw_beca18_perfil_social_indicadores`
- `vw_beca18_region_postulacion`
- `vw_mef_presupuesto_ejecucion_anual`
- `vw_mef_presupuesto_ejecucion_temporal`
- `vw_pronabec_becas_vs_presupuesto_anual`
- `vw_pronabec_beca18_resumen_analitico`

---

## 14. Validar Calidad y Audit

Al finalizar la corrida del DAG, verifique los resultados de auditoría y controles de calidad en la tabla de BigQuery:

```sql
SELECT *
FROM `tu-proyecto-gcp-id.audit.audit_data_quality_results`
ORDER BY check_timestamp DESC
LIMIT 50;
```

> [!NOTE]
> Las tablas temporales de MEF no tienen validaciones de no-negatividad en montos de forma estricta. No hay validaciones globales duras para `devengado < 0`, permitiendo ajustes negativos propios de los registros de Consulta Amigable.

---

## 15. Exclusiones críticas

Es importante tener en cuenta que para esta versión del pipeline:

- **`convocatorias_carrera_sede` es Bronze-only**: los datos se conservan únicamente en la capa Bronze para trazabilidad técnica. No son procesados por Dataflow, no se exponen en Silver ni se referencian en vistas Gold o calidad.
- **MEF Bronze-only**: las tablas `presupuesto_departamento`, `presupuesto_fuente` y `presupuesto_rubro` no se transforman hacia Silver.
- **Power BI**: queda fuera de esta fase y no se modifican ni cargan modelos DAX o archivos `.pbix`.

---

## 16. Resolución de problemas comunes

- **Falta de archivos DDL en `build/generated/sql/`**: los archivos DDL no se versionan. Ejecute `.\scripts\generate_bigquery_ddl.ps1` y `.\scripts\render_sql_templates.ps1` antes de desplegar.
- **Fallas del DirectRunner en Windows**: al probar Dataflow localmente en Windows, pueden ocurrir errores por bloqueos de archivos o límites de multiprocesamiento (`WinError 5` o `WinError 32`). Para corridas productivas o de validación final, despliegue y valide usando `DataflowRunner` directamente en GCP.
- **El job de staging falla**: si los archivos originales en `landing/pronabec_reports/` no están presentes o sus nombres no coinciden con las convenciones de `endpoints.yaml`, la tarea fallará.
- **Composer no ejecuta correctamente los reportes**: valide que las variables `SOURCE_DATASET`, `INPUT_PATH` y `OUTPUT_TABLE` se estén sobreescribiendo adecuadamente en la configuración de la tarea del DAG.
