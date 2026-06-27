# dags/pronabec_medallion_batch_dag.py

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


PROJECT_ID = "{{ var.value.gcp_project_id }}"
REGION = "{{ var.value.gcp_region }}"
BUCKET_NAME = "{{ var.value.gcs_bucket_name }}"

PRONABEC_EXTRACT_JOB = "{{ var.value.pronabec_extract_job_name }}"
MEF_EXTRACT_JOB = "{{ var.value.mef_extract_job_name }}"
PRONABEC_REPORTS_STAGE_JOB = "{{ var.value.pronabec_reports_stage_job_name }}"
QUALITY_CHECKS_JOB = "{{ var.value.quality_checks_job_name }}"

DATAFLOW_PRONABEC_REPORT_JOB = "{{ var.value.get('dataflow_pronabec_report_job_name', 'dataflow-pronabec-report-job') }}"

EXTRACTION_DATE = "{{ dag_run.conf.get('extraction_date', ds) }}"
RUN_PRONABEC = "{{ dag_run.conf.get('run_pronabec', true) }}"
RUN_MEF = "{{ dag_run.conf.get('run_mef', true) }}"
RUN_PRONABEC_REPORTS_STAGING = "{{ dag_run.conf.get('run_pronabec_reports_staging', true) }}"
RUN_DATAFLOW_PRONABEC = "{{ dag_run.conf.get('run_dataflow_pronabec', true) }}"
RUN_DATAFLOW_MEF = "{{ dag_run.conf.get('run_dataflow_mef', true) }}"
RUN_DATAFLOW_REPORTS = "{{ dag_run.conf.get('run_dataflow_reports', true) }}"
RUN_QUALITY = "{{ dag_run.conf.get('run_quality', true) }}"


# Mapeo de datasets de reportes a sus correspondientes subsets en Bronze
REPORT_SUBSETS = {
    "report_beca18_universitarios_carrera_anual": "beca18_universitarios_2012_2026",
    "report_beca18_universitarios_universidad_anual": "beca18_universitarios_2012_2026",
}

PRONABEC_SILVER_DATASETS = [
    {
        "source_dataset": "convocatorias",
        "job_name": "{{ var.value.get('dataflow_pronabec_convocatorias_job_name', 'dataflow-pronabec-convocatorias-job') }}",
        "output_table": "pronabec_convocatorias",
        "input_path_template": "gs://{bucket}/bronze/pronabec/convocatorias/extraction_date={extraction_date}/data.jsonl",
    },
    {
        "source_dataset": "ubigeo_postulacion",
        "job_name": "{{ var.value.get('dataflow_pronabec_ubigeo_postulacion_job_name', 'dataflow-pronabec-ubigeo-postulacion-job') }}",
        "output_table": "pronabec_ubigeo_postulacion",
        "input_path_template": "gs://{bucket}/bronze/pronabec/ubigeo_postulacion/extraction_date={extraction_date}/data.jsonl",
    },
    {
        "source_dataset": "becarios_pais_estudio",
        "job_name": "{{ var.value.get('dataflow_pronabec_becarios_pais_estudio_job_name', 'dataflow-pronabec-becarios-pais-estudio-job') }}",
        "output_table": "pronabec_becarios_pais_estudio",
        "input_path_template": "gs://{bucket}/bronze/pronabec/becarios_pais_estudio/extraction_date={extraction_date}/data.jsonl",
    },
    {
        "source_dataset": "colegios_habiles",
        "job_name": "{{ var.value.get('dataflow_pronabec_colegios_habiles_job_name', 'dataflow-pronabec-colegios-habiles-job') }}",
        "output_table": "pronabec_colegios_elegibles",
        "input_path_template": "gs://{bucket}/bronze/pronabec/colegios_habiles/extraction_date={extraction_date}/data.jsonl",
    },
    {
        "source_dataset": "becarios_provincia",
        "job_name": "{{ var.value.get('dataflow_pronabec_becarios_provincia_job_name', 'dataflow-pronabec-becarios-provincia-job') }}",
        "output_table": "pronabec_beca18_becarios_provincia_2016",
        "input_path_template": "gs://{bucket}/bronze/pronabec/becarios_provincia/extraction_date={extraction_date}/data.jsonl",
    },
]

MEF_SILVER_DATASETS = [
    {
        "source_dataset": "presupuesto",
        "job_name": "{{ var.value.get('dataflow_mef_presupuesto_job_name', 'dataflow-mef-presupuesto-job') }}",
        "output_table": "presupuesto_mef",
    },
    {
        "source_dataset": "presupuesto_temporal",
        "job_name": "{{ var.value.get('dataflow_mef_presupuesto_temporal_job_name', 'dataflow-mef-presupuesto-temporal-job') }}",
        "output_table": "presupuesto_mef_temporal",
    },
    {
        "source_dataset": "presupuesto_producto",
        "job_name": "{{ var.value.get('dataflow_mef_producto_job_name', 'dataflow-mef-producto-job') }}",
        "output_table": "presupuesto_mef_producto",
    },
    {
        "source_dataset": "presupuesto_producto_temporal",
        "job_name": "{{ var.value.get('dataflow_mef_producto_temporal_job_name', 'dataflow-mef-producto-temporal-job') }}",
        "output_table": "presupuesto_mef_producto_temporal",
    },
    {
        "source_dataset": "presupuesto_actividad",
        "job_name": "{{ var.value.get('dataflow_mef_actividad_job_name', 'dataflow-mef-actividad-job') }}",
        "output_table": "presupuesto_mef_actividad",
    },
    {
        "source_dataset": "presupuesto_actividad_temporal",
        "job_name": "{{ var.value.get('dataflow_mef_actividad_temporal_job_name', 'dataflow-mef-actividad-temporal-job') }}",
        "output_table": "presupuesto_mef_actividad_temporal",
    },
    {
        "source_dataset": "presupuesto_generica",
        "job_name": "{{ var.value.get('dataflow_mef_generica_job_name', 'dataflow-mef-generica-job') }}",
        "output_table": "presupuesto_mef_generica",
    },
    {
        "source_dataset": "presupuesto_generica_temporal",
        "job_name": "{{ var.value.get('dataflow_mef_generica_temporal_job_name', 'dataflow-mef-generica-temporal-job') }}",
        "output_table": "presupuesto_mef_generica_temporal",
    },
    {
        "source_dataset": "presupuesto_hierarchy",
        "job_name": "{{ var.value.get('dataflow_mef_hierarchy_job_name', 'dataflow-mef-hierarchy-job') }}",
        "output_table": "presupuesto_mef_hierarchy",
    },
]

PRONABEC_REPORT_SILVER_DATASETS = [
    "report_beca18_autoidentificacion_etnica_modalidad_2025",
    "report_beca18_becas_otorgadas_modalidad_anual",
    "report_beca18_colegio_gestion_2025",
    "report_beca18_enp_promedio_caracteristica_2025",
    "report_beca18_enp_promedio_region_2025",
    "report_beca18_lengua_materna_modalidad_2025",
    "report_beca18_migracion_region_acumulada",
    "report_beca18_migracion_region_anual",
    "report_beca18_no_continuaria_sin_beca_caracteristica_2025",
    "report_beca18_padres_nivel_educativo_2025",
    "report_beca18_periodo_ingreso_ies_genero_2025",
    "report_beca18_preparacion_ies_meses_caracteristica_2025",
    "report_beca18_preparacion_ies_tipo_2025",
    "report_beca18_primera_generacion_region",
    "report_beca18_razones_eleccion_carrera_gestion_ies_2025",
    "report_beca18_razones_eleccion_carrera_sexo_2025",
    "report_beca18_razones_eleccion_ies_gestion_2025",
    "report_beca18_region_postulacion_2025",
    "report_beca18_region_postulacion_acumulada",
    "report_beca18_region_postulacion_anual",
    "report_beca18_sexo_anual",
    "report_beca18_universitarios_carrera_anual",
    "report_beca18_universitarios_universidad_anual",
]

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


def cloud_run_execute_command(
    job_name: str,
    enabled_expression: str,
    extra_env_vars: dict[str, str] | None = None,
) -> str:
    env_vars = [
        f"BRONZE_EXTRACTION_DATE={EXTRACTION_DATE}",
        "PIPELINE_RUN_ID={{ run_id }}",
    ]
    if extra_env_vars:
        env_vars.extend(
            f"{key}={value}"
            for key, value in extra_env_vars.items()
        )
    joined_env_vars = ",".join(env_vars)

    return f"""
if [ "{enabled_expression}" = "True" ] || [ "{enabled_expression}" = "true" ]; then
  gcloud run jobs execute {job_name} \
    --project {PROJECT_ID} \
    --region {REGION} \
    --update-env-vars {joined_env_vars} \
    --wait
else
  echo "Task disabled by DAG configuration."
fi
""".strip()


with DAG(
    dag_id="pronabec_medallion_batch",
    description="Orquestación batch Medallion para extracción, transformación Bronze a Silver y calidad de PRONABEC Cloud BI Platform.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 5 * * 6",  # Ejecución semanal los sábados a las 05:00.
    catchup=False,
    max_active_runs=1,
    tags=["pronabec", "medallion", "batch", "cloud-run", "dataflow", "composer"],
    params={
        "extraction_date": Param(
            default="",
            type="string",
            description="Fecha lógica de extracción. Si no se envía, se usa la fecha de ejecución del DAG.",
        ),
        "run_pronabec": Param(
            default=True,
            type="boolean",
            description="Controla la ejecución del job de extracción PRONABEC.",
        ),
        "run_mef": Param(
            default=True,
            type="boolean",
            description="Controla la ejecución del job de extracción MEF.",
        ),
        "run_pronabec_reports_staging": Param(
            default=True,
            type="boolean",
            description="Controla el staging de reportes PRONABEC desde Landing hacia Bronze.",
        ),
        "run_dataflow_pronabec": Param(
            default=True,
            type="boolean",
            description="Controla la transformación PRONABEC Bronze a Silver.",
        ),
        "run_dataflow_mef": Param(
            default=True,
            type="boolean",
            description="Controla la transformación MEF Bronze a Silver.",
        ),
        "run_dataflow_reports": Param(
            default=True,
            type="boolean",
            description="Controla la transformación de reportes PRONABEC Bronze a Silver.",
        ),
        "run_quality": Param(
            default=True,
            type="boolean",
            description="Controla la ejecución del job de calidad.",
        ),
    },
) as dag:
    start = EmptyOperator(task_id="start")

    run_pronabec_extract = BashOperator(
        task_id="run_pronabec_extract",
        bash_command=cloud_run_execute_command(
            job_name=PRONABEC_EXTRACT_JOB,
            enabled_expression=RUN_PRONABEC,
        ),
    )

    run_mef_extract = BashOperator(
        task_id="run_mef_extract",
        bash_command=cloud_run_execute_command(
            job_name=MEF_EXTRACT_JOB,
            enabled_expression=RUN_MEF,
        ),
    )

    stage_pronabec_reports_pes_2025 = BashOperator(
        task_id="stage_pronabec_reports_pes_2025",
        bash_command=cloud_run_execute_command(
            job_name=PRONABEC_REPORTS_STAGE_JOB,
            enabled_expression=RUN_PRONABEC_REPORTS_STAGING,
            extra_env_vars={
                "SOURCE_SUBSET": "pes_2025",
            },
        ),
    )

    stage_pronabec_reports_universitarios = BashOperator(
        task_id="stage_pronabec_reports_universitarios",
        bash_command=cloud_run_execute_command(
            job_name=PRONABEC_REPORTS_STAGE_JOB,
            enabled_expression=RUN_PRONABEC_REPORTS_STAGING,
            extra_env_vars={
                "SOURCE_SUBSET": "beca18_universitarios_2012_2026",
            },
        ),
    )

    # Tareas de transformación Dataflow para PRONABEC API
    pronabec_tasks = []
    for dataset in PRONABEC_SILVER_DATASETS:
        source_ds = dataset["source_dataset"]
        task = BashOperator(
            task_id=f"run_dataflow_pronabec_{source_ds}",
            bash_command=cloud_run_execute_command(
                job_name=dataset["job_name"],
                enabled_expression=RUN_DATAFLOW_PRONABEC,
            ),
        )
        pronabec_tasks.append(task)

    # Tareas de transformación Dataflow para MEF
    mef_tasks = []
    for dataset in MEF_SILVER_DATASETS:
        source_ds = dataset["source_dataset"]
        task = BashOperator(
            task_id=f"run_dataflow_mef_{source_ds}",
            bash_command=cloud_run_execute_command(
                job_name=dataset["job_name"],
                enabled_expression=RUN_DATAFLOW_MEF,
            ),
        )
        mef_tasks.append(task)

    # Tareas de transformación Dataflow para PRONABEC Reports (Job parametrizable)
    report_tasks = []
    for dataset in PRONABEC_REPORT_SILVER_DATASETS:
        subset = REPORT_SUBSETS.get(dataset, "pes_2025")
        input_path = f"gs://{BUCKET_NAME}/bronze/pronabec_reports/{subset}/{dataset}/extraction_date={EXTRACTION_DATE}/data.csv"
        output_table = f"pronabec_{dataset}"

        task = BashOperator(
            task_id=f"run_dataflow_report_{dataset.replace('report_beca18_', '')}",
            bash_command=cloud_run_execute_command(
                job_name=DATAFLOW_PRONABEC_REPORT_JOB,
                enabled_expression=RUN_DATAFLOW_REPORTS,
                extra_env_vars={
                    "SOURCE_SYSTEM": "pronabec_reports",
                    "SOURCE_DATASET": dataset,
                    "INPUT_FORMAT": "csv",
                    "INPUT_PATH": input_path,
                    "OUTPUT_TABLE": f"{PROJECT_ID}.silver.{output_table}",
                },
            ),
        )
        report_tasks.append(task)

    run_quality_checks = BashOperator(
        task_id="run_quality_checks",
        bash_command=cloud_run_execute_command(
            job_name=QUALITY_CHECKS_JOB,
            enabled_expression=RUN_QUALITY,
        ),
        trigger_rule="all_done",
    )

    end = EmptyOperator(task_id="end")

    # Dependencias de extracción y staging
    start >> run_pronabec_extract
    run_pronabec_extract >> run_mef_extract
    run_mef_extract >> stage_pronabec_reports_pes_2025
    stage_pronabec_reports_pes_2025 >> stage_pronabec_reports_universitarios

    # Dependencias de los grupos Dataflow
    run_pronabec_extract >> pronabec_tasks
    run_mef_extract >> mef_tasks
    stage_pronabec_reports_pes_2025 >> report_tasks
    stage_pronabec_reports_universitarios >> report_tasks

    # Dependencias hacia calidad y finalización
    pronabec_tasks + mef_tasks + report_tasks >> run_quality_checks
    run_quality_checks >> end
