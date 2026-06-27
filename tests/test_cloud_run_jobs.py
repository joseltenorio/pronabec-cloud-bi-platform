# -*- coding: utf-8 -*-
"""Pruebas unitarias para validar los jobs de Cloud Run registrados."""

from pathlib import Path
import re

REPO_ROOT = Path(__file__).resolve().parents[1]
DEPLOY_SCRIPT_PATH = REPO_ROOT / "scripts" / "deploy_cloud_run_jobs.ps1"


def test_deploy_script_exists():
    assert DEPLOY_SCRIPT_PATH.exists(), f"El script {DEPLOY_SCRIPT_PATH} no existe"


def test_deploy_script_job_definitions():
    # Leer el script de despliegue
    content = DEPLOY_SCRIPT_PATH.read_text(encoding="utf-8")
    
    # 1. Validar que contenga los jobs requeridos para selected silver
    required_job_names = [
        "dataflow-pronabec-becarios-provincia-job",
        "dataflow-mef-hierarchy-job",
        "dataflow-pronabec-report-job"
    ]
    for job in required_job_names:
        assert job in content, f"El job '{job}' debe estar definido en el script de despliegue"

    # 2. Validar que no despliegue jobs para los datasets Bronze-only
    forbidden_jobs = [
        "convocatorias-carrera-sede",
        "convocatorias_carrera_sede",
        "presupuesto-departamento",
        "presupuesto_departamento",
        "presupuesto-fuente",
        "presupuesto_fuente",
        "presupuesto-rubro",
        "presupuesto_rubro"
    ]
    
    # Buscamos de forma precisa si se está definiendo o registrando un job de Cloud Run con estos nombres
    # Un job de Cloud Run se declara típicamente en los parámetros o se crea/actualiza con gcloud
    for job in forbidden_jobs:
        # Excluir comentarios que digan "es Bronze-only"
        # Buscamos patrones de registro de job o variables asignando ese nombre de job.
        # Por ejemplo: -JobName "..." o $Dataflow...JobName = "..."
        # Si buscamos la cadena exacta como asignación de job o en comando gcloud:
        # Ejemplo: "dataflow-...-job"
        match = re.search(rf'[\"\']dataflow-{job.replace("_", "-")}-job[\"\']', content)
        assert not match, f"Se detectó un job prohibido en el script: {job}"
        
        # También validar si se asigna a una variable o parámetro
        var_match = re.search(rf'\${job}JobName\b', content, re.IGNORECASE)
        assert not var_match, f"Se detectó una variable de job prohibida en el script: {job}"
