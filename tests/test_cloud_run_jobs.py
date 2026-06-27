# -*- coding: utf-8 -*-
"""Pruebas unitarias para validar los jobs de Cloud Run registrados."""

from pathlib import Path
import re

REPO_ROOT = Path(__file__).resolve().parents[1]
DEPLOY_SCRIPT_PATH = REPO_ROOT / "scripts" / "deploy_cloud_run_jobs.ps1"


def _read_deploy_script() -> str:
    return DEPLOY_SCRIPT_PATH.read_text(encoding="utf-8")


def test_deploy_script_exists():
    assert DEPLOY_SCRIPT_PATH.exists(), f"El script {DEPLOY_SCRIPT_PATH} no existe"


def test_selected_pronabec_api_jobs_are_defined():
    content = _read_deploy_script()

    required_jobs = [
        "pronabec-extract-job",
        "pronabec-stage-reports-job",
        "dataflow-pronabec-convocatorias-job",
        "dataflow-pronabec-ubigeo-postulacion-job",
        "dataflow-pronabec-becarios-pais-estudio-job",
        "dataflow-pronabec-colegios-habiles-job",
        "dataflow-pronabec-becarios-provincia-job",
    ]

    for job_name in required_jobs:
        assert job_name in content, f"Falta el job PRONABEC esperado: {job_name}"


def test_selected_mef_jobs_are_defined():
    content = _read_deploy_script()

    required_jobs = [
        "mef-extract-job",
        "dataflow-mef-presupuesto-job",
        "dataflow-mef-presupuesto-temporal-job",
        "dataflow-mef-producto-job",
        "dataflow-mef-producto-temporal-job",
        "dataflow-mef-actividad-job",
        "dataflow-mef-actividad-temporal-job",
        "dataflow-mef-generica-job",
        "dataflow-mef-generica-temporal-job",
        "dataflow-mef-hierarchy-job",
    ]

    for job_name in required_jobs:
        assert job_name in content, f"Falta el job MEF esperado: {job_name}"


def test_parameterized_pronabec_report_job_is_the_only_report_job():
    content = _read_deploy_script()

    assert "dataflow-pronabec-report-job" in content
    assert "DataflowPronabecReportJobName" in content
    assert "SOURCE_DATASET=placeholder_dataset" in content
    assert "INPUT_PATH=gs://" in content
    assert "OUTPUT_TABLE=" in content

    forbidden_report_references = [
        "dataflow-report-universitarios-job",
        "DataflowReportUniversitariosJobName",
    ]

    for forbidden_reference in forbidden_report_references:
        assert forbidden_reference not in content, (
            "No debe existir un job dedicado para reportes universitarios. "
            "Los 23 reportes deben usar dataflow-pronabec-report-job."
        )


def test_bronze_only_jobs_are_not_defined():
    content = _read_deploy_script()

    forbidden_patterns = [
        r"dataflow-pronabec-convocatorias-carrera-sede-job",
        r"dataflow-mef-departamento-job",
        r"dataflow-mef-fuente-job",
        r"dataflow-mef-rubro-job",
        r"presupuesto_departamento",
        r"presupuesto_fuente",
        r"presupuesto_rubro",
        r"convocatorias_carrera_sede",
    ]

    for pattern in forbidden_patterns:
        assert re.search(pattern, content) is None, (
            f"Se detectó una referencia operativa prohibida en Cloud Run: {pattern}"
        )


def test_wrong_cloud_run_job_names_are_not_documented_in_script():
    content = _read_deploy_script()

    wrong_job_names = [
        "dataflow-pronabec-colegios-elegibles-job",
        "dataflow-mef-presupuesto-producto-job",
        "dataflow-mef-presupuesto-producto-temporal-job",
        "dataflow-mef-presupuesto-actividad-job",
        "dataflow-mef-presupuesto-actividad-temporal-job",
        "dataflow-mef-presupuesto-generica-job",
        "dataflow-mef-presupuesto-generica-temporal-job",
    ]

    for job_name in wrong_job_names:
        assert job_name not in content, f"Nombre de job incorrecto detectado: {job_name}"