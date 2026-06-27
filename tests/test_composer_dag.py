# -*- coding: utf-8 -*-
"""Pruebas unitarias para validar la configuración del DAG de Composer."""

import sys
from pathlib import Path
from unittest.mock import MagicMock

# Mock de Airflow para importar el DAG sin instalar Airflow localmente.
sys.modules["airflow"] = MagicMock()
sys.modules["airflow.models"] = MagicMock()
sys.modules["airflow.models.param"] = MagicMock()
sys.modules["airflow.operators"] = MagicMock()
sys.modules["airflow.operators.bash"] = MagicMock()
sys.modules["airflow.operators.empty"] = MagicMock()

import dags.pronabec_medallion_batch_dag as dag_mod  # noqa: E402


def _read_dag_source() -> str:
    return Path(dag_mod.__file__).read_text(encoding="utf-8")


def test_pronabec_selected_silver_datasets():
    datasets = [dataset["source_dataset"] for dataset in dag_mod.PRONABEC_SILVER_DATASETS]

    expected = {
        "convocatorias",
        "ubigeo_postulacion",
        "becarios_pais_estudio",
        "colegios_habiles",
        "becarios_provincia",
    }

    assert set(datasets) == expected
    assert "convocatorias_carrera_sede" not in datasets


def test_mef_selected_silver_datasets():
    datasets = [dataset["source_dataset"] for dataset in dag_mod.MEF_SILVER_DATASETS]

    expected = {
        "presupuesto",
        "presupuesto_temporal",
        "presupuesto_producto",
        "presupuesto_producto_temporal",
        "presupuesto_actividad",
        "presupuesto_actividad_temporal",
        "presupuesto_generica",
        "presupuesto_generica_temporal",
        "presupuesto_hierarchy",
    }

    assert set(datasets) == expected

    forbidden = {
        "presupuesto_departamento",
        "presupuesto_fuente",
        "presupuesto_rubro",
    }

    for dataset in forbidden:
        assert dataset not in datasets


def test_reports_count_and_parameterized_job():
    assert len(dag_mod.PRONABEC_REPORT_SILVER_DATASETS) == 23

    content = _read_dag_source()

    assert "dataflow-pronabec-report-job" in content
    assert "dataflow-report-universitarios-job" not in content
    assert "SOURCE_DATASET" in content
    assert "INPUT_PATH" in content
    assert "OUTPUT_TABLE" in content


def test_dag_does_not_reference_bronze_only_datasets():
    content = _read_dag_source()

    forbidden_references = [
        "convocatorias_carrera_sede",
        "presupuesto_departamento",
        "presupuesto_fuente",
        "presupuesto_rubro",
    ]

    for reference in forbidden_references:
        assert reference not in content, f"Referencia Bronze-only no permitida en DAG: {reference}"


def test_dag_schedule_is_weekly_without_catchup():
    content = _read_dag_source()

    assert 'schedule_interval="0 5 * * 6"' in content or 'schedule="0 5 * * 6"' in content
    assert "catchup=False" in content