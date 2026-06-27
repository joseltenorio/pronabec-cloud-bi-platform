# -*- coding: utf-8 -*-
"""Pruebas unitarias para validar la configuración del DAG de Composer."""

import sys
import pytest
from unittest.mock import MagicMock

# Mock Airflow modules to allow importing the DAG without airflow installed
sys.modules['airflow'] = MagicMock()
sys.modules['airflow.models'] = MagicMock()
sys.modules['airflow.models.param'] = MagicMock()
sys.modules['airflow.operators'] = MagicMock()
sys.modules['airflow.operators.bash'] = MagicMock()
sys.modules['airflow.operators.empty'] = MagicMock()

import dags.pronabec_medallion_batch_dag as dag_mod


def test_pronabec_selected_silver_datasets():
    # Valida que los datasets PRONABEC Silver seleccionados estén en el DAG
    datasets = [d["source_dataset"] for d in dag_mod.PRONABEC_SILVER_DATASETS]
    expected = {"convocatorias", "ubigeo_postulacion", "becarios_pais_estudio", "colegios_habiles", "becarios_provincia"}
    
    for item in expected:
        assert item in datasets, f"Falta el dataset PRONABEC seleccionado: {item}"
        
    assert "convocatorias_carrera_sede" not in datasets, "convocatorias_carrera_sede no debe estar en Silver"


def test_mef_selected_silver_datasets():
    # Valida que los datasets MEF Silver seleccionados estén en el DAG
    datasets = [d["source_dataset"] for d in dag_mod.MEF_SILVER_DATASETS]
    expected = {
        "presupuesto",
        "presupuesto_temporal",
        "presupuesto_producto",
        "presupuesto_producto_temporal",
        "presupuesto_actividad",
        "presupuesto_actividad_temporal",
        "presupuesto_generica",
        "presupuesto_generica_temporal",
        "presupuesto_hierarchy"
    }
    
    for item in expected:
        assert item in datasets, f"Falta el dataset MEF seleccionado: {item}"
        
    forbidden = {"presupuesto_departamento", "presupuesto_fuente", "presupuesto_rubro"}
    for item in forbidden:
        assert item not in datasets, f"El dataset Bronze-only MEF '{item}' no debe estar en Silver"


def test_reports_count_and_job():
    # Valida que la lista de reportes documentales contenga exactamente 23 elementos
    assert len(dag_mod.PRONABEC_REPORT_SILVER_DATASETS) == 23, "Debería haber exactamente 23 reportes"
    
    # Valida que se use el job de reportes parametrizable único
    # Buscamos en el código del archivo DAG o en variables si existe
    # El archivo del DAG declara el job name para reportes en su plantilla
    with open(dag_mod.__file__, "r", encoding="utf-8") as f:
        content = f.read()
    assert "dataflow-pronabec-report-job" in content, "Falta el job parametrizable único de reportes"
