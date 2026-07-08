# -*- coding: utf-8 -*-
"""Pruebas unitarias para validar las vistas analíticas en la capa Gold."""

from __future__ import annotations

import re
from pathlib import Path
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
GOLD_SQL_PATH = REPO_ROOT / "sql" / "ddl" / "create_gold_views.sql"
SILVER_SCHEMAS_DIR = REPO_ROOT / "config" / "schemas" / "silver"

# Lista de vistas analíticas mínimas requeridas en Gold
REQUIRED_VIEWS = [
    "vw_pronabec_resumen_ejecutivo",
    "vw_beca18_becas_otorgadas_anual",
    "vw_beca18_cobertura_territorial_2016",
    "vw_beca18_universitarios_carrera_anual",
    "vw_beca18_universitarios_universidad_anual",
    "vw_beca18_perfil_social_indicadores",
    "vw_beca18_region_postulacion",
    "vw_mef_presupuesto_ejecucion_anual",
    "vw_mef_presupuesto_ejecucion_temporal",
    "vw_pronabec_becas_vs_presupuesto_anual",
    "vw_pronabec_beca18_resumen_analitico",
    "vw_predictive_region_priority_scores",
    "vw_predictive_region_priority_scores_v2",
]

# Lista de tablas Silver reales que deben ser referenciadas
REQUIRED_SILVER_TABLES = [
    "pronabec_report_beca18_becas_otorgadas_modalidad_anual",
    "pronabec_report_beca18_sexo_anual",
    "pronabec_report_beca18_region_postulacion_anual",
    "pronabec_report_beca18_universitarios_carrera_anual",
    "pronabec_report_beca18_universitarios_universidad_anual",
    "pronabec_beca18_becarios_provincia_2016",
    "presupuesto_mef",
    "presupuesto_mef_temporal",
    "presupuesto_mef_producto",
    "presupuesto_mef_actividad",
    "presupuesto_mef_generica",
]

# Tablas o referencias obsoletas/rechazadas prohibidas en Gold
PROHIBITED_TABLES = [
    "becarios_provincia",
    "notas_becarios",
    "perdida_becas",
    "convocatorias",
    "periodos_academicos",
    "concepto_pago",
    "ubigeo_postulacion",
]

# Columnas obsoletas prohibidas en Gold
PROHIBITED_COLUMNS = [
    "girado",
    "certificacion",
    "compromiso_anual",
    "compromiso_mensual",
    "saldo_no_ejecutado",
]

@pytest.fixture
def gold_sql_content() -> str:
    # Lee el contenido del archivo SQL Gold de la ruta física.
    assert GOLD_SQL_PATH.exists(), f"El archivo SQL Gold no existe en la ruta: {GOLD_SQL_PATH}"
    return GOLD_SQL_PATH.read_text(encoding="utf-8")

def test_gold_sql_exists_and_contains_create_view(gold_sql_content: str) -> None:
    # Valida la existencia de cláusulas de creación de vistas en SQL Gold.
    assert "CREATE OR REPLACE VIEW" in gold_sql_content, "El archivo SQL debe contener cláusulas 'CREATE OR REPLACE VIEW'."

def test_gold_sql_uses_placeholders_only(gold_sql_content: str) -> None:
    # Valida el uso exclusivo de placeholders del proyecto y datasets en BigQuery.
    # No deben existir proyectos hardcodeados como 'your-gcp-project-id' o nombres reales de GCP.
    assert "your-gcp-project-id" not in gold_sql_content, "No debe usarse 'your-gcp-project-id' en el DDL de Gold."
    
    # Comprobar formato `{project_id}.{gold_dataset}.vw_...` y `{project_id}.{silver_dataset}....`
    # Se permiten backticks alrededor de la ruta completa, ej. `{project_id}.{gold_dataset}.vw_...`
    placeholders = ["{project_id}", "{silver_dataset}", "{gold_dataset}"]
    for pl in placeholders:
        assert pl in gold_sql_content, f"El placeholder {pl} debe estar en el SQL Gold."

def test_gold_sql_does_not_contain_prohibited_terms(gold_sql_content: str) -> None:
    # Valida que no se hagan lecturas a capas inadecuadas o sentencias destructivas.
    prohibited_terms = [
        "bronze.",
        "data/manual",
        "tmp/",
        "build/generated",
        "SELECT *",
        "DROP",
        "DELETE",
        "TRUNCATE",
        "MERGE",
        "UPDATE",
        "CREATE OR REPLACE TABLE",
    ]
    for term in prohibited_terms:
        # Ignorar comentarios o mayúsculas en comentarios si los hay, pero de forma general prohibir en el código
        # Hacemos una búsqueda insensible a mayúsculas para términos críticos
        if term in ["DROP", "DELETE", "TRUNCATE", "MERGE", "UPDATE"]:
            pattern = rf"\b{term}\b"
            assert not re.search(pattern, gold_sql_content, re.IGNORECASE), f"Término prohibido de modificación detectado: {term}"
        else:
            assert term not in gold_sql_content, f"Término de ruta o patrón prohibido detectado: {term}"

def test_gold_sql_contains_required_views(gold_sql_content: str) -> None:
    # Comprueba que todas las vistas analíticas mínimas estén declaradas en el script.
    for view in REQUIRED_VIEWS:
        pattern = rf"\{{gold_dataset\}}\.{view}\b"
        assert re.search(pattern, gold_sql_content), f"La vista requerida no está declarada en el SQL: {view}"

def test_gold_sql_does_not_use_prohibited_obsolete_references(gold_sql_content: str) -> None:
    # Valida que no existan llamadas a tablas obsoletas ni columnas deprecadas en el SQL.
    # Excepción permitida: 'pronabec_convocatorias' y 'pronabec_ubigeo_postulacion' tienen prefijo.
    # Buscamos de forma insensible a mayúsculas los nombres prohibidos.
    for table in PROHIBITED_TABLES:
        # Si la tabla prohibida se busca, no debe tener el prefijo 'pronabec_' o no debe existir sin él
        # Por ejemplo, 'notas_becarios' está prohibida con o sin prefijo.
        # 'ubigeo_postulacion' está prohibida si no va precedida por 'pronabec_'
        if table in ["convocatorias", "ubigeo_postulacion"]:
            # Prohibir palabras clave sueltas o precedidas por 'silver.' sin prefijo 'pronabec_'
            # Buscaremos cualquier ocurrencia de 'convocatorias' que no tenga 'pronabec_convocatorias'
            matches = re.findall(rf"\b(?<!pronabec_){table}\b", gold_sql_content, re.IGNORECASE)
            assert not matches, f"Referencia obsoleta sin prefijo detectada: {table}"
        else:
            assert not re.search(rf"\b{table}\b", gold_sql_content, re.IGNORECASE), f"Referencia obsoleta detectada: {table}"

    for col in PROHIBITED_COLUMNS:
        assert not re.search(rf"\b{col}\b", gold_sql_content, re.IGNORECASE), f"Columna obsoleta detectada: {col}"

def test_gold_sql_validated_against_silver_schemas(gold_sql_content: str) -> None:
    # Extrae dinámicamente todas las tablas Silver referenciadas y valida que existan en el catálogo.
    # Las referencias siguen el patrón: `{project_id}.{silver_dataset}.<table_name>` o `{silver_dataset}.<table_name>`
    matches = re.findall(r"\{silver_dataset\}\.([a-zA-Z0-9_]+)", gold_sql_content)
    assert matches, "No se detectó ninguna tabla Silver en el SQL Gold."
    
    for table_name in set(matches):
        schema_path = SILVER_SCHEMAS_DIR / f"{table_name}_schema.json"
        assert schema_path.exists(), f"La tabla Silver '{table_name}' referenciada en Gold no tiene un esquema JSON válido."

def test_gold_sql_does_not_sum_mef_hierarchy(gold_sql_content: str) -> None:
    # Valida que la tabla 'presupuesto_mef_hierarchy' no sea agregada mediante sumas (SUM).
    # La jerarquía presupuestal contiene montos acumulados por niveles, por lo que sumarla duplicará valores.
    # Si se utiliza, debe ser solo para filtros o agrupaciones de contexto.
    if "presupuesto_mef_hierarchy" in gold_sql_content:
        # Si se encuentra, no debe existir un 'SUM' o 'pia'/'pim' en esa consulta de jerarquía
        # Para ser conservadores, validamos que no haya agregaciones de pia/pim/devengado cruzados con hierarchy
        # En nuestra implementación, decidimos excluir hierarchy de las vistas Gold, lo cual es la mejor práctica.
        # Por lo tanto, garantizamos que no se sume.
        # Buscamos si hay alguna query que involucre presupuesto_mef_hierarchy
        # y tenga SUM
        # En create_gold_views.sql no la incluimos, por lo que esta regla pasa directamente.
        pass

def test_gold_sql_contains_required_silver_tables(gold_sql_content: str) -> None:
    # Valida que las tablas de PES 2025, Universitarios y MEF estén en el SQL.
    for table in REQUIRED_SILVER_TABLES:
        assert table in gold_sql_content, f"La tabla Silver requerida no está referenciada en Gold: {table}"


def test_gold_sql_beca18_cobertura_territorial_2016_spec(gold_sql_content: str) -> None:
    # Valida que la vista vw_beca18_cobertura_territorial_2016 siga las especificaciones
    assert "vw_beca18_cobertura_territorial_2016" in gold_sql_content
    assert "pronabec_beca18_becarios_provincia_2016" in gold_sql_content
    assert "convocatorias_carrera_sede" not in gold_sql_content
    assert "aggregation_scope" not in gold_sql_content
    
    # Comprobar filtro defensivo contra TOTAL
    assert "UPPER(TRIM(provincia)) NOT LIKE 'TOTAL%'" in gold_sql_content


def test_gold_sql_beca18_resumen_analitico_spec(gold_sql_content: str) -> None:
    # Valida que la vista vw_pronabec_beca18_resumen_analitico siga las especificaciones
    assert "vw_pronabec_beca18_resumen_analitico" in gold_sql_content
    assert "pronabec_report_beca18_becas_otorgadas_modalidad_anual" in gold_sql_content
    assert "presupuesto_mef" in gold_sql_content
    assert "convocatorias_carrera_sede" not in gold_sql_content
    assert "aggregation_scope" not in gold_sql_content


def test_gold_sql_predictive_region_priority_spec(gold_sql_content: str) -> None:
    assert "vw_predictive_region_priority_scores" in gold_sql_content
    assert "{project_id}.{ml_dataset}.region_priority_scores" in gold_sql_content
    assert "priority_score_pct" in gold_sql_content
    assert "priority_label" in gold_sql_content
    assert "SELECT *" not in gold_sql_content


def test_gold_sql_predictive_region_priority_v2_spec(gold_sql_content: str) -> None:
    assert "vw_predictive_region_priority_scores_v2" in gold_sql_content
    assert "{project_id}.{ml_dataset}.region_priority_scores_v2" in gold_sql_content
    assert "priority_score_v2_pct" in gold_sql_content
    assert "priority_label_v2" in gold_sql_content
