# -*- coding: utf-8 -*-
"""Pruebas de calidad y validación estática para el DDL de vistas Gold de BigQuery."""

from __future__ import annotations

import re
from pathlib import Path
import pytest

# Ruta del repositorio y archivos clave
REPO_ROOT = Path(__file__).resolve().parents[1]
GOLD_SQL_PATH = REPO_ROOT / "sql" / "ddl" / "create_gold_views.sql"
SILVER_SCHEMAS_DIR = REPO_ROOT / "config" / "schemas" / "silver"

# Lista de vistas Gold mínimas que deben estar declaradas
REQUIRED_VIEWS = [
    "vw_pronabec_resumen_ejecutivo",
    "vw_beca18_becas_otorgadas_anual",
    "vw_beca18_universitarios_carrera_anual",
    "vw_beca18_universitarios_universidad_anual",
    "vw_beca18_perfil_social_indicadores",
    "vw_beca18_region_postulacion",
    "vw_mef_presupuesto_ejecucion_anual",
    "vw_mef_presupuesto_ejecucion_temporal",
    "vw_pronabec_becas_vs_presupuesto_anual",
]

# Lista de tablas Silver críticas que deben ser consumidas
REQUIRED_SILVER_TABLES = {
    "pronabec_report_beca18_becas_otorgadas_modalidad_anual",
    "pronabec_report_beca18_sexo_anual",
    "pronabec_report_beca18_region_postulacion_anual",
    "pronabec_report_beca18_universitarios_carrera_anual",
    "pronabec_report_beca18_universitarios_universidad_anual",
    "presupuesto_mef",
    "presupuesto_mef_temporal",
    "presupuesto_mef_producto",
    "presupuesto_mef_actividad",
    "presupuesto_mef_generica",
}

# Tablas o referencias obsoletas/viejas que NO deben usarse en Gold
FORBIDDEN_TABLE_REFERENCES = {
    "silver.becarios_provincia",
    "silver.notas_becarios",
    "silver.perdida_becas",
    "silver.convocatorias",
    "becarios_provincia",
    "notas_becarios",
    "perdida_becas",
    "periodos_academicos",
    "concepto_pago",
    "ubigeo_postulacion",
    "convocatorias",
}

# Columnas MEF antiguas/obsoletas que ya no están en Silver y NO deben usarse
FORBIDDEN_MEF_COLUMNS = [
    "girado",
    "certificacion",
    "compromiso_anual",
    "compromiso_mensual",
    "saldo_no_ejecutado",
]

@pytest.fixture
def gold_sql_content() -> str:
    # Retorna el contenido del archivo SQL Gold si existe.
    assert GOLD_SQL_PATH.exists(), f"El archivo SQL Gold no existe en: {GOLD_SQL_PATH}"
    return GOLD_SQL_PATH.read_text(encoding="utf-8")

def test_gold_sql_exists_and_contains_views(gold_sql_content: str) -> None:
    # Valida que el archivo SQL exista y contenga sentencias para crear vistas.
    assert "CREATE OR REPLACE VIEW" in gold_sql_content

def test_gold_sql_uses_placeholders_and_no_real_project_id(gold_sql_content: str) -> None:
    # Valida que no existan IDs de proyecto hardcodeados y se usen placeholders.
    assert "your-gcp-project-id" not in gold_sql_content
    
    # Comprobar que las vistas se declaran con {project_id}.{gold_dataset}
    views_declared = re.findall(r"CREATE OR REPLACE VIEW\s+`([^`]+)`", gold_sql_content, re.IGNORECASE)
    assert views_declared
    for view in views_declared:
        assert view.startswith("{project_id}.{gold_dataset}."), f"La vista {view} no usa placeholders adecuados"

def test_gold_sql_source_dependencies_use_placeholders(gold_sql_content: str) -> None:
    # Valida que las lecturas a Silver usen {project_id}.{silver_dataset}.
    # Buscamos todas las referencias tipo `tabla` en FROM o JOIN
    references = re.findall(r"FROM\s+`([^`]+)`|JOIN\s+`([^`]+)`", gold_sql_content, re.IGNORECASE)
    flat_refs = [ref[0] or ref[1] for ref in references if ref[0] or ref[1]]
    assert flat_refs
    for ref in flat_refs:
        assert ref.startswith("{project_id}.{silver_dataset}."), f"La referencia {ref} no usa placeholders adecuados"

def test_gold_sql_does_not_contain_forbidden_locations(gold_sql_content: str) -> None:
    # Valida que no se lea de Bronze, data/manual, tmp o build/generated.
    # Eliminamos comentarios SQL para evitar falsos positivos
    sql_without_comments = re.sub(r"--.*", "", gold_sql_content)
    lowered = sql_without_comments.lower()
    assert "bronze" not in lowered
    assert "data/manual" not in lowered
    assert "tmp/" not in lowered
    assert "build/generated" not in lowered

def test_gold_sql_does_not_use_select_star(gold_sql_content: str) -> None:
    # Valida que no se use SELECT * en ninguna consulta de Gold.
    assert "select *" not in gold_sql_content.lower()

def test_gold_sql_does_not_contain_mutations(gold_sql_content: str) -> None:
    # Valida que las operaciones sean de solo lectura (CREATE VIEW) y no de modificación.
    lowered = gold_sql_content.lower()
    for verb in ["drop", "delete", "truncate", "merge", "update", "create or replace table"]:
        # Asegurar que si aparece, no sea como palabra clave/comando SQL
        pattern = rf"\b{verb}\b"
        assert not re.search(pattern, lowered), f"Se encontró operación de mutación prohibida: {verb}"

def test_gold_sql_excludes_obsolete_tables_and_columns(gold_sql_content: str) -> None:
    # Valida que no se utilicen tablas obsoletas ni columnas MEF deprecadas.
    lowered = gold_sql_content.lower()
    for ref in FORBIDDEN_TABLE_REFERENCES:
        pattern = rf"\b{ref}\b"
        # Ojo: convocatorias y ubigeo_postulacion son inválidas sin prefijo pronabec_
        if ref in ["convocatorias", "ubigeo_postulacion"]:
            # Validamos que si aparecen, sea precedido de 'pronabec_'
            matches = re.findall(r"(`(?:[a-zA-Z0-9_-]+\.)?([a-zA-Z0-9_-]+)`)", gold_sql_content)
            for full_match, table_name in matches:
                if table_name == ref:
                    pytest.fail(f"Referencia obsoleta sin prefijo detectada: {full_match}")
        else:
            assert not re.search(pattern, lowered), f"Referencia obsoleta detectada: {ref}"

    for col in FORBIDDEN_MEF_COLUMNS:
        pattern = rf"\b{col}\b"
        assert not re.search(pattern, lowered), f"Columna MEF obsoleta detectada: {col}"

def test_gold_sql_references_only_existing_silver_schemas(gold_sql_content: str) -> None:
    # Valida que todas las tablas Silver consumidas tengan un esquema JSON físico.
    references = re.findall(r"FROM\s+`\{project_id\}\.\{silver_dataset\}\.([^`]+)`|JOIN\s+`\{project_id\}\.\{silver_dataset\}\.([^`]+)`", gold_sql_content, re.IGNORECASE)
    flat_refs = {ref[0] or ref[1] for ref in references if ref[0] or ref[1]}
    assert flat_refs
    for table in flat_refs:
        schema_file = SILVER_SCHEMAS_DIR / f"{table}_schema.json"
        assert schema_file.exists(), f"La tabla Silver '{table}' no tiene esquema JSON en: {schema_file}"

def test_gold_sql_declares_all_required_views(gold_sql_content: str) -> None:
    # Valida que se creen todas las vistas Gold mínimas requeridas.
    declared_views = {
        view.split(".")[-1]
        for view in re.findall(r"CREATE OR REPLACE VIEW\s+`\{project_id\}\.\{gold_dataset\}\.([^`]+)`", gold_sql_content, re.IGNORECASE)
    }
    for req in REQUIRED_VIEWS:
        assert req in declared_views, f"La vista mínima '{req}' no está declarada en el SQL Gold"

def test_gold_sql_includes_required_silver_tables(gold_sql_content: str) -> None:
    # Valida que el SQL contenga referencias a las tablas Silver requeridas.
    for table in REQUIRED_SILVER_TABLES:
        assert table in gold_sql_content, f"La tabla Silver requerida '{table}' no es consumida en el SQL Gold"

def test_gold_sql_presupuesto_mef_hierarchy_not_summed(gold_sql_content: str) -> None:
    # Valida que no se aplique SUM() sobre la tabla de jerarquía presupuesto_mef_hierarchy.
    # Encontramos la sección de la query que hace referencia a presupuesto_mef_hierarchy
    # Dividimos las queries
    queries = gold_sql_content.split(";")
    for query in queries:
        if "presupuesto_mef_hierarchy" in query:
            # Comprobar que no haya una agregación SUM sobre esta query
            # El uso de presupuesto_mef_hierarchy debe ser solo de lectura/contexto, no para fact sumable
            assert "sum(" not in query.lower(), "Se detectó agregación SUM() prohibida sobre la tabla presupuesto_mef_hierarchy"
