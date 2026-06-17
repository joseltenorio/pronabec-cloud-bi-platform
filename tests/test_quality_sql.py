"""
Pruebas unitarias para validar las consultas de calidad de datos en SQL.
Garantiza que las consultas sigan las reglas de diseño y estructura del repositorio.
"""

import re
from pathlib import Path

# Obtener la ruta raíz del proyecto
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SQL_FILE_PATH = PROJECT_ROOT / "sql" / "quality" / "data_quality_checks.sql"
SILVER_SCHEMAS_DIR = PROJECT_ROOT / "config" / "schemas" / "silver"


def get_queries() -> list[str]:
    """
    Lee el archivo SQL de calidad de datos y devuelve la lista de consultas
    divididas por punto y coma (;), filtrando comentarios y segmentos vacíos.
    """
    if not SQL_FILE_PATH.exists():
        return []
    
    content = SQL_FILE_PATH.read_text(encoding="utf-8")
    raw_queries = content.split(";")
    
    queries = []
    for q in raw_queries:
        trimmed = q.strip()
        # Quitar líneas que son comentarios para validar si queda SQL ejecutable
        sql_lines = [line.strip() for line in trimmed.splitlines() if not line.strip().startswith("--")]
        sql_content = "\n".join(sql_lines).strip()
        
        if sql_content and any(c.isalnum() for c in sql_content):
            queries.append(trimmed)
            
    return queries


def test_sql_file_exists_and_is_not_empty():
    """Valida que el archivo de calidad de datos exista y no esté vacío."""
    assert SQL_FILE_PATH.exists(), f"El archivo {SQL_FILE_PATH} no existe."
    queries = get_queries()
    assert len(queries) > 0, "El archivo SQL no contiene consultas ejecutables."


def test_sql_contains_required_placeholders():
    """Valida que el archivo SQL contenga los placeholders obligatorios {project_id} y {silver_dataset}."""
    content = SQL_FILE_PATH.read_text(encoding="utf-8")
    assert "{project_id}" in content, "Falta el placeholder {project_id} en el archivo SQL."
    assert "{silver_dataset}" in content, "Falta el placeholder {silver_dataset} en el archivo SQL."


def test_sql_does_not_contain_hardcoded_project_ids():
    """Valida que el archivo SQL no contenga nombres de proyectos reales quemados (hardcoded)."""
    content = SQL_FILE_PATH.read_text(encoding="utf-8")
    
    # Rechazar patrones sospechosos de nombres de proyectos reales de GCP
    forbidden_patterns = [
        r"my-real-project",
        r"pronabec-cloud-bi-platform-[a-zA-Z0-9_-]+",
    ]
    for pattern in forbidden_patterns:
        assert not re.search(pattern, content, re.IGNORECASE), f"Se encontró un patrón prohibido en el SQL: {pattern}"

    # Adicionalmente, validar que no se referencien datasets en backticks con proyecto literal que no sean placeholders
    # Ejemplo: `proyecto-real.silver.tabla` en lugar de `{project_id}.{silver_dataset}.tabla`
    backtick_references = re.findall(r"`([^`]+)`", content)
    for ref in backtick_references:
        # Si contiene puntos, debe comenzar con {project_id}
        if "." in ref:
            assert ref.startswith("{project_id}."), f"La referencia a tabla '{ref}' está hardcodeada. Debe usar '{{project_id}}'"


def test_homogeneous_query_shape_and_metadata():
    """Valida que cada consulta devuelva la estructura homogénea requerida (check_id, layer, table_name, severity, failed_rows, passed, details)."""
    queries = get_queries()
    allowed_severities = {"ERROR", "WARNING", "INFO"}
    
    for i, query in enumerate(queries):
        # Limpiar comentarios del inicio y saltos de línea para facilitar el parseo
        clean_query = "\n".join(line for line in query.splitlines() if not line.strip().startswith("--"))
        
        # Buscar check_id
        check_id_match = re.search(r"'\s*([a-zA-Z0-9_-]+)\s*'\s+AS\s+check_id", clean_query, re.IGNORECASE)
        # Buscar layer
        layer_match = re.search(r"'\s*([a-zA-Z0-9_-]+)\s*'\s+AS\s+layer", clean_query, re.IGNORECASE)
        # Buscar table_name
        table_name_match = re.search(r"'\s*([a-zA-Z0-9_-]+)\s*'\s+AS\s+table_name", clean_query, re.IGNORECASE)
        # Buscar severity
        severity_match = re.search(r"'\s*([a-zA-Z0-9_-]+)\s*'\s+AS\s+severity", clean_query, re.IGNORECASE)
        
        # Validar presencia de campos requeridos
        assert check_id_match is not None, f"La consulta {i} no define 'check_id' con la nomenclatura esperada."
        check_id = check_id_match.group(1)
        
        assert layer_match is not None, f"El check '{check_id}' no define 'layer'."
        assert table_name_match is not None, f"El check '{check_id}' no define 'table_name'."
        assert severity_match is not None, f"El check '{check_id}' no define 'severity'."
        
        # Validar severidades permitidas
        severity = severity_match.group(1)
        assert severity in allowed_severities, f"El check '{check_id}' tiene una severidad no permitida: {severity}. Permitidas: {allowed_severities}"
        
        # Validar presencia de alias para columnas de salida obligatorias
        assert re.search(r"AS\s+failed_rows", clean_query, re.IGNORECASE) is not None, f"El check '{check_id}' no define la columna 'failed_rows'."
        assert re.search(r"AS\s+passed", clean_query, re.IGNORECASE) is not None, f"El check '{check_id}' no define la columna 'passed'."
        assert re.search(r"AS\s+details", clean_query, re.IGNORECASE) is not None, f"El check '{check_id}' no define la columna 'details'."


def test_checks_reference_existing_silver_tables():
    """Valida que los checks de la capa silver hagan referencia a tablas que tienen un esquema definido en config/schemas/silver."""
    # Leer tablas silver existentes basadas en los archivos de esquema JSON
    silver_tables = {p.name.replace("_schema.json", "") for p in SILVER_SCHEMAS_DIR.glob("*.json")}
    
    queries = get_queries()
    for query in queries:
        clean_query = "\n".join(line for line in query.splitlines() if not line.strip().startswith("--"))
        
        # Buscar check_id y table_name
        check_id_match = re.search(r"'\s*([a-zA-Z0-9_-]+)\s*'\s+AS\s+check_id", clean_query, re.IGNORECASE)
        table_name_match = re.search(r"'\s*([a-zA-Z0-9_-]+)\s*'\s+AS\s+table_name", clean_query, re.IGNORECASE)
        layer_match = re.search(r"'\s*([a-zA-Z0-9_-]+)\s*'\s+AS\s+layer", clean_query, re.IGNORECASE)
        
        if check_id_match and table_name_match and layer_match:
            check_id = check_id_match.group(1)
            table_name = table_name_match.group(1)
            layer = layer_match.group(1)
            
            # Solo validamos contra esquemas Silver si el check es de la capa Silver
            if layer == "silver":
                assert table_name in silver_tables, f"El check '{check_id}' hace referencia a una tabla inexistente en Silver: '{table_name}'"


def test_required_tables_are_covered():
    """Valida que existan reglas de calidad específicas para las tablas mandatorias del bloque."""
    queries = get_queries()
    covered_tables = set()
    
    for query in queries:
        clean_query = "\n".join(line for line in query.splitlines() if not line.strip().startswith("--"))
        table_name_match = re.search(r"'\s*([a-zA-Z0-9_-]+)\s*'\s+AS\s+table_name", clean_query, re.IGNORECASE)
        if table_name_match:
            covered_tables.add(table_name_match.group(1))
            
    required_tables = {
        "pronabec_convocatorias",
        "pronabec_becarios_pais_estudio",
        "pronabec_report_beca18_universitarios_carrera_anual",
        "pronabec_report_beca18_universitarios_universidad_anual",
        "presupuesto_mef",
        "presupuesto_mef_temporal",
        "presupuesto_mef_hierarchy"
    }
    
    for t in required_tables:
        assert t in covered_tables, f"La tabla requerida '{t}' no tiene cobertura de calidad de datos en el archivo SQL."


def test_pes_2025_coverage():
    """Valida que la familia de reportes PRONABEC (PES 2025) tenga al menos un check de calidad implementado si el esquema existe."""
    queries = get_queries()
    covered_tables = set()
    
    for query in queries:
        clean_query = "\n".join(line for line in query.splitlines() if not line.strip().startswith("--"))
        table_name_match = re.search(r"'\s*([a-zA-Z0-9_-]+)\s*'\s+AS\s+table_name", clean_query, re.IGNORECASE)
        if table_name_match:
            covered_tables.add(table_name_match.group(1))
            
    # Verificar si hay alguna tabla del reporte PES 2025
    pes_tables = [t for t in covered_tables if t.startswith("pronabec_report_beca18_") and "universitarios" not in t]
    assert len(pes_tables) > 0, "No se encontró ningún check de calidad para reportes de la familia PES 2025."


def test_no_destructive_operations_or_select_all():
    """Valida que no se utilicen operaciones destructivas ni SELECT * en las consultas de calidad."""
    queries = get_queries()
    
    forbidden_keywords = ["drop", "delete", "truncate", "merge", "update", "create or replace table"]
    
    for i, query in enumerate(queries):
        clean_query = "\n".join(line for line in query.splitlines() if not line.strip().startswith("--")).lower()
        
        # Validar SELECT *
        assert not re.search(r"\bselect\s+(\w+\s+)?\*", clean_query), f"La consulta {i} utiliza 'SELECT *' lo cual no está permitido."
        
        # Validar operaciones destructivas
        for keyword in forbidden_keywords:
            pattern = rf"\b{keyword}\b"
            assert not re.search(pattern, clean_query), f"La consulta {i} contiene la operación no permitida: '{keyword}'"


def test_no_mef_hierarchy_sum():
    """Valida que no se intente sumar jerarquías MEF en los checks de calidad."""
    queries = get_queries()
    
    for query in queries:
        clean_query = "\n".join(line for line in query.splitlines() if not line.strip().startswith("--"))
        table_name_match = re.search(r"'\s*([a-zA-Z0-9_-]+)\s*'\s+AS\s+table_name", clean_query, re.IGNORECASE)
        if table_name_match and table_name_match.group(1) == "presupuesto_mef_hierarchy":
            # Verificar que no contenga SUM
            assert "SUM(" not in clean_query.upper(), "El check sobre presupuesto_mef_hierarchy realiza una suma (SUM), lo cual está prohibido por diseño jerárquico."


def test_no_global_negative_mef_restriction():
    """Valida que no se prohíban números negativos globalmente en las tablas temporales de MEF sin justificación."""
    queries = get_queries()
    
    for i, query in enumerate(queries):
        clean_query = "\n".join(line for line in query.splitlines() if not line.strip().startswith("--"))
        table_name_match = re.search(r"'\s*([a-zA-Z0-9_-]+)\s*'\s+AS\s+table_name", clean_query, re.IGNORECASE)
        
        if table_name_match:
            table_name = table_name_match.group(1)
            # Si es una tabla temporal de MEF
            if table_name.startswith("presupuesto_mef") and table_name.endswith("temporal"):
                # Verificar que no haya un filtro general ciego como "devengado < 0" o "pia < 0"
                devengado_neg_check = re.search(r"\bdevengado\s*<\s*0", clean_query, re.IGNORECASE)
                assert not devengado_neg_check, f"La consulta {i} para la tabla temporal '{table_name}' prohíbe devengado negativo globalmente, lo cual no es correcto para ajustes MEF."
