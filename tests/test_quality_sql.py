"""
Pruebas unitarias para validar las consultas de calidad de datos en SQL.
Garantiza que las consultas sigan las reglas de diseño y estructura del repositorio.
"""

import json
import re
from pathlib import Path

from pipelines.quality_checks import split_sql_queries

# Obtener la ruta raíz del proyecto
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SQL_FILE_PATH = PROJECT_ROOT / "sql" / "quality" / "data_quality_checks.sql"
SILVER_SCHEMAS_DIR = PROJECT_ROOT / "config" / "schemas" / "silver"
GOLD_DDL_PATH = PROJECT_ROOT / "sql" / "ddl" / "create_gold_views.sql"
EXPECTED_QUALITY_CHECKS = 62
QUALITY_OUTPUT_COLUMNS = {
    "check_id",
    "layer",
    "table_name",
    "severity",
    "failed_rows",
    "passed",
    "details",
}
SQL_FUNCTIONS_AND_KEYWORDS = {
    "AND",
    "AS",
    "BETWEEN",
    "BY",
    "CAST",
    "CONCAT",
    "COUNT",
    "COUNTIF",
    "DATE",
    "ELSE",
    "FALSE",
    "FROM",
    "GROUP",
    "HAVING",
    "IF",
    "IN",
    "IS",
    "LENGTH",
    "LIKE",
    "NOT",
    "NULL",
    "OR",
    "ORDER",
    "SELECT",
    "STRING",
    "THEN",
    "TRIM",
    "TRUE",
    "UPPER",
    "WHEN",
    "WHERE",
}
QUALITY_LOCAL_ALIASES = {
    "cnt",
    "dup_cnt",
    "failed_cnt",
    "total_cnt",
}


def _legacy_get_queries() -> list[str]:
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


def get_queries() -> list[str]:
    """Lee el SQL real usando el mismo splitter que ejecuta quality_checks.py."""
    if not SQL_FILE_PATH.exists():
        return []

    content = SQL_FILE_PATH.read_text(encoding="utf-8")
    return split_sql_queries(content)


def _clean_query(query: str) -> str:
    return "\n".join(line for line in query.splitlines() if not line.strip().startswith("--"))


def _extract_literal_alias(query: str, alias: str) -> str:
    match = re.search(rf"'\s*([a-zA-Z0-9_-]+)\s*'\s+AS\s+{alias}\b", query, re.IGNORECASE)
    assert match is not None, f"La consulta no define '{alias}' con la nomenclatura esperada."
    return match.group(1)


def _load_silver_schemas() -> dict[str, set[str]]:
    schemas = {}
    for path in SILVER_SCHEMAS_DIR.glob("*.json"):
        table_name = path.name.replace("_schema.json", "")
        fields = json.loads(path.read_text(encoding="utf-8"))
        schemas[table_name] = {field["name"] for field in fields}
    return schemas


def _extract_gold_views() -> set[str]:
    ddl = GOLD_DDL_PATH.read_text(encoding="utf-8")
    return set(
        re.findall(
            r"CREATE\s+OR\s+REPLACE\s+VIEW\s+`?\{project_id\}\.\{gold_dataset\}\.([A-Za-z0-9_]+)`?",
            ddl,
            flags=re.IGNORECASE,
        )
    )


def _extract_target_refs(query: str) -> list[tuple[str, str]]:
    return re.findall(
        r"`\{project_id\}\.\{(silver|gold)_dataset\}\.([A-Za-z0-9_]+)`",
        query,
        flags=re.IGNORECASE,
    )


def _strip_literals_and_table_refs(sql: str) -> str:
    without_strings = re.sub(r"'(?:[^']|'')*'", " ", sql)
    return re.sub(r"`[^`]+`", " ", without_strings)


def _extract_simple_column_references(query: str) -> set[str]:
    """Extrae columnas obvias usadas en predicados y claves, evitando aliases de salida."""
    clean_query = _strip_literals_and_table_refs(_clean_query(query))
    clauses = []

    for pattern in [
        r"\bWHERE\b(?P<clause>.*?)(?:\)\s*$|\bGROUP\s+BY\b|\bHAVING\b)",
        r"\bGROUP\s+BY\b(?P<clause>.*?)(?:\bHAVING\b|\)\s*$)",
        r"\bCOUNTIF\s*\((?P<clause>.*?)\)",
    ]:
        clauses.extend(
            match.group("clause")
            for match in re.finditer(pattern, clean_query, re.IGNORECASE | re.DOTALL)
        )

    identifiers = set()
    for clause in clauses:
        identifiers.update(
            identifier.lower()
            for identifier in re.findall(r"\b[a-z_][a-z0-9_]*\b", clause, flags=re.IGNORECASE)
        )

    ignored = {word.lower() for word in SQL_FUNCTIONS_AND_KEYWORDS}
    ignored.update(QUALITY_LOCAL_ALIASES)
    ignored.update(QUALITY_OUTPUT_COLUMNS)
    ignored.update({"float64", "int64"})
    return {identifier for identifier in identifiers if identifier.lower() not in ignored}


def test_sql_file_exists_and_is_not_empty():
    """Valida que el archivo de calidad de datos exista y no esté vacío."""
    assert SQL_FILE_PATH.exists(), f"El archivo {SQL_FILE_PATH} no existe."
    queries = get_queries()
    assert len(queries) > 0, "El archivo SQL no contiene consultas ejecutables."


def test_quality_sql_parses_expected_number_of_checks():
    queries = get_queries()

    assert len(queries) == EXPECTED_QUALITY_CHECKS


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


def test_real_quality_sql_has_no_invalid_fragments():
    queries = get_queries()

    assert queries
    for index, query in enumerate(queries, start=1):
        stripped = query.lstrip()
        assert stripped.upper().startswith(("SELECT", "WITH")), (
            f"La consulta {index} no inicia con SELECT/WITH: {stripped[:80]}"
        )
        assert not stripped.startswith(")"), (
            f"La consulta {index} es un fragmento colgante: {stripped[:80]}"
        )


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


def test_checks_reference_existing_physical_targets():
    silver_schemas = _load_silver_schemas()
    gold_views = _extract_gold_views()

    for index, query in enumerate(get_queries(), start=1):
        clean_query = _clean_query(query)
        check_id = _extract_literal_alias(clean_query, "check_id")
        refs = _extract_target_refs(clean_query)

        assert refs, f"El check {index} ({check_id}) no referencia una tabla o vista objetivo."
        for dataset_kind, target_name in refs:
            if dataset_kind.lower() == "silver":
                assert target_name in silver_schemas, (
                    f"El check {index} ({check_id}) referencia una tabla Silver inexistente: {target_name}"
                )
            elif dataset_kind.lower() == "gold":
                assert target_name in gold_views, (
                    f"El check {index} ({check_id}) referencia una vista Gold inexistente: {target_name}"
                )


def test_quality_sql_does_not_reference_legacy_colegios_columns():
    content = SQL_FILE_PATH.read_text(encoding="utf-8")

    assert "codigo_modular" not in content
    assert re.search(r"\banexo\b", content, re.IGNORECASE) is None


def test_pronabec_colegios_elegibles_checks_split_critical_and_completeness_fields():
    silver_schemas = _load_silver_schemas()
    colegios_columns = silver_schemas["pronabec_colegios_elegibles"]
    critical_query = next(
        query
        for query in get_queries()
        if "silver_pronabec_colegios_elegibles_nulls" in query
    )
    completeness_query = next(
        query
        for query in get_queries()
        if "silver_pronabec_colegios_elegibles_fields_format" in query
    )

    critical_columns = _extract_simple_column_references(critical_query)
    completeness_columns = _extract_simple_column_references(completeness_query)

    assert _extract_literal_alias(critical_query, "severity") == "ERROR"
    assert critical_columns <= colegios_columns
    assert {
        "ugel",
        "institucion_educativa",
        "tipo_gestion_colegio",
    } <= critical_columns
    assert "distrito" not in critical_columns

    assert _extract_literal_alias(completeness_query, "severity") == "WARNING"
    assert completeness_columns <= colegios_columns
    assert {
        "nivel_modalidad",
        "forma_atencion",
        "distrito",
    } <= completeness_columns


def test_pronabec_ubigeo_postulacion_allows_known_foreign_province_nulls():
    query = next(
        query
        for query in get_queries()
        if "silver_pronabec_ubigeo_postulacion_fields_nulls" in query
    )
    normalized = " ".join(_clean_query(query).split()).upper()

    assert _extract_literal_alias(query, "severity") == "ERROR"
    assert "PROVINCIA IS NULL OR TRIM(PROVINCIA) = ''" in normalized
    assert "NOT IN ('CHILE', 'COLOMBIA', 'MEXICO')" in normalized


def test_silver_checks_use_declared_schema_columns_for_simple_predicates():
    silver_schemas = _load_silver_schemas()

    for index, query in enumerate(get_queries(), start=1):
        clean_query = _clean_query(query)
        check_id = _extract_literal_alias(clean_query, "check_id")
        layer = _extract_literal_alias(clean_query, "layer")
        table_name = _extract_literal_alias(clean_query, "table_name")

        if layer != "silver":
            continue

        schema_columns = silver_schemas[table_name]
        referenced_columns = _extract_simple_column_references(clean_query)
        unknown_columns = sorted(referenced_columns - schema_columns)

        assert not unknown_columns, (
            f"El check {index} ({check_id}) referencia columnas no declaradas "
            f"en config/schemas/silver/{table_name}_schema.json: {unknown_columns}"
        )


def test_gold_checks_reference_declared_gold_views_when_present():
    gold_views = _extract_gold_views()

    for index, query in enumerate(get_queries(), start=1):
        clean_query = _clean_query(query)
        check_id = _extract_literal_alias(clean_query, "check_id")
        layer = _extract_literal_alias(clean_query, "layer")
        table_name = _extract_literal_alias(clean_query, "table_name")

        if layer == "gold":
            assert table_name in gold_views, (
                f"El check {index} ({check_id}) referencia una vista Gold no declarada: {table_name}"
            )
