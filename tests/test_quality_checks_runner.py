"""
Pruebas unitarias para el ejecutor de calidad de datos (pipelines/quality_checks.py).
Valida el funcionamiento del runner, la interpolación de placeholders, la separación
de consultas, el manejo de excepciones y la persistencia en la tabla Audit utilizando mocks.
"""

from __future__ import annotations

import re
from pathlib import Path
import pytest

from pipelines.quality_checks import (
    deduce_source_metadata,
    expand_env_placeholders,
    main,
    run_quality_checks,
    split_sql_queries,
)


# ============================================================================
# Mocks y Clases Auxiliares
# ============================================================================

class MockRow:
    """Simula una fila devuelta por el cliente de BigQuery."""
    def __init__(self, data: dict):
        self._data = data

    def items(self):
        return self._data.items()

    def __getitem__(self, key):
        return self._data[key]

    def get(self, key, default=None):
        return self._data.get(key, default)


class MockQueryJob:
    """Simula el job retornado por client.query()."""
    def __init__(self, rows):
        self._rows = rows

    def result(self):
        return self._rows


class MockBigQueryClient:
    """Simula el cliente de BigQuery para evitar llamadas reales a GCP."""
    def __init__(self, project=None):
        self.project = project
        self.queries_run = []
        self.insert_calls = []
        self.mock_rows_by_query = {}  # Mapeo de subcadena SQL -> lista de dicts
        self.insert_errors = []
        self.query_exception = None

    def query(self, query_text):
        if self.query_exception:
            raise self.query_exception

        self.queries_run.append(query_text)

        # Buscar si hay una fila mockeada específica para esta query
        matched_rows = None
        for q_sub, rows in self.mock_rows_by_query.items():
            if q_sub in query_text:
                matched_rows = rows
                break

        # Si no hay específica, construir una por defecto que pase
        if matched_rows is None:
            # Extraer metadatos para construir respuesta coherente
            check_id = "mock_check"
            table_name = "mock_table"
            severity = "ERROR"
            layer = "silver"

            check_id_match = re.search(r"'\s*([a-zA-Z0-9_-]+)\s*'\s+AS\s+check_id", query_text, re.IGNORECASE)
            table_name_match = re.search(r"'\s*([a-zA-Z0-9_-]+)\s*'\s+AS\s+table_name", query_text, re.IGNORECASE)
            severity_match = re.search(r"'\s*([a-zA-Z0-9_-]+)\s*'\s+AS\s+severity", query_text, re.IGNORECASE)
            layer_match = re.search(r"'\s*([a-zA-Z0-9_-]+)\s*'\s+AS\s+layer", query_text, re.IGNORECASE)

            if check_id_match:
                check_id = check_id_match.group(1)
            if table_name_match:
                table_name = table_name_match.group(1)
            if severity_match:
                severity = severity_match.group(1)
            if layer_match:
                layer = layer_match.group(1)

            matched_rows = [{
                "check_id": check_id,
                "layer": layer,
                "table_name": table_name,
                "severity": severity,
                "passed": True,
                "failed_rows": 0,
                "details": "Validación mockeada exitosa"
            }]

        rows_objects = [MockRow(r) if isinstance(r, dict) else r for r in matched_rows]
        return MockQueryJob(rows_objects)

    def insert_rows_json(self, table_ref, json_rows):
        self.insert_calls.append((table_ref, json_rows))
        return self.insert_errors


# ============================================================================
# Pruebas Unitarias
# ============================================================================

def test_deduce_source_metadata():
    """Valida la deducción del sistema origen y dataset basado en el nombre de la tabla."""
    # Caso pronabec
    sys, ds = deduce_source_metadata("pronabec_convocatorias")
    assert sys == "pronabec"
    assert ds == "convocatorias"

    # Caso pronabec_reports
    sys, ds = deduce_source_metadata("pronabec_report_beca18_universitarios_carrera_anual")
    assert sys == "pronabec_reports"
    assert ds == "beca18_universitarios_carrera_anual"

    # Caso MEF
    sys, ds = deduce_source_metadata("presupuesto_mef_temporal")
    assert sys == "mef"
    assert ds == "presupuesto_temporal"

    sys, ds = deduce_source_metadata("presupuesto_mef")
    assert sys == "mef"
    assert ds == "presupuesto"

    # Caso desconocido
    sys, ds = deduce_source_metadata("tabla_desconocida")
    assert sys == "unknown"
    assert ds == "unknown"


def test_split_sql_queries():
    """Valida que la división de consultas por punto y coma funcione e ignore segmentos vacíos y comentarios."""
    sql = """
    -- Primer check
    SELECT 'check_1' AS check_id, 'silver' AS layer;
    
    -- Segundo check
    SELECT 'check_2' AS check_id, 'silver' AS layer;
    
    -- Segmento vacío con comentarios
    -- Otro comentario
    ;
    """
    queries = split_sql_queries(sql)
    assert len(queries) == 2
    assert "check_1" in queries[0]
    assert "check_2" in queries[1]


def test_split_sql_queries_keeps_subquery_as_single_query():
    sql = """
    SELECT 'check_1' AS check_id
    FROM (
      SELECT COUNT(*) AS cnt
      FROM `{project_id}.{silver_dataset}.table`
    );
    """

    queries = split_sql_queries(sql)

    assert len(queries) == 1
    assert queries[0].count("FROM (") == 1
    assert not queries[0].lstrip().startswith(")")


def test_split_sql_queries_never_returns_dangling_closing_parenthesis():
    sql = """
    -- Comentario con punto y coma; no debe partir la query.
    SELECT 'check_1' AS check_id FROM (SELECT 1 AS x);
    """

    queries = split_sql_queries(sql)

    assert len(queries) == 1
    assert all(not query.lstrip().startswith(")") for query in queries)


def test_split_sql_queries_ignores_comments_and_empty_segments():
    sql = """
    -- Comentario inicial;
    ;
    /* Bloque comentado; tambien ignorado */
    SELECT 'check_1' AS check_id;
    -- comentario final;
    ;
    """

    queries = split_sql_queries(sql)

    assert queries == ["SELECT 'check_1' AS check_id"]


def test_split_sql_queries_respects_semicolon_inside_strings():
    sql = "SELECT 'texto; con punto y coma' AS details;"

    queries = split_sql_queries(sql)

    assert queries == ["SELECT 'texto; con punto y coma' AS details"]


def test_split_sql_queries_rejects_unbalanced_parentheses():
    with pytest.raises(ValueError, match="Unbalanced parentheses"):
        split_sql_queries("SELECT * FROM (SELECT 1 AS x;")


def test_split_sql_queries_rejects_non_select_fragment():
    with pytest.raises(ValueError, match="Invalid quality check SQL fragment at index 1"):
        split_sql_queries("INSERT INTO audit.table SELECT 1;")


def test_real_quality_sql_splits_into_select_or_with_queries():
    sql_path = Path("sql/quality/data_quality_checks.sql")
    queries = split_sql_queries(sql_path.read_text(encoding="utf-8"))

    assert queries
    for query in queries:
        normalized = query.lstrip().upper()
        assert normalized.startswith(("SELECT", "WITH"))


def test_real_quality_sql_does_not_generate_dangling_parenthesis_fragment():
    sql_path = Path("sql/quality/data_quality_checks.sql")
    queries = split_sql_queries(sql_path.read_text(encoding="utf-8"))

    assert all(not query.lstrip().startswith(")") for query in queries)


def test_runner_dry_run(tmp_path, monkeypatch):
    """Valida que el modo dry-run no instancie el cliente de BigQuery y complete con éxito."""
    # Crear archivo de checks temporal
    checks_file = tmp_path / "data_quality_checks.sql"
    checks_file.write_text(
        "SELECT 'check_dry_run' AS check_id, 'silver' AS layer, 'pronabec_convocatorias' AS table_name, 'ERROR' AS severity, 0 AS failed_rows, TRUE AS passed, 'Ok' AS details;",
        encoding="utf-8"
    )

    # Mockear BigQuery.Client para arrojar error si se intenta instanciar
    def fail_instantiation(*args, **kwargs):
        pytest.fail("Se intentó instanciar el cliente de BigQuery real en modo dry-run.")

    monkeypatch.setattr("google.cloud.bigquery.Client", fail_instantiation)

    exit_code = run_quality_checks(
        project_id="test-project",
        silver_dataset="silver_ds",
        gold_dataset="gold_ds",
        audit_dataset="audit_ds",
        checks_file=str(checks_file),
        pipeline_run_id="run-dry-run",
        dry_run=True,
        fail_on_error=False
    )

    assert exit_code == 0


def test_quality_checks_cli_requires_project_id(monkeypatch, capsys):
    monkeypatch.setattr(
        "sys.argv",
        [
            "quality_checks.py",
            "--silver-dataset",
            "silver",
            "--gold-dataset",
            "gold",
            "--audit-dataset",
            "audit",
        ],
    )

    with pytest.raises(SystemExit) as excinfo:
        main()

    captured = capsys.readouterr()
    assert excinfo.value.code == 2
    assert "the following arguments are required: --project-id" in captured.err


def test_quality_checks_expands_pipeline_run_id_env_placeholder(monkeypatch):
    monkeypatch.setenv("PIPELINE_RUN_ID", "manual_20260702")

    assert expand_env_placeholders("${PIPELINE_RUN_ID}") == "manual_20260702"


def test_quality_checks_rejects_missing_pipeline_run_id_env_placeholder(monkeypatch):
    monkeypatch.delenv("PIPELINE_RUN_ID", raising=False)

    with pytest.raises(ValueError, match="Variable de entorno requerida no definida: PIPELINE_RUN_ID"):
        expand_env_placeholders("${PIPELINE_RUN_ID}")


def test_runner_successful_execution(tmp_path, monkeypatch):
    """Valida la ejecución exitosa del runner con inserción exitosa en audit."""
    checks_file = tmp_path / "checks.sql"
    checks_file.write_text(
        "SELECT 'check_ok' AS check_id, 'silver' AS layer, 'pronabec_convocatorias' AS table_name, 'ERROR' AS severity, 0 AS failed_rows, TRUE AS passed, 'Ok' AS details;",
        encoding="utf-8"
    )

    mock_client = MockBigQueryClient()
    monkeypatch.setattr("google.cloud.bigquery.Client", lambda project: mock_client)

    exit_code = run_quality_checks(
        project_id="test-project",
        silver_dataset="silver_ds",
        gold_dataset="gold_ds",
        audit_dataset="audit_ds",
        checks_file=str(checks_file),
        pipeline_run_id="run-success",
        dry_run=False,
        fail_on_error=False
    )

    # Debe retornar 0 (sin fallos de calidad)
    assert exit_code == 0
    assert len(mock_client.queries_run) == 1
    # Validar interpolación de placeholders
    assert "test-project" not in mock_client.queries_run[0]  # El query del file no tenía placeholders, pero verifiquemos que se ejecutó
    
    # Validar persistencia en Audit
    assert len(mock_client.insert_calls) == 1
    table_ref, rows = mock_client.insert_calls[0]
    assert table_ref == "test-project.audit_ds.data_quality_results"
    assert len(rows) == 1
    
    row = rows[0]
    assert row["check_id"] == "check_ok"
    assert row["passed"] is True
    assert row["failed_rows"] == 0
    assert row["pipeline_run_id"] == "run-success"
    assert "quality_run_id" in row
    assert "execution_timestamp" in row
    assert row["query_file"] == "checks.sql"
    assert row["source_system"] == "pronabec"
    assert row["source_dataset"] == "convocatorias"


def test_runner_failed_check(tmp_path, monkeypatch):
    """Valida que un check fallido retorne código de salida 1 y se guarde como fallido en Audit."""
    checks_file = tmp_path / "checks.sql"
    checks_file.write_text(
        "SELECT 'check_failed' AS check_id, 'silver' AS layer, 'pronabec_becarios_pais_estudio' AS table_name, 'ERROR' AS severity, 15 AS failed_rows, FALSE AS passed, '15 nulos' AS details;",
        encoding="utf-8"
    )

    mock_client = MockBigQueryClient()
    # Mockear resultado específico para el test de fallo
    mock_client.mock_rows_by_query["check_failed"] = [{
        "check_id": "check_failed",
        "layer": "silver",
        "table_name": "pronabec_becarios_pais_estudio",
        "severity": "ERROR",
        "passed": False,
        "failed_rows": 15,
        "details": "Se encontraron 15 nulos"
    }]

    monkeypatch.setattr("google.cloud.bigquery.Client", lambda project: mock_client)

    exit_code = run_quality_checks(
        project_id="test-project",
        silver_dataset="silver_ds",
        gold_dataset="gold_ds",
        audit_dataset="audit_ds",
        checks_file=str(checks_file),
        pipeline_run_id="run-failed",
        dry_run=False,
        fail_on_error=False
    )

    # Debe retornar 1 por el fallo de calidad
    assert exit_code == 1
    assert len(mock_client.insert_calls) == 1
    _, rows = mock_client.insert_calls[0]
    assert rows[0]["check_id"] == "check_failed"
    assert rows[0]["passed"] is False
    assert rows[0]["failed_rows"] == 15
    assert rows[0]["details"] == "Se encontraron 15 nulos"


def test_runner_sql_exception_fail_on_error_false(tmp_path, monkeypatch):
    """Valida que si falla la query en BigQuery y fail_on_error=False, el runner no se detiene y registra el fallo en Audit."""
    checks_file = tmp_path / "checks.sql"
    checks_file.write_text(
        "SELECT 'check_exception' AS check_id, 'silver' AS layer, 'presupuesto_mef' AS table_name, 'ERROR' AS severity, 0 AS failed_rows, TRUE AS passed, 'Ok' AS details;",
        encoding="utf-8"
    )

    mock_client = MockBigQueryClient()
    mock_client.query_exception = RuntimeError("BigQuery syntax error near SELECT")

    monkeypatch.setattr("google.cloud.bigquery.Client", lambda project: mock_client)

    exit_code = run_quality_checks(
        project_id="test-project",
        silver_dataset="silver_ds",
        gold_dataset="gold_ds",
        audit_dataset="audit_ds",
        checks_file=str(checks_file),
        pipeline_run_id="run-exception",
        dry_run=False,
        fail_on_error=False
    )

    # Debe retornar 1 por error de consulta
    assert exit_code == 1
    # Debe haber persistido el error en Audit
    assert len(mock_client.insert_calls) == 1
    _, rows = mock_client.insert_calls[0]
    assert rows[0]["check_id"] == "check_exception"
    assert rows[0]["passed"] is False
    assert rows[0]["failed_rows"] == 1
    assert "Excepción en ejecución SQL: BigQuery syntax error" in rows[0]["details"]


def test_runner_sql_exception_fail_on_error_true(tmp_path, monkeypatch):
    """Valida que si falla la query en BigQuery y fail_on_error=True, el runner lanza la excepción y se interrumpe."""
    checks_file = tmp_path / "checks.sql"
    checks_file.write_text(
        "SELECT 'check_exception' AS check_id, 'silver' AS layer, 'presupuesto_mef' AS table_name, 'ERROR' AS severity, 0 AS failed_rows, TRUE AS passed, 'Ok' AS details;",
        encoding="utf-8"
    )

    mock_client = MockBigQueryClient()
    mock_client.query_exception = RuntimeError("BigQuery syntax error near SELECT")

    monkeypatch.setattr("google.cloud.bigquery.Client", lambda project: mock_client)

    with pytest.raises(RuntimeError, match="BigQuery syntax error"):
        run_quality_checks(
            project_id="test-project",
            silver_dataset="silver_ds",
            gold_dataset="gold_ds",
            audit_dataset="audit_ds",
            checks_file=str(checks_file),
            pipeline_run_id="run-exception",
            dry_run=False,
            fail_on_error=True
        )


def test_runner_placeholders_are_interpolated(tmp_path, monkeypatch):
    """Valida que los placeholders del archivo SQL se reemplacen correctamente con los datasets provistos."""
    checks_file = tmp_path / "checks.sql"
    checks_file.write_text(
        "SELECT * FROM `{project_id}.{silver_dataset}.tabla_ejemplo`;",
        encoding="utf-8"
    )

    mock_client = MockBigQueryClient()
    monkeypatch.setattr("google.cloud.bigquery.Client", lambda project: mock_client)

    run_quality_checks(
        project_id="my-gcp-project",
        silver_dataset="my_silver_dataset",
        gold_dataset="my_gold_dataset",
        audit_dataset="my_audit_dataset",
        checks_file=str(checks_file),
        pipeline_run_id="run-placeholders",
        dry_run=False,
        fail_on_error=False
    )

    assert len(mock_client.queries_run) == 1
    assert "my-gcp-project.my_silver_dataset.tabla_ejemplo" in mock_client.queries_run[0]
