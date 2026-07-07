from __future__ import annotations

from pathlib import Path


SCRIPT_PATH = Path("scripts/deploy_bigquery_sql.sh")


def test_deploy_bigquery_uses_sql_file_content_pipeline():
    content = SCRIPT_PATH.read_text(encoding="utf-8")

    assert 'bq query \\' in content
    assert '--use_legacy_sql=false' in content
    assert '--location "$BQ_LOCATION"' in content
    assert '< "$sql_path"' in content


def test_deploy_bigquery_checks_command_failures_with_strict_bash():
    content = SCRIPT_PATH.read_text(encoding="utf-8")

    assert "set -euo pipefail" in content
    assert "run_bq_sql_file" in content


def test_deploy_bigquery_rejects_missing_or_empty_sql_file():
    content = SCRIPT_PATH.read_text(encoding="utf-8")

    assert '[[ ! -f "$sql_path" ]]' in content
    assert '[[ ! -s "$sql_path" ]]' in content


def test_deploy_bigquery_generates_and_renders_sql_before_execution():
    content = SCRIPT_PATH.read_text(encoding="utf-8")

    assert "scripts/generate_bigquery_ddl.sh" in content
    assert "scripts/render_sql_templates.sh" in content
    assert "create_bronze_external_tables.sql" in content
    assert "create_silver_tables.sql" in content
    assert "create_audit_tables.rendered.sql" in content
    assert "create_gold_views.rendered.sql" in content
