from __future__ import annotations

from pathlib import Path


SCRIPT_PATH = Path("scripts/deploy_bigquery_sql.ps1")


def test_deploy_bigquery_uses_sql_file_content_pipeline():
    content = SCRIPT_PATH.read_text(encoding="utf-8")

    assert "Get-Content -Path $ResolvedSqlPath -Raw -Encoding UTF8" in content
    assert "$SqlContent | & bq @BqArgs" in content


def test_deploy_bigquery_checks_last_exit_code():
    content = SCRIPT_PATH.read_text(encoding="utf-8")

    assert "$LASTEXITCODE -ne 0" in content
    assert "Falló BigQuery SQL" in content


def test_deploy_bigquery_rejects_missing_or_empty_sql_file():
    content = SCRIPT_PATH.read_text(encoding="utf-8")

    assert "Test-Path $SqlPath" in content
    assert "IsNullOrWhiteSpace($SqlContent)" in content