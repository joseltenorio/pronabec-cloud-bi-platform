# -*- coding: utf-8 -*-
"""Pruebas unitarias para la validacion Gold."""

from __future__ import annotations

import re
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

from pipelines.validate_gold import (
    GoldValidationSettings,
    load_validation_queries,
    render_validation_query,
    validate_gold_views,
)


def test_render_validation_query_replaces_placeholders() -> None:
    rendered = render_validation_query(
        "SELECT '{project_id}' AS project_id, '{gold_dataset}' AS gold_dataset, '{audit_dataset}' AS audit_dataset, '{silver_dataset}' AS silver_dataset",
        project_id="project-1",
        gold_dataset="gold",
        audit_dataset="audit",
        silver_dataset="silver",
    )

    assert "{project_id}" not in rendered
    assert "project-1" in rendered
    assert "gold" in rendered
    assert "audit" in rendered
    assert "silver" in rendered


def test_render_validation_query_rejects_unresolved_placeholders() -> None:
    with pytest.raises(ValueError, match="placeholders sin resolver"):
        render_validation_query(
            "SELECT '{missing}' AS missing",
            project_id="project-1",
            gold_dataset="gold",
            audit_dataset="audit",
            silver_dataset="silver",
        )


def test_load_validation_queries_reads_manifest() -> None:
    queries = load_validation_queries()

    assert queries
    assert all("name" in query and "query" in query for query in queries)


def test_gold_validation_queries_do_not_use_reserved_rows_alias() -> None:
    queries = load_validation_queries()

    assert len(queries) == 21
    for query in queries:
        query_text = query["query"]
        assert not re.search(r"\bAS\s+ROWS\b", query_text, flags=re.IGNORECASE)
        assert not re.search(r"\bAS\s+`?rows`?\b", query_text, flags=re.IGNORECASE)


def test_gold_validation_queries_render_with_rows_count_alias() -> None:
    queries = load_validation_queries()

    for query in queries:
        rendered = render_validation_query(
            query["query"],
            project_id="project-1",
            gold_dataset="gold",
            audit_dataset="audit",
            silver_dataset="silver",
        )

        assert "SELECT COUNT(*) AS rows_count FROM" in rendered
        assert not re.search(r"\bAS\s+ROWS\b", rendered, flags=re.IGNORECASE)


def test_validate_gold_views_executes_configured_queries(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_job = MagicMock()
    fake_job.result.return_value = MagicMock(schema=[object()])

    fake_client = MagicMock()
    fake_client.query.return_value = fake_job
    monkeypatch.setattr("pipelines.validate_gold.bigquery.Client", lambda project: fake_client)

    settings = GoldValidationSettings(
        project_id="project-1",
        silver_dataset="silver",
        gold_dataset="gold",
        audit_dataset="audit",
        bq_location="US",
    )

    executed = validate_gold_views(settings)

    assert executed > 0
    assert fake_client.query.call_count == executed
    first_call_args, first_call_kwargs = fake_client.query.call_args_list[0]
    assert "SELECT" in first_call_args[0]
    assert first_call_kwargs["location"] == "US"


def test_all_gold_views_have_validation_queries() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    gold_sql = (repo_root / "sql" / "ddl" / "create_gold_views.sql").read_text(encoding="utf-8")
    config = yaml.safe_load((repo_root / "config" / "orchestration.yaml").read_text(encoding="utf-8"))

    created_views = set(
        re.findall(
            r"CREATE\s+OR\s+REPLACE\s+VIEW\s+`?\{project_id\}\.\{gold_dataset\}\.(\w+)`?",
            gold_sql,
            flags=re.IGNORECASE,
        )
    )
    validation_views = {item["name"] for item in config["gold"]["validation_queries"]}

    missing = created_views - validation_views
    assert not missing, f"Gold views missing validation queries: {sorted(missing)}"

    extra = validation_views - created_views
    assert not extra, f"Validation queries without matching Gold view: {sorted(extra)}"
