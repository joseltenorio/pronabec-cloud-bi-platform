# -*- coding: utf-8 -*-
"""Pruebas unitarias para la validacion Gold."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from pipelines.validate_gold import (
    GoldValidationSettings,
    load_validation_queries,
    validate_gold_views,
    render_validation_query,
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
