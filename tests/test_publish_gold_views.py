# -*- coding: utf-8 -*-
"""Pruebas unitarias para la publicacion Gold."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pipelines.publish_gold_views import (
    GoldPublishSettings,
    load_gold_publish_settings,
    publish_gold_views,
    render_gold_sql,
)


def test_render_gold_sql_replaces_placeholders() -> None:
    rendered = render_gold_sql(
        "SELECT '{project_id}' AS project_id, '{gold_dataset}' AS gold_dataset, '{silver_dataset}' AS silver_dataset, '{audit_dataset}' AS audit_dataset",
        project_id="project-1",
        silver_dataset="silver",
        gold_dataset="gold",
        audit_dataset="audit",
    )

    assert "{project_id}" not in rendered
    assert "project-1" in rendered
    assert "gold" in rendered
    assert "silver" in rendered
    assert "audit" in rendered


def test_render_gold_sql_rejects_unresolved_placeholders() -> None:
    with pytest.raises(ValueError, match="placeholders sin resolver"):
        render_gold_sql(
            "SELECT '{project_id}' AS project_id, '{missing}' AS missing",
            project_id="project-1",
            silver_dataset="silver",
            gold_dataset="gold",
            audit_dataset="audit",
        )


def test_load_gold_publish_settings_requires_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in [
        "GCP_PROJECT_ID",
        "BQ_SILVER_DATASET",
        "BQ_GOLD_DATASET",
        "BQ_AUDIT_DATASET",
        "BQ_LOCATION",
    ]:
        monkeypatch.delenv(key, raising=False)

    with pytest.raises(Exception):
        load_gold_publish_settings()


def test_publish_gold_views_executes_each_statement(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    sql_path = tmp_path / "create_gold_views.sql"
    sql_path.write_text(
        "CREATE OR REPLACE VIEW `{project_id}.{gold_dataset}.vw_a` AS SELECT 1 AS x;\n"
        "CREATE OR REPLACE VIEW `{project_id}.{gold_dataset}.vw_b` AS SELECT 2 AS x;",
        encoding="utf-8",
    )

    fake_job = MagicMock()
    fake_job.result.return_value = None

    fake_client = MagicMock()
    fake_client.query.return_value = fake_job
    monkeypatch.setattr("pipelines.publish_gold_views.bigquery.Client", lambda project: fake_client)

    settings = GoldPublishSettings(
        project_id="project-1",
        silver_dataset="silver",
        gold_dataset="gold",
        audit_dataset="audit",
        bq_location="US",
        sql_path=sql_path,
    )

    executed = publish_gold_views(settings)

    assert executed == 2
    assert fake_client.query.call_count == 2
    first_call_args, first_call_kwargs = fake_client.query.call_args_list[0]
    assert "CREATE OR REPLACE VIEW" in first_call_args[0]
    assert first_call_kwargs["location"] == "US"
