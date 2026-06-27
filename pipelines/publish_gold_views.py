"""Publish analytical Gold views in BigQuery."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from google.cloud import bigquery

from pipelines.common.config import ConfigError, get_env_var
from pipelines.common.logging import log_event, setup_structured_logger
from pipelines.common.sql_rendering import (
    find_unresolved_placeholders,
    load_sql_file,
    render_template,
    split_sql_statements,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SQL_PATH = REPO_ROOT / "sql" / "ddl" / "create_gold_views.sql"
logger = setup_structured_logger("publish_gold_views")


@dataclass(frozen=True)
class GoldPublishSettings:
    project_id: str
    silver_dataset: str
    gold_dataset: str
    audit_dataset: str
    bq_location: str
    sql_path: Path = DEFAULT_SQL_PATH


def load_gold_publish_settings() -> GoldPublishSettings:
    """Load runtime settings from environment variables."""
    return GoldPublishSettings(
        project_id=_required_env("GCP_PROJECT_ID"),
        silver_dataset=_required_env("BQ_SILVER_DATASET"),
        gold_dataset=_required_env("BQ_GOLD_DATASET"),
        audit_dataset=_required_env("BQ_AUDIT_DATASET"),
        bq_location=_required_env("BQ_LOCATION"),
    )


def _required_env(name: str) -> str:
    value = get_env_var(name, required=True)
    if value is None:
        raise ConfigError(f"Variable de entorno requerida no definida: {name}")
    return value


def render_gold_sql(
    sql: str,
    *,
    project_id: str,
    silver_dataset: str,
    gold_dataset: str,
    audit_dataset: str,
) -> str:
    """Render the Gold SQL template and validate placeholders."""
    rendered = render_template(
        sql,
        {
            "project_id": project_id,
            "silver_dataset": silver_dataset,
            "gold_dataset": gold_dataset,
            "audit_dataset": audit_dataset,
        },
    )

    unresolved = find_unresolved_placeholders(rendered)
    if unresolved:
        raise ValueError(
            "El SQL Gold conserva placeholders sin resolver: "
            + ", ".join(unresolved)
        )
    return rendered


def publish_gold_views(settings: GoldPublishSettings) -> int:
    """Execute the Gold DDL statements in BigQuery."""
    sql = load_sql_file(settings.sql_path)
    rendered_sql = render_gold_sql(
        sql,
        project_id=settings.project_id,
        silver_dataset=settings.silver_dataset,
        gold_dataset=settings.gold_dataset,
        audit_dataset=settings.audit_dataset,
    )

    statements = split_sql_statements(rendered_sql)
    if not statements:
        raise ValueError("El archivo SQL Gold no contiene sentencias ejecutables.")

    client = bigquery.Client(project=settings.project_id)
    log_event(
        logger,
        "INFO",
        "Iniciando publicacion de vistas Gold.",
        project_id=settings.project_id,
        silver_dataset=settings.silver_dataset,
        gold_dataset=settings.gold_dataset,
        audit_dataset=settings.audit_dataset,
        statements=len(statements),
    )

    for index, statement in enumerate(statements, start=1):
        log_event(
            logger,
            "INFO",
            "Ejecutando sentencia Gold.",
            statement_index=index,
            statement_count=len(statements),
        )
        job = client.query(statement, location=settings.bq_location)
        job.result()

    log_event(
        logger,
        "INFO",
        "Publicacion Gold completada.",
        project_id=settings.project_id,
        gold_dataset=settings.gold_dataset,
        statements=len(statements),
    )
    return len(statements)


def main() -> None:
    """CLI entrypoint for Cloud Run and local execution."""
    settings = load_gold_publish_settings()
    publish_gold_views(settings)


if __name__ == "__main__":
    main()
