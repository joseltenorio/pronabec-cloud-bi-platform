"""Validate published Gold views by executing configured BigQuery queries."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from google.cloud import bigquery

from pipelines.common.config import ConfigError, get_env_var
from pipelines.common.logging import log_event, setup_structured_logger
from pipelines.common.orchestration_config import load_orchestration_config
from pipelines.common.sql_rendering import find_unresolved_placeholders, render_template


REPO_ROOT = Path(__file__).resolve().parents[1]
ORCHESTRATION_PATH = REPO_ROOT / "config" / "orchestration.yaml"
logger = setup_structured_logger("validate_gold")


@dataclass(frozen=True)
class GoldValidationSettings:
    project_id: str
    silver_dataset: str
    gold_dataset: str
    audit_dataset: str
    bq_location: str
    orchestration_path: Path = ORCHESTRATION_PATH


def load_gold_validation_settings() -> GoldValidationSettings:
    """Load runtime settings from environment variables."""
    return GoldValidationSettings(
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


def render_validation_query(
    query: str,
    *,
    project_id: str,
    gold_dataset: str,
    audit_dataset: str,
    silver_dataset: str = "silver",
) -> str:
    rendered = render_template(
        query,
        {
            "project_id": project_id,
            "gold_dataset": gold_dataset,
            "audit_dataset": audit_dataset,
            "silver_dataset": silver_dataset,
        },
    )

    unresolved = find_unresolved_placeholders(rendered)
    if unresolved:
        raise ValueError(
            "La query de validacion Gold conserva placeholders sin resolver: "
            + ", ".join(unresolved)
        )
    return rendered


def load_validation_queries(orchestration_path: Path = ORCHESTRATION_PATH) -> list[dict[str, str]]:
    """Load configured validation queries from orchestration manifest."""
    config = load_orchestration_config(orchestration_path)
    gold = config["gold"]
    queries = gold.get("validation_queries", [])
    if not isinstance(queries, list):
        raise ValueError("gold.validation_queries debe ser una lista.")
    return queries


def validate_gold_views(settings: GoldValidationSettings) -> int:
    """Execute configured validation queries against BigQuery."""
    config = load_orchestration_config(settings.orchestration_path)
    gold = config["gold"]
    queries = gold.get("validation_queries", [])
    if not queries:
        raise ValueError("No hay consultas de validacion Gold configuradas.")

    client = bigquery.Client(project=settings.project_id)
    log_event(
        logger,
        "INFO",
        "Iniciando validacion Gold.",
        project_id=settings.project_id,
        gold_dataset=settings.gold_dataset,
        audit_dataset=settings.audit_dataset,
        queries=len(queries),
    )

    for index, query_spec in enumerate(queries, start=1):
        if not isinstance(query_spec, dict):
            raise ValueError("Cada consulta de validacion Gold debe ser un objeto.")
        query_name = query_spec.get("name")
        query_text = query_spec.get("query")
        if not query_name or not query_text:
            raise ValueError("Cada consulta de validacion Gold requiere name y query.")

        rendered_query = render_validation_query(
            query_text,
            project_id=settings.project_id,
            gold_dataset=settings.gold_dataset,
            audit_dataset=settings.audit_dataset,
            silver_dataset=settings.silver_dataset,
        )

        log_event(
            logger,
            "INFO",
            "Ejecutando validacion Gold.",
            validation_name=query_name,
            validation_index=index,
            validation_count=len(queries),
        )
        job = client.query(rendered_query, location=settings.bq_location)
        rows = job.result()

        schema = getattr(rows, "schema", None)
        if schema is not None and len(schema) == 0:
            raise ValueError(
                f"La validacion Gold '{query_name}' no devolvio columnas."
            )

    log_event(
        logger,
        "INFO",
        "Validacion Gold completada.",
        project_id=settings.project_id,
        gold_dataset=settings.gold_dataset,
        queries=len(queries),
    )
    return len(queries)


def main() -> None:
    """CLI entrypoint for Cloud Run and local execution."""
    settings = load_gold_validation_settings()
    validate_gold_views(settings)


if __name__ == "__main__":
    main()
