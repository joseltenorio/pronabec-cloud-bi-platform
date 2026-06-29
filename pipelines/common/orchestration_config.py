"""Helpers for the declarative batch orchestration manifest."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from pipelines.common.config import ConfigError, load_yaml_config
from pipelines.common.gcs import build_gs_uri

DEFAULT_ORCHESTRATION_CONFIG_PATH = Path("config/orchestration.yaml")
DEFAULT_ENDPOINTS_CONFIG_PATH = Path("config/endpoints.yaml")
PRONABEC_EXTRACTION_MODES = {"single", "chunked"}


@dataclass(frozen=True)
class DatasetExtractionPolicy:
    source_dataset: str
    extraction_enabled: bool
    silver_enabled: bool
    extraction_mode: str
    required_for_e2e: bool
    chunk_size_pages: int | None
    max_parallel_chunks: int


def load_orchestration_config(path: str | Path = DEFAULT_ORCHESTRATION_CONFIG_PATH) -> dict[str, Any]:
    """Load and validate the orchestration manifest."""
    config = load_yaml_config(path)
    validate_orchestration_config(config)
    return config


def load_endpoints_config(path: str | Path = DEFAULT_ENDPOINTS_CONFIG_PATH) -> dict[str, Any]:
    """Load the source endpoints catalog used by the DAG."""
    return load_yaml_config(path)


def resolve_airflow_var_name(config: dict[str, Any], key: str) -> str:
    """Resolve a variable name from the orchestration manifest."""
    for section_name in ("runtime", "jobs"):
        section = config.get(section_name, {})
        if isinstance(section, dict) and key in section:
            value = section[key]
            if not isinstance(value, str) or not value.strip():
                raise ConfigError(f"Variable Airflow vacia o invalida: {key}")
            return value.strip()

    raise ConfigError(f"No existe una variable Airflow declarada para: {key}")


def build_gcs_uri(bucket: str, path: str) -> str:
    """Build a gs:// URI from bucket and object path."""
    normalized_bucket = str(bucket).strip()
    normalized_path = str(path).strip()
    if not normalized_bucket:
        raise ConfigError("bucket no puede estar vacio")
    if not normalized_path:
        raise ConfigError("path no puede estar vacio")
    return build_gs_uri(normalized_bucket, normalized_path)


def build_bq_table_ref(project_id: str, dataset: str, table: str) -> str:
    """Build a BigQuery table reference in project:dataset.table format."""
    normalized_project = str(project_id).strip()
    normalized_dataset = str(dataset).strip()
    normalized_table = str(table).strip()

    if not normalized_project or not normalized_dataset or not normalized_table:
        raise ConfigError("project_id, dataset y table son requeridos para construir la referencia BigQuery")

    return f"{normalized_project}:{normalized_dataset}.{normalized_table}"


def validate_orchestration_config(config: dict[str, Any]) -> None:
    """Validate the orchestration manifest."""
    required_top_level = {"dag", "runtime", "jobs", "datasets", "gold"}
    missing = [key for key in required_top_level if key not in config]
    if missing:
        raise ConfigError(f"Faltan secciones requeridas en orchestration.yaml: {', '.join(sorted(missing))}")

    _validate_pronabec_reports_config(config)
    _validate_pronabec_dataset_policies(config)
    _validate_gold_config(config)


def get_pronabec_dataset_policies(config: dict[str, Any]) -> list[DatasetExtractionPolicy]:
    """Return PRONABEC API extraction policies declared in orchestration.yaml."""
    pronabec_api = config.get("datasets", {}).get("pronabec_api", {})
    policies = pronabec_api.get("extraction_policies", [])
    if not isinstance(policies, list):
        raise ConfigError("datasets.pronabec_api.extraction_policies debe ser una lista")

    return [_build_pronabec_dataset_policy(item) for item in policies]


def get_enabled_pronabec_datasets(config: dict[str, Any]) -> list[str]:
    """Return PRONABEC API datasets enabled for Bronze extraction."""
    return [
        policy.source_dataset
        for policy in get_pronabec_dataset_policies(config)
        if policy.extraction_enabled
    ]


def get_required_pronabec_datasets(config: dict[str, Any]) -> list[str]:
    """Return PRONABEC API datasets required for the E2E path."""
    return [
        policy.source_dataset
        for policy in get_pronabec_dataset_policies(config)
        if policy.required_for_e2e
    ]


def get_chunked_pronabec_datasets(config: dict[str, Any]) -> list[str]:
    """Return PRONABEC API datasets configured for chunked extraction."""
    return [
        policy.source_dataset
        for policy in get_pronabec_dataset_policies(config)
        if policy.extraction_mode == "chunked"
    ]


def resolve_pronabec_report_groups(
    orchestration_config: dict[str, Any],
    endpoints_config: dict[str, Any],
) -> list[dict[str, Any]]:
    """Resolve PRONABEC report groups from endpoints metadata."""
    reports_config = orchestration_config["datasets"]["pronabec_reports"]
    if not reports_config.get("enabled", False):
        return []

    documents = _get_nested_items(endpoints_config, ("pronabec_reports", "documents"))
    groups: list[dict[str, Any]] = []
    for document in documents:
        if not isinstance(document, dict):
            continue
        if document.get("enabled", True) is False:
            continue

        landing_subset = document.get("landing_subset") or document.get("document_id")
        datasets = []
        for dataset in document.get("datasets", []):
            if not isinstance(dataset, dict) or dataset.get("enabled", True) is False:
                continue
            dataset_name = dataset.get("name")
            if not dataset_name:
                continue
            datasets.append(
                {
                    "source_subset": landing_subset,
                    "source_dataset": dataset_name,
                    "file_name": dataset.get("file_name"),
                    "document_id": document.get("document_id"),
                    "document_storage_paths": _collect_document_storage_paths(document),
                }
            )

        groups.append(
            {
                "source_subset": landing_subset,
                "document_id": document.get("document_id"),
                "datasets": datasets,
                "landing_documents_path": reports_config["landing_documents_path_template"].format(
                    source_subset=landing_subset
                ),
                "landing_path": reports_config["landing_path_template"].format(
                    source_subset=landing_subset
                ),
            }
        )

    return groups


def resolve_pronabec_report_datasets(
    orchestration_config: dict[str, Any],
    endpoints_config: dict[str, Any],
) -> list[dict[str, Any]]:
    """Flatten PRONABEC report datasets resolved from endpoints metadata."""
    datasets: list[dict[str, Any]] = []
    for group in resolve_pronabec_report_groups(orchestration_config, endpoints_config):
        datasets.extend(group["datasets"])
    return datasets


def _validate_pronabec_dataset_policies(config: dict[str, Any]) -> None:
    get_pronabec_dataset_policies(config)


def _build_pronabec_dataset_policy(item: Any) -> DatasetExtractionPolicy:
    if not isinstance(item, dict):
        raise ConfigError("Cada policy PRONABEC debe ser un objeto")

    source_dataset = item.get("source_dataset")
    if not isinstance(source_dataset, str) or not source_dataset.strip():
        raise ConfigError("Cada policy PRONABEC requiere source_dataset no vacio")

    extraction_enabled = _require_bool(item, "extraction_enabled", source_dataset)
    silver_enabled = _require_bool(item, "silver_enabled", source_dataset)
    required_for_e2e = _require_bool(item, "required_for_e2e", source_dataset)

    extraction_mode = item.get("extraction_mode")
    if extraction_mode not in PRONABEC_EXTRACTION_MODES:
        raise ConfigError(
            f"extraction_mode invalido para {source_dataset}: {extraction_mode}. "
            "Debe ser single o chunked"
        )

    chunk_size_pages = item.get("chunk_size_pages")
    max_parallel_chunks = item.get("max_parallel_chunks", 1)

    if extraction_mode == "chunked":
        if not isinstance(chunk_size_pages, int) or chunk_size_pages <= 0:
            raise ConfigError(
                f"chunk_size_pages debe ser entero positivo para {source_dataset}"
            )
    elif chunk_size_pages is not None:
        raise ConfigError(
            f"chunk_size_pages debe ser null u omitirse para extraction_mode=single en {source_dataset}"
        )

    if not isinstance(max_parallel_chunks, int) or max_parallel_chunks <= 0:
        raise ConfigError(
            f"max_parallel_chunks debe ser entero positivo para {source_dataset}"
        )
    if extraction_mode == "single" and max_parallel_chunks != 1:
        raise ConfigError(
            f"max_parallel_chunks debe ser 1 para extraction_mode=single en {source_dataset}"
        )

    return DatasetExtractionPolicy(
        source_dataset=source_dataset.strip(),
        extraction_enabled=extraction_enabled,
        silver_enabled=silver_enabled,
        extraction_mode=extraction_mode,
        required_for_e2e=required_for_e2e,
        chunk_size_pages=chunk_size_pages,
        max_parallel_chunks=max_parallel_chunks,
    )


def _require_bool(item: dict[str, Any], key: str, source_dataset: str) -> bool:
    value = item.get(key)
    if not isinstance(value, bool):
        raise ConfigError(f"{key} debe ser boolean para {source_dataset}")
    return value


def _validate_pronabec_reports_config(config: dict[str, Any]) -> None:
    reports = config["datasets"].get("pronabec_reports")
    if not isinstance(reports, dict):
        raise ConfigError("datasets.pronabec_reports debe ser un objeto")

    landing_path_template = reports.get("landing_path_template")
    landing_documents_path_template = reports.get("landing_documents_path_template")
    bronze_path_template = reports.get("bronze_path_template")
    silver_table_template = reports.get("silver_table_template")

    if not isinstance(landing_path_template, str) or "{extraction_date}" in landing_path_template:
        raise ConfigError("landing_path_template para PRONABEC reports no debe contener extraction_date")
    if not isinstance(landing_documents_path_template, str):
        raise ConfigError("landing_documents_path_template para PRONABEC reports es requerido")
    if not landing_documents_path_template.startswith("landing/") or "/_documents" not in landing_documents_path_template:
        raise ConfigError("landing_documents_path_template debe apuntar a landing/.../_documents")
    if "bronze/" in landing_documents_path_template:
        raise ConfigError("landing_documents_path_template no puede apuntar a bronze/")
    if not isinstance(bronze_path_template, str) or "{extraction_date}" not in bronze_path_template:
        raise ConfigError("bronze_path_template para PRONABEC reports debe contener extraction_date")
    if "{source_subset}" in bronze_path_template:
        raise ConfigError("bronze_path_template para PRONABEC reports no debe contener source_subset")
    if not isinstance(silver_table_template, str):
        raise ConfigError("silver_table_template para PRONABEC reports es requerido")

    rendered = silver_table_template.format(
        project_id="project",
        silver_dataset="silver",
        dataset="dataset",
        silver_table="table",
    )
    if ":" not in rendered or rendered.count(":") != 1 or rendered.count(".") != 1:
        raise ConfigError("silver_table_template debe renderizar a project:dataset.table")

    items_from = reports.get("items_from")
    if not isinstance(items_from, str) or not items_from.strip():
        raise ConfigError("items_from es requerido para PRONABEC reports")


def _validate_gold_config(config: dict[str, Any]) -> None:
    gold = config["gold"]
    if not isinstance(gold, dict):
        raise ConfigError("gold debe ser un objeto")

    sql_template = gold.get("sql_template")
    if not isinstance(sql_template, str) or not sql_template.strip():
        raise ConfigError("gold.sql_template es requerido")

    validation_queries = gold.get("validation_queries")
    if not isinstance(validation_queries, list) or not validation_queries:
        raise ConfigError("gold.validation_queries es requerido")

    for query in validation_queries:
        if not isinstance(query, dict) or not query.get("name") or not query.get("query"):
            raise ConfigError("Cada gold.validation_queries item requiere name y query")


def _get_nested_items(config: dict[str, Any], path: Iterable[str]) -> list[Any]:
    current: Any = config
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return []
        current = current[key]
    if isinstance(current, list):
        return current
    return []


def _collect_document_storage_paths(document: dict[str, Any]) -> list[str]:
    storage_paths: list[str] = []
    direct_path = document.get("document_storage_path")
    if isinstance(direct_path, str) and direct_path.strip():
        storage_paths.append(direct_path.strip())
    for item in document.get("documents", []):
        if isinstance(item, dict):
            path = item.get("document_storage_path")
            if isinstance(path, str) and path.strip():
                storage_paths.append(path.strip())
    return storage_paths
