"""Helpers for configuring Apache Beam BigQuery writes."""

from __future__ import annotations

from dataclasses import dataclass

from apache_beam.io.gcp.bigquery import WriteToBigQuery
from google.cloud import bigquery

VALID_WRITE_DISPOSITIONS = {
    "WRITE_APPEND",
    "WRITE_TRUNCATE",
    "WRITE_EMPTY",
}

VALID_CREATE_DISPOSITIONS = {
    "CREATE_NEVER",
    "CREATE_IF_NEEDED",
}

VALID_SILVER_WRITE_MODES = {
    "append",
    "replace_by_source_date",
}


@dataclass(frozen=True)
class BigQueryWriteConfig:
    """Validated BigQuery sink configuration."""

    output_table: str
    write_disposition: str = "WRITE_APPEND"
    create_disposition: str = "CREATE_NEVER"
    custom_gcs_temp_location: str | None = None


def validate_bigquery_table_reference(output_table: str | None) -> str:
    """Validate a BigQuery table reference in project:dataset.table format."""
    if output_table is None:
        raise ValueError("El argumento --output-table es requerido en modo no dry-run.")

    normalized = str(output_table).strip()
    if not normalized:
        raise ValueError("El argumento --output-table no puede estar vacio.")

    if normalized.count(":") != 1:
        raise ValueError(
            "Formato invalido de --output-table. Debe ser project:dataset.table."
        )

    project, table_ref = normalized.split(":", 1)
    if not project.strip():
        raise ValueError("Formato invalido de --output-table. Falta project.")

    if table_ref.count(".") != 1:
        raise ValueError(
            "Formato invalido de --output-table. Debe ser project:dataset.table."
        )

    dataset, table = table_ref.split(".", 1)
    if not dataset.strip() or not table.strip():
        raise ValueError(
            "Formato invalido de --output-table. Debe ser project:dataset.table."
        )

    return normalized


def normalize_bigquery_table_reference_for_sql(output_table: str | None) -> str:
    """Return a BigQuery SQL table reference in project.dataset.table format."""
    return validate_bigquery_table_reference(output_table).replace(":", ".")


def validate_silver_write_mode(value: str | None) -> str:
    """Validate the Silver write mode used before appending rows."""
    normalized = "replace_by_source_date" if value is None else str(value).strip().lower()
    if normalized not in VALID_SILVER_WRITE_MODES:
        allowed = ", ".join(sorted(VALID_SILVER_WRITE_MODES))
        raise ValueError(
            f"Valor invalido para SILVER_WRITE_MODE: {value}. Valores permitidos: {allowed}."
        )
    return normalized


def build_silver_delete_query(
    output_table: str | None,
    extraction_date: str | None,
    source_system: str | None,
    source_dataset: str | None,
) -> tuple[str, list[bigquery.ScalarQueryParameter]]:
    """Build a scoped DELETE for idempotent Silver reloads."""
    table_ref = normalize_bigquery_table_reference_for_sql(output_table)
    required_values = {
        "BRONZE_EXTRACTION_DATE": extraction_date,
        "SOURCE_SYSTEM": source_system,
        "SOURCE_DATASET": source_dataset,
    }
    missing = [name for name, value in required_values.items() if value is None or not str(value).strip()]
    if missing:
        raise ValueError(
            "No se puede limpiar Silver sin filtros completos: "
            f"{', '.join(missing)}."
        )

    query = (
        f"DELETE FROM `{table_ref}` "
        "WHERE extraction_date = @extraction_date "
        "AND source_system = @source_system "
        "AND source_dataset = @source_dataset"
    )
    parameters = [
        bigquery.ScalarQueryParameter("extraction_date", "DATE", str(extraction_date).strip()),
        bigquery.ScalarQueryParameter("source_system", "STRING", str(source_system).strip()),
        bigquery.ScalarQueryParameter("source_dataset", "STRING", str(source_dataset).strip()),
    ]
    return query, parameters


def cleanup_silver_rows_for_source_date(
    output_table: str | None,
    extraction_date: str | None,
    source_system: str | None,
    source_dataset: str | None,
    *,
    project_id: str | None = None,
    client: bigquery.Client | None = None,
) -> int | None:
    """Delete only the target source/date slice before appending new Silver rows."""
    query, parameters = build_silver_delete_query(
        output_table=output_table,
        extraction_date=extraction_date,
        source_system=source_system,
        source_dataset=source_dataset,
    )
    if client is None:
        client = bigquery.Client(project=project_id)
    job = client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=parameters))
    job.result()
    return getattr(job, "num_dml_affected_rows", None)


def validate_write_disposition(value: str | None) -> str:
    """Validate Beam BigQuery write disposition."""
    allowed = {"WRITE_APPEND", "WRITE_TRUNCATE", "WRITE_EMPTY"}
    normalized = _validate_enum_value(
        value=value,
        allowed_values=allowed,
        default="WRITE_APPEND",
        argument_name="--write-disposition",
    )
    return normalized


def validate_create_disposition(value: str | None) -> str:
    """Validate Beam BigQuery create disposition."""
    allowed = {"CREATE_NEVER", "CREATE_IF_NEEDED"}
    normalized = _validate_enum_value(
        value=value,
        allowed_values=allowed,
        default="CREATE_NEVER",
        argument_name="--create-disposition",
    )
    return normalized


def validate_gcs_temp_location(value: str | None) -> str | None:
    """Validate a GCS temp location URI."""
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized:
        return None
    if not normalized.startswith("gs://"):
        raise ValueError(
            f"Formato invalido de ubicacion temporal GCS: '{value}'. Debe empezar con 'gs://'."
        )
    return normalized


def build_bigquery_write_config(
    output_table: str | None,
    write_disposition: str | None = None,
    create_disposition: str | None = None,
    custom_gcs_temp_location: str | None = None,
) -> BigQueryWriteConfig:
    """Return a validated sink configuration for BigQuery writes."""
    return BigQueryWriteConfig(
        output_table=validate_bigquery_table_reference(output_table),
        write_disposition=validate_write_disposition(write_disposition),
        create_disposition=validate_create_disposition(create_disposition),
        custom_gcs_temp_location=validate_gcs_temp_location(custom_gcs_temp_location),
    )


def build_bigquery_write_transform(config: BigQueryWriteConfig) -> WriteToBigQuery:
    """Build the Beam BigQuery sink for a validated configuration."""
    return WriteToBigQuery(
        table=config.output_table,
        write_disposition=config.write_disposition,
        create_disposition=config.create_disposition,
        custom_gcs_temp_location=config.custom_gcs_temp_location,
    )


def _validate_enum_value(
    value: str | None,
    allowed_values: set[str],
    default: str,
    argument_name: str,
) -> str:
    normalized = default if value is None else str(value).strip().upper()
    if normalized not in allowed_values:
        allowed = ", ".join(sorted(allowed_values))
        raise ValueError(f"Valor invalido para {argument_name}: {value}. Valores permitidos: {allowed}.")
    return normalized
