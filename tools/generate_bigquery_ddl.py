from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable


DEFAULT_PROJECT_ID = "your-gcp-project-id"
DEFAULT_BUCKET = "project-cloud-bi-platform-lake"
DEFAULT_OUTPUT_DIR = "build/generated/sql"
DEFAULT_BRONZE_SCHEMAS_DIR = "config/schemas/bronze"
DEFAULT_SILVER_SCHEMAS_DIR = "config/schemas/silver"

HEADER = """-- ============================================================================
-- Archivo Generado Automáticamente - No editar manualmente
-- ============================================================================
-- Generado por: tools/generate_bigquery_ddl.py
-- Esquemas de origen:
--   - config/schemas/bronze/*.json
--   - config/schemas/silver/*.json
--
-- Este archivo se genera en build/generated/sql/ pra validación local o CI/CD
-- Para cambiar columnas o tipos, actualice los archivos de esquema JSON y vuelva a generarlos.
-- ============================================================================
"""


def dataset_name_from_schema_path(schema_path: Path) -> str:
    suffix = "_schema"
    stem = schema_path.stem
    if stem.endswith(suffix):
        return stem[: -len(suffix)]
    return stem


def load_schema(schema_path: Path) -> list[dict[str, str]]:
    with schema_path.open(encoding="utf-8") as schema_file:
        schema = json.load(schema_file)

    if not isinstance(schema, list):
        raise ValueError(f"{schema_path} debe contener una lista de campos de BigQuery")

    return schema


def render_column(field: dict[str, str]) -> str:
    name = field["name"]
    field_type = field["type"]
    mode = field["mode"]

    if mode == "REPEATED":
        return f"  {name} ARRAY<{field_type}>"
    if mode == "REQUIRED":
        return f"  {name} {field_type} NOT NULL"
    return f"  {name} {field_type}"


def render_columns(schema: Iterable[dict[str, str]]) -> str:
    return ",\n".join(render_column(field) for field in schema)


def render_bronze_table(
    *,
    dataset: str,
    schema: list[dict[str, str]],
    project_id: str,
    bucket: str,
) -> str:
    columns = render_columns(schema)

    if dataset == "presupuesto_mef":
        table_name = f"{project_id}.bronze.mef_presupuesto_raw"
        source_uri = f"gs://{bucket}/bronze/mef/presupuesto/extraction_date=*/data.csv"
        options = f"""OPTIONS (
  format = 'CSV',
  uris = ['{source_uri}'],
  skip_leading_rows = 1
)"""
    else:
        table_name = f"{project_id}.bronze.pronabec_{dataset}_raw"
        source_uri = (
            f"gs://{bucket}/bronze/pronabec/{dataset}/extraction_date=*/data.jsonl"
        )
        options = f"""OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['{source_uri}'],
  ignore_unknown_values = TRUE,
  max_bad_records = 0
)"""

    return f"""CREATE OR REPLACE EXTERNAL TABLE `{table_name}` (
{columns}
)
{options};"""


def render_silver_table(
    *,
    dataset: str,
    schema: list[dict[str, str]],
    project_id: str,
) -> str:
    columns = render_columns(schema)

    return f"""CREATE OR REPLACE TABLE `{project_id}.silver.{dataset}` (
{columns}
);"""


def iter_schema_paths(schemas_dir: Path) -> list[Path]:
    return sorted(schemas_dir.glob("*.json"))


def generate_ddl(
    *,
    project_id: str,
    bucket: str,
    output_dir: Path,
    bronze_schemas_dir: Path,
    silver_schemas_dir: Path,
) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    bronze_statements = []
    for schema_path in iter_schema_paths(bronze_schemas_dir):
        dataset = dataset_name_from_schema_path(schema_path)
        bronze_statements.append(
            render_bronze_table(
                dataset=dataset,
                schema=load_schema(schema_path),
                project_id=project_id,
                bucket=bucket,
            )
        )

    silver_statements = []
    for schema_path in iter_schema_paths(silver_schemas_dir):
        dataset = dataset_name_from_schema_path(schema_path)
        silver_statements.append(
            render_silver_table(
                dataset=dataset,
                schema=load_schema(schema_path),
                project_id=project_id,
            )
        )

    bronze_output = output_dir / "create_bronze_external_tables.sql"
    silver_output = output_dir / "create_silver_tables.sql"

    bronze_output.write_text(
        HEADER + "\n\n".join(bronze_statements) + "\n",
        encoding="utf-8",
    )
    silver_output.write_text(
        HEADER + "\n\n".join(silver_statements) + "\n",
        encoding="utf-8",
    )

    return bronze_output, silver_output


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Genera DDL de BigQuery Bronze/Silver a partir de esquemas JSON."
    )
    parser.add_argument("--project-id", default=DEFAULT_PROJECT_ID)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--bronze-schemas-dir", default=DEFAULT_BRONZE_SCHEMAS_DIR)
    parser.add_argument("--silver-schemas-dir", default=DEFAULT_SILVER_SCHEMAS_DIR)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    bronze_output, silver_output = generate_ddl(
        project_id=args.project_id,
        bucket=args.bucket,
        output_dir=Path(args.output_dir),
        bronze_schemas_dir=Path(args.bronze_schemas_dir),
        silver_schemas_dir=Path(args.silver_schemas_dir),
    )
    print(f"Generated {bronze_output}")
    print(f"Generated {silver_output}")


if __name__ == "__main__":
    main()
