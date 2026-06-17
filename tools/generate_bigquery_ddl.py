import argparse
import json
import os
from pathlib import Path
from typing import Iterable


PROJECT_ID_ENV_VAR = "GCP_PROJECT_ID"
BUCKET_ENV_VAR = "GCS_BUCKET_NAME"
DEFAULT_OUTPUT_DIR = "build/generated/sql"
DEFAULT_BRONZE_SCHEMAS_DIR = "config/schemas/bronze"
DEFAULT_SILVER_SCHEMAS_DIR = "config/schemas/silver"

HEADER = """-- ============================================================================
-- Project Cloud BI Platform
-- DDL BigQuery generado automáticamente
-- ============================================================================
--
-- Archivo Generado Automáticamente - No editar manualmente
--
-- Generado por:
--   tools/generate_bigquery_ddl.py
--
-- Fuente de verdad:
--   config/schemas/bronze/*.json
--   config/schemas/silver/*.json
--
-- Ubicación esperada:
--   build/generated/sql/
--
-- Este archivo se genera para validación local y despliegue CI/CD.
-- No debe versionarse como fuente principal del esquema.
--
-- Para modificar columnas, tipos o modos:
--   1. Actualizar el JSON schema correspondiente.
--   2. Ejecutar nuevamente tools/generate_bigquery_ddl.py.
--   3. Validar el DDL generado antes de desplegarlo en BigQuery.
--
-- Configuración requerida:
--   - Proyecto GCP: --project-id o variable GCP_PROJECT_ID.
--   - Bucket GCS:   --bucket o variable GCS_BUCKET_NAME.
--
-- Decisiones de diseño:
--   - Bronze físico vive en Cloud Storage.
--   - BigQuery Bronze usa tablas externas sobre archivos Bronze.
--   - PRONABEC se consulta desde data.jsonl normalizado estructuralmente.
--   - PRONABEC conserva data_raw.json para trazabilidad, pero las tablas
--     externas se definen sobre data.jsonl.
--   - MEF se consulta desde data.csv generado por el scraper controlado.
-- ============================================================================


"""


def load_dotenv_if_available() -> None:
    """Carga variables desde .env en ejecución local, salvo que se desactive explícitamente."""
    if os.getenv("DISABLE_DOTENV") == "1":
        return

    try:
        from dotenv import load_dotenv
    except ImportError:
        return

    load_dotenv()


def clean_config_value(value: str | None) -> str | None:
    if value is None:
        return None

    value = value.strip()
    return value or None


def resolve_required_config(
    *,
    cli_value: str | None,
    env_var_name: str,
    option_name: str,
    parser: argparse.ArgumentParser,
) -> str:
    value = clean_config_value(cli_value) or clean_config_value(os.getenv(env_var_name))

    if value is None:
        parser.error(
            f"Falta configuración requerida para {option_name}. "
            f"Use {option_name} o defina la variable {env_var_name} en el entorno/.env."
        )

    return value


def resolve_optional_config(
    *,
    cli_value: str | None,
    env_var_name: str,
) -> str | None:
    return clean_config_value(cli_value) or clean_config_value(os.getenv(env_var_name))



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
    bronze_extraction_date: str | None = None,
) -> str:
    columns = render_columns(schema)

    if dataset.startswith("presupuesto_mef"):
        if dataset == "presupuesto_mef":
            slice_name = "presupuesto"
        else:
            slice_name = dataset.replace("presupuesto_mef_", "presupuesto_")

        if not bronze_extraction_date:
            raise ValueError(
                f"MEF external table DDL for '{dataset}' requires --bronze-extraction-date to be BigQuery-compatible."
            )
        date_folder = f"extraction_date={bronze_extraction_date}" if bronze_extraction_date else "extraction_date=*"
        table_name = f"{project_id}.bronze.mef_{slice_name}_raw"
        source_uri = f"gs://{bucket}/bronze/mef/{slice_name}/{date_folder}/year=*/data.csv"
        options = f"""OPTIONS (
  format = 'CSV',
  uris = ['{source_uri}'],
  skip_leading_rows = 1
)"""
    elif dataset.startswith("report_"):
        table_name = f"{project_id}.bronze.pronabec_{dataset}_raw"
        date_folder = f"extraction_date={bronze_extraction_date}" if bronze_extraction_date else "extraction_date=*"
        source_uri = f"gs://{bucket}/bronze/pronabec_reports/{dataset}/{date_folder}/data.csv"
        options = f"""OPTIONS (
  format = 'CSV',
  uris = ['{source_uri}'],
  skip_leading_rows = 1
)"""
    else:
        table_name = f"{project_id}.bronze.pronabec_{dataset}_raw"
        date_folder = f"extraction_date={bronze_extraction_date}" if bronze_extraction_date else "extraction_date=*"
        source_uri = (
            f"gs://{bucket}/bronze/pronabec/{dataset}/{date_folder}/data.jsonl"
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
    bronze_extraction_date: str | None = None,
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
                bronze_extraction_date=bronze_extraction_date,
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
    load_dotenv_if_available()

    parser = argparse.ArgumentParser(
        description="Genera DDL de BigQuery Bronze/Silver a partir de esquemas JSON."
    )
    parser.add_argument(
        "--project-id",
        default=None,
        help=f"ID del proyecto GCP. También puede definirse con {PROJECT_ID_ENV_VAR}.",
    )
    parser.add_argument(
        "--bucket",
        default=None,
        help=f"Bucket de Cloud Storage. También puede definirse con {BUCKET_ENV_VAR}.",
    )
    parser.add_argument(
        "--bronze-extraction-date",
        default=None,
        help="Fecha de extracción Bronze para URIs externas (formato YYYY-MM-DD). También se puede definir con BRONZE_EXTRACTION_DATE.",
    )
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--bronze-schemas-dir", default=DEFAULT_BRONZE_SCHEMAS_DIR)
    parser.add_argument("--silver-schemas-dir", default=DEFAULT_SILVER_SCHEMAS_DIR)

    args = parser.parse_args()

    args.project_id = resolve_required_config(
        cli_value=args.project_id,
        env_var_name=PROJECT_ID_ENV_VAR,
        option_name="--project-id",
        parser=parser,
    )
    args.bucket = resolve_required_config(
        cli_value=args.bucket,
        env_var_name=BUCKET_ENV_VAR,
        option_name="--bucket",
        parser=parser,
    )
    args.bronze_extraction_date = resolve_optional_config(
        cli_value=args.bronze_extraction_date,
        env_var_name="BRONZE_EXTRACTION_DATE",
    )

    return args


def main() -> None:
    args = parse_args()

    bronze_output, silver_output = generate_ddl(
        project_id=args.project_id,
        bucket=args.bucket,
        output_dir=Path(args.output_dir),
        bronze_schemas_dir=Path(args.bronze_schemas_dir),
        silver_schemas_dir=Path(args.silver_schemas_dir),
        bronze_extraction_date=args.bronze_extraction_date,
    )

    print(f"Generated {bronze_output}")
    print(f"Generated {silver_output}")


if __name__ == "__main__":
    main()