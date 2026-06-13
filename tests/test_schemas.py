import json
import subprocess
import sys
from pathlib import Path

from pipelines.common.config import load_yaml_config


PROJECT_ROOT = Path(__file__).resolve().parents[1]
BRONZE_SCHEMAS_DIR = PROJECT_ROOT / "config" / "schemas" / "bronze"
SILVER_SCHEMAS_DIR = PROJECT_ROOT / "config" / "schemas" / "silver"

ALLOWED_TYPES = {
    "STRING",
    "INTEGER",
    "INT64",
    "NUMERIC",
    "FLOAT",
    "FLOAT64",
    "DATE",
    "DATETIME",
    "TIMESTAMP",
    "BOOLEAN",
    "BOOL",
}
ALLOWED_MODES = {"NULLABLE", "REQUIRED", "REPEATED"}
SILVER_METADATA_FIELDS = {
    "source_system",
    "source_dataset",
    "extraction_date",
    "ingestion_timestamp",
    "pipeline_run_id",
}


def schema_paths() -> list[Path]:
    return sorted(BRONZE_SCHEMAS_DIR.glob("*.json")) + sorted(
        SILVER_SCHEMAS_DIR.glob("*.json")
    )


def load_schema(path: Path) -> list[dict[str, str]]:
    return json.loads(path.read_text(encoding="utf-8"))


def field_names(schema: list[dict[str, str]]) -> set[str]:
    return {field["name"] for field in schema}


def test_json_schemas_are_valid_bigquery_field_lists() -> None:
    for schema_path in schema_paths():
        schema = load_schema(schema_path)

        assert isinstance(schema, list), f"{schema_path} must be a list"
        for field in schema:
            assert "name" in field, f"{schema_path} field is missing name"
            assert "type" in field, f"{schema_path} field is missing type"
            assert "mode" in field, f"{schema_path} field is missing mode"
            assert field["type"] in ALLOWED_TYPES, (
                f"{schema_path} has unsupported type {field['type']}"
            )
            assert field["mode"] in ALLOWED_MODES, (
                f"{schema_path} has unsupported mode {field['mode']}"
            )


def test_enabled_pronabec_endpoints_have_bronze_schemas() -> None:
    endpoints_config = load_yaml_config(PROJECT_ROOT / "config" / "endpoints.yaml")

    for endpoint in endpoints_config["pronabec"]["endpoints"]:
        if not endpoint.get("enabled", False):
            continue

        dataset = endpoint["name"]
        schema_path = BRONZE_SCHEMAS_DIR / f"{dataset}_schema.json"
        assert schema_path.exists(), f"Missing Bronze schema for {dataset}"

        names = field_names(load_schema(schema_path))
        for expected_column in endpoint["expected_columns"]:
            assert expected_column in names, (
                f"{schema_path} is missing expected column {expected_column}"
            )
        assert "source_row_id" in names, f"{schema_path} is missing source_row_id"


def test_silver_schemas_contain_metadata_fields() -> None:
    for schema_path in sorted(SILVER_SCHEMAS_DIR.glob("*.json")):
        names = field_names(load_schema(schema_path))

        missing = SILVER_METADATA_FIELDS - names
        assert not missing, f"{schema_path} is missing metadata fields {sorted(missing)}"


def test_bigquery_ddl_generator_writes_bronze_and_silver_sql(tmp_path: Path) -> None:
    output_dir = tmp_path / "generated" / "sql"

    subprocess.run(
        [
            sys.executable,
            "tools/generate_bigquery_ddl.py",
            "--output-dir",
            str(output_dir),
        ],
        cwd=PROJECT_ROOT,
        check=True,
    )

    bronze_sql_path = output_dir / "create_bronze_external_tables.sql"
    silver_sql_path = output_dir / "create_silver_tables.sql"

    assert bronze_sql_path.exists()
    assert silver_sql_path.exists()

    bronze_sql = bronze_sql_path.read_text(encoding="utf-8")
    silver_sql = silver_sql_path.read_text(encoding="utf-8")

    assert "AUTO-GENERATED FILE" in bronze_sql
    assert "CREATE OR REPLACE EXTERNAL TABLE" in bronze_sql
    assert "NEWLINE_DELIMITED_JSON" in bronze_sql
    assert "CSV" in bronze_sql
    assert "bronze.pronabec_notas_becarios_raw" in bronze_sql
    assert "bronze.mef_presupuesto_raw" in bronze_sql

    assert "AUTO-GENERATED FILE" in silver_sql
    assert "CREATE OR REPLACE TABLE" in silver_sql
    assert "silver.notas_becarios" in silver_sql
    assert "nota_promedio NUMERIC" in silver_sql
