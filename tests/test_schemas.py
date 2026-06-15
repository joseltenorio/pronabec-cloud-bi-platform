import json
import os
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


def env_without_ddl_config() -> dict[str, str]:
    env = dict(os.environ)
    env.pop("GCP_PROJECT_ID", None)
    env.pop("GCS_BUCKET_NAME", None)
    env["DISABLE_DOTENV"] = "1"
    return env


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
            "--project-id",
            "test-project-id",
            "--bucket",
            "test-bucket-name",
            "--output-dir",
            str(output_dir),
        ],
        cwd=PROJECT_ROOT,
        env=env_without_ddl_config(),
        check=True,
    )

    bronze_sql_path = output_dir / "create_bronze_external_tables.sql"
    silver_sql_path = output_dir / "create_silver_tables.sql"

    assert bronze_sql_path.exists()
    assert silver_sql_path.exists()

    bronze_sql = bronze_sql_path.read_text(encoding="utf-8")
    silver_sql = silver_sql_path.read_text(encoding="utf-8")

    assert "Archivo Generado Automáticamente - No editar manualmente" in bronze_sql
    assert "CREATE OR REPLACE EXTERNAL TABLE" in bronze_sql
    assert "NEWLINE_DELIMITED_JSON" in bronze_sql
    assert "CSV" in bronze_sql
    assert "test-project-id.bronze.pronabec_notas_becarios_raw" in bronze_sql
    assert "test-project-id.bronze.mef_presupuesto_raw" in bronze_sql
    assert "test-project-id.bronze.mef_presupuesto_hierarchy_raw" in bronze_sql
    assert "test-project-id.bronze.mef_presupuesto_producto_raw" in bronze_sql
    assert "gs://test-bucket-name/bronze/pronabec/notas_becarios" in bronze_sql
    assert "gs://test-bucket-name/bronze/mef/presupuesto/extraction_date" in bronze_sql
    assert "gs://test-bucket-name/bronze/mef/presupuesto_hierarchy/extraction_date" in bronze_sql
    assert "gs://test-bucket-name/bronze/mef/presupuesto_producto/extraction_date" in bronze_sql
    assert "test-project-id.bronze.mef_presupuesto_producto_temporal_raw" in bronze_sql
    assert "test-project-id.bronze.mef_presupuesto_actividad_raw" in bronze_sql
    assert "test-project-id.bronze.mef_presupuesto_actividad_temporal_raw" in bronze_sql
    assert "test-project-id.bronze.mef_presupuesto_generica_temporal_raw" in bronze_sql
    assert "gs://test-bucket-name/bronze/mef/presupuesto_producto_temporal/extraction_date=*/year=*/data.csv" in bronze_sql
    assert "gs://test-bucket-name/bronze/mef/presupuesto_actividad/extraction_date=*/year=*/data.csv" in bronze_sql
    assert "gs://test-bucket-name/bronze/mef/presupuesto_actividad_temporal/extraction_date=*/year=*/data.csv" in bronze_sql
    assert "gs://test-bucket-name/bronze/mef/presupuesto_generica_temporal/extraction_date=*/year=*/data.csv" in bronze_sql

    assert "CREATE OR REPLACE TABLE" in silver_sql
    assert "test-project-id.silver.pronabec_convocatorias" in silver_sql
    assert "vacantes INTEGER" in silver_sql

def test_bigquery_ddl_generator_uses_environment_config(tmp_path: Path) -> None:
    output_dir = tmp_path / "generated" / "sql"
    env = {
        **env_without_ddl_config(),
        "GCP_PROJECT_ID": "env-project-id",
        "GCS_BUCKET_NAME": "env-bucket-name",
    }

    subprocess.run(
        [
            sys.executable,
            "tools/generate_bigquery_ddl.py",
            "--output-dir",
            str(output_dir),
        ],
        cwd=PROJECT_ROOT,
        env=env,
        check=True,
    )

    bronze_sql = (output_dir / "create_bronze_external_tables.sql").read_text(
        encoding="utf-8"
    )
    silver_sql = (output_dir / "create_silver_tables.sql").read_text(
        encoding="utf-8"
    )

    assert "env-project-id.bronze" in bronze_sql
    assert "gs://env-bucket-name/bronze" in bronze_sql
    assert "env-project-id.silver" in silver_sql


def test_bigquery_ddl_generator_cli_overrides_environment(tmp_path: Path) -> None:
    output_dir = tmp_path / "generated" / "sql"
    env = {
        **env_without_ddl_config(),
        "GCP_PROJECT_ID": "env-project-id",
        "GCS_BUCKET_NAME": "env-bucket-name",
    }

    subprocess.run(
        [
            sys.executable,
            "tools/generate_bigquery_ddl.py",
            "--project-id",
            "cli-project-id",
            "--bucket",
            "cli-bucket-name",
            "--output-dir",
            str(output_dir),
        ],
        cwd=PROJECT_ROOT,
        env=env,
        check=True,
    )

    bronze_sql = (output_dir / "create_bronze_external_tables.sql").read_text(
        encoding="utf-8"
    )
    silver_sql = (output_dir / "create_silver_tables.sql").read_text(
        encoding="utf-8"
    )

    assert "cli-project-id.bronze" in bronze_sql
    assert "gs://cli-bucket-name/bronze" in bronze_sql
    assert "cli-project-id.silver" in silver_sql
    assert "env-project-id" not in bronze_sql
    assert "env-bucket-name" not in bronze_sql


def test_bigquery_ddl_generator_requires_project_and_bucket(tmp_path: Path) -> None:
    output_dir = tmp_path / "generated" / "sql"

    result = subprocess.run(
        [
            sys.executable,
            "tools/generate_bigquery_ddl.py",
            "--output-dir",
            str(output_dir),
        ],
        cwd=PROJECT_ROOT,
        env=env_without_ddl_config(),
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert "Falta configuración requerida" in result.stderr
    assert "--project-id" in result.stderr


def test_mef_expanded_budget_schemas_integrity() -> None:
    # 1. Verify files exist
    expected_schemas = [
        "presupuesto_mef_producto_temporal_schema.json",
        "presupuesto_mef_actividad_schema.json",
        "presupuesto_mef_actividad_temporal_schema.json",
        "presupuesto_mef_generica_temporal_schema.json",
    ]
    for s in expected_schemas:
        path = BRONZE_SCHEMAS_DIR / s
        assert path.exists(), f"Schema file {s} does not exist"

    # 2. Check all fields in all MEF Bronze schemas are STRING and NULLABLE
    mef_schemas = sorted(BRONZE_SCHEMAS_DIR.glob("presupuesto_mef*.json"))
    for path in mef_schemas:
        schema = load_schema(path)
        for field in schema:
            assert field["type"] == "STRING", f"Field {field['name']} in {path.name} is not STRING"
            assert field["mode"] == "NULLABLE", f"Field {field['name']} in {path.name} is not NULLABLE"

    # 3. Base schema contains ejecutora_codigo and ejecutora_nombre
    base_schema = load_schema(BRONZE_SCHEMAS_DIR / "presupuesto_mef_schema.json")
    base_fields = field_names(base_schema)
    assert "ejecutora_codigo" in base_fields
    assert "ejecutora_nombre" in base_fields

    # 4. Temporal general schema contains temporal fields
    temp_general = load_schema(BRONZE_SCHEMAS_DIR / "presupuesto_mef_temporal_schema.json")
    temp_general_fields = field_names(temp_general)
    temporal_fields = {"periodo_tipo", "periodo_valor", "trimestre", "mes_numero", "mes_nombre"}
    assert temporal_fields.issubset(temp_general_fields)

    # 5. Producto temporal schema contains product fields and temporal fields
    prod_temp = load_schema(BRONZE_SCHEMAS_DIR / "presupuesto_mef_producto_temporal_schema.json")
    prod_temp_fields = field_names(prod_temp)
    assert {"codigo_producto", "producto"}.issubset(prod_temp_fields)
    assert temporal_fields.issubset(prod_temp_fields)

    # 6. Actividad schema contains product and activity fields
    act = load_schema(BRONZE_SCHEMAS_DIR / "presupuesto_mef_actividad_schema.json")
    act_fields = field_names(act)
    assert {"codigo_producto", "producto", "codigo_actividad", "actividad"}.issubset(act_fields)
    # verify it does not contain temporal slice columns (periodo_tipo, periodo_valor)
    assert "periodo_tipo" not in act_fields
    assert "periodo_valor" not in act_fields

    # 7. Actividad temporal schema contains activity, product, and temporal fields
    act_temp = load_schema(BRONZE_SCHEMAS_DIR / "presupuesto_mef_actividad_temporal_schema.json")
    act_temp_fields = field_names(act_temp)
    assert {"codigo_producto", "producto", "codigo_actividad", "actividad"}.issubset(act_temp_fields)
    assert temporal_fields.issubset(act_temp_fields)

    # 8. Generica temporal schema contains generic expense and temporal fields
    gen_temp = load_schema(BRONZE_SCHEMAS_DIR / "presupuesto_mef_generica_temporal_schema.json")
    gen_temp_fields = field_names(gen_temp)
    assert {"codigo_generica", "generica"}.issubset(gen_temp_fields)
    assert temporal_fields.issubset(gen_temp_fields)