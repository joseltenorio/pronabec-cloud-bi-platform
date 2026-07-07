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
            "--bronze-extraction-date",
            "2026-06-17",
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
    assert "gs://test-bucket-name/bronze/mef/presupuesto_producto_temporal/extraction_date=2026-06-17/year=*/data.csv" in bronze_sql
    assert "gs://test-bucket-name/bronze/mef/presupuesto_actividad/extraction_date=2026-06-17/year=*/data.csv" in bronze_sql
    assert "gs://test-bucket-name/bronze/mef/presupuesto_actividad_temporal/extraction_date=2026-06-17/year=*/data.csv" in bronze_sql
    assert "gs://test-bucket-name/bronze/mef/presupuesto_generica_temporal/extraction_date=2026-06-17/year=*/data.csv" in bronze_sql
    
    # Validate pronabec_report family generates as CSV external tables
    assert "test-project-id.bronze.pronabec_report_beca18_sexo_anual_raw" in bronze_sql
    assert "gs://test-bucket-name/bronze/pronabec_reports/report_beca18_sexo_anual/extraction_date=2026-06-17/data.csv" in bronze_sql
    assert "test-project-id.bronze.pronabec_report_beca18_universitarios_universidad_anual_raw" in bronze_sql
    assert "gs://test-bucket-name/bronze/pronabec_reports/report_beca18_universitarios_universidad_anual/extraction_date=2026-06-17/data.csv" in bronze_sql
    assert "test-project-id.bronze.pronabec_report_beca18_universitarios_carrera_anual_raw" in bronze_sql
    assert "gs://test-bucket-name/bronze/pronabec_reports/report_beca18_universitarios_carrera_anual/extraction_date=2026-06-17/data.csv" in bronze_sql
    assert "test-project-id.bronze.inei_population_youth_region_raw" in bronze_sql
    assert "gs://test-bucket-name/bronze/inei_reports/inei_population_youth_region/extraction_date=2026-06-17/data.csv" in bronze_sql
    assert "test-project-id.bronze.minedu_matricula_secundaria_departamental_raw" in bronze_sql
    assert "gs://test-bucket-name/bronze/minedu/escale_matricula_secundaria/extraction_date=2026-06-17/data.csv" in bronze_sql
    assert "CREATE OR REPLACE TABLE" in silver_sql
    assert "test-project-id.silver.pronabec_convocatorias" in silver_sql
    assert "vacantes INTEGER" in silver_sql
    assert "test-project-id.silver.pronabec_report_beca18_universitarios_universidad_anual" in silver_sql
    assert "test-project-id.silver.pronabec_report_beca18_universitarios_carrera_anual" in silver_sql
    assert "test-project-id.silver.inei_population_youth_region" in silver_sql
    assert "test-project-id.silver.minedu_matricula_secundaria_departamental" in silver_sql

    # Validate approved MEF Silver tables are generated
    approved_silver_tables = [
        "presupuesto_mef",
        "presupuesto_mef_temporal",
        "presupuesto_mef_producto",
        "presupuesto_mef_producto_temporal",
        "presupuesto_mef_actividad",
        "presupuesto_mef_actividad_temporal",
        "presupuesto_mef_generica",
        "presupuesto_mef_generica_temporal",
        "presupuesto_mef_hierarchy"
    ]
    for table in approved_silver_tables:
        assert f"test-project-id.silver.{table}" in silver_sql

    # Validate rejected MEF Silver tables are NOT generated
    rejected_silver_tables = [
        "presupuesto_mef_fuente",
        "presupuesto_mef_rubro",
        "presupuesto_mef_departamento"
    ]
    for table in rejected_silver_tables:
        assert f"silver.{table}" not in silver_sql


def test_bigquery_ddl_generator_ci_mode_does_not_require_bronze_date(tmp_path: Path) -> None:
    output_dir = tmp_path / "generated" / "sql"

    subprocess.run(
        [
            sys.executable,
            "tools/generate_bigquery_ddl.py",
            "--project-id",
            "test-project-id",
            "--bucket",
            "test-bucket-name",
            "--generation-mode",
            "ci",
            "--output-dir",
            str(output_dir),
        ],
        cwd=PROJECT_ROOT,
        env=env_without_ddl_config(),
        check=True,
    )

    bronze_sql = (output_dir / "create_bronze_external_tables.sql").read_text(
        encoding="utf-8"
    )
    silver_sql = (output_dir / "create_silver_tables.sql").read_text(
        encoding="utf-8"
    )

    assert "test-project-id.silver.presupuesto_mef_actividad" in silver_sql
    assert (
        "gs://test-bucket-name/bronze/mef/presupuesto_actividad/"
        "extraction_date=${BRONZE_EXTRACTION_DATE}/year=*/data.csv"
    ) in bronze_sql
    assert "extraction_date=*/year=*/data.csv" not in bronze_sql


def test_bigquery_ddl_generator_uses_environment_config(tmp_path: Path) -> None:
    output_dir = tmp_path / "generated" / "sql"
    env = {
        **env_without_ddl_config(),
        "GCP_PROJECT_ID": "env-project-id",
        "GCS_BUCKET_NAME": "env-bucket-name",
        "BRONZE_EXTRACTION_DATE": "2026-06-17",
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
            "--bronze-extraction-date",
            "2026-06-17",
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


def test_mef_silver_schemas_integrity() -> None:
    # 1. Verify approved files exist
    approved_datasets = [
        "presupuesto_mef",
        "presupuesto_mef_temporal",
        "presupuesto_mef_producto",
        "presupuesto_mef_producto_temporal",
        "presupuesto_mef_actividad",
        "presupuesto_mef_actividad_temporal",
        "presupuesto_mef_generica",
        "presupuesto_mef_generica_temporal",
        "presupuesto_mef_hierarchy"
    ]
    for dataset in approved_datasets:
        schema_path = SILVER_SCHEMAS_DIR / f"{dataset}_schema.json"
        assert schema_path.exists(), f"Schema file {schema_path.name} does not exist"

    # 2. Verify rejected files do NOT exist
    rejected_datasets = [
        "presupuesto_mef_fuente",
        "presupuesto_mef_rubro",
        "presupuesto_mef_departamento"
    ]
    for dataset in rejected_datasets:
        schema_path = SILVER_SCHEMAS_DIR / f"{dataset}_schema.json"
        assert not schema_path.exists(), f"Rejected schema file {schema_path.name} should not exist"

    # 3. Check types and modes in approved Silver schemas
    mef_silver_schemas = {d: load_schema(SILVER_SCHEMAS_DIR / f"{d}_schema.json") for d in approved_datasets}

    # Helper function to get field structure
    def get_field(schema: list[dict[str, str]], name: str) -> dict[str, str] | None:
        for f in schema:
            if f["name"] == name:
                return f
        return None

    # ano is INT64 and REQUIRED in all approved schemas
    for dataset, schema in mef_silver_schemas.items():
        field = get_field(schema, "ano")
        assert field is not None, f"Field 'ano' not found in {dataset}"
        assert field["type"] in ("INT64", "INTEGER"), f"Field 'ano' in {dataset} has type {field['type']}, expected INT64/INTEGER"
        assert field["mode"] == "REQUIRED", f"Field 'ano' in {dataset} has mode {field['mode']}, expected REQUIRED"

    # period_valor is STRING and REQUIRED in temporal schemas
    temporal_datasets = [d for d in approved_datasets if d.endswith("temporal")]
    for dataset in temporal_datasets:
        schema = mef_silver_schemas[dataset]
        # check periodo_valor
        field = get_field(schema, "periodo_valor")
        assert field is not None, f"Field 'periodo_valor' not found in temporal schema {dataset}"
        assert field["type"] == "STRING"
        assert field["mode"] == "REQUIRED"

        # check trimestre
        field = get_field(schema, "trimestre")
        assert field is not None, f"Field 'trimestre' not found in temporal schema {dataset}"
        assert field["type"] in ("INT64", "INTEGER")
        assert field["mode"] == "NULLABLE"

        # check mes_numero
        field = get_field(schema, "mes_numero")
        assert field is not None, f"Field 'mes_numero' not found in temporal schema {dataset}"
        assert field["type"] in ("INT64", "INTEGER")
        assert field["mode"] == "NULLABLE"

        # check mes_nombre
        field = get_field(schema, "mes_nombre")
        assert field is not None, f"Field 'mes_nombre' not found in temporal schema {dataset}"
        assert field["type"] == "STRING"
        assert field["mode"] == "NULLABLE"

    # Code fields must be STRING (when they exist)
    code_fields = ["codigo_entidad", "codigo_producto", "codigo_actividad", "codigo_generica"]
    for dataset, schema in mef_silver_schemas.items():
        for code_field in code_fields:
            field = get_field(schema, code_field)
            if field is not None:
                assert field["type"] == "STRING", f"Field '{code_field}' in {dataset} must be STRING"

    # Budget fields must be NUMERIC (when they exist)
    budget_fields = ["pia", "pim", "devengado", "avance_porcentaje"]
    for dataset, schema in mef_silver_schemas.items():
        for budget_field in budget_fields:
            field = get_field(schema, budget_field)
            if field is not None:
                assert field["type"] == "NUMERIC", f"Field '{budget_field}' in {dataset} must be NUMERIC"

    # 4. Check exclusions
    excluded_fields = ["certificacion", "compromiso_anual", "compromiso_mensual", "girado"]
    for dataset, schema in mef_silver_schemas.items():
        fields = field_names(schema)
        for f in excluded_fields:
            assert f not in fields, f"Field '{f}' should be excluded in {dataset} Silver schema"

    # Temporal schemas must not contain pia, pim, avance_porcentaje
    temporal_exclusions = ["pia", "pim", "avance_porcentaje"]
    for dataset in temporal_datasets:
        schema = mef_silver_schemas[dataset]
        fields = field_names(schema)
        for f in temporal_exclusions:
            assert f not in fields, f"Field '{f}' should be excluded in temporal schema {dataset}"

    # Hierarchy schema must not contain periodo_tipo or periodo_valor
    hierarchy_fields = field_names(mef_silver_schemas["presupuesto_mef_hierarchy"])
    assert "periodo_tipo" not in hierarchy_fields
    assert "periodo_valor" not in hierarchy_fields

    # 5. Technical metadata check
    metadata_fields = {
        "source_system": ("STRING", "REQUIRED"),
        "source_dataset": ("STRING", "REQUIRED"),
        "extraction_date": ("DATE", "REQUIRED"),
        "ingestion_timestamp": ("TIMESTAMP", "REQUIRED"),
        "pipeline_run_id": ("STRING", "REQUIRED")
    }
    for dataset, schema in mef_silver_schemas.items():
        for meta_field, (expected_type, expected_mode) in metadata_fields.items():
            field = get_field(schema, meta_field)
            assert field is not None, f"Technical metadata field '{meta_field}' not found in {dataset}"
            assert field["type"] == expected_type, f"Field '{meta_field}' in {dataset} has type {field['type']}, expected {expected_type}"
            assert field["mode"] == expected_mode, f"Field '{meta_field}' in {dataset} has mode {field['mode']}, expected {expected_mode}"


def test_bronze_external_table_wildcard_generation(tmp_path: Path) -> None:
    output_dir = tmp_path / "generated" / "sql"

    subprocess.run(
        [
            sys.executable,
            "tools/generate_bigquery_ddl.py",
            "--project-id",
            "test-project-id",
            "--bucket",
            "test-bucket-name",
            "--bronze-extraction-date",
            "2026-06-17",
            "--output-dir",
            str(output_dir),
        ],
        cwd=PROJECT_ROOT,
        env=env_without_ddl_config(),
        check=True,
    )

    bronze_sql = (output_dir / "create_bronze_external_tables.sql").read_text(
        encoding="utf-8"
    )

    # 1. Validar que ninguna URI externa generada contiene más de un wildcard '*'
    import re
    uri_lines = re.findall(r"uris\s*=\s*\[([^\]]+)\]", bronze_sql)
    for line in uri_lines:
        uris = [u.strip(" '\"") for u in line.split(",")]
        for uri in uris:
            assert uri.count("*") <= 1, f"La URI '{uri}' contiene más de un wildcard '*'"

    # 2. Validar que no exista la URI con doble wildcard para MEF
    assert "extraction_date=*/year=*/data.csv" not in bronze_sql

    # 3. Validar que no quede ninguna URI con wildcard de fecha cuando se provee la fecha
    assert "extraction_date=*" not in bronze_sql

    # 4. Validar rutas específicas de MEF
    assert "gs://test-bucket-name/bronze/mef/presupuesto/extraction_date=2026-06-17/year=*/data.csv" in bronze_sql
    assert "gs://test-bucket-name/bronze/mef/presupuesto_temporal/extraction_date=2026-06-17/year=*/data.csv" in bronze_sql

    # 5. Validar rutas específicas de PRONABEC API
    assert "gs://test-bucket-name/bronze/pronabec/convocatorias/extraction_date=2026-06-17/data.jsonl" in bronze_sql
    assert "gs://test-bucket-name/bronze/pronabec/ubigeo_postulacion/extraction_date=2026-06-17/data.jsonl" in bronze_sql

    # 6. Validar rutas específicas de PRONABEC reports
    assert "gs://test-bucket-name/bronze/pronabec_reports/report_beca18_universitarios_carrera_anual/extraction_date=2026-06-17/data.csv" in bronze_sql
    assert "gs://test-bucket-name/bronze/pronabec_reports/report_beca18_universitarios_universidad_anual/extraction_date=2026-06-17/data.csv" in bronze_sql

    # 7. Validar rutas específicas de INEI reports
    assert "gs://test-bucket-name/bronze/inei_reports/inei_population_youth_region/extraction_date=2026-06-17/data.csv" in bronze_sql
    assert "gs://test-bucket-name/bronze/inei_reports/inei_internet_acceso_region/extraction_date=2026-06-17/data.csv" in bronze_sql


def test_render_bronze_table_fallback_behavior() -> None:
    """
    Valida que al renderizar individualmente sin fecha de extracción:
    - Las tablas PRONABEC API y PRONABEC reports usen por defecto 'extraction_date=*'.
    - Las tablas MEF lancen un ValueError por requerir fecha explícita.
    """
    from tools.generate_bigquery_ddl import render_bronze_table

    # 1. PRONABEC API
    api_ddl = render_bronze_table(
        dataset="notas_becarios",
        schema=[{"name": "col1", "type": "STRING", "mode": "NULLABLE"}],
        project_id="test-project",
        bucket="test-bucket",
        bronze_extraction_date=None,
    )
    assert "gs://test-bucket/bronze/pronabec/notas_becarios/extraction_date=*/data.jsonl" in api_ddl

    # 2. PRONABEC Reports
    report_ddl = render_bronze_table(
        dataset="report_beca18_sexo_anual",
        schema=[{"name": "col1", "type": "STRING", "mode": "NULLABLE"}],
        project_id="test-project",
        bucket="test-bucket",
        bronze_extraction_date=None,
    )
    assert "gs://test-bucket/bronze/pronabec_reports/report_beca18_sexo_anual/extraction_date=*/data.csv" in report_ddl

    # 2b. INEI Reports
    inei_ddl = render_bronze_table(
        dataset="inei_population_youth_region",
        schema=[{"name": "col1", "type": "STRING", "mode": "NULLABLE"}],
        project_id="test-project",
        bucket="test-bucket",
        bronze_extraction_date=None,
    )
    assert "gs://test-bucket/bronze/inei_reports/inei_population_youth_region/extraction_date=*/data.csv" in inei_ddl

    minedu_ddl = render_bronze_table(
        dataset="minedu_matricula_secundaria_departamental",
        schema=[{"name": "col1", "type": "STRING", "mode": "NULLABLE"}],
        project_id="test-project",
        bucket="test-bucket",
        bronze_extraction_date=None,
    )
    assert "gs://test-bucket/bronze/minedu/escale_matricula_secundaria/extraction_date=*/data.csv" in minedu_ddl

    # 3. MEF
    import pytest
    with pytest.raises(ValueError) as excinfo:
        render_bronze_table(
            dataset="presupuesto_mef",
            schema=[{"name": "col1", "type": "STRING", "mode": "NULLABLE"}],
            project_id="test-project",
            bucket="test-bucket",
            bronze_extraction_date=None,
        )
    assert "requires --bronze-extraction-date to be BigQuery-compatible" in str(excinfo.value)

    ci_mef_ddl = render_bronze_table(
        dataset="presupuesto_mef_actividad",
        schema=[{"name": "col1", "type": "STRING", "mode": "NULLABLE"}],
        project_id="test-project",
        bucket="test-bucket",
        bronze_extraction_date=None,
        generation_mode="ci",
    )
    assert (
        "gs://test-bucket/bronze/mef/presupuesto_actividad/"
        "extraction_date=${BRONZE_EXTRACTION_DATE}/year=*/data.csv"
    ) in ci_mef_ddl
    assert "extraction_date=*/year=*/data.csv" not in ci_mef_ddl



def test_mef_external_table_generation_fails_without_date(tmp_path: Path) -> None:
    output_dir = tmp_path / "generated" / "sql"

    # Debe fallar de forma controlada cuando no se pasa fecha para MEF
    result = subprocess.run(
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
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "requires --bronze-extraction-date to be BigQuery-compatible" in result.stderr
