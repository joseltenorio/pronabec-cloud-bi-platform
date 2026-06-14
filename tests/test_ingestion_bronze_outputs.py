import csv
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

from pipelines.common.config import load_yaml_config
from pipelines.extract_pronabec import (
    consolidate_raw_payload,
    normalize_rows,
    write_dataset_to_local,
)
from pipelines.scrape_mef_budget import (
    normalize_mef_records,
    write_mef_to_local,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_pronabec_bronze_outputs_validation(tmp_path: Path) -> None:
    # 1. Load endpoints config to get expected columns
    endpoints_config = load_yaml_config(PROJECT_ROOT / "config" / "endpoints.yaml")
    endpoint = next(
        ep for ep in endpoints_config["pronabec"]["endpoints"] if ep["name"] == "perdida_becas"
    )
    expected_columns = endpoint["expected_columns"]

    # 2. Simulate raw jqGrid pagination response
    raw_rows = [
        {
            "id": "row_101",
            "cell": [
                "1",
                "2024",
                "Lima",
                "Rendimiento Académico",
                "Resolución 123",
                "2024-01-15",
                "2024-03-01",
                "Privada",
                "Universidad X",
                "Sede Central",
                "Carrera A",
                "M",
                "2024-06-10",
            ],
        }
    ]

    # 3. Test normalization mapping
    normalized_records = normalize_rows(raw_rows, expected_columns)
    assert len(normalized_records) == 1
    record = normalized_records[0]

    assert record["source_row_id"] == "row_101"
    for col in expected_columns:
        assert col in record

    # 4. Test consolidate raw payload
    raw_payload = consolidate_raw_payload(
        dataset_name="perdida_becas",
        url="https://datosabiertos.pronabec.gob.pe/Dataset/ListarPerdidaDeBecas",
        pages=[
            {
                "total": 1,
                "page": 1,
                "records": 1,
                "rows": raw_rows,
            }
        ],
    )
    assert raw_payload["dataset"] == "perdida_becas"
    assert raw_payload["pages_read"] == 1
    assert raw_payload["reported_records"] == 1

    # 5. Test local write in dry-run mode
    logger_mock = type("MockLogger", (), {"log": lambda *a, **k: None})()
    
    uris = write_dataset_to_local(
        dataset_name="perdida_becas",
        raw_payload=raw_payload,
        normalized_records=normalized_records,
        extraction_date="2026-06-10",
        output_dir=tmp_path,
        run_id="test_run_123",
        logger=logger_mock,
    )

    # 6. Verify correct file output paths and content
    raw_file = Path(uris["raw_uri"])
    normalized_file = Path(uris["normalized_uri"])
    metadata_file = Path(uris["metadata_path"])

    assert str(raw_file.parent).replace("\\", "/").endswith("bronze/pronabec/perdida_becas/extraction_date=2026-06-10")
    assert raw_file.exists()
    assert normalized_file.exists()
    assert metadata_file.exists()

    # Verify raw JSON format
    raw_data = json.loads(raw_file.read_text(encoding="utf-8"))
    assert "total_pages" in raw_data
    assert raw_data["dataset"] == "perdida_becas"

    # Verify JSONL content
    lines = normalized_file.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    jsonl_record = json.loads(lines[0])
    assert jsonl_record["source_row_id"] == "row_101"
    assert jsonl_record["convocatoria"] == "2024"


def test_mef_bronze_outputs_validation(tmp_path: Path) -> None:
    # 1. Load schema from bronze configuration to get expected columns
    schema_path = PROJECT_ROOT / "config" / "schemas" / "bronze" / "presupuesto_mef_schema.json"
    with schema_path.open("r", encoding="utf-8") as f:
        schema = json.load(f)
    expected_columns = [field["name"] for field in schema]

    # 2. Simulate raw scraper records
    raw_records = [
        {
            "ano": "2026",
            "ejecutora_nombre": "PRONABEC",
            "pia": "1200000.00",
            "pim": "1250000.00",
            "certificacion": "1150000.00",
            "compromiso_anual": "1100000.00",
            "compromiso_mensual": "95000.00",
            "devengado": "900000.00",
            "girado": "850000.00",
            "avance_porcentaje": "72.0",
        }
    ]

    # 3. Test MEF normalization
    normalized_records = normalize_mef_records(raw_records, expected_columns)
    assert len(normalized_records) == 1
    record = normalized_records[0]
    for col in expected_columns:
        assert col in record
        assert record[col] == raw_records[0][col]

    # 4. Test local write in dry-run mode
    logger_mock = type("MockLogger", (), {"log": lambda *a, **k: None})()

    uris = write_mef_to_local(
        records=normalized_records,
        fieldnames=expected_columns,
        extraction_date="2026-06-10",
        output_dir=tmp_path,
        run_id="test_mef_run_123",
        records_read=len(raw_records),
        source_url="http://test-url/mef.csv",
        source_file=None,
        logger=logger_mock,
    )

    csv_file = Path(uris["output_uri"])
    metadata_file = Path(uris["metadata_path"])

    assert str(csv_file.parent).replace("\\", "/").endswith("bronze/mef/presupuesto/extraction_date=2026-06-10")
    assert csv_file.exists()
    assert metadata_file.exists()

    # 5. Verify CSV headers and non-empty content
    with csv_file.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        row = next(reader)

    assert header == expected_columns
    assert row[0] == "2026"
    assert row[1] == "PRONABEC"


def test_gcs_paths_consistency_with_ddl(tmp_path: Path) -> None:
    # 1. Load paths config
    pipeline_settings = load_yaml_config(PROJECT_ROOT / "config" / "pipeline.yaml")
    pronabec_norm_tmpl = pipeline_settings["gcs_paths"]["pronabec_bronze_normalized"]
    mef_tmpl = pipeline_settings["gcs_paths"]["mef_bronze"]

    # 2. Generate DDL dynamically into tmp_path using generate_bigquery_ddl.py
    output_dir = tmp_path / "generated" / "sql"
    env = dict(os.environ)
    env.pop("GCP_PROJECT_ID", None)
    env.pop("GCS_BUCKET_NAME", None)
    env["DISABLE_DOTENV"] = "1"

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
        env=env,
        check=True,
    )

    ddl_path = output_dir / "create_bronze_external_tables.sql"
    assert ddl_path.exists()
    ddl_content = ddl_path.read_text(encoding="utf-8")

    # 3. Find GCS Wildcard URIs in DDL options
    gcs_uris = re.findall(r"uris\s*=\s*\[\s*'gs://([^']+)'\s*\]", ddl_content)
    assert len(gcs_uris) > 0, "No external GCS URIs found in BQ DDL SQL"

    for uri in gcs_uris:
        # Split off bucket name
        parts = uri.split("/", 1)
        assert len(parts) == 2
        relative_path = parts[1]

        # Verify against templates
        if "bronze/pronabec/" in relative_path:
            # Format is bronze/pronabec/{dataset}/extraction_date={extraction_date}/data.jsonl
            # Extract dataset name
            match = re.match(r"bronze/pronabec/([^/]+)/extraction_date=\*/data\.jsonl", relative_path)
            assert match, f"DDL PRONABEC path structure is invalid: {relative_path}"
            dataset_name = match.group(1)

            # Format the template with wildcards and check match
            expected = pronabec_norm_tmpl.format(dataset=dataset_name, extraction_date="*")
            assert relative_path == expected, f"DDL path '{relative_path}' mismatch with pipeline.yaml expected '{expected}'"
        elif "bronze/mef/" in relative_path:
            # Format is bronze/mef/<slice>/extraction_date={extraction_date}/data.csv
            match = re.match(r"bronze/mef/([^/]+)/extraction_date=\*/data\.csv", relative_path)
            assert match, f"DDL MEF path structure is invalid: {relative_path}"
            slice_name = match.group(1)

            expected = mef_tmpl.replace("presupuesto", slice_name).format(extraction_date="*")
            assert relative_path == expected, f"DDL path '{relative_path}' mismatch with pipeline.yaml expected '{expected}'"
        else:
            pytest.fail(f"Unknown bronze folder in external table DDL: {relative_path}")
