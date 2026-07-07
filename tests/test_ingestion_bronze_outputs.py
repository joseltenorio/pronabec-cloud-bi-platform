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
    write_mef_breakdown_to_local,
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
            "ejecutora_codigo": "117-1438",
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

    assert str(csv_file.parent).replace("\\", "/").endswith("bronze/mef/presupuesto/extraction_date=2026-06-10/year=2026")
    assert csv_file.exists()
    assert metadata_file.exists()

    # 5. Verify CSV headers and non-empty content
    with csv_file.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        row = next(reader)

    assert header == expected_columns
    assert row[0] == "2026"
    assert row[1] == "117-1438"
    assert row[2] == "PRONABEC"


def test_gcs_paths_consistency_with_ddl(tmp_path: Path) -> None:
    # 1. Load paths config
    pipeline_settings = load_yaml_config(PROJECT_ROOT / "config" / "pipeline.yaml")
    pronabec_norm_tmpl = pipeline_settings["gcs_paths"]["pronabec_bronze_normalized"]
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
            "--bronze-extraction-date",
            "2026-06-17",
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
            match = re.match(r"bronze/pronabec/([^/]+)/extraction_date=(\d{4}-\d{2}-\d{2}|\*)/data\.jsonl", relative_path)
            assert match, f"DDL PRONABEC path structure is invalid: {relative_path}"
            dataset_name = match.group(1)
            ext_date = match.group(2)

            # Format the template with wildcards and check match
            expected = pronabec_norm_tmpl.format(dataset=dataset_name, extraction_date=ext_date)
            assert relative_path == expected, f"DDL path '{relative_path}' mismatch with pipeline.yaml expected '{expected}'"
        elif "bronze/mef/" in relative_path:
            # Format is bronze/mef/<slice>/extraction_date=YYYY-MM-DD/year=*/data.csv
            match = re.match(r"bronze/mef/([^/]+)/extraction_date=(\d{4}-\d{2}-\d{2})/year=\*/data\.csv", relative_path)
            assert match, f"DDL MEF path structure is invalid: {relative_path}"
            slice_name = match.group(1)
            ext_date = match.group(2)

            expected = f"bronze/mef/{slice_name}/extraction_date={ext_date}/year=*/data.csv"
            assert relative_path == expected, f"DDL path '{relative_path}' mismatch with expected '{expected}'"
        elif "bronze/pronabec_reports/" in relative_path:
            # Format is bronze/pronabec_reports/{dataset}/extraction_date=*/data.csv
            match = re.match(r"bronze/pronabec_reports/([^/]+)/extraction_date=(\d{4}-\d{2}-\d{2}|\*)/data\.csv", relative_path)
            assert match, f"DDL PRONABEC Reports path structure is invalid: {relative_path}"
            dataset_name = match.group(1)
            ext_date = match.group(2)

            expected = pipeline_settings["gcs_paths"]["pronabec_reports_bronze_csv"].format(dataset=dataset_name, extraction_date=ext_date)
            assert relative_path == expected, f"DDL path '{relative_path}' mismatch with pipeline.yaml expected '{expected}'"
        elif "bronze/inei_reports/" in relative_path:
            match = re.match(r"bronze/inei_reports/([^/]+)/extraction_date=(\d{4}-\d{2}-\d{2}|\*)/data\.csv", relative_path)
            assert match, f"DDL INEI Reports path structure is invalid: {relative_path}"
            dataset_name = match.group(1)
            ext_date = match.group(2)

            expected = pipeline_settings["gcs_paths"]["inei_reports_bronze_csv"].format(dataset=dataset_name, extraction_date=ext_date)
            assert relative_path == expected, f"DDL path '{relative_path}' mismatch with pipeline.yaml expected '{expected}'"
        elif "bronze/minedu/" in relative_path:
            match = re.match(
                r"bronze/minedu/escale_matricula_secundaria/extraction_date=(\d{4}-\d{2}-\d{2}|\*)/data\.csv",
                relative_path,
            )
            assert match, f"DDL MINEDU path structure is invalid: {relative_path}"
            ext_date = match.group(1)

            expected = pipeline_settings["gcs_paths"]["minedu_escale_bronze"].format(
                extraction_date=ext_date
            )
            assert relative_path == expected, f"DDL path '{relative_path}' mismatch with pipeline.yaml expected '{expected}'"
        else:
            pytest.fail(f"Unknown bronze folder in external table DDL: {relative_path}")


def test_mef_temporal_slice_outputs_validation(tmp_path: Path) -> None:
    # 1. Simulate temporal records with mixed granularities
    records = [
        {
            "ano": "2026",
            "periodo_tipo": "MENSUAL",
            "periodo_valor": "2026-01",
            "trimestre": "1",
            "mes_numero": "01",
            "mes_nombre": "ENERO",
            "pia": "1000",
            "pim": "2000",
            "certificacion": "1900",
            "compromiso_anual": "1500",
            "compromiso_mensual": "1100",
            "devengado": "1000",
            "girado": "900",
            "avance_porcentaje": "50.0",
        },
        {
            "ano": "2026",
            "periodo_tipo": "TRIMESTRAL",
            "periodo_valor": "2026-T1",
            "trimestre": "1",
            "mes_numero": "",
            "mes_nombre": "",
            "pia": "3000",
            "pim": "4000",
            "certificacion": "3900",
            "compromiso_anual": "3500",
            "compromiso_mensual": "3100",
            "devengado": "3000",
            "girado": "2900",
            "avance_porcentaje": "75.0",
        }
    ]

    logger_mock = type("MockLogger", (), {"log": lambda *a, **k: None})()

    # 2. Test writing temporal slice
    res = write_mef_breakdown_to_local(
        records=records,
        extraction_date="2026-06-10",
        output_dir=tmp_path,
        run_id="test_mef_run_123",
        records_read=len(records),
        source_url="http://test-url/mef.html",
        slice_name="temporal",
        logger=logger_mock,
        fiscal_year="2026",
    )

    csv_file = Path(res["output_uri"])
    meta_file = Path(res["metadata_path"])

    assert str(csv_file.parent).replace("\\", "/").endswith("bronze/mef/presupuesto_temporal/extraction_date=2026-06-10/year=2026")
    assert csv_file.exists()
    assert meta_file.exists()


def test_mef_new_slices_outputs_validation(tmp_path: Path) -> None:
    # 1. Product temporal records
    prod_temp_records = [
        {
            "ano": "2026",
            "periodo_tipo": "MENSUAL",
            "periodo_valor": "2026-01",
            "trimestre": "1",
            "mes_numero": "01",
            "mes_nombre": "ENERO",
            "codigo_producto": "3000885",
            "producto": "ENTREGA DE BECA",
            "pia": "1000",
            "pim": "2000",
            "certificacion": "1900",
            "compromiso_anual": "1500",
            "compromiso_mensual": "1100",
            "devengado": "1000",
            "girado": "900",
            "avance_porcentaje": "50.0",
        }
    ]

    logger_mock = type("MockLogger", (), {"log": lambda *a, **k: None})()

    # Write producto_temporal
    res1 = write_mef_breakdown_to_local(
        records=prod_temp_records,
        extraction_date="2026-06-10",
        output_dir=tmp_path,
        run_id="test_run_123",
        records_read=len(prod_temp_records),
        source_url="http://test/url",
        slice_name="producto_temporal",
        logger=logger_mock,
        fiscal_year="2026",
    )
    csv1 = Path(res1["output_uri"])
    meta1 = Path(res1["metadata_path"])
    assert str(csv1.parent).replace("\\", "/").endswith("bronze/mef/presupuesto_producto_temporal/extraction_date=2026-06-10/year=2026")
    assert csv1.exists()
    assert meta1.exists()

    # 2. Actividad records
    actividad_records = [
        {
            "ano": "2026",
            "codigo_producto": "3000885",
            "producto": "ENTREGA DE BECA",
            "codigo_actividad": "5006319",
            "actividad": "APLICACION DE MECANISMOS",
            "pia": "1000",
            "pim": "2000",
            "certificacion": "1900",
            "compromiso_anual": "1500",
            "compromiso_mensual": "1100",
            "devengado": "1000",
            "girado": "900",
            "avance_porcentaje": "50.0",
        }
    ]

    # 3. Actividad temporal records
    act_temp_records = [
        {
            "ano": "2026",
            "periodo_tipo": "MENSUAL",
            "periodo_valor": "2026-01",
            "trimestre": "1",
            "mes_numero": "01",
            "mes_nombre": "ENERO",
            "codigo_producto": "3000885",
            "producto": "ENTREGA DE BECA",
            "codigo_actividad": "5006319",
            "actividad": "APLICACION DE MECANISMOS",
            "pia": "1000",
            "pim": "2000",
            "certificacion": "1900",
            "compromiso_anual": "1500",
            "compromiso_mensual": "1100",
            "devengado": "1000",
            "girado": "900",
            "avance_porcentaje": "50.0",
        }
    ]

    # Write actividad
    res2 = write_mef_breakdown_to_local(
        records=actividad_records,
        extraction_date="2026-06-10",
        output_dir=tmp_path,
        run_id="test_run_123",
        records_read=len(actividad_records),
        source_url="http://test/url",
        slice_name="actividad",
        logger=logger_mock,
        fiscal_year="2026",
    )
    csv2 = Path(res2["output_uri"])
    meta2 = Path(res2["metadata_path"])
    assert str(csv2.parent).replace("\\", "/").endswith("bronze/mef/presupuesto_actividad/extraction_date=2026-06-10/year=2026")
    assert csv2.exists()
    assert meta2.exists()

    # Write actividad_temporal
    res3 = write_mef_breakdown_to_local(
        records=act_temp_records,
        extraction_date="2026-06-10",
        output_dir=tmp_path,
        run_id="test_run_123",
        records_read=len(act_temp_records),
        source_url="http://test/url",
        slice_name="actividad_temporal",
        logger=logger_mock,
        fiscal_year="2026",
    )
    csv3 = Path(res3["output_uri"])
    meta3 = Path(res3["metadata_path"])
    assert str(csv3.parent).replace("\\", "/").endswith("bronze/mef/presupuesto_actividad_temporal/extraction_date=2026-06-10/year=2026")
    assert csv3.exists()
    assert meta3.exists()

    # 4. Generica temporal records
    gen_temp_records = [
        {
            "ano": "2026",
            "periodo_tipo": "MENSUAL",
            "periodo_valor": "2026-05",
            "trimestre": "2",
            "mes_numero": "05",
            "mes_nombre": "MAYO",
            "codigo_generica": "5-23",
            "generica": "BIENES Y SERVICIOS",
            "pia": "1000",
            "pim": "2000",
            "certificacion": "1900",
            "compromiso_anual": "1500",
            "compromiso_mensual": "1100",
            "devengado": "1000",
            "girado": "900",
            "avance_porcentaje": "50.0",
        }
    ]

    # Write generica_temporal
    res4 = write_mef_breakdown_to_local(
        records=gen_temp_records,
        extraction_date="2026-06-10",
        output_dir=tmp_path,
        run_id="test_run_123",
        records_read=len(gen_temp_records),
        source_url="http://test/url",
        slice_name="generica_temporal",
        logger=logger_mock,
        fiscal_year="2026",
    )
    csv4 = Path(res4["output_uri"])
    meta4 = Path(res4["metadata_path"])
    assert str(csv4.parent).replace("\\", "/").endswith("bronze/mef/presupuesto_generica_temporal/extraction_date=2026-06-10/year=2026")
    assert csv4.exists()
    assert meta4.exists()


def test_mef_breakdown_fields_alignment(tmp_path: Path) -> None:
    from pipelines.scrape_mef_budget import build_mef_breakdown_record, write_csv_file

    # 1. Test that build_mef_breakdown_record for 'actividad' does NOT contain 'periodo_tipo' or 'periodo_valor'
    act_record = build_mef_breakdown_record(
        descriptor_cells=["5006319: APLICACION DE MECANISMOS"],
        budget_values=["1000", "2000", "1900", "1500", "1100", "1000", "900", "50.0"],
        ano="2026",
        slice_name="actividad",
        periodo_tipo="ANUAL",
        periodo_valor="2026",
    )
    assert act_record is not None
    assert "periodo_tipo" not in act_record
    assert "periodo_valor" not in act_record
    assert "codigo_actividad" in act_record
    assert act_record["codigo_actividad"] == "5006319"
    assert act_record["actividad"] == "APLICACION DE MECANISMOS"

    # 2. Test that 'actividad_temporal' records (which use build_mef_temporal_record) indeed contain 'periodo_tipo' and 'periodo_valor'
    from pipelines.scrape_mef_budget import build_mef_temporal_record
    temp_record = build_mef_temporal_record(
        descriptor_cells=["ENERO"],
        budget_values=["1000", "2000", "1900", "1500", "1100", "1000", "900", "50.0"],
        ano="2026",
    )
    assert temp_record is not None
    assert "periodo_tipo" in temp_record
    assert temp_record["periodo_tipo"] == "MENSUAL"
    assert "periodo_valor" in temp_record
    assert temp_record["periodo_valor"] == "2026-01"

    # 3. Test that writing to CSV fails if a record contains extra fields not defined in the slice's fieldnames
    record_with_extras = {
        "ano": "2026",
        "codigo_producto": "3000885",
        "producto": "ENTREGA DE BECA",
        "codigo_actividad": "5006319",
        "actividad": "APLICACION DE MECANISMOS",
        "pia": "1000",
        "pim": "2000",
        "certificacion": "1900",
        "compromiso_anual": "1500",
        "compromiso_mensual": "1100",
        "devengado": "1000",
        "girado": "900",
        "avance_porcentaje": "50.0",
        "periodo_tipo": "ANUAL",       # Extra field for 'actividad' slice
        "periodo_valor": "2026",       # Extra field for 'actividad' slice
    }

    from pipelines.scrape_mef_budget import MEF_BREAKDOWN_CONFIG
    fieldnames = MEF_BREAKDOWN_CONFIG["actividad"]["fieldnames"]

    csv_path = tmp_path / "test_extras_fail.csv"
    with pytest.raises(ValueError) as excinfo:
        write_csv_file(
            path=csv_path,
            records=[record_with_extras],
            fieldnames=fieldnames,
        )
    assert "dict contains fields not in fieldnames" in str(excinfo.value)




