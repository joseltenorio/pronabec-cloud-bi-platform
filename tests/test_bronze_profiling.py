import csv
import json
from pathlib import Path

from tools.profile_bronze_local import (
    discover_resources,
    run_profile,
)


def write_jsonl(path: Path, records: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        for record in records:
            file.write(json.dumps(record, ensure_ascii=False) + "\n")


def write_csv(path: Path, records: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(records[0]))
        writer.writeheader()
        writer.writerows(records)


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as file:
        return list(csv.DictReader(file))


def test_bronze_profiler_discovers_pronabec_jsonl_and_mef_year_csv(
    tmp_path: Path,
) -> None:
    bronze_dir = tmp_path / "bronze"
    write_jsonl(
        bronze_dir
        / "pronabec"
        / "notas_becarios"
        / "extraction_date=2026-06-14"
        / "data.jsonl",
        [{"source_row_id": "1", "codigo_ubigeo": "010101", "nota": "18"}],
    )
    write_csv(
        bronze_dir
        / "mef"
        / "presupuesto"
        / "extraction_date=2026-06-14"
        / "year=2026"
        / "data.csv",
        [
            {
                "ano": "2026",
                "pia": "1,25",
                "codigo_producto": "3000885",
            }
        ],
    )

    resources = discover_resources(bronze_dir)

    assert {(resource.source_system, resource.dataset) for resource in resources} == {
        ("PRONABEC", "notas_becarios"),
        ("MEF", "presupuesto"),
    }


def test_bronze_profiler_generates_required_reports_and_type_candidates(
    tmp_path: Path,
) -> None:
    bronze_dir = tmp_path / "bronze"
    output_dir = tmp_path / "profiling" / "bronze"
    write_jsonl(
        bronze_dir
        / "pronabec"
        / "notas_becarios"
        / "extraction_date=2026-06-14"
        / "data.jsonl",
        [
            {
                "source_row_id": "row_1",
                "codigo_ubigeo": "010101",
                "ruc": "20123456789",
                "telefono": "999888777",
                "cantidad": "10",
                "nota_decimal": "18,5",
                "fecha": "2026-06-14",
                "activo": "Sí",
                "empty_col": "",
            },
            {
                "source_row_id": "row_2",
                "codigo_ubigeo": "010102",
                "ruc": "20123456780",
                "telefono": "999888778",
                "cantidad": "20",
                "nota_decimal": "17,25",
                "fecha": "2026-06-15",
                "activo": "No",
                "empty_col": "",
            },
        ],
    )
    write_csv(
        bronze_dir
        / "mef"
        / "presupuesto"
        / "extraction_date=2026-06-14"
        / "year=2026"
        / "data.csv",
        [
            {
                "ano": "2026",
                "pia": "1,429,676,488",
                "pim": "1607111495.5",
                "certificacion": "1590100549",
                "codigo_producto": "3000885",
                "rubro": "RECURSOS ORDINARIOS",
            },
            {
                "ano": "2026",
                "pia": "1,25",
                "pim": "",
                "certificacion": "1590100550",
                "codigo_producto": "3000886",
                "rubro": "RECURSOS ORDINARIOS",
            },
        ],
    )

    summary = run_profile(
        bronze_dir=bronze_dir,
        output_dir=output_dir,
        sample_size=20,
        max_distinct_samples=10,
    )

    expected_reports = {
        "dataset_summary.csv",
        "column_profile.csv",
        "nulls_report.csv",
        "type_candidates.csv",
        "quality_warnings.csv",
        "silver_candidates.md",
        "profile_summary.json",
    }
    assert expected_reports == {path.name for path in output_dir.iterdir()}
    assert summary["total_datasets"] == 2
    assert summary["total_files"] == 2
    assert summary["total_rows"] == 4

    dataset_rows = read_csv_rows(output_dir / "dataset_summary.csv")
    notas_summary = next(row for row in dataset_rows if row["dataset"] == "notas_becarios")
    mef_summary = next(row for row in dataset_rows if row["dataset"] == "presupuesto")
    assert notas_summary["source_system"] == "PRONABEC"
    assert notas_summary["row_count"] == "2"
    assert notas_summary["years_detected"] == ""
    assert mef_summary["source_system"] == "MEF"
    assert mef_summary["row_count"] == "2"
    assert mef_summary["years_detected"] == "2026"

    column_rows = read_csv_rows(output_dir / "column_profile.csv")
    cantidad_profile = next(row for row in column_rows if row["column_name"] == "cantidad")
    empty_profile = next(row for row in column_rows if row["column_name"] == "empty_col")
    assert cantidad_profile["row_count"] == "2"
    assert cantidad_profile["null_percentage"] == "0.0"
    assert empty_profile["empty_string_percentage"] == "100.0"

    type_rows = read_csv_rows(output_dir / "type_candidates.csv")
    type_by_column = {
        (row["source_system"], row["dataset"], row["column_name"]): row["candidate_type"]
        for row in type_rows
    }
    assert type_by_column[("PRONABEC", "notas_becarios", "cantidad")] == "INTEGER"
    assert type_by_column[("PRONABEC", "notas_becarios", "nota_decimal")] == "NUMERIC"
    assert type_by_column[("PRONABEC", "notas_becarios", "fecha")] == "DATE"
    assert type_by_column[("PRONABEC", "notas_becarios", "activo")] == "BOOLEAN_LIKE"
    assert type_by_column[("PRONABEC", "notas_becarios", "codigo_ubigeo")] == "STRING"
    assert type_by_column[("PRONABEC", "notas_becarios", "ruc")] == "STRING"
    assert type_by_column[("PRONABEC", "notas_becarios", "telefono")] == "STRING"
    assert type_by_column[("MEF", "presupuesto", "ano")] == "INTEGER"
    assert type_by_column[("MEF", "presupuesto", "pia")] == "NUMERIC"
    assert type_by_column[("MEF", "presupuesto", "pim")] == "NUMERIC"
    assert type_by_column[("MEF", "presupuesto", "certificacion")] == "NUMERIC"
    assert type_by_column[("MEF", "presupuesto", "codigo_producto")] == "STRING"

    warning_rows = read_csv_rows(output_dir / "quality_warnings.csv")
    warning_types = {row["warning_type"] for row in warning_rows}
    assert "ALL_EMPTY" in warning_types
    assert "HIGH_NULL_PERCENTAGE" in warning_types
    assert "POSSIBLE_IDENTIFIER_AS_NUMBER" in warning_types
    assert "POSSIBLE_DECIMAL_COMMA" in warning_types
    assert "LOW_DISTINCT" in warning_types

    null_rows = read_csv_rows(output_dir / "nulls_report.csv")
    assert null_rows[0]["column_name"] == "empty_col"

    markdown = (output_dir / "silver_candidates.md").read_text(encoding="utf-8")
    assert "# Bronze profiling summary" in markdown
    assert "## PRONABEC.notas_becarios" in markdown
    assert "## MEF.presupuesto" in markdown
    assert "KEEP_RECOMMENDED" in markdown
    assert "DROP_CANDIDATE" in markdown

    json_summary = json.loads((output_dir / "profile_summary.json").read_text())
    assert json_summary["total_datasets"] == 2
    assert json_summary["bronze_dir"] == str(bronze_dir)
    assert json_summary["output_dir"] == str(output_dir)
