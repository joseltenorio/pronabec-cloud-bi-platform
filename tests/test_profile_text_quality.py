"""Tests for the local text quality profiler."""

from __future__ import annotations

import csv
import json
from pathlib import Path

from tools.profile_text_quality import OUTPUT_FILES, build_profile


def write_jsonl(path: Path, records: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(json.dumps(record, ensure_ascii=False) for record in records) + "\n",
        encoding="utf-8",
    )


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def test_profile_text_quality_generates_required_reports(tmp_path: Path) -> None:
    bronze_dir = tmp_path / "bronze"
    output_dir = tmp_path / "profiling" / "text_quality"

    write_jsonl(
        bronze_dir / "pronabec" / "convocatorias" / "extraction_date=2026-06-15" / "data.jsonl",
        [
            {"source_row_id": 1, "programa": "BECA   18", "modalidad": "ACADÃ‰MICA"},
            {
                "source_row_id": 2,
                "programa": "BECA CENTENARIO                                ",
                "modalidad": "Beca\nEspecial",
            },
            {"source_row_id": 3, "programa": "BECA 18", "modalidad": "ACADÉMICA"},
        ],
    )
    write_csv(
        bronze_dir / "mef" / "presupuesto" / "extraction_date=2026-06-15" / "year=2026" / "data.csv",
        [
            {"ano": "2026", "categoria": "EDUCACIÃ“N", "monto": "123"},
            {"ano": "2026", "categoria": "EDUCACIÓN", "monto": "456"},
        ],
    )
    write_csv(
        bronze_dir
        / "pronabec_reports"
        / "report_test"
        / "extraction_date=2026-06-15"
        / "data.csv",
        [
            {"region": "AMAZONAS", "provincia": "TOTAL", "becarios": "123"},
            {"region": "AMAZONAS", "provincia": "CHACHAPOYAS", "becarios": "45"},
            {"region": "AMAZONAS", "provincia": "BAGUA", "becarios": "78"},
        ],
    )

    summary = build_profile(bronze_dir=bronze_dir, output_dir=output_dir)

    for output_file in OUTPUT_FILES:
        assert (output_dir / output_file).exists()

    text_anomalies = (output_dir / "text_anomalies.csv").read_text(encoding="utf-8")
    assert "REPEATED_SPACES" in text_anomalies
    assert "LEADING_OR_TRAILING_SPACE" in text_anomalies
    assert "MOJIBAKE_PATTERN" in text_anomalies
    assert "TAB_OR_NEWLINE" in text_anomalies
    assert "TERRITORIAL_TOTAL_VALUE" in text_anomalies

    normalized_preview = (output_dir / "normalized_value_preview.csv").read_text(encoding="utf-8")
    assert "after_fix_mojibake" in normalized_preview
    assert "after_normalize_for_matching" in normalized_preview
    assert "ACADÉMICA" in normalized_preview

    alias_groups = (output_dir / "possible_alias_groups.csv").read_text(encoding="utf-8")
    assert "BECA 18" in alias_groups
    assert "BECA   18" in alias_groups

    territorial_rows = (output_dir / "territorial_aggregate_rows.csv").read_text(
        encoding="utf-8"
    )
    assert "AMAZONAS" in territorial_rows
    assert "TOTAL" in territorial_rows
    assert "AGGREGATE_TOTAL_ROW" in territorial_rows
    assert "REVIEW_EXCLUDE_FROM_DETAIL_SILVER" in territorial_rows

    summary_payload = json.loads(
        (output_dir / "profile_text_quality_summary.json").read_text(encoding="utf-8")
    )
    assert summary_payload["total_territorial_aggregate_rows"] >= 1
    assert "report_test" in summary_payload["datasets_with_territorial_aggregate_rows"]
    assert summary["total_territorial_aggregate_rows"] >= 1

    html_report = (output_dir / "text_quality_report.html").read_text(encoding="utf-8")
    assert "Territorial aggregate / total rows" in html_report
