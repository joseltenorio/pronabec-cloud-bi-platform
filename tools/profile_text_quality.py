"""Profile local Bronze text quality and territorial aggregate rows."""

from __future__ import annotations

import argparse
import csv
import html
import json
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.common.text_normalization import (  # noqa: E402
    TERRITORIAL_ISSUE_MIXED_GRANULARITY,
    detect_mixed_detail_and_total_granularity,
    detect_territorial_aggregate_issues,
    detect_text_issues,
    is_territorial_column,
    normalize_text_for_matching,
    normalize_whitespace,
    preview_text_normalization,
)


OUTPUT_FILES = [
    "column_text_quality_summary.csv",
    "text_anomalies.csv",
    "normalized_value_preview.csv",
    "frequent_values_by_column.csv",
    "possible_alias_groups.csv",
    "territorial_aggregate_rows.csv",
    "text_quality_report.html",
    "profile_text_quality_summary.json",
]
SUMMARY_FIELDS = [
    "dataset",
    "source_system",
    "column",
    "total_values",
    "non_empty_values",
    "distinct_values",
    "empty_or_whitespace_count",
    "leading_or_trailing_space_count",
    "repeated_spaces_count",
    "tab_or_newline_count",
    "control_character_count",
    "mojibake_count",
    "replacement_character_count",
    "accented_character_count",
    "non_ascii_count",
    "suspicious_symbol_count",
    "lowercase_or_mixed_case_count",
    "long_text_variant_count",
    "territorial_total_value_count",
]
ANOMALY_FIELDS = [
    "dataset",
    "source_system",
    "column",
    "issue_type",
    "original_value",
    "suggested_value",
    "normalized_for_matching",
    "count",
    "example_record_id",
    "extraction_date",
    "year",
]
PREVIEW_FIELDS = [
    "dataset",
    "source_system",
    "column",
    "original_value",
    "after_fix_mojibake",
    "after_remove_control_chars",
    "after_normalize_whitespace",
    "after_strip_accents",
    "after_normalize_for_matching",
    "issues",
    "count",
]
FREQUENT_FIELDS = [
    "dataset",
    "source_system",
    "column",
    "original_value",
    "normalized_for_matching",
    "count",
    "share_percent",
]
ALIAS_FIELDS = [
    "dataset",
    "source_system",
    "column",
    "normalized_for_matching",
    "distinct_original_values",
    "total_count",
    "sample_original_values",
]
TERRITORIAL_FIELDS = [
    "dataset",
    "source_system",
    "parent_column",
    "parent_value",
    "territorial_column",
    "territorial_value",
    "issue_type",
    "recommended_action",
    "example_record_id",
    "extraction_date",
    "year",
]
ISSUE_TO_SUMMARY_FIELD = {
    "EMPTY_OR_WHITESPACE": "empty_or_whitespace_count",
    "LEADING_OR_TRAILING_SPACE": "leading_or_trailing_space_count",
    "REPEATED_SPACES": "repeated_spaces_count",
    "TAB_OR_NEWLINE": "tab_or_newline_count",
    "CONTROL_CHARACTER": "control_character_count",
    "MOJIBAKE_PATTERN": "mojibake_count",
    "REPLACEMENT_CHARACTER": "replacement_character_count",
    "ACCENTED_CHARACTER": "accented_character_count",
    "NON_ASCII_CHARACTER": "non_ascii_count",
    "SUSPICIOUS_SYMBOL": "suspicious_symbol_count",
    "LOWERCASE_OR_MIXED_CASE": "lowercase_or_mixed_case_count",
    "LONG_TEXT_VARIANT": "long_text_variant_count",
    "TERRITORIAL_TOTAL_VALUE": "territorial_total_value_count",
}


@dataclass(frozen=True)
class BronzeFile:
    source_system: str
    dataset: str
    path: Path
    extraction_date: str | None = None
    year: str | None = None


def extract_partition_value(path: Path, prefix: str) -> str | None:
    for part in path.parts:
        if part.startswith(prefix):
            return part.removeprefix(prefix)
    return None


def discover_bronze_files(bronze_dir: Path, include_datasets: set[str] | None = None) -> list[BronzeFile]:
    files: list[BronzeFile] = []
    patterns = [
        ("pronabec", "pronabec", "*/extraction_date=*/data.jsonl"),
        ("mef", "mef", "*/extraction_date=*/year=*/data.csv"),
        ("pronabec_reports", "pronabec_reports", "*/extraction_date=*/data.csv"),
    ]
    for source_system, root_name, pattern in patterns:
        root = bronze_dir / root_name
        if not root.exists():
            continue
        for path in sorted(root.glob(pattern)):
            dataset = path.relative_to(root).parts[0]
            if include_datasets and dataset not in include_datasets:
                continue
            files.append(
                BronzeFile(
                    source_system=source_system,
                    dataset=dataset,
                    path=path,
                    extraction_date=extract_partition_value(path, "extraction_date="),
                    year=extract_partition_value(path, "year="),
                )
            )
    return files


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            record = json.loads(stripped)
            if "source_row_id" not in record:
                record["source_row_id"] = line_number
            records.append(record)
    return records


def read_csv_records(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as file:
        records = list(csv.DictReader(file))
    for index, record in enumerate(records, start=1):
        record.setdefault("source_row_id", index)
    return records


def read_bronze_file(bronze_file: BronzeFile) -> list[dict[str, Any]]:
    if bronze_file.path.suffix == ".jsonl":
        return read_jsonl(bronze_file.path)
    return read_csv_records(bronze_file.path)


def empty_summary_row(dataset: str, source_system: str, column: str) -> dict[str, Any]:
    row = {field: 0 for field in SUMMARY_FIELDS}
    row.update({"dataset": dataset, "source_system": source_system, "column": column})
    return row


def suggested_value(value: Any) -> str | None:
    preview = preview_text_normalization(value)
    return preview["after_normalize_whitespace"]


def build_profile(
    bronze_dir: Path,
    output_dir: Path,
    max_examples_per_column: int = 20,
    max_values_per_column: int = 50,
    include_datasets: set[str] | None = None,
) -> dict[str, Any]:
    files = discover_bronze_files(bronze_dir, include_datasets)
    summary_by_column: dict[tuple[str, str, str], dict[str, Any]] = {}
    value_counts: Counter[tuple[str, str, str, str]] = Counter()
    anomaly_counts: Counter[tuple[str, str, str, str, str]] = Counter()
    anomaly_examples: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
    preview_counts: Counter[tuple[str, str, str, str]] = Counter()
    alias_counts: Counter[tuple[str, str, str, str, str]] = Counter()
    territorial_rows: list[dict[str, Any]] = []
    dataset_records: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    dataset_meta: dict[tuple[str, str], dict[str, str]] = {}

    for bronze_file in files:
        records = read_bronze_file(bronze_file)
        dataset_key = (bronze_file.source_system, bronze_file.dataset)
        dataset_records[dataset_key].extend(records)
        dataset_meta[dataset_key] = {
            "extraction_date": bronze_file.extraction_date or "",
            "year": bronze_file.year or "",
        }

        for record_index, record in enumerate(records, start=1):
            record_id = str(record.get("source_row_id") or record_index)
            for column, raw_value in record.items():
                value = "" if raw_value is None else str(raw_value)
                key = (bronze_file.dataset, bronze_file.source_system, column)
                summary = summary_by_column.setdefault(
                    key,
                    empty_summary_row(bronze_file.dataset, bronze_file.source_system, column),
                )
                summary["total_values"] += 1
                if normalize_whitespace(value) is not None:
                    summary["non_empty_values"] += 1
                value_counts[(bronze_file.dataset, bronze_file.source_system, column, value)] += 1
                normalized = normalize_text_for_matching(value) or ""
                alias_counts[(bronze_file.dataset, bronze_file.source_system, column, normalized, value)] += 1

                issues = detect_text_issues(value, column_name=column)
                if is_territorial_column(column) and any(
                    issue["issue_type"] == "TERRITORIAL_TOTAL_VALUE"
                    for issue in detect_territorial_aggregate_issues({column: value})
                ):
                    issues.append("TERRITORIAL_TOTAL_VALUE")
                for issue in issues:
                    field = ISSUE_TO_SUMMARY_FIELD.get(issue)
                    if field:
                        summary[field] += 1
                    anomaly_key = (bronze_file.dataset, bronze_file.source_system, column, issue, value)
                    anomaly_counts[anomaly_key] += 1
                    anomaly_examples.setdefault(
                        anomaly_key,
                        {
                            "example_record_id": record_id,
                            "extraction_date": bronze_file.extraction_date or "",
                            "year": bronze_file.year or "",
                        },
                    )
                if issues:
                    preview_counts[(bronze_file.dataset, bronze_file.source_system, column, value)] += 1

            for issue in detect_territorial_aggregate_issues(record, dataset=bronze_file.dataset):
                territorial_rows.append(
                    {
                        "dataset": bronze_file.dataset,
                        "source_system": bronze_file.source_system,
                        **issue,
                        "example_record_id": record_id,
                        "extraction_date": bronze_file.extraction_date or "",
                        "year": bronze_file.year or "",
                    }
                )

    for (source_system, dataset), records in dataset_records.items():
        if not detect_mixed_detail_and_total_granularity(records):
            continue
        meta = dataset_meta.get((source_system, dataset), {})
        territorial_rows.append(
            {
                "dataset": dataset,
                "source_system": source_system,
                "parent_column": "",
                "parent_value": "",
                "territorial_column": "",
                "territorial_value": "",
                "issue_type": TERRITORIAL_ISSUE_MIXED_GRANULARITY,
                "recommended_action": "REVIEW_EXCLUDE_FROM_DETAIL_SILVER",
                "example_record_id": "",
                "extraction_date": meta.get("extraction_date", ""),
                "year": meta.get("year", ""),
            }
        )

    summary_rows = list(summary_by_column.values())
    for row in summary_rows:
        values = {
            value
            for dataset, source_system, column, value in value_counts
            if dataset == row["dataset"] and source_system == row["source_system"] and column == row["column"]
        }
        row["distinct_values"] = len(values)

    anomaly_rows = build_anomaly_rows(anomaly_counts, anomaly_examples)
    preview_rows = build_preview_rows(preview_counts, max_examples_per_column)
    frequent_rows = build_frequent_rows(value_counts, max_values_per_column)
    alias_rows = build_alias_rows(alias_counts, max_examples_per_column)

    output_dir.mkdir(parents=True, exist_ok=True)
    write_csv(output_dir / "column_text_quality_summary.csv", sorted_rows(summary_rows), SUMMARY_FIELDS)
    write_csv(output_dir / "text_anomalies.csv", anomaly_rows, ANOMALY_FIELDS)
    write_csv(output_dir / "normalized_value_preview.csv", preview_rows, PREVIEW_FIELDS)
    write_csv(output_dir / "frequent_values_by_column.csv", frequent_rows, FREQUENT_FIELDS)
    write_csv(output_dir / "possible_alias_groups.csv", alias_rows, ALIAS_FIELDS)
    write_csv(output_dir / "territorial_aggregate_rows.csv", territorial_rows, TERRITORIAL_FIELDS)
    write_html_report(output_dir / "text_quality_report.html", summary_rows, anomaly_rows, preview_rows, territorial_rows)

    summary = {
        "total_datasets": len(dataset_records),
        "total_columns_profiled": len(summary_rows),
        "total_text_values": sum(int(row["total_values"]) for row in summary_rows),
        "total_anomalies": sum(int(row["count"]) for row in anomaly_rows),
        "total_territorial_aggregate_rows": len(territorial_rows),
        "datasets_with_territorial_aggregate_rows": sorted(
            {row["dataset"] for row in territorial_rows}
        ),
        "generated_at": datetime.now(UTC).isoformat(),
        "output_files": OUTPUT_FILES,
    }
    (output_dir / "profile_text_quality_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return summary


def build_anomaly_rows(
    anomaly_counts: Counter[tuple[str, str, str, str, str]],
    anomaly_examples: dict[tuple[str, str, str, str, str], dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key, count in anomaly_counts.items():
        dataset, source_system, column, issue, value = key
        example = anomaly_examples[key]
        rows.append(
            {
                "dataset": dataset,
                "source_system": source_system,
                "column": column,
                "issue_type": issue,
                "original_value": value,
                "suggested_value": suggested_value(value),
                "normalized_for_matching": normalize_text_for_matching(value),
                "count": count,
                **example,
            }
        )
    return sorted_rows(rows)


def build_preview_rows(
    preview_counts: Counter[tuple[str, str, str, str]],
    max_examples_per_column: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen_by_column: Counter[tuple[str, str, str]] = Counter()
    for (dataset, source_system, column, value), count in preview_counts.most_common():
        column_key = (dataset, source_system, column)
        if seen_by_column[column_key] >= max_examples_per_column:
            continue
        preview = preview_text_normalization(value)
        rows.append(
            {
                "dataset": dataset,
                "source_system": source_system,
                "column": column,
                "original_value": value,
                "after_fix_mojibake": preview["after_fix_mojibake"],
                "after_remove_control_chars": preview["after_remove_control_chars"],
                "after_normalize_whitespace": preview["after_normalize_whitespace"],
                "after_strip_accents": preview["after_strip_accents"],
                "after_normalize_for_matching": preview["after_normalize_for_matching"],
                "issues": "|".join(preview["issues"]),
                "count": count,
            }
        )
        seen_by_column[column_key] += 1
    return sorted_rows(rows)


def build_frequent_rows(
    value_counts: Counter[tuple[str, str, str, str]],
    max_values_per_column: int,
) -> list[dict[str, Any]]:
    totals_by_column: Counter[tuple[str, str, str]] = Counter()
    for dataset, source_system, column, _value in value_counts:
        totals_by_column[(dataset, source_system, column)] += value_counts[
            (dataset, source_system, column, _value)
        ]

    rows: list[dict[str, Any]] = []
    seen_by_column: Counter[tuple[str, str, str]] = Counter()
    for (dataset, source_system, column, value), count in value_counts.most_common():
        column_key = (dataset, source_system, column)
        if seen_by_column[column_key] >= max_values_per_column:
            continue
        total = totals_by_column[column_key]
        rows.append(
            {
                "dataset": dataset,
                "source_system": source_system,
                "column": column,
                "original_value": value,
                "normalized_for_matching": normalize_text_for_matching(value),
                "count": count,
                "share_percent": round((count / total) * 100, 2) if total else 0,
            }
        )
        seen_by_column[column_key] += 1
    return sorted_rows(rows)


def build_alias_rows(
    alias_counts: Counter[tuple[str, str, str, str, str]],
    max_examples_per_column: int,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str, str], Counter[str]] = defaultdict(Counter)
    for (dataset, source_system, column, normalized, original), count in alias_counts.items():
        if not normalized:
            continue
        grouped[(dataset, source_system, column, normalized)][original] += count

    rows: list[dict[str, Any]] = []
    for (dataset, source_system, column, normalized), originals in grouped.items():
        if len(originals) <= 1:
            continue
        samples = [value for value, _count in originals.most_common(max_examples_per_column)]
        rows.append(
            {
                "dataset": dataset,
                "source_system": source_system,
                "column": column,
                "normalized_for_matching": normalized,
                "distinct_original_values": len(originals),
                "total_count": sum(originals.values()),
                "sample_original_values": "|".join(samples),
            }
        )
    return sorted_rows(rows)


def sorted_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: (
            str(row.get("source_system", "")),
            str(row.get("dataset", "")),
            str(row.get("column", "")),
            str(row.get("issue_type", "")),
            str(row.get("original_value", "")),
        ),
    )


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_html_report(
    path: Path,
    summary_rows: list[dict[str, Any]],
    anomaly_rows: list[dict[str, Any]],
    preview_rows: list[dict[str, Any]],
    territorial_rows: list[dict[str, Any]],
) -> None:
    total_values = sum(int(row["total_values"]) for row in summary_rows)
    total_anomalies = sum(int(row["count"]) for row in anomaly_rows)
    sections = [
        "<!doctype html><html><head><meta charset='utf-8'><title>Text quality report</title>",
        "<style>body{font-family:Arial,sans-serif;margin:24px;color:#222}table{border-collapse:collapse;width:100%;margin:12px 0 28px}th,td{border:1px solid #ddd;padding:6px;text-align:left;vertical-align:top}th{background:#f3f3f3}code{background:#f5f5f5;padding:1px 3px}</style>",
        "</head><body>",
        "<h1>Text quality report</h1>",
        f"<p>Datasets: {len({(row['source_system'], row['dataset']) for row in summary_rows})}. "
        f"Columns: {len(summary_rows)}. Values: {total_values}. Anomalies: {total_anomalies}.</p>",
        "<h2>Top datasets with anomalies</h2>",
        render_table(counter_rows(anomaly_rows, ["source_system", "dataset"]), ["source_system", "dataset", "count"]),
        "<h2>Top columns with mojibake</h2>",
        render_table(top_summary_rows(summary_rows, "mojibake_count"), ["source_system", "dataset", "column", "mojibake_count"]),
        "<h2>Top columns with repeated spaces</h2>",
        render_table(top_summary_rows(summary_rows, "repeated_spaces_count"), ["source_system", "dataset", "column", "repeated_spaces_count"]),
        "<h2>Top columns with leading/trailing spaces</h2>",
        render_table(top_summary_rows(summary_rows, "leading_or_trailing_space_count"), ["source_system", "dataset", "column", "leading_or_trailing_space_count"]),
        "<h2>Top values before/after</h2>",
        render_table(preview_rows[:50], PREVIEW_FIELDS),
        "<h2>Top territorial TOTAL/SUBTOTAL/NACIONAL values</h2>",
        render_table(territorial_rows[:50], TERRITORIAL_FIELDS),
        "<h2>Territorial aggregate / total rows</h2>",
        render_table(territorial_rows, TERRITORIAL_FIELDS),
        "<h2>Main anomalies</h2>",
        render_table(anomaly_rows[:100], ANOMALY_FIELDS),
        "<h2>Issue codes</h2>",
        "<p><code>MOJIBAKE_PATTERN</code>, <code>REPEATED_SPACES</code>, <code>LEADING_OR_TRAILING_SPACE</code>, "
        "<code>TAB_OR_NEWLINE</code>, <code>CONTROL_CHARACTER</code>, <code>REPLACEMENT_CHARACTER</code>, "
        "<code>ACCENTED_CHARACTER</code>, <code>NON_ASCII_CHARACTER</code>, <code>SUSPICIOUS_SYMBOL</code>, "
        "<code>LOWERCASE_OR_MIXED_CASE</code>, <code>PUNCTUATION_VARIANT</code>, <code>LONG_TEXT_VARIANT</code>, "
        "<code>TERRITORIAL_TOTAL_VALUE</code>, <code>AGGREGATE_TOTAL_ROW</code>, "
        "<code>MIXED_DETAIL_AND_TOTAL_GRANULARITY</code>.</p>",
        "</body></html>",
    ]
    path.write_text("\n".join(sections), encoding="utf-8")


def counter_rows(rows: list[dict[str, Any]], keys: list[str]) -> list[dict[str, Any]]:
    counter: Counter[tuple[str, ...]] = Counter()
    for row in rows:
        counter[tuple(str(row.get(key, "")) for key in keys)] += int(row.get("count", 1))
    return [
        {**dict(zip(keys, values, strict=True)), "count": count}
        for values, count in counter.most_common(20)
    ]


def top_summary_rows(rows: list[dict[str, Any]], field: str) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda row: int(row.get(field, 0)), reverse=True)[:20]


def render_table(rows: list[dict[str, Any]], fields: list[str]) -> str:
    if not rows:
        return "<p>No rows.</p>"
    header = "".join(f"<th>{html.escape(field)}</th>" for field in fields)
    body_rows = []
    for row in rows:
        cells = "".join(
            f"<td>{html.escape(str(row.get(field, '') if row.get(field, '') is not None else ''))}</td>"
            for field in fields
        )
        body_rows.append(f"<tr>{cells}</tr>")
    return f"<table><thead><tr>{header}</tr></thead><tbody>{''.join(body_rows)}</tbody></table>"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Profile local Bronze text quality.")
    parser.add_argument("--bronze-dir", default="tmp/bronze")
    parser.add_argument("--output-dir", default="tmp/profiling/text_quality")
    parser.add_argument("--max-examples-per-column", type=int, default=20)
    parser.add_argument("--max-values-per-column", type=int, default=50)
    parser.add_argument("--include-datasets", nargs="*", default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = build_profile(
        bronze_dir=Path(args.bronze_dir),
        output_dir=Path(args.output_dir),
        max_examples_per_column=args.max_examples_per_column,
        max_values_per_column=args.max_values_per_column,
        include_datasets=set(args.include_datasets) if args.include_datasets else None,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
