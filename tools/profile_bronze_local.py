"""Profile local Bronze outputs before Silver design.

Reads local Bronze files from tmp/bronze and writes temporary profiling reports.
No GCP, SQL, notebooks, or external services are used.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


DATASET_SUMMARY_FIELDS = [
    "source_system",
    "dataset",
    "resource_path",
    "file_count",
    "row_count",
    "column_count",
    "extraction_dates_detected",
    "years_detected",
]
COLUMN_PROFILE_FIELDS = [
    "source_system",
    "dataset",
    "column_name",
    "row_count",
    "non_null_count",
    "null_count",
    "null_percentage",
    "empty_string_count",
    "empty_string_percentage",
    "distinct_count",
    "sample_values",
    "max_length",
]
NULLS_REPORT_FIELDS = [
    "source_system",
    "dataset",
    "column_name",
    "null_percentage",
    "empty_string_percentage",
    "non_null_count",
    "row_count",
]
TYPE_CANDIDATE_FIELDS = [
    "source_system",
    "dataset",
    "column_name",
    "candidate_type",
    "confidence",
    "details",
]
QUALITY_WARNING_FIELDS = [
    "source_system",
    "dataset",
    "column_name",
    "warning_type",
    "severity",
    "details",
]
MEF_NUMERIC_COLUMNS = {
    "pia",
    "pim",
    "certificacion",
    "compromiso_anual",
    "compromiso_mensual",
    "devengado",
    "girado",
    "avance_porcentaje",
}
MEF_STRING_IDENTIFIER_COLUMNS = {
    "codigo_producto",
    "codigo_generica",
    "codigo_fuente",
    "codigo_rubro",
}
BOOLEAN_VALUES = {"si", "sí", "no", "true", "false", "0", "1", "y", "n"}


@dataclass
class DatasetResource:
    source_system: str
    dataset: str
    resource_root: Path
    files: list[Path]


@dataclass
class ColumnStats:
    source_system: str
    dataset: str
    column_name: str
    row_count: int = 0
    null_count: int = 0
    empty_string_count: int = 0
    values: list[str] = field(default_factory=list)
    distinct_values: set[str] = field(default_factory=set)
    samples: list[str] = field(default_factory=list)
    max_length: int = 0

    def add(self, value: Any, max_distinct_samples: int) -> None:
        self.row_count += 1
        if value is None:
            self.null_count += 1
            return

        normalized = str(value).strip()
        if normalized == "":
            self.empty_string_count += 1
            return

        self.values.append(normalized)
        self.distinct_values.add(normalized)
        self.max_length = max(self.max_length, len(normalized))
        if normalized not in self.samples and len(self.samples) < max_distinct_samples:
            self.samples.append(normalized)

    @property
    def non_null_count(self) -> int:
        return len(self.values)

    @property
    def null_percentage(self) -> float:
        if self.row_count == 0:
            return 0.0
        return round((self.null_count / self.row_count) * 100, 2)

    @property
    def empty_string_percentage(self) -> float:
        if self.row_count == 0:
            return 0.0
        return round((self.empty_string_count / self.row_count) * 100, 2)


def discover_resources(bronze_dir: Path) -> list[DatasetResource]:
    resources: list[DatasetResource] = []

    pronabec_root = bronze_dir / "pronabec"
    if pronabec_root.exists():
        for dataset_dir in sorted(path for path in pronabec_root.iterdir() if path.is_dir()):
            files = sorted(dataset_dir.glob("extraction_date=*/data.jsonl"))
            if files:
                resources.append(
                    DatasetResource(
                        source_system="PRONABEC",
                        dataset=dataset_dir.name,
                        resource_root=dataset_dir,
                        files=files,
                    )
                )

    mef_root = bronze_dir / "mef"
    if mef_root.exists():
        for dataset_dir in sorted(path for path in mef_root.iterdir() if path.is_dir()):
            files = sorted(dataset_dir.glob("extraction_date=*/year=*/data.csv"))
            if files:
                resources.append(
                    DatasetResource(
                        source_system="MEF",
                        dataset=dataset_dir.name,
                        resource_root=dataset_dir,
                        files=files,
                    )
                )

    return resources


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as file:
        for line in file:
            stripped = line.strip()
            if stripped:
                records.append(json.loads(stripped))
    return records


def read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as file:
        return list(csv.DictReader(file))


def read_resource_records(resource: DatasetResource) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in resource.files:
        if path.name != "data.jsonl" and path.name != "data.csv":
            continue
        if ".ipynb_checkpoints" in path.parts:
            continue
        if path.suffix == ".jsonl":
            records.extend(read_jsonl(path))
        elif path.suffix == ".csv":
            records.extend(read_csv(path))
    return records


def extract_partition_value(path: Path, prefix: str) -> str | None:
    for part in path.parts:
        if part.startswith(prefix):
            return part.removeprefix(prefix)
    return None


def detect_years(resource: DatasetResource, records: list[dict[str, Any]]) -> list[str]:
    years = {
        value
        for path in resource.files
        if (value := extract_partition_value(path, "year="))
    }
    years.update(
        str(record.get("ano")).strip()
        for record in records
        if record.get("ano") not in {None, ""}
    )
    return sorted(years)


def build_dataset_summary(
    resource: DatasetResource,
    records: list[dict[str, Any]],
) -> dict[str, Any]:
    columns = sorted({key for record in records for key in record})
    extraction_dates = sorted(
        value
        for path in resource.files
        if (value := extract_partition_value(path, "extraction_date="))
    )
    years = detect_years(resource, records) if resource.source_system == "MEF" else []

    return {
        "source_system": resource.source_system,
        "dataset": resource.dataset,
        "resource_path": str(resource.resource_root),
        "file_count": len(resource.files),
        "row_count": len(records),
        "column_count": len(columns),
        "extraction_dates_detected": "|".join(extraction_dates),
        "years_detected": "|".join(years),
    }


def profile_columns(
    resource: DatasetResource,
    records: list[dict[str, Any]],
    max_distinct_samples: int,
) -> list[ColumnStats]:
    columns = sorted({key for record in records for key in record})
    stats = [
        ColumnStats(
            source_system=resource.source_system,
            dataset=resource.dataset,
            column_name=column,
        )
        for column in columns
    ]
    stats_by_column = {stat.column_name: stat for stat in stats}

    for record in records:
        for column in columns:
            stats_by_column[column].add(record.get(column), max_distinct_samples)

    return stats


def is_forced_string_column(column_name: str) -> bool:
    name = column_name.lower()
    if name in {"codigo_ubigeo", "ubigeo", "ruc", "telefono", "source_row_id"}:
        return True
    if name.startswith("codigo_"):
        return True
    if name.endswith("_codigo"):
        return True
    if name.endswith("_id"):
        return True
    return False


def is_integer(value: str) -> bool:
    return bool(re.fullmatch(r"[+-]?\d+", value.strip()))


def is_numeric(value: str) -> bool:
    cleaned = value.strip().replace("%", "")
    if re.fullmatch(r"[+-]?\d{1,3}(\.\d{3})+,\d+", cleaned):
        cleaned = cleaned.replace(".", "").replace(",", ".")
    elif re.fullmatch(r"[+-]?\d{1,3}(,\d{3})+(\.\d+)?", cleaned):
        cleaned = cleaned.replace(",", "")
    else:
        cleaned = cleaned.replace(",", ".")

    try:
        float(cleaned)
    except ValueError:
        return False
    return bool(re.search(r"\d", cleaned))


def looks_like_date(value: str) -> bool:
    patterns = [
        r"\d{4}-\d{2}-\d{2}",
        r"\d{2}/\d{2}/\d{4}",
    ]
    return any(re.fullmatch(pattern, value.strip()) for pattern in patterns)


def looks_like_timestamp(value: str) -> bool:
    patterns = [
        r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2})?.*",
        r"\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}(:\d{2})?",
    ]
    return any(re.fullmatch(pattern, value.strip()) for pattern in patterns)


def detect_type_candidate(stat: ColumnStats) -> dict[str, Any]:
    values = stat.values
    name = stat.column_name.lower()

    if not values:
        candidate = "UNKNOWN"
        details = "No non-empty values found."
    elif stat.source_system == "MEF" and name == "ano":
        candidate = "INTEGER"
        details = "MEF year column."
    elif stat.source_system == "MEF" and name in MEF_NUMERIC_COLUMNS:
        candidate = "NUMERIC"
        details = "MEF budget metric column."
    elif stat.source_system == "MEF" and name in MEF_STRING_IDENTIFIER_COLUMNS:
        candidate = "STRING"
        details = "MEF code identifier column."
    elif is_forced_string_column(name):
        candidate = "STRING"
        details = "Identifier-like column preserved as STRING."
    elif all(value.lower() in BOOLEAN_VALUES for value in values):
        candidate = "BOOLEAN_LIKE"
        details = "Values match boolean-like domain."
    elif all(looks_like_timestamp(value) for value in values):
        candidate = "TIMESTAMP"
        details = "All values look like timestamps."
    elif all(looks_like_date(value) for value in values):
        candidate = "DATE"
        details = "All values look like dates."
    elif all(is_integer(value) for value in values):
        candidate = "INTEGER"
        details = "All values are clean integers."
    elif all(is_numeric(value) for value in values):
        candidate = "NUMERIC"
        details = "All values are numeric."
    else:
        candidate = "STRING"
        details = "Conservative default."

    confidence = "HIGH" if candidate in {"INTEGER", "NUMERIC", "DATE", "TIMESTAMP"} else "MEDIUM"
    if candidate in {"UNKNOWN", "STRING"}:
        confidence = "LOW" if not values else "MEDIUM"

    return {
        "source_system": stat.source_system,
        "dataset": stat.dataset,
        "column_name": stat.column_name,
        "candidate_type": candidate,
        "confidence": confidence,
        "details": details,
    }


def classify_value(value: str) -> str:
    if is_integer(value):
        return "INTEGER"
    if is_numeric(value):
        return "NUMERIC"
    if looks_like_timestamp(value):
        return "TIMESTAMP"
    if looks_like_date(value):
        return "DATE"
    if value.lower() in BOOLEAN_VALUES:
        return "BOOLEAN_LIKE"
    return "STRING"


def build_quality_warnings(
    stat: ColumnStats,
    candidate: str,
) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    emptyish_percentage = stat.null_percentage + stat.empty_string_percentage

    def add(warning_type: str, severity: str, details: str) -> None:
        warnings.append(
            {
                "source_system": stat.source_system,
                "dataset": stat.dataset,
                "column_name": stat.column_name,
                "warning_type": warning_type,
                "severity": severity,
                "details": details,
            }
        )

    if stat.row_count > 0 and emptyish_percentage >= 80:
        add("HIGH_NULL_PERCENTAGE", "HIGH", f"Null/empty percentage is {emptyish_percentage:.2f}%.")
    if stat.row_count > 0 and stat.non_null_count == 0:
        add("ALL_EMPTY", "HIGH", "Column has no non-empty values.")
    if stat.row_count >= 2 and len(stat.distinct_values) <= 1 and stat.non_null_count > 0:
        add("LOW_DISTINCT", "LOW", "Column has one or fewer distinct non-empty values.")
    if is_forced_string_column(stat.column_name) and candidate == "STRING":
        numeric_values = [value for value in stat.values if is_numeric(value)]
        if numeric_values:
            add("POSSIBLE_IDENTIFIER_AS_NUMBER", "MEDIUM", "Identifier-like numeric values preserved as STRING.")
    if any(re.fullmatch(r"[+-]?\d+,\d+", value.strip()) for value in stat.values):
        add("POSSIBLE_DECIMAL_COMMA", "MEDIUM", "Some values may use comma as decimal separator.")

    value_types = {classify_value(value) for value in stat.values}
    if len(value_types) > 1 and not value_types <= {"INTEGER", "NUMERIC"}:
        add("MIXED_TYPE_VALUES", "MEDIUM", f"Observed value classes: {sorted(value_types)}.")

    return warnings


def column_profile_row(stat: ColumnStats) -> dict[str, Any]:
    return {
        "source_system": stat.source_system,
        "dataset": stat.dataset,
        "column_name": stat.column_name,
        "row_count": stat.row_count,
        "non_null_count": stat.non_null_count,
        "null_count": stat.null_count,
        "null_percentage": stat.null_percentage,
        "empty_string_count": stat.empty_string_count,
        "empty_string_percentage": stat.empty_string_percentage,
        "distinct_count": len(stat.distinct_values),
        "sample_values": "|".join(stat.samples),
        "max_length": stat.max_length,
    }


def nulls_report_row(stat: ColumnStats) -> dict[str, Any]:
    return {
        "source_system": stat.source_system,
        "dataset": stat.dataset,
        "column_name": stat.column_name,
        "null_percentage": stat.null_percentage,
        "empty_string_percentage": stat.empty_string_percentage,
        "non_null_count": stat.non_null_count,
        "row_count": stat.row_count,
    }


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def recommendation_label(candidate: str, warning_types: set[str]) -> str:
    if "ALL_EMPTY" in warning_types:
        return "DROP_CANDIDATE"
    if candidate == "UNKNOWN" or "MIXED_TYPE_VALUES" in warning_types:
        return "REVIEW_REQUIRED"
    return "KEEP_RECOMMENDED"


def build_silver_candidates_markdown(
    dataset_summaries: list[dict[str, Any]],
    type_rows: list[dict[str, Any]],
    warning_rows: list[dict[str, Any]],
) -> str:
    type_by_dataset: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    warnings_by_dataset: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in type_rows:
        type_by_dataset[(row["source_system"], row["dataset"])].append(row)
    for row in warning_rows:
        warnings_by_dataset[(row["source_system"], row["dataset"])].append(row)

    lines = ["# Bronze profiling summary", ""]
    for summary in dataset_summaries:
        key = (summary["source_system"], summary["dataset"])
        warnings = warnings_by_dataset.get(key, [])
        warning_types_by_column: dict[str, set[str]] = defaultdict(set)
        for warning in warnings:
            warning_types_by_column[warning["column_name"]].add(warning["warning_type"])

        lines.extend(
            [
                f"## {summary['source_system']}.{summary['dataset']}",
                "",
                "### Row/column summary",
                "",
                f"- Rows: {summary['row_count']}",
                f"- Columns: {summary['column_count']}",
                f"- Files: {summary['file_count']}",
                "",
                "### Columns likely useful for Silver",
                "",
            ]
        )

        removable: list[str] = []
        type_lines: list[str] = []
        for type_row in type_by_dataset.get(key, []):
            warning_types = warning_types_by_column.get(type_row["column_name"], set())
            label = recommendation_label(
                candidate=type_row["candidate_type"],
                warning_types=warning_types,
            )
            if label == "DROP_CANDIDATE":
                removable.append(type_row["column_name"])
            else:
                lines.append(f"- `{type_row['column_name']}`: {label}")
            type_lines.append(
                f"- `{type_row['column_name']}`: {type_row['candidate_type']} ({type_row['confidence']})"
            )

        lines.extend(["", "### Columns likely removable", ""])
        if removable:
            lines.extend(f"- `{column}`: DROP_CANDIDATE" for column in removable)
        else:
            lines.append("- None identified automatically.")

        lines.extend(["", "### Type recommendations", "", *type_lines])

        lines.extend(["", "### Data quality warnings", ""])
        if warnings:
            for warning in warnings:
                lines.append(
                    f"- `{warning['column_name']}`: {warning['warning_type']} "
                    f"({warning['severity']}) - {warning['details']}"
                )
        else:
            lines.append("- No automatic warnings.")

        lines.extend(
            [
                "",
                "### Questions before Silver",
                "",
                "- REVIEW_REQUIRED columns must be checked before changing schemas.",
                "- Generated recommendations are advisory and require human review.",
                "",
            ]
        )

    return "\n".join(lines) + "\n"


def run_profile(
    bronze_dir: Path,
    output_dir: Path,
    sample_size: int,
    max_distinct_samples: int,
) -> dict[str, Any]:
    resources = discover_resources(bronze_dir)
    dataset_summaries: list[dict[str, Any]] = []
    column_rows: list[dict[str, Any]] = []
    null_rows: list[dict[str, Any]] = []
    type_rows: list[dict[str, Any]] = []
    warning_rows: list[dict[str, Any]] = []

    for resource in resources:
        records = read_resource_records(resource)

        summary = build_dataset_summary(resource, records)
        dataset_summaries.append(summary)
        stats = profile_columns(resource, records, max_distinct_samples)

        for stat in stats:
            column_rows.append(column_profile_row(stat))
            null_rows.append(nulls_report_row(stat))
            type_row = detect_type_candidate(stat)
            type_rows.append(type_row)
            warning_rows.extend(
                build_quality_warnings(stat, candidate=type_row["candidate_type"])
            )

    null_rows.sort(
        key=lambda row: (
            float(row["null_percentage"]) + float(row["empty_string_percentage"]),
            row["source_system"],
            row["dataset"],
            row["column_name"],
        ),
        reverse=True,
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    write_csv(output_dir / "dataset_summary.csv", dataset_summaries, DATASET_SUMMARY_FIELDS)
    write_csv(output_dir / "column_profile.csv", column_rows, COLUMN_PROFILE_FIELDS)
    write_csv(output_dir / "nulls_report.csv", null_rows, NULLS_REPORT_FIELDS)
    write_csv(output_dir / "type_candidates.csv", type_rows, TYPE_CANDIDATE_FIELDS)
    write_csv(output_dir / "quality_warnings.csv", warning_rows, QUALITY_WARNING_FIELDS)

    (output_dir / "silver_candidates.md").write_text(
        build_silver_candidates_markdown(dataset_summaries, type_rows, warning_rows),
        encoding="utf-8",
    )

    profile_summary = {
        "total_datasets": len(dataset_summaries),
        "total_files": sum(int(summary["file_count"]) for summary in dataset_summaries),
        "total_rows": sum(int(summary["row_count"]) for summary in dataset_summaries),
        "generated_at": datetime.now(UTC).isoformat(),
        "bronze_dir": str(bronze_dir),
        "output_dir": str(output_dir),
    }
    (output_dir / "profile_summary.json").write_text(
        json.dumps(profile_summary, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    return profile_summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Profile local Bronze outputs for pre-Silver analysis."
    )
    parser.add_argument("--bronze-dir", default="tmp/bronze")
    parser.add_argument("--output-dir", default="tmp/profiling/bronze")
    parser.add_argument("--sample-size", type=int, default=20)
    parser.add_argument("--max-distinct-samples", type=int, default=10)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = run_profile(
        bronze_dir=Path(args.bronze_dir),
        output_dir=Path(args.output_dir),
        sample_size=args.sample_size,
        max_distinct_samples=args.max_distinct_samples,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
