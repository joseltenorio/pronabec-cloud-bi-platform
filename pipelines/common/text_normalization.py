"""Shared technical text normalization and profiling helpers."""

from __future__ import annotations

import re
import string
import unicodedata
from collections import Counter
from typing import Any

from ftfy import fix_text


TEXT_ISSUE_EMPTY_OR_WHITESPACE = "EMPTY_OR_WHITESPACE"
TEXT_ISSUE_LEADING_OR_TRAILING_SPACE = "LEADING_OR_TRAILING_SPACE"
TEXT_ISSUE_REPEATED_SPACES = "REPEATED_SPACES"
TEXT_ISSUE_TAB_OR_NEWLINE = "TAB_OR_NEWLINE"
TEXT_ISSUE_CONTROL_CHARACTER = "CONTROL_CHARACTER"
TEXT_ISSUE_MOJIBAKE_PATTERN = "MOJIBAKE_PATTERN"
TEXT_ISSUE_REPLACEMENT_CHARACTER = "REPLACEMENT_CHARACTER"
TEXT_ISSUE_ACCENTED_CHARACTER = "ACCENTED_CHARACTER"
TEXT_ISSUE_NON_ASCII_CHARACTER = "NON_ASCII_CHARACTER"
TEXT_ISSUE_SUSPICIOUS_SYMBOL = "SUSPICIOUS_SYMBOL"
TEXT_ISSUE_LOWERCASE_OR_MIXED_CASE = "LOWERCASE_OR_MIXED_CASE"
TEXT_ISSUE_PUNCTUATION_VARIANT = "PUNCTUATION_VARIANT"
TEXT_ISSUE_LONG_TEXT_VARIANT = "LONG_TEXT_VARIANT"

TERRITORIAL_ISSUE_AGGREGATE_TOTAL_ROW = "AGGREGATE_TOTAL_ROW"
TERRITORIAL_ISSUE_TOTAL_VALUE = "TERRITORIAL_TOTAL_VALUE"
TERRITORIAL_ISSUE_POSSIBLE_NON_TERRITORIAL_VALUE = "POSSIBLE_NON_TERRITORIAL_VALUE"
TERRITORIAL_ISSUE_MIXED_GRANULARITY = "MIXED_DETAIL_AND_TOTAL_GRANULARITY"

REVIEW_EXCLUDE_FROM_DETAIL_SILVER = "REVIEW_EXCLUDE_FROM_DETAIL_SILVER"
REVIEW_KEEP_AS_AGGREGATE_TABLE = "REVIEW_KEEP_AS_AGGREGATE_TABLE"
REVIEW_MAP_TO_NATIONAL_TOTAL = "REVIEW_MAP_TO_NATIONAL_TOTAL"
REVIEW_UNKNOWN = "REVIEW_UNKNOWN"

TERRITORIAL_COLUMNS = {
    "region",
    "departamento",
    "provincia",
    "distrito",
    "ubigeo",
    "codigo_ubigeo",
    "region_postulacion",
    "departamento_postulacion",
    "provincia_postulacion",
    "distrito_postulacion",
}
TERRITORIAL_PARENT_COLUMNS = ("region", "departamento", "region_postulacion", "departamento_postulacion")
TERRITORIAL_CHILD_COLUMNS = ("provincia", "distrito", "provincia_postulacion", "distrito_postulacion")
AGGREGATE_TOTAL_VALUES = {
    "TOTAL",
    "TOTAL GENERAL",
    "SUBTOTAL",
    "TODOS",
    "TODAS",
    "NACIONAL",
    "PERU",
    "PERÚ",
    "PAIS",
    "PAÍS",
    "EXTRANJERO",
    "NO ESPECIFICADO",
    "SIN INFORMACION",
    "SIN INFORMACIÓN",
}
NATIONAL_TOTAL_VALUES = {"NACIONAL", "PERU", "PERÚ", "PAIS", "PAÍS", "TOTAL GENERAL"}
MOJIBAKE_MARKERS = ("Ã", "Â", "â", "ð", "Ð", "�")
SUSPICIOUS_SYMBOLS = set("@#$%^*_=+~`|\\<>[]{}")
PUNCTUATION_VARIANT_SYMBOLS = set("-/()")
CONTROL_CHARACTER_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]")
WHITESPACE_RE = re.compile(r"\s+")
REPEATED_SPACE_RE = re.compile(r" {2,}")


def _to_text(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def fix_mojibake(text: Any) -> str | None:
    """Fix recognizable mojibake without applying business-level changes."""
    value = _to_text(text)
    if value is None:
        return None
    return fix_text(value)


def contains_mojibake(text: Any) -> bool:
    value = _to_text(text)
    if value is None:
        return False
    if any(marker in value for marker in MOJIBAKE_MARKERS):
        return True
    fixed = fix_mojibake(value)
    return fixed is not None and fixed != value


def contains_replacement_char(text: Any) -> bool:
    value = _to_text(text)
    return value is not None and "\ufffd" in value


def contains_control_characters(text: Any) -> bool:
    value = _to_text(text)
    return value is not None and bool(CONTROL_CHARACTER_RE.search(value))


def has_tabs_or_newlines(text: Any) -> bool:
    value = _to_text(text)
    return value is not None and any(char in value for char in ("\t", "\n", "\r"))


def has_leading_or_trailing_spaces(text: Any) -> bool:
    value = _to_text(text)
    return value is not None and value != value.strip()


def has_repeated_spaces(text: Any) -> bool:
    value = _to_text(text)
    return value is not None and bool(REPEATED_SPACE_RE.search(value))


def has_non_ascii(text: Any) -> bool:
    value = _to_text(text)
    return value is not None and any(ord(char) > 127 for char in value)


def has_accents(text: Any) -> bool:
    value = _to_text(text)
    if value is None:
        return False
    return any(
        unicodedata.category(char) == "Mn"
        for char in unicodedata.normalize("NFD", value)
    )


def contains_suspicious_symbols(text: Any) -> bool:
    value = _to_text(text)
    return value is not None and any(char in SUSPICIOUS_SYMBOLS for char in value)


def remove_control_characters(text: Any) -> str | None:
    value = _to_text(text)
    if value is None:
        return None
    value = value.replace("\t", " ").replace("\r", " ").replace("\n", " ")
    return CONTROL_CHARACTER_RE.sub("", value)


def normalize_whitespace(text: Any) -> str | None:
    value = _to_text(text)
    if value is None:
        return None
    normalized = WHITESPACE_RE.sub(" ", value).strip()
    return normalized or None


def strip_accents(text: Any) -> str | None:
    value = _to_text(text)
    if value is None:
        return None
    normalized = unicodedata.normalize("NFD", value)
    return "".join(char for char in normalized if unicodedata.category(char) != "Mn")


def normalize_case(text: Any, mode: str = "upper") -> str | None:
    value = _to_text(text)
    if value is None:
        return None
    if mode == "upper":
        return value.upper()
    if mode == "lower":
        return value.lower()
    if mode == "title":
        return value.title()
    raise ValueError(f"Unsupported case normalization mode: {mode}")


def normalize_text_for_matching(text: Any) -> str | None:
    value = fix_mojibake(text)
    value = remove_control_characters(value)
    value = normalize_whitespace(value)
    if value is None:
        return None
    value = strip_accents(value)
    value = normalize_case(value, mode="upper")
    value = re.sub(rf"[{re.escape(string.punctuation)}]", " ", value)
    return normalize_whitespace(value)


def _has_lowercase_or_mixed_case(value: str) -> bool:
    letters = [char for char in value if char.isalpha()]
    return bool(letters) and any(char.islower() for char in letters)


def detect_text_issues(text: Any, column_name: str | None = None) -> list[str]:
    del column_name
    value = _to_text(text)
    if value is None or value.strip() == "":
        return [TEXT_ISSUE_EMPTY_OR_WHITESPACE]

    issues: list[str] = []
    if has_leading_or_trailing_spaces(value):
        issues.append(TEXT_ISSUE_LEADING_OR_TRAILING_SPACE)
    if has_repeated_spaces(value):
        issues.append(TEXT_ISSUE_REPEATED_SPACES)
    if has_tabs_or_newlines(value):
        issues.append(TEXT_ISSUE_TAB_OR_NEWLINE)
    if contains_control_characters(value):
        issues.append(TEXT_ISSUE_CONTROL_CHARACTER)
    if contains_mojibake(value):
        issues.append(TEXT_ISSUE_MOJIBAKE_PATTERN)
    if contains_replacement_char(value):
        issues.append(TEXT_ISSUE_REPLACEMENT_CHARACTER)
    if has_accents(value):
        issues.append(TEXT_ISSUE_ACCENTED_CHARACTER)
    if has_non_ascii(value):
        issues.append(TEXT_ISSUE_NON_ASCII_CHARACTER)
    if contains_suspicious_symbols(value):
        issues.append(TEXT_ISSUE_SUSPICIOUS_SYMBOL)
    if _has_lowercase_or_mixed_case(value):
        issues.append(TEXT_ISSUE_LOWERCASE_OR_MIXED_CASE)
    if any(char in PUNCTUATION_VARIANT_SYMBOLS for char in value):
        issues.append(TEXT_ISSUE_PUNCTUATION_VARIANT)
    if len(normalize_whitespace(value) or "") >= 50:
        issues.append(TEXT_ISSUE_LONG_TEXT_VARIANT)
    return issues


def preview_text_normalization(text: Any) -> dict[str, Any]:
    after_fix_mojibake = fix_mojibake(text)
    after_remove_control_chars = remove_control_characters(after_fix_mojibake)
    after_normalize_whitespace = normalize_whitespace(after_remove_control_chars)
    after_strip_accents = strip_accents(after_normalize_whitespace)
    return {
        "original_value": text,
        "after_fix_mojibake": after_fix_mojibake,
        "after_remove_control_chars": after_remove_control_chars,
        "after_normalize_whitespace": after_normalize_whitespace,
        "after_strip_accents": after_strip_accents,
        "after_normalize_for_matching": normalize_text_for_matching(text),
        "issues": detect_text_issues(text),
    }


def is_territorial_column(column_name: str | None) -> bool:
    if not column_name:
        return False
    return column_name.strip().lower() in TERRITORIAL_COLUMNS


def is_aggregate_total_value(value: Any) -> bool:
    normalized = normalize_text_for_matching(value)
    return normalized in {normalize_text_for_matching(item) for item in AGGREGATE_TOTAL_VALUES}


def recommended_action_for_territorial_value(value: Any) -> str:
    normalized = normalize_text_for_matching(value)
    if normalized in {normalize_text_for_matching(item) for item in NATIONAL_TOTAL_VALUES}:
        return REVIEW_MAP_TO_NATIONAL_TOTAL
    if normalized in {"TOTAL", "SUBTOTAL", "TODOS", "TODAS"}:
        return REVIEW_EXCLUDE_FROM_DETAIL_SILVER
    if is_aggregate_total_value(value):
        return REVIEW_KEEP_AS_AGGREGATE_TABLE
    return REVIEW_UNKNOWN


def detect_territorial_aggregate_issues(
    record: dict[str, Any],
    dataset: str | None = None,
) -> list[dict[str, Any]]:
    del dataset
    issues: list[dict[str, Any]] = []
    normalized_record = {str(key).lower(): value for key, value in record.items()}
    parent_values = {
        column: normalized_record.get(column)
        for column in TERRITORIAL_PARENT_COLUMNS
        if normalized_record.get(column) not in {None, ""}
    }

    for column, value in normalized_record.items():
        if not is_territorial_column(column) or value in {None, ""}:
            continue
        if not is_aggregate_total_value(value):
            continue

        issue_types = [TERRITORIAL_ISSUE_TOTAL_VALUE]
        if column in TERRITORIAL_CHILD_COLUMNS and parent_values:
            issue_types.append(TERRITORIAL_ISSUE_AGGREGATE_TOTAL_ROW)
        elif normalize_text_for_matching(value) in {"TOTAL", "TOTAL GENERAL", "SUBTOTAL"}:
            issue_types.append(TERRITORIAL_ISSUE_AGGREGATE_TOTAL_ROW)
        else:
            issue_types.append(TERRITORIAL_ISSUE_POSSIBLE_NON_TERRITORIAL_VALUE)

        for issue_type in issue_types:
            issues.append(
                {
                    "parent_column": next(iter(parent_values), ""),
                    "parent_value": next(iter(parent_values.values()), ""),
                    "territorial_column": column,
                    "territorial_value": value,
                    "issue_type": issue_type,
                    "recommended_action": recommended_action_for_territorial_value(value),
                }
            )

    return issues


def detect_mixed_detail_and_total_granularity(records: list[dict[str, Any]]) -> bool:
    aggregate_columns: Counter[str] = Counter()
    detail_columns: Counter[str] = Counter()
    for record in records:
        for column, value in record.items():
            if not is_territorial_column(str(column)):
                continue
            if value in {None, ""}:
                continue
            if is_aggregate_total_value(value):
                aggregate_columns[str(column).lower()] += 1
            else:
                detail_columns[str(column).lower()] += 1
    return any(column in detail_columns for column in aggregate_columns)
