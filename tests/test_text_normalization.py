"""Tests for shared text normalization and territorial profiling helpers."""

from __future__ import annotations

from pipelines.common.text_normalization import (
    detect_territorial_aggregate_issues,
    detect_text_issues,
    fix_mojibake,
    is_aggregate_total_value,
    is_territorial_column,
    normalize_text_for_matching,
    normalize_whitespace,
    preview_text_normalization,
    strip_accents,
)


def test_fix_mojibake_with_ftfy() -> None:
    assert fix_mojibake("ACADÃ‰MICA") == "ACADÉMICA"
    assert fix_mojibake("INSTITUCIÃ“N") == "INSTITUCIÓN"
    assert fix_mojibake("EDUCACIÃ“N") == "EDUCACIÓN"
    assert fix_mojibake(None) is None


def test_normalize_whitespace() -> None:
    assert normalize_whitespace("BECA   18") == "BECA 18"
    assert normalize_whitespace("BECA CENTENARIO                                ") == "BECA CENTENARIO"
    assert normalize_whitespace("   ") is None
    assert normalize_whitespace("Beca\t18") == "Beca 18"
    assert normalize_whitespace("Beca\n18") == "Beca 18"


def test_strip_accents() -> None:
    assert strip_accents("ACADÉMICA") == "ACADEMICA"
    assert strip_accents("EDUCACIÓN") == "EDUCACION"
    assert strip_accents("NIÑOS") == "NINOS"


def test_normalize_text_for_matching() -> None:
    assert normalize_text_for_matching(" Beca   18 ") == "BECA 18"
    assert normalize_text_for_matching(" ACADÃ‰MICA  ") == "ACADEMICA"
    assert (
        normalize_text_for_matching("Beca de Permanencia - Convocatoria 2024")
        == "BECA DE PERMANENCIA CONVOCATORIA 2024"
    )


def test_detect_text_issues() -> None:
    assert "REPEATED_SPACES" in detect_text_issues("BECA   18")
    assert "LEADING_OR_TRAILING_SPACE" in detect_text_issues(
        "BECA CENTENARIO                                "
    )
    assert "MOJIBAKE_PATTERN" in detect_text_issues("ACADÃ‰MICA")
    assert "REPLACEMENT_CHARACTER" in detect_text_issues("ACAD�MICA")
    assert "TAB_OR_NEWLINE" in detect_text_issues("BECA\t18")
    assert "SUSPICIOUS_SYMBOL" in detect_text_issues("ACAD$'MICA")


def test_preview_text_normalization_has_expected_stages() -> None:
    preview = preview_text_normalization(" ACADÃ‰MICA  ")

    assert set(preview) == {
        "original_value",
        "after_fix_mojibake",
        "after_remove_control_chars",
        "after_normalize_whitespace",
        "after_strip_accents",
        "after_normalize_for_matching",
        "issues",
    }
    assert preview["after_fix_mojibake"] == " ACADÉMICA  "
    assert preview["after_normalize_for_matching"] == "ACADEMICA"


def test_territorial_column_detection() -> None:
    assert is_territorial_column("provincia") is True
    assert is_territorial_column("distrito") is True
    assert is_territorial_column("programa") is False


def test_aggregate_total_value_detection() -> None:
    assert is_aggregate_total_value("TOTAL") is True
    assert is_aggregate_total_value("TOTAL GENERAL") is True
    assert is_aggregate_total_value("CHACHAPOYAS") is False


def test_detect_territorial_aggregate_row() -> None:
    issues = detect_territorial_aggregate_issues(
        {"region": "AMAZONAS", "provincia": "TOTAL", "becarios": "123"}
    )
    issue_types = {issue["issue_type"] for issue in issues}

    assert "TERRITORIAL_TOTAL_VALUE" in issue_types
    assert "AGGREGATE_TOTAL_ROW" in issue_types


def test_detect_territorial_detail_row_without_total_issue() -> None:
    issues = detect_territorial_aggregate_issues(
        {"region": "AMAZONAS", "provincia": "CHACHAPOYAS", "becarios": "45"}
    )
    issue_types = {issue["issue_type"] for issue in issues}

    assert "TERRITORIAL_TOTAL_VALUE" not in issue_types
