from datetime import date, datetime, timezone

from pipelines.common.audit import (
    calculate_duration_seconds,
    create_audit_event,
    create_extraction_audit_event,
    generate_run_id,
)
from pipelines.common.validation import (
    count_records,
    is_blank,
    merge_validation_results,
    split_valid_invalid_records,
    validate_no_unexpected_columns,
    validate_numeric_range,
    validate_required_columns,
    validate_required_fields,
)


def test_generate_run_id_uses_prefix() -> None:
    run_id = generate_run_id("test run")

    assert run_id.startswith("test_run_")
    assert len(run_id) > len("test_run_")


def test_calculate_duration_seconds() -> None:
    started_at = datetime(2026, 6, 10, 10, 0, 0, tzinfo=timezone.utc)
    finished_at = datetime(2026, 6, 10, 10, 1, 30, tzinfo=timezone.utc)

    duration = calculate_duration_seconds(started_at, finished_at)

    assert duration == 90


def test_create_audit_event_serializes_dates() -> None:
    event = create_audit_event(
        event_type="test",
        pipeline_name="project_cloud_bi_platform",
        status="SUCCESS",
        execution_date=date(2026, 6, 10),
        metadata={"dataset": "notas_becarios"},
    )

    payload = event.to_dict()

    assert payload["event_type"] == "test"
    assert payload["pipeline_name"] == "project_cloud_bi_platform"
    assert payload["status"] == "SUCCESS"
    assert payload["execution_date"] == "2026-06-10"
    assert payload["metadata"] == {"dataset": "notas_becarios"}


def test_create_extraction_audit_event() -> None:
    event = create_extraction_audit_event(
        pipeline_name="project_cloud_bi_platform",
        source_name="PRONABEC Datos Abiertos",
        source_dataset="notas_becarios",
        status="SUCCESS",
        records_read=100,
        records_written=100,
    )

    assert event.event_type == "extraction"
    assert event.source_name == "PRONABEC Datos Abiertos"
    assert event.source_dataset == "notas_becarios"
    assert event.records_read == 100
    assert event.records_written == 100


def test_validate_required_columns_returns_missing_columns() -> None:
    missing = validate_required_columns(
        actual_columns=["codigo_becario", "semestre"],
        expected_columns=["codigo_becario", "semestre", "nota_promedio"],
    )

    assert missing == ["nota_promedio"]


def test_validate_no_unexpected_columns_returns_extra_columns() -> None:
    unexpected = validate_no_unexpected_columns(
        actual_columns=["codigo_becario", "semestre", "extra_field"],
        expected_columns=["codigo_becario", "semestre"],
    )

    assert unexpected == ["extra_field"]


def test_is_blank() -> None:
    assert is_blank(None)
    assert is_blank("")
    assert is_blank("   ")
    assert not is_blank("abc")
    assert not is_blank(0)


def test_validate_required_fields_detects_missing_and_blank_fields() -> None:
    result = validate_required_fields(
        record={"codigo_becario": "B001", "semestre": ""},
        required_fields=["codigo_becario", "semestre", "nota_promedio"],
    )

    assert not result.is_valid
    assert [error.error_code for error in result.errors] == [
        "BLANK_FIELD",
        "MISSING_FIELD",
    ]


def test_validate_numeric_range_accepts_comma_decimal() -> None:
    result = validate_numeric_range(
        record={"nota_promedio": "16,43"},
        field_name="nota_promedio",
        min_value=0,
        max_value=20,
    )

    assert result.is_valid


def test_validate_numeric_range_rejects_invalid_numeric() -> None:
    result = validate_numeric_range(
        record={"nota_promedio": "no disponible"},
        field_name="nota_promedio",
        min_value=0,
        max_value=20,
    )

    assert not result.is_valid
    assert result.errors[0].error_code == "INVALID_NUMERIC"


def test_validate_numeric_range_rejects_out_of_range_value() -> None:
    result = validate_numeric_range(
        record={"nota_promedio": "25"},
        field_name="nota_promedio",
        min_value=0,
        max_value=20,
    )

    assert not result.is_valid
    assert result.errors[0].error_code == "NUMERIC_ABOVE_MAX"


def test_merge_validation_results() -> None:
    required_result = validate_required_fields(
        record={"codigo_becario": ""},
        required_fields=["codigo_becario"],
    )
    numeric_result = validate_numeric_range(
        record={"nota_promedio": "25"},
        field_name="nota_promedio",
        min_value=0,
        max_value=20,
    )

    merged = merge_validation_results([required_result, numeric_result])

    assert not merged.is_valid
    assert len(merged.errors) == 2


def test_split_valid_invalid_records() -> None:
    records = [
        {"codigo_becario": "B001", "semestre": "2024-I"},
        {"codigo_becario": "", "semestre": "2024-I"},
    ]

    valid_records, invalid_records = split_valid_invalid_records(
        records,
        required_fields=["codigo_becario", "semestre"],
    )

    assert len(valid_records) == 1
    assert len(invalid_records) == 1
    assert invalid_records[0]["validation_errors"][0]["error_code"] == "BLANK_FIELD"


def test_count_records() -> None:
    records = [
        {"id": 1},
        {"id": 2},
        {"id": 3},
    ]

    assert count_records(records) == 3