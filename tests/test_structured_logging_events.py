import json
import logging

from pipelines.common.logging import (
    JsonFormatter,
    log_pipeline_completed,
    log_pipeline_event,
    log_pipeline_failed,
    log_pipeline_metric,
    log_pipeline_started,
)


def _build_test_logger():
    logger = logging.getLogger("test_structured_pipeline_events")
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    logger.propagate = False

    records = []

    class ListHandler(logging.Handler):
        def emit(self, record):
            records.append(record)

    handler = ListHandler()
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)

    return logger, records, handler


def _format_last_record(records, handler):
    assert records
    return json.loads(handler.format(records[-1]))


def test_log_pipeline_event_adds_standard_fields():
    logger, records, handler = _build_test_logger()

    log_pipeline_event(
        logger,
        event_type="custom_event",
        pipeline_name="test_pipeline",
        pipeline_run_id="run-001",
        status="STARTED",
        source_system="pronabec",
        source_dataset="convocatorias",
        extraction_date="2026-06-25",
        empty_value="",
        none_value=None,
    )

    payload = _format_last_record(records, handler)

    assert payload["event_type"] == "custom_event"
    assert payload["pipeline_name"] == "test_pipeline"
    assert payload["pipeline_run_id"] == "run-001"
    assert payload["status"] == "STARTED"
    assert payload["source_system"] == "pronabec"
    assert payload["source_dataset"] == "convocatorias"
    assert payload["extraction_date"] == "2026-06-25"
    assert "empty_value" not in payload
    assert "none_value" not in payload


def test_log_pipeline_started_emits_started_status():
    logger, records, handler = _build_test_logger()

    log_pipeline_started(
        logger,
        pipeline_name="bronze_extract",
        pipeline_run_id="run-002",
    )

    payload = _format_last_record(records, handler)

    assert payload["event_type"] == "pipeline_started"
    assert payload["status"] == "STARTED"
    assert payload["pipeline_name"] == "bronze_extract"


def test_log_pipeline_completed_emits_counts_and_output():
    logger, records, handler = _build_test_logger()

    log_pipeline_completed(
        logger,
        pipeline_name="bronze_to_silver",
        pipeline_run_id="run-003",
        records_read=100,
        records_valid=95,
        records_rejected=5,
        rejection_rate=5.0,
        output_table="project:silver.table",
        duration_seconds=12.5,
    )

    payload = _format_last_record(records, handler)

    assert payload["event_type"] == "pipeline_completed"
    assert payload["status"] == "SUCCEEDED"
    assert payload["records_read"] == 100
    assert payload["records_valid"] == 95
    assert payload["records_rejected"] == 5
    assert payload["rejection_rate"] == 5.0
    assert payload["output_table"] == "project:silver.table"
    assert payload["duration_seconds"] == 12.5


def test_log_pipeline_failed_emits_error_fields():
    logger, records, handler = _build_test_logger()

    log_pipeline_failed(
        logger,
        pipeline_name="quality_checks",
        pipeline_run_id="run-004",
        error_code="QUALITY_ERROR",
        error_message="Required field contains nulls.",
    )

    payload = _format_last_record(records, handler)

    assert payload["event_type"] == "pipeline_failed"
    assert payload["status"] == "FAILED"
    assert payload["error_code"] == "QUALITY_ERROR"
    assert payload["error_message"] == "Required field contains nulls."


def test_log_pipeline_metric_emits_metric_payload():
    logger, records, handler = _build_test_logger()

    log_pipeline_metric(
        logger,
        pipeline_name="dataflow",
        metric_name="records_rejected",
        metric_value=3,
        pipeline_run_id="run-005",
    )

    payload = _format_last_record(records, handler)

    assert payload["event_type"] == "pipeline_metric"
    assert payload["metric_name"] == "records_rejected"
    assert payload["metric_value"] == 3
    assert payload["pipeline_run_id"] == "run-005"