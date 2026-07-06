from __future__ import annotations

import json
from pathlib import Path

import apache_beam as beam
import pytest

from pipelines.common.bigquery import (
    BigQueryWriteConfig,
    build_bigquery_write_config,
    build_silver_delete_query,
    build_bigquery_write_transform,
    cleanup_silver_rows_for_source_date,
    validate_bigquery_table_reference,
    validate_create_disposition,
    validate_silver_write_mode,
    validate_write_disposition,
)
from pipelines.dataflow_bronze_to_silver import (
    parse_arguments,
    run,
    validate_arguments,
)
from pipelines.transforms.mef import transform_mef_record
from pipelines.transforms.pronabec import transform_pronabec_record
from pipelines.transforms.pronabec_reports import transform_pronabec_report_record


def _schema_fields(path: Path) -> set[str]:
    return {field["name"] for field in json.loads(path.read_text(encoding="utf-8"))}


@pytest.mark.parametrize(
    "output_table, expected",
    [
        ("test-project:silver.pronabec_convocatorias", "test-project:silver.pronabec_convocatorias"),
        (" my-project:dataset.table_name ", "my-project:dataset.table_name"),
        ("project-123:dataset_abc.table_abc", "project-123:dataset_abc.table_abc"),
    ],
)
def test_validate_bigquery_table_reference_accepts_valid_values(
    output_table: str,
    expected: str,
) -> None:
    assert validate_bigquery_table_reference(output_table) == expected


@pytest.mark.parametrize(
    "output_table",
    [
        "silver.pronabec_convocatorias",
        "project.dataset.table",
        "project:silver",
        ":dataset.table",
        "project:.table",
        "project:dataset.",
        "",
        None,
    ],
)
def test_validate_bigquery_table_reference_rejects_invalid_values(
    output_table: str | None,
) -> None:
    with pytest.raises(ValueError, match="output-table"):
        validate_bigquery_table_reference(output_table)


def test_bigquery_write_disposition_defaults_and_validation() -> None:
    config = build_bigquery_write_config("test-project:silver.table")
    assert config.write_disposition == "WRITE_APPEND"
    assert config.create_disposition == "CREATE_NEVER"

    assert validate_write_disposition("write_append") == "WRITE_APPEND"
    assert validate_create_disposition("create_if_needed") == "CREATE_IF_NEEDED"

    with pytest.raises(ValueError, match="write-disposition"):
        validate_write_disposition("bad_value")

    with pytest.raises(ValueError, match="create-disposition"):
        validate_create_disposition("bad_value")


def test_silver_write_mode_defaults_to_replace_by_source_date() -> None:
    assert validate_silver_write_mode(None) == "replace_by_source_date"
    assert validate_silver_write_mode("append") == "append"
    assert validate_silver_write_mode("REPLACE_BY_SOURCE_DATE") == "replace_by_source_date"

    with pytest.raises(ValueError, match="SILVER_WRITE_MODE"):
        validate_silver_write_mode("truncate")


def test_build_silver_delete_query_scopes_cleanup_by_source_and_date() -> None:
    query, parameters = build_silver_delete_query(
        output_table="test-project:silver.presupuesto_mef",
        extraction_date="2026-07-02",
        source_system="mef",
        source_dataset="presupuesto",
    )

    assert query == (
        "DELETE FROM `test-project.silver.presupuesto_mef` "
        "WHERE extraction_date = @extraction_date "
        "AND source_system = @source_system "
        "AND source_dataset = @source_dataset"
    )
    assert [param.name for param in parameters] == [
        "extraction_date",
        "source_system",
        "source_dataset",
    ]
    assert [param.value for param in parameters] == [
        "2026-07-02",
        "mef",
        "presupuesto",
    ]


@pytest.mark.parametrize(
    "kwargs, expected_message",
    [
        ({"output_table": None}, "output-table"),
        ({"extraction_date": None}, "BRONZE_EXTRACTION_DATE"),
        ({"source_system": ""}, "SOURCE_SYSTEM"),
        ({"source_dataset": " "}, "SOURCE_DATASET"),
    ],
)
def test_build_silver_delete_query_requires_full_filters(
    kwargs: dict[str, str | None],
    expected_message: str,
) -> None:
    values = {
        "output_table": "test-project:silver.presupuesto_mef",
        "extraction_date": "2026-07-02",
        "source_system": "mef",
        "source_dataset": "presupuesto",
    }
    values.update(kwargs)

    with pytest.raises(ValueError, match=expected_message):
        build_silver_delete_query(**values)


def test_cleanup_silver_rows_executes_scoped_delete() -> None:
    captured: dict[str, object] = {}

    class FakeJob:
        num_dml_affected_rows = 12

        def result(self) -> None:
            captured["waited"] = True

    class FakeClient:
        def query(self, query: str, job_config: object) -> FakeJob:
            captured["query"] = query
            captured["job_config"] = job_config
            return FakeJob()

    deleted_rows = cleanup_silver_rows_for_source_date(
        output_table="test-project:silver.presupuesto_mef",
        extraction_date="2026-07-02",
        source_system="mef",
        source_dataset="presupuesto",
        client=FakeClient(),
    )

    assert deleted_rows == 12
    assert "DELETE FROM `test-project.silver.presupuesto_mef`" in str(captured["query"])
    assert "extraction_date = @extraction_date" in str(captured["query"])
    assert captured["waited"] is True


def test_build_bigquery_write_transform_uses_validated_config(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class FakeWriteToBigQuery:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

    monkeypatch.setattr("pipelines.common.bigquery.WriteToBigQuery", FakeWriteToBigQuery)

    config = BigQueryWriteConfig(
        output_table="test-project:silver.table",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        custom_gcs_temp_location="gs://test-bucket/temp",
    )
    sink = build_bigquery_write_transform(config)

    assert isinstance(sink, FakeWriteToBigQuery)
    assert captured == {
        "table": "test-project:silver.table",
        "write_disposition": "WRITE_TRUNCATE",
        "create_disposition": "CREATE_IF_NEEDED",
        "custom_gcs_temp_location": "gs://test-bucket/temp",
    }


def test_bigquery_write_config_preserves_temp_location() -> None:
    config = build_bigquery_write_config(
        "test-project:silver.table",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        custom_gcs_temp_location="gs://my-bucket/tmp",
    )
    assert config.custom_gcs_temp_location == "gs://my-bucket/tmp"

    with pytest.raises(ValueError, match="Formato invalido de ubicacion temporal GCS"):
        build_bigquery_write_config(
            "test-project:silver.table",
            custom_gcs_temp_location="invalid-non-gcs-path",
        )


def test_validate_arguments_allows_dry_run_without_output_table() -> None:
    args, _ = parse_arguments(
        [
            "--source-system",
            "pronabec",
            "--source-dataset",
            "convocatorias",
            "--extraction-date",
            "2026-06-15",
            "--input-path",
            "tmp/data.csv",
            "--input-format",
            "csv",
            "--runner",
            "DirectRunner",
            "--dry-run",
        ]
    )

    validate_arguments(args)
    assert args.write_disposition == "WRITE_APPEND"
    assert args.create_disposition == "CREATE_NEVER"


def test_validate_arguments_requires_output_table_when_not_dry_run() -> None:
    args, _ = parse_arguments(
        [
            "--source-system",
            "pronabec",
            "--source-dataset",
            "convocatorias",
            "--extraction-date",
            "2026-06-15",
            "--input-path",
            "tmp/data.csv",
            "--input-format",
            "csv",
            "--runner",
            "DirectRunner",
        ]
    )

    with pytest.raises(ValueError, match="output-table"):
        validate_arguments(args)


def test_validate_arguments_requires_temp_location_when_not_dry_run() -> None:
    # Caso 1: dry_run=False, output_table presente, pero temp_location ausente -> debe fallar
    args, _ = parse_arguments(
        [
            "--source-system", "pronabec",
            "--source-dataset", "convocatorias",
            "--extraction-date", "2026-06-15",
            "--input-path", "tmp/data.csv",
            "--input-format", "csv",
            "--output-table", "test-project:silver.pronabec_convocatorias",
            "--runner", "DirectRunner",
        ]
    )
    with pytest.raises(ValueError, match="temp-location es requerido"):
        validate_arguments(args)

    # Caso 2: dry_run=False, output_table presente y temp_location presente -> debe pasar
    args_ok, _ = parse_arguments(
        [
            "--source-system", "pronabec",
            "--source-dataset", "convocatorias",
            "--extraction-date", "2026-06-15",
            "--input-path", "tmp/data.csv",
            "--input-format", "csv",
            "--output-table", "test-project:silver.pronabec_convocatorias",
            "--temp-location", "gs://test-bucket/temp",
            "--runner", "DirectRunner",
        ]
    )
    validate_arguments(args_ok)
    assert args_ok.temp_location == "gs://test-bucket/temp"


def test_dry_run_skips_bigquery_sink(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    input_file = tmp_path / "data.csv"
    input_file.write_text(
        "source_row_id,id_convocatoria,codigo_anual,description_conv,modalidad,programa,vacantes\n"
        "1,10,2026-01, Beca   Especial , ACADÃ‰MICA , BECA CENTENARIO ,12\n",
        encoding="utf-8",
    )

    def fail_if_called(*args: object, **kwargs: object) -> None:
        raise AssertionError("BigQuery sink no debe construirse en dry-run")

    monkeypatch.setattr(
        "pipelines.dataflow_bronze_to_silver.build_bigquery_write_transform",
        fail_if_called,
    )

    run(
        [
            "--source-system",
            "pronabec",
            "--source-dataset",
            "convocatorias",
            "--extraction-date",
            "2026-06-15",
            "--input-path",
            str(input_file),
            "--input-format",
            "csv",
            "--runner",
            "DirectRunner",
            "--dry-run",
        ]
    )


def test_no_dry_run_configures_bigquery_sink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_file = tmp_path / "data.csv"
    input_file.write_text(
        "source_row_id,id_convocatoria,codigo_anual,description_conv,modalidad,programa,vacantes\n"
        "1,10,2026-01, Beca   Especial , ACADÃ‰MICA , BECA CENTENARIO ,12\n",
        encoding="utf-8",
    )

    captured: dict[str, object] = {}

    def fake_build_sink(config: BigQueryWriteConfig) -> beam.PTransform:
        captured["config"] = config
        return beam.Map(lambda record: record)

    def fail_if_cleanup_called(*args: object, **kwargs: object) -> None:
        raise AssertionError("Silver cleanup no debe ejecutarse en modo append")

    monkeypatch.setattr(
        "pipelines.dataflow_bronze_to_silver.build_bigquery_write_transform",
        fake_build_sink,
    )
    monkeypatch.setattr(
        "pipelines.dataflow_bronze_to_silver.cleanup_silver_rows_for_source_date",
        fail_if_cleanup_called,
    )

    run(
        [
            "--source-system",
            "pronabec",
            "--source-dataset",
            "convocatorias",
            "--extraction-date",
            "2026-06-15",
            "--input-path",
            str(input_file),
            "--input-format",
            "csv",
            "--output-table",
            "test-project:silver.pronabec_convocatorias",
            "--temp-location",
            "gs://test-bucket/temp",
            "--write-disposition",
            "WRITE_TRUNCATE",
            "--create-disposition",
            "CREATE_IF_NEEDED",
            "--silver-write-mode",
            "append",
            "--runner",
            "DirectRunner",
        ]
    )

    assert "config" in captured
    config = captured["config"]
    assert isinstance(config, BigQueryWriteConfig)
    assert config.output_table == "test-project:silver.pronabec_convocatorias"
    assert config.write_disposition == "WRITE_TRUNCATE"
    assert config.create_disposition == "CREATE_IF_NEEDED"
    assert config.custom_gcs_temp_location == "gs://test-bucket/temp"


def test_replace_by_source_date_cleans_up_before_bigquery_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_file = tmp_path / "data.csv"
    input_file.write_text(
        "source_row_id,id_convocatoria,codigo_anual,description_conv,modalidad,programa,vacantes\n"
        "1,10,2026-01,Beca Especial,ACADEMICA,BECA CENTENARIO,12\n",
        encoding="utf-8",
    )

    events: list[tuple[str, dict[str, object]]] = []

    def fake_cleanup(**kwargs: object) -> int:
        events.append(("cleanup", kwargs))
        return 7

    def fake_build_sink(config: BigQueryWriteConfig) -> beam.PTransform:
        events.append(("write", {"write_disposition": config.write_disposition}))
        return beam.Map(lambda record: record)

    monkeypatch.setattr(
        "pipelines.dataflow_bronze_to_silver.cleanup_silver_rows_for_source_date",
        fake_cleanup,
    )
    monkeypatch.setattr(
        "pipelines.dataflow_bronze_to_silver.build_bigquery_write_transform",
        fake_build_sink,
    )

    run(
        [
            "--source-system",
            "pronabec",
            "--source-dataset",
            "convocatorias",
            "--extraction-date",
            "2026-07-02",
            "--input-path",
            str(input_file),
            "--input-format",
            "csv",
            "--output-table",
            "test-project:silver.pronabec_convocatorias",
            "--temp-location",
            "gs://test-bucket/temp",
            "--runner",
            "DirectRunner",
            "--disable-dlq",
        ]
    )

    assert events[0] == (
        "cleanup",
        {
            "output_table": "test-project:silver.pronabec_convocatorias",
            "extraction_date": "2026-07-02",
            "source_system": "pronabec",
            "source_dataset": "convocatorias",
            "project_id": "test-project",
        },
    )
    assert events[1] == ("write", {"write_disposition": "WRITE_APPEND"})


def test_repeated_same_source_date_runs_cleanup_each_time(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_file = tmp_path / "data.csv"
    input_file.write_text(
        "source_row_id,id_convocatoria,codigo_anual,description_conv,modalidad,programa,vacantes\n"
        "1,10,2026-01,Beca Especial,ACADEMICA,BECA CENTENARIO,12\n",
        encoding="utf-8",
    )

    cleanup_calls: list[dict[str, object]] = []

    def fake_cleanup(**kwargs: object) -> int:
        cleanup_calls.append(kwargs)
        return 1

    monkeypatch.setattr(
        "pipelines.dataflow_bronze_to_silver.cleanup_silver_rows_for_source_date",
        fake_cleanup,
    )
    monkeypatch.setattr(
        "pipelines.dataflow_bronze_to_silver.build_bigquery_write_transform",
        lambda config: beam.Map(lambda record: record),
    )

    argv = [
        "--source-system",
        "pronabec",
        "--source-dataset",
        "convocatorias",
        "--extraction-date",
        "2026-07-02",
        "--input-path",
        str(input_file),
        "--input-format",
        "csv",
        "--output-table",
        "test-project:silver.pronabec_convocatorias",
        "--temp-location",
        "gs://test-bucket/temp",
        "--runner",
        "DirectRunner",
        "--disable-dlq",
    ]

    run(argv)
    run(argv)

    assert len(cleanup_calls) == 2
    assert cleanup_calls[0] == cleanup_calls[1]


@pytest.mark.parametrize(
    "transform, source_dataset, record, schema_name",
    [
        (
            transform_pronabec_record,
            "convocatorias",
            {
                "source_row_id": "10",
                "id_convocatoria": "123",
                "codigo_anual": " 2021-02 ",
                "description_conv": " Beca   Especial ",
                "modalidad": " ACADÃ‰MICA ",
                "programa": "BECA CENTENARIO                                ",
                "vacantes": "100",
            },
            "pronabec_convocatorias_schema.json",
        ),
        (
            transform_pronabec_report_record,
            "report_beca18_universitarios_carrera_anual",
            {
                "carrera_estudio": "INGENIERÍA   DE SISTEMAS",
                "2012": "1,000",
                "2026 (*)": "10",
                "Total": "1,010",
            },
            "pronabec_report_beca18_universitarios_carrera_anual_schema.json",
        ),
        (
            transform_mef_record,
            "presupuesto_mef",
            {
                "ano": "2026",
                "ejecutora_codigo": "117-1438",
                "ejecutora_nombre": " PROGRAMA NACIONAL DE BECAS Y CREDITO EDUCATIVO ",
                "pia": "1,000",
                "pim": "2,000",
                "devengado": "1,500",
                "avance_porcentaje": "75.0",
            },
            "presupuesto_mef_schema.json",
        ),
    ],
)
def test_transformed_records_match_silver_schema(
    transform,
    source_dataset: str,
    record: dict[str, str],
    schema_name: str,
) -> None:
    context = {
        "extraction_date": "2026-06-15",
        "ingestion_timestamp": "2026-06-16T00:00:00+00:00",
        "pipeline_run_id": "manual-bq-check",
    }

    if transform is transform_pronabec_report_record:
        outputs = transform(source_dataset, record, context)
        assert outputs
        output = outputs[0]
    else:
        output = transform(source_dataset, record, context)

    repo_root = Path(__file__).resolve().parents[1]
    schema_path = repo_root / "config" / "schemas" / "silver" / schema_name
    schema_fields = _schema_fields(schema_path)

    assert set(output).issubset(schema_fields)
