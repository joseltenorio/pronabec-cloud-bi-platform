import pytest

from pipelines.common.config import ConfigError
from pipelines.common.gcs import (
    GCSPathError,
    build_gs_uri,
    build_mef_bronze_path,
    build_pronabec_normalized_path,
    build_pronabec_raw_path,
    build_rejected_records_path,
    parse_gs_uri,
    upload_csv,
    upload_json,
    upload_jsonl,
)


class FakeBlob:
    def __init__(self) -> None:
        self.uploaded_content = None
        self.uploaded_content_type = None
        self.uploaded_filename = None

    def upload_from_string(self, content: str, content_type: str) -> None:
        self.uploaded_content = content
        self.uploaded_content_type = content_type

    def upload_from_filename(self, filename: str, content_type: str) -> None:
        self.uploaded_filename = filename
        self.uploaded_content_type = content_type


class FakeBucket:
    def __init__(self) -> None:
        self.blobs = {}

    def blob(self, object_path: str) -> FakeBlob:
        blob = FakeBlob()
        self.blobs[object_path] = blob
        return blob


class FakeStorageClient:
    def __init__(self) -> None:
        self.fake_bucket = FakeBucket()

    def bucket(self, bucket_name: str) -> FakeBucket:
        return self.fake_bucket


def test_build_gs_uri_returns_valid_uri() -> None:
    uri = build_gs_uri(
        "project-cloud-bi-platform-lake",
        "bronze/pronabec/notas_becarios/extraction_date=2026-06-10/data.jsonl",
    )

    assert uri == (
        "gs://project-cloud-bi-platform-lake/"
        "bronze/pronabec/notas_becarios/extraction_date=2026-06-10/data.jsonl"
    )


def test_build_gs_uri_accepts_bucket_with_scheme() -> None:
    uri = build_gs_uri(
        "gs://project-cloud-bi-platform-lake",
        "/bronze/mef/presupuesto/extraction_date=2026-06-10/data.csv",
    )

    assert uri == (
        "gs://project-cloud-bi-platform-lake/"
        "bronze/mef/presupuesto/extraction_date=2026-06-10/data.csv"
    )


def test_build_gs_uri_raises_when_bucket_is_empty() -> None:
    with pytest.raises(GCSPathError):
        build_gs_uri("", "bronze/file.jsonl")


def test_build_gs_uri_raises_when_object_path_is_empty() -> None:
    with pytest.raises(GCSPathError):
        build_gs_uri("project-cloud-bi-platform-lake", "")


def test_parse_gs_uri_returns_bucket_and_object_path() -> None:
    bucket, object_path = parse_gs_uri(
        "gs://project-cloud-bi-platform-lake/bronze/pronabec/data.jsonl"
    )

    assert bucket == "project-cloud-bi-platform-lake"
    assert object_path == "bronze/pronabec/data.jsonl"


def test_parse_gs_uri_raises_for_invalid_uri() -> None:
    with pytest.raises(GCSPathError):
        parse_gs_uri("https://example.com/file.csv")


def test_build_pronabec_raw_path() -> None:
    path = build_pronabec_raw_path(
        "bronze/pronabec/{dataset}/extraction_date={extraction_date}/data_raw.json",
        dataset="notas_becarios",
        extraction_date="2026-06-10",
    )

    assert path == (
        "bronze/pronabec/notas_becarios/"
        "extraction_date=2026-06-10/data_raw.json"
    )


def test_build_pronabec_normalized_path() -> None:
    path = build_pronabec_normalized_path(
        "bronze/pronabec/{dataset}/extraction_date={extraction_date}/data.jsonl",
        dataset="notas_becarios",
        extraction_date="2026-06-10",
    )

    assert path == (
        "bronze/pronabec/notas_becarios/"
        "extraction_date=2026-06-10/data.jsonl"
    )


def test_build_mef_bronze_path() -> None:
    path = build_mef_bronze_path(
        "bronze/mef/presupuesto/extraction_date={extraction_date}/data.csv",
        extraction_date="2026-06-10",
    )

    assert path == "bronze/mef/presupuesto/extraction_date=2026-06-10/data.csv"


def test_build_rejected_records_path() -> None:
    path = build_rejected_records_path(
        "dlq/{dataset}/extraction_date={extraction_date}/rejected_records.jsonl",
        dataset="notas_becarios",
        extraction_date="2026-06-10",
    )

    assert path == (
        "dlq/notas_becarios/"
        "extraction_date=2026-06-10/rejected_records.jsonl"
    )


def test_build_rejected_records_path_raises_when_extraction_date_is_missing() -> None:
    with pytest.raises(ConfigError):
        build_rejected_records_path(
            "dlq/{dataset}/extraction_date={extraction_date}/rejected_records.jsonl",
            dataset="notas_becarios",
            extraction_date=None,
        )


def test_upload_json_uses_application_json_content_type() -> None:
    client = FakeStorageClient()

    uri = upload_json(
        bucket_name="project-cloud-bi-platform-lake",
        object_path="bronze/test/data_raw.json",
        payload={"records": 1},
        client=client,
    )

    blob = client.fake_bucket.blobs["bronze/test/data_raw.json"]

    assert uri == "gs://project-cloud-bi-platform-lake/bronze/test/data_raw.json"
    assert blob.uploaded_content_type == "application/json"
    assert '"records": 1' in blob.uploaded_content


def test_upload_jsonl_uses_ndjson_content_type() -> None:
    client = FakeStorageClient()

    uri = upload_jsonl(
        bucket_name="project-cloud-bi-platform-lake",
        object_path="bronze/test/data.jsonl",
        records=[{"id": 1}, {"id": 2}],
        client=client,
    )

    blob = client.fake_bucket.blobs["bronze/test/data.jsonl"]

    assert uri == "gs://project-cloud-bi-platform-lake/bronze/test/data.jsonl"
    assert blob.uploaded_content_type == "application/x-ndjson"
    assert blob.uploaded_content == '{"id": 1}\n{"id": 2}\n'


def test_upload_csv_uses_csv_content_type() -> None:
    client = FakeStorageClient()

    uri = upload_csv(
        bucket_name="project-cloud-bi-platform-lake",
        object_path="bronze/mef/presupuesto/data.csv",
        records=[{"ano": 2026, "pim": 100}],
        fieldnames=["ano", "pim"],
        client=client,
    )

    blob = client.fake_bucket.blobs["bronze/mef/presupuesto/data.csv"]

    assert uri == "gs://project-cloud-bi-platform-lake/bronze/mef/presupuesto/data.csv"
    assert blob.uploaded_content_type == "text/csv"
    assert blob.uploaded_filename is not None


def test_upload_csv_raises_when_fieldnames_are_empty() -> None:
    with pytest.raises(ConfigError):
        upload_csv(
            bucket_name="project-cloud-bi-platform-lake",
            object_path="bronze/mef/presupuesto/data.csv",
            records=[],
            fieldnames=[],
            client=FakeStorageClient(),
        )