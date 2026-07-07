import pytest

from pipelines.common.config import ConfigError
from pipelines.common.gcs import (
    GCSPathError,
    build_gs_uri,
    build_mef_bronze_path,
    build_pronabec_normalized_path,
    build_pronabec_raw_path,
    build_rejected_records_path,
    is_gcs_uri,
    join_gcs_uri,
    list_gcs_objects,
    parse_gs_uri,
    parse_gcs_uri,
    read_gcs_bytes,
    upload_csv,
    upload_json,
    upload_jsonl,
    write_gcs_bytes,
    write_gcs_text,
)


class FakeBlob:
    def __init__(self, name: str = "") -> None:
        self.name = name
        self.uploaded_content = None
        self.uploaded_content_type = None
        self.uploaded_filename = None
        self.download_content = b""

    def upload_from_string(self, content: str, content_type: str) -> None:
        self.uploaded_content = content
        self.uploaded_content_type = content_type

    def upload_from_filename(self, filename: str, content_type: str) -> None:
        self.uploaded_filename = filename
        self.uploaded_content_type = content_type

    def download_as_bytes(self) -> bytes:
        return self.download_content


class FakeBucket:
    def __init__(self) -> None:
        self.blobs = {}

    def blob(self, object_path: str) -> FakeBlob:
        if object_path not in self.blobs:
            self.blobs[object_path] = FakeBlob(name=object_path)
        return self.blobs[object_path]


class FakeStorageClient:
    def __init__(self) -> None:
        self.fake_bucket = FakeBucket()
        self.listed_blobs = []

    def bucket(self, bucket_name: str) -> FakeBucket:
        return self.fake_bucket

    def list_blobs(self, bucket_name: str, prefix: str):
        return [
            blob
            for blob in self.listed_blobs
            if blob.name.startswith(prefix)
        ]


def test_is_gcs_uri() -> None:
    assert is_gcs_uri("gs://bucket/path.csv") is True
    assert is_gcs_uri("data/manual/file.csv") is False


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


def test_parse_gcs_uri_alias_returns_bucket_and_object_path() -> None:
    assert parse_gcs_uri("gs://bucket/landing/file.csv") == (
        "bucket",
        "landing/file.csv",
    )


def test_parse_gs_uri_raises_for_invalid_uri() -> None:
    with pytest.raises(GCSPathError):
        parse_gs_uri("https://example.com/file.csv")


def test_parse_gcs_uri_raises_for_missing_object_path() -> None:
    with pytest.raises(GCSPathError):
        parse_gcs_uri("gs://bucket")


def test_join_gcs_uri_joins_segments() -> None:
    uri = join_gcs_uri(
        "gs://bucket/landing/pronabec_reports/",
        "/pes_2025/",
        "data.csv",
    )

    assert uri == "gs://bucket/landing/pronabec_reports/pes_2025/data.csv"


def test_list_gcs_objects_uses_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeStorageClient()
    client.listed_blobs = [
        FakeBlob("landing/pronabec_reports/pes_2025/a.csv"),
        FakeBlob("landing/pronabec_reports/pes_2025/_documents/doc.pdf"),
        FakeBlob("landing/pronabec_reports/other/b.csv"),
    ]
    monkeypatch.setattr("pipelines.common.gcs.get_storage_client", lambda: client)

    objects = list_gcs_objects("gs://bucket/landing/pronabec_reports/pes_2025")

    assert objects == [
        "gs://bucket/landing/pronabec_reports/pes_2025/a.csv",
        "gs://bucket/landing/pronabec_reports/pes_2025/_documents/doc.pdf",
    ]


def test_read_gcs_bytes_downloads_blob(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeStorageClient()
    client.fake_bucket.blob("landing/file.csv").download_content = b"a,b\n1,2\n"
    monkeypatch.setattr("pipelines.common.gcs.get_storage_client", lambda: client)

    assert read_gcs_bytes("gs://bucket/landing/file.csv") == b"a,b\n1,2\n"


def test_write_gcs_bytes_uploads_blob(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeStorageClient()
    monkeypatch.setattr("pipelines.common.gcs.get_storage_client", lambda: client)

    write_gcs_bytes("gs://bucket/bronze/data.csv", b"a,b\n", content_type="text/csv")

    blob = client.fake_bucket.blobs["bronze/data.csv"]
    assert blob.uploaded_content == b"a,b\n"
    assert blob.uploaded_content_type == "text/csv"


def test_write_gcs_text_uploads_utf8_text(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeStorageClient()
    monkeypatch.setattr("pipelines.common.gcs.get_storage_client", lambda: client)

    write_gcs_text("gs://bucket/bronze/metadata.json", "{}")

    blob = client.fake_bucket.blobs["bronze/metadata.json"]
    assert blob.uploaded_content == b"{}"
    assert blob.uploaded_content_type == "text/plain; charset=utf-8"


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


def test_build_minedu_escale_bronze_path_from_generic_builder() -> None:
    from pipelines.common.config import build_gcs_path

    path = build_gcs_path(
        "bronze/minedu/escale_matricula_secundaria/extraction_date={extraction_date}/data.csv",
        extraction_date="2026-07-08",
    )

    assert path == "bronze/minedu/escale_matricula_secundaria/extraction_date=2026-07-08/data.csv"


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
