"""
Utilidades para Cloud Storage en Project Cloud BI Platform.

Este módulo centraliza construcción de rutas GCS y escritura de archivos usados
por la capa Bronze, DLQ y futuras zonas temporales del pipeline.
"""

from __future__ import annotations

import csv
import json
import tempfile
from pathlib import Path
from typing import Any, Iterable

from google.cloud import storage

from pipelines.common.config import ConfigError, build_gcs_path


class GCSPathError(ValueError):
    """Error asociado a rutas GCS inválidas."""


def build_gs_uri(bucket_name: str, object_path: str) -> str:
    """
    Construye una URI gs:// a partir de bucket y ruta de objeto.

    Args:
        bucket_name: Nombre del bucket sin prefijo gs://.
        object_path: Ruta del objeto dentro del bucket.

    Returns:
        URI completa en formato gs://bucket/path.
    """
    clean_bucket = bucket_name.replace("gs://", "").strip("/")
    clean_path = object_path.lstrip("/")

    if not clean_bucket:
        raise GCSPathError("El nombre del bucket no puede estar vacío.")

    if not clean_path:
        raise GCSPathError("La ruta del objeto no puede estar vacía.")

    return f"gs://{clean_bucket}/{clean_path}"


def is_gcs_uri(path: str) -> bool:
    """Indica si una ruta usa el esquema gs://."""
    return isinstance(path, str) and path.startswith("gs://")


def parse_gs_uri(uri: str) -> tuple[str, str]:
    """
    Divide una URI gs://bucket/path en bucket y object_path.

    Args:
        uri: URI de Cloud Storage.

    Returns:
        Tupla (bucket_name, object_path).

    Raises:
        GCSPathError: Si la URI no tiene formato válido.
    """
    if not uri.startswith("gs://"):
        raise GCSPathError(f"La URI debe iniciar con gs://: {uri}")

    without_scheme = uri.removeprefix("gs://")
    parts = without_scheme.split("/", 1)

    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise GCSPathError(f"URI GCS inválida: {uri}")

    return parts[0], parts[1]


def parse_gcs_uri(uri: str) -> tuple[str, str]:
    """Alias semántico de parse_gs_uri para helpers genéricos de GCS."""
    return parse_gs_uri(uri)


def join_gcs_uri(base_uri: str, *parts: str) -> str:
    """
    Une una URI base gs://bucket/prefix con segmentos adicionales.
    """
    bucket_name, object_path = parse_gcs_uri(base_uri)
    clean_parts = [object_path.strip("/")]

    for part in parts:
        clean_part = str(part).strip("/")
        if clean_part:
            clean_parts.append(clean_part)

    return build_gs_uri(bucket_name, "/".join(clean_parts))


def list_gcs_objects(uri: str) -> list[str]:
    """
    Lista objetos bajo una URI prefijo de Cloud Storage.
    """
    bucket_name, prefix = parse_gcs_uri(uri)
    storage_client = get_storage_client()
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix.rstrip("/") + "/")
    return [build_gs_uri(bucket_name, blob.name) for blob in blobs]


def read_gcs_bytes(uri: str) -> bytes:
    """Lee un objeto de Cloud Storage como bytes."""
    bucket_name, object_path = parse_gcs_uri(uri)
    storage_client = get_storage_client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(object_path)
    return blob.download_as_bytes()


def write_gcs_bytes(
    uri: str,
    content: bytes,
    content_type: str | None = None,
) -> None:
    """Escribe bytes en un objeto de Cloud Storage."""
    bucket_name, object_path = parse_gcs_uri(uri)
    storage_client = get_storage_client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(object_path)
    blob.upload_from_string(content, content_type=content_type)


def write_gcs_text(uri: str, content: str) -> None:
    """Escribe texto UTF-8 en un objeto de Cloud Storage."""
    write_gcs_bytes(
        uri,
        content.encode("utf-8"),
        content_type="text/plain; charset=utf-8",
    )


def build_pronabec_raw_path(
    template: str,
    dataset: str,
    extraction_date: str,
) -> str:
    """
    Construye la ruta relativa para data_raw.json de PRONABEC.
    """
    return build_gcs_path(
        template,
        dataset=dataset,
        extraction_date=extraction_date,
    )


def build_pronabec_normalized_path(
    template: str,
    dataset: str,
    extraction_date: str,
) -> str:
    """
    Construye la ruta relativa para data.jsonl de PRONABEC.
    """
    return build_gcs_path(
        template,
        dataset=dataset,
        extraction_date=extraction_date,
    )


def build_mef_bronze_path(
    template: str,
    extraction_date: str,
    fiscal_year: str | int | None = None,
) -> str:
    """
    Construye la ruta relativa para data.csv de MEF, opcionalmente particionada por año fiscal.
    """
    base_path = build_gcs_path(
        template,
        extraction_date=extraction_date,
    )
    if fiscal_year is not None:
        p = Path(base_path)
        return str(p.parent / f"year={fiscal_year}" / p.name).replace("\\", "/")
    return base_path


def build_rejected_records_path(
    template: str,
    dataset: str,
    extraction_date: str,
) -> str:
    """
    Construye la ruta relativa para registros rechazados en DLQ.
    """
    return build_gcs_path(
        template,
        dataset=dataset,
        extraction_date=extraction_date,
    )


def get_storage_client(project_id: str | None = None) -> storage.Client:
    """
    Crea un cliente de Cloud Storage.

    Args:
        project_id: ID del proyecto GCP. Si se omite, usa configuración del entorno.

    Returns:
        Cliente de google-cloud-storage.
    """
    if project_id:
        return storage.Client(project=project_id)

    return storage.Client()


def bucket_exists(
    bucket_name: str,
    client: storage.Client | None = None,
) -> bool:
    """
    Verifica si un bucket existe y es accesible.

    Args:
        bucket_name: Nombre del bucket.
        client: Cliente opcional de Cloud Storage.

    Returns:
        True si existe, False si no existe o no es accesible.
    """
    storage_client = client or get_storage_client()
    bucket = storage_client.lookup_bucket(bucket_name)
    return bucket is not None


def upload_text(
    bucket_name: str,
    object_path: str,
    content: str,
    content_type: str = "text/plain",
    client: storage.Client | None = None,
) -> str:
    """
    Sube texto a Cloud Storage.

    Args:
        bucket_name: Nombre del bucket.
        object_path: Ruta destino dentro del bucket.
        content: Contenido textual.
        content_type: MIME type del archivo.
        client: Cliente opcional de Cloud Storage.

    Returns:
        URI gs:// del objeto escrito.
    """
    storage_client = client or get_storage_client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(object_path)

    blob.upload_from_string(
        content,
        content_type=content_type,
    )

    return build_gs_uri(bucket_name, object_path)


def upload_json(
    bucket_name: str,
    object_path: str,
    payload: Any,
    client: storage.Client | None = None,
) -> str:
    """
    Sube un objeto JSON a Cloud Storage.
    """
    content = json.dumps(payload, ensure_ascii=False, indent=2)
    return upload_text(
        bucket_name=bucket_name,
        object_path=object_path,
        content=content,
        content_type="application/json",
        client=client,
    )


def upload_jsonl(
    bucket_name: str,
    object_path: str,
    records: Iterable[dict[str, Any]],
    client: storage.Client | None = None,
) -> str:
    """
    Sube registros en formato JSON Lines a Cloud Storage.

    Cada registro se escribe como una línea JSON independiente.
    """
    lines = [
        json.dumps(record, ensure_ascii=False)
        for record in records
    ]
    content = "\n".join(lines)

    if content:
        content += "\n"

    return upload_text(
        bucket_name=bucket_name,
        object_path=object_path,
        content=content,
        content_type="application/x-ndjson",
        client=client,
    )


def upload_csv(
    bucket_name: str,
    object_path: str,
    records: Iterable[dict[str, Any]],
    fieldnames: list[str],
    client: storage.Client | None = None,
) -> str:
    """
    Sube registros tabulares como CSV a Cloud Storage.

    Args:
        bucket_name: Nombre del bucket.
        object_path: Ruta destino dentro del bucket.
        records: Iterable de diccionarios.
        fieldnames: Columnas del CSV.
        client: Cliente opcional de Cloud Storage.

    Returns:
        URI gs:// del objeto escrito.
    """
    if not fieldnames:
        raise ConfigError("fieldnames no puede estar vacío para escribir CSV.")

    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="",
        suffix=".csv",
        delete=False,
    ) as temp_file:
        writer = csv.DictWriter(temp_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
        temp_path = Path(temp_file.name)

    try:
        storage_client = client or get_storage_client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(object_path)
        blob.upload_from_filename(
            str(temp_path),
            content_type="text/csv",
        )
        return build_gs_uri(bucket_name, object_path)
    finally:
        temp_path.unlink(missing_ok=True)


def upload_file(
    bucket_name: str,
    object_path: str,
    local_path: str | Path,
    content_type: str = "application/octet-stream",
    client: storage.Client | None = None,
) -> str:
    """
    Sube un archivo local a Cloud Storage.

    Args:
        bucket_name: Nombre del bucket.
        object_path: Ruta destino dentro del bucket.
        local_path: Ruta al archivo local.
        content_type: MIME type del archivo.
        client: Cliente opcional de Cloud Storage.

    Returns:
        URI gs:// del objeto escrito.
    """
    storage_client = client or get_storage_client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(object_path)
    blob.upload_from_filename(
        str(local_path),
        content_type=content_type,
    )
    return build_gs_uri(bucket_name, object_path)

