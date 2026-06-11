"""
Utilidades de configuración para Project Cloud BI Platform.

Este módulo centraliza la lectura de archivos YAML y variables de entorno para
evitar valores quemados dentro de extractores, pipelines Dataflow y scripts de
calidad de datos.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml


class ConfigError(Exception):
    """Error asociado a configuración faltante o inválida."""


def load_yaml_config(path: str | Path) -> dict[str, Any]:
    """
    Carga un archivo YAML y devuelve su contenido como diccionario.

    Args:
        path: Ruta del archivo YAML.

    Returns:
        Diccionario con la configuración cargada.

    Raises:
        FileNotFoundError: Si el archivo no existe.
        ConfigError: Si el YAML está vacío o no contiene un diccionario.
    """
    config_path = Path(path)

    if not config_path.exists():
        raise FileNotFoundError(f"No existe el archivo de configuración: {config_path}")

    with config_path.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    if config is None:
        raise ConfigError(f"El archivo de configuración está vacío: {config_path}")

    if not isinstance(config, dict):
        raise ConfigError(
            f"El archivo de configuración debe contener un objeto YAML raíz: {config_path}"
        )

    return config


def get_env_var(
    name: str,
    default: str | None = None,
    required: bool = False,
) -> str | None:
    """
    Obtiene una variable de entorno.

    Args:
        name: Nombre de la variable de entorno.
        default: Valor por defecto si la variable no existe.
        required: Si es True, lanza error cuando la variable no está definida.

    Returns:
        Valor de la variable de entorno o el valor por defecto.

    Raises:
        ConfigError: Si required=True y la variable no existe.
    """
    value = os.getenv(name, default)

    if required and (value is None or value == ""):
        raise ConfigError(f"Variable de entorno requerida no definida: {name}")

    return value


def get_nested_value(
    config: dict[str, Any],
    path: str,
    default: Any = None,
    required: bool = False,
) -> Any:
    """
    Obtiene un valor anidado usando una ruta con puntos.

    Ejemplo:
        get_nested_value(config, "pipeline.name")

    Args:
        config: Diccionario de configuración.
        path: Ruta anidada separada por puntos.
        default: Valor por defecto.
        required: Si es True, lanza error si la ruta no existe.

    Returns:
        Valor encontrado o valor por defecto.

    Raises:
        ConfigError: Si required=True y la ruta no existe.
    """
    current: Any = config

    for key in path.split("."):
        if not isinstance(current, dict) or key not in current:
            if required:
                raise ConfigError(f"Configuración requerida no encontrada: {path}")
            return default

        current = current[key]

    return current


def get_pipeline_settings(config_path: str | Path = "config/pipeline.yaml") -> dict[str, Any]:
    """
    Carga configuración principal del pipeline y la devuelve enriquecida con
    variables de entorno relevantes.

    Args:
        config_path: Ruta al archivo config/pipeline.yaml.

    Returns:
        Diccionario con configuración base y valores derivados.
    """
    config = load_yaml_config(config_path)

    pipeline_name = get_nested_value(config, "pipeline.name", required=True)
    environment = get_nested_value(config, "pipeline.environment", default="dev")
    timezone = get_nested_value(config, "pipeline.timezone", default="America/Lima")

    bucket_env_var = get_nested_value(
        config,
        "storage.bucket_env_var",
        default="GCS_BUCKET_NAME",
    )
    bucket_name = get_env_var(bucket_env_var, default=None, required=False)

    log_level_env_var = get_nested_value(
        config,
        "observability.log_level_env_var",
        default="LOG_LEVEL",
    )
    log_level = get_env_var(log_level_env_var, default="INFO", required=False)

    return {
        "config": config,
        "pipeline_name": pipeline_name,
        "environment": environment,
        "timezone": timezone,
        "bucket_env_var": bucket_env_var,
        "bucket_name": bucket_name,
        "log_level": log_level,
        "bigquery_datasets": get_nested_value(config, "bigquery.datasets", default={}),
        "gcs_paths": get_nested_value(config, "gcs_paths", default={}),
    }


def build_gcs_path(template: str, **values: str) -> str:
    """
    Construye una ruta GCS relativa a partir de una plantilla.

    Ejemplo:
        build_gcs_path(
            "bronze/pronabec/{dataset}/extraction_date={extraction_date}/data.jsonl",
            dataset="notas_becarios",
            extraction_date="2026-06-10",
        )

    Args:
        template: Plantilla de ruta.
        values: Valores para reemplazar en la plantilla.

    Returns:
        Ruta renderizada.

    Raises:
        ConfigError: Si falta una variable requerida por la plantilla.
        ConfigError: Si una variable requerida viene vacía o como None.
    """
    invalid_values = [
        key
        for key, value in values.items()
        if value is None or str(value).strip() == ""
    ]

    if invalid_values:
        invalid_keys = ", ".join(invalid_values)
        raise ConfigError(f"Valores inválidos para construir ruta GCS: {invalid_keys}")

    try:
        return template.format(**values)
    except KeyError as exc:
        missing_key = exc.args[0]
        raise ConfigError(f"Falta valor para construir ruta GCS: {missing_key}") from exc