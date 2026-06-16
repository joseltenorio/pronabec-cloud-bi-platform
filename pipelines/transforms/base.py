"""
Utilidades y transformaciones base para los pipelines de datos.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any


def utc_now() -> datetime:
    """Devuelve el timestamp actual en UTC."""
    return datetime.now(timezone.utc)


def empty_to_none(value: Any) -> Any:
    """
    Convierte cadenas vacías o compuestas solo por espacios en None.
    Mantiene otros tipos intactos.
    """
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return None if stripped == "" else value
    return value


def clean_text_basic(value: Any) -> str | None:
    """
    Realiza una limpieza básica de texto:
    - Convierte a cadena si no es None.
    - Elimina espacios en los extremos (strip).
    - Normaliza espacios múltiples a un solo espacio.
    - Devuelve None si la cadena queda vacía.
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    # Reemplazar múltiples espacios o caracteres de control por un solo espacio
    text = re.sub(r"\s+", " ", text)
    return text if text != "" else None


def parse_int_safe(value: Any) -> int | None:
    """
    Intenta convertir un valor a entero de forma segura.
    Remueve comas de miles si existen.
    Devuelve None si el valor es vacío o no se puede convertir.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
        
    cleaned = clean_text_basic(value)
    if cleaned is None:
        return None
        
    # Remover comas que actúan como separadores de miles
    cleaned = cleaned.replace(",", "")
    try:
        return int(cleaned)
    except ValueError:
        # Intentar parsear como float primero por si viene con decimales ".0"
        try:
            return int(float(cleaned))
        except ValueError:
            return None


def parse_numeric_safe(value: Any) -> float | None:
    """
    Intenta convertir un valor a flotante de forma segura y conservadora.
    Soporta formatos con coma decimal y separadores de miles.
    Si el formato es ambiguo (ej. un solo punto/coma seguido de 3 dígitos),
    se devuelve None para evitar adivinar.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
        
    cleaned = clean_text_basic(value)
    if cleaned is None:
        return None
        
    # Limpieza básica de caracteres monetarios o porcentuales
    cleaned = cleaned.replace("$", "").replace("%", "").strip()
    
    # Manejar signo
    sign = 1.0
    if cleaned.startswith("-"):
        sign = -1.0
        cleaned = cleaned[1:].strip()
    elif cleaned.startswith("+"):
        cleaned = cleaned[1:].strip()
        
    if not cleaned:
        return None
        
    has_comma = "," in cleaned
    has_dot = "." in cleaned
    
    if has_comma and has_dot:
        # Ambos presentes: no ambiguo (ej. 1,234.56 o 1.234,56)
        comma_idx = cleaned.rfind(",")
        dot_idx = cleaned.rfind(".")
        if comma_idx > dot_idx:
            # Comma es decimal (ej. 1.234,56)
            normalized = cleaned.replace(".", "").replace(",", ".")
        else:
            # Dot es decimal (ej. 1,234.56)
            normalized = cleaned.replace(",", "")
            
        try:
            return sign * float(normalized)
        except ValueError:
            return None
            
    elif has_comma:
        # Solo coma presente
        parts = cleaned.split(",")
        if len(parts) == 2 and len(parts[1]) == 3:
            # Ambiguo: "1,234" podría ser 1.234 o 1234 en distintas regiones
            return None
        elif len(parts) > 2:
            # Múltiples comas (separadores de miles)
            if all(len(p) == 3 for p in parts[1:]):
                normalized = cleaned.replace(",", "")
                try:
                    return sign * float(normalized)
                except ValueError:
                    return None
            else:
                return None
        else:
            # Una sola coma y no seguida de 3 dígitos -> decimal
            normalized = cleaned.replace(",", ".")
            try:
                return sign * float(normalized)
            except ValueError:
                return None
                
    elif has_dot:
        # Solo punto presente
        parts = cleaned.split(".")
        if len(parts) == 2 and len(parts[1]) == 3:
            # Ambiguo: "1.234" podría ser 1.234 o 1234
            return None
        elif len(parts) > 2:
            # Múltiples puntos (separadores de miles)
            if all(len(p) == 3 for p in parts[1:]):
                normalized = cleaned.replace(".", "")
                try:
                    return sign * float(normalized)
                except ValueError:
                    return None
            else:
                return None
        else:
            # Un solo punto y no seguido de 3 dígitos -> decimal
            try:
                return sign * float(cleaned)
            except ValueError:
                return None
    else:
        # Sin separadores
        try:
            return sign * float(cleaned)
        except ValueError:
            return None


def parse_bool_safe(value: Any) -> bool | None:
    """
    Intenta convertir un valor a booleano de forma segura.
    Soporta representaciones como True/False, 1/0, si/no, yes/no.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if value == 1:
            return True
        if value == 0:
            return False
        return None
        
    cleaned = clean_text_basic(value)
    if cleaned is None:
        return None
        
    lower_val = cleaned.lower()
    if lower_val in ("true", "1", "yes", "y", "si", "s"):
        return True
    if lower_val in ("false", "0", "no", "n"):
        return False
        
    return None


def add_technical_metadata(
    record: dict[str, Any],
    source_system: str,
    source_dataset: str,
    extraction_date: str,
    pipeline_run_id: str,
) -> dict[str, Any]:
    """
    Agrega los campos de metadata técnica estándar a un registro.
    Retorna un nuevo diccionario para evitar efectos secundarios.
    """
    new_record = dict(record)
    new_record["source_system"] = source_system
    new_record["source_dataset"] = source_dataset
    new_record["extraction_date"] = extraction_date
    new_record["ingestion_timestamp"] = utc_now().isoformat()
    new_record["pipeline_run_id"] = pipeline_run_id
    return new_record
