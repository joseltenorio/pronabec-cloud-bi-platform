"""
Utilidades de validación para Project Cloud BI Platform.

Estas funciones permiten validar columnas esperadas, campos requeridos y separar
registros válidos e inválidos antes de escribir en Bronze, Silver o DLQ.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable


@dataclass
class ValidationErrorDetail:
    """Detalle de un error de validación para un registro."""

    field_name: str | None
    error_code: str
    error_message: str


@dataclass
class ValidationResult:
    """Resultado de validar un registro."""

    is_valid: bool
    errors: list[ValidationErrorDetail] = field(default_factory=list)


def validate_required_columns(
    actual_columns: Iterable[str],
    expected_columns: Iterable[str],
) -> list[str]:
    """
    Valida que todas las columnas esperadas existan.

    Args:
        actual_columns: Columnas reales disponibles.
        expected_columns: Columnas esperadas.

    Returns:
        Lista de columnas faltantes.
    """
    actual_set = set(actual_columns)
    expected_set = set(expected_columns)

    return sorted(expected_set - actual_set)


def validate_no_unexpected_columns(
    actual_columns: Iterable[str],
    expected_columns: Iterable[str],
) -> list[str]:
    """
    Identifica columnas no esperadas.

    Args:
        actual_columns: Columnas reales disponibles.
        expected_columns: Columnas esperadas.

    Returns:
        Lista de columnas inesperadas.
    """
    actual_set = set(actual_columns)
    expected_set = set(expected_columns)

    return sorted(actual_set - expected_set)


def is_blank(value: Any) -> bool:
    """
    Determina si un valor debe considerarse vacío.
    """
    if value is None:
        return True

    if isinstance(value, str):
        return value.strip() == ""

    return False


def validate_required_fields(
    record: dict[str, Any],
    required_fields: Iterable[str],
) -> ValidationResult:
    """
    Valida que un registro tenga campos requeridos no vacíos.
    """
    errors: list[ValidationErrorDetail] = []

    for field_name in required_fields:
        if field_name not in record:
            errors.append(
                ValidationErrorDetail(
                    field_name=field_name,
                    error_code="MISSING_FIELD",
                    error_message=f"Campo requerido no existe: {field_name}",
                )
            )
            continue

        if is_blank(record.get(field_name)):
            errors.append(
                ValidationErrorDetail(
                    field_name=field_name,
                    error_code="BLANK_FIELD",
                    error_message=f"Campo requerido vacío: {field_name}",
                )
            )

    return ValidationResult(
        is_valid=len(errors) == 0,
        errors=errors,
    )


def validate_numeric_range(
    record: dict[str, Any],
    field_name: str,
    min_value: float | None = None,
    max_value: float | None = None,
) -> ValidationResult:
    """
    Valida que un campo numérico esté dentro de un rango.
    """
    value = record.get(field_name)
    errors: list[ValidationErrorDetail] = []

    if is_blank(value):
        return ValidationResult(is_valid=True)

    try:
        numeric_value = float(str(value).replace(",", "."))
    except ValueError:
        return ValidationResult(
            is_valid=False,
            errors=[
                ValidationErrorDetail(
                    field_name=field_name,
                    error_code="INVALID_NUMERIC",
                    error_message=f"Valor no numérico en campo {field_name}: {value}",
                )
            ],
        )

    if min_value is not None and numeric_value < min_value:
        errors.append(
            ValidationErrorDetail(
                field_name=field_name,
                error_code="NUMERIC_BELOW_MIN",
                error_message=(
                    f"Valor {numeric_value} menor que mínimo permitido "
                    f"{min_value} en campo {field_name}"
                ),
            )
        )

    if max_value is not None and numeric_value > max_value:
        errors.append(
            ValidationErrorDetail(
                field_name=field_name,
                error_code="NUMERIC_ABOVE_MAX",
                error_message=(
                    f"Valor {numeric_value} mayor que máximo permitido "
                    f"{max_value} en campo {field_name}"
                ),
            )
        )

    return ValidationResult(
        is_valid=len(errors) == 0,
        errors=errors,
    )


def merge_validation_results(results: Iterable[ValidationResult]) -> ValidationResult:
    """
    Une múltiples resultados de validación.
    """
    all_errors: list[ValidationErrorDetail] = []

    for result in results:
        all_errors.extend(result.errors)

    return ValidationResult(
        is_valid=len(all_errors) == 0,
        errors=all_errors,
    )


def split_valid_invalid_records(
    records: Iterable[dict[str, Any]],
    required_fields: Iterable[str] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Separa registros válidos e inválidos según campos requeridos.

    Args:
        records: Registros a validar.
        required_fields: Campos obligatorios.

    Returns:
        Tupla (valid_records, invalid_records). Los inválidos incluyen
        validation_errors.
    """
    valid_records: list[dict[str, Any]] = []
    invalid_records: list[dict[str, Any]] = []
    required = list(required_fields or [])

    for record in records:
        result = validate_required_fields(record, required)

        if result.is_valid:
            valid_records.append(record)
        else:
            invalid_record = {
                **record,
                "validation_errors": [
                    {
                        "field_name": error.field_name,
                        "error_code": error.error_code,
                        "error_message": error.error_message,
                    }
                    for error in result.errors
                ],
            }
            invalid_records.append(invalid_record)

    return valid_records, invalid_records


def count_records(records: Iterable[dict[str, Any]]) -> int:
    """
    Cuenta registros de un iterable materializándolo de forma controlada.

    Nota:
        Usar esta función con iterables pequeños o listas. Para Dataflow se usarán
        métricas propias de Beam.
    """
    return sum(1 for _ in records)