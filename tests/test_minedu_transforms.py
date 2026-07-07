from __future__ import annotations

from decimal import Decimal

from pipelines.transforms.minedu import transform_minedu_matricula_secundaria_departamental


def _record(**overrides: str) -> dict[str, str]:
    base = {
        "anio": "2025",
        "codigo_departamento": "02",
        "region": "Áncash",
        "nivel_educativo": "SECUNDARIA",
        "grado": "QUINTO_GRADO",
        "matricula_total": "100",
        "matricula_publica": "80",
        "matricula_privada": "20",
        "matricula_urbana": "70",
        "matricula_rural": "30",
        "matricula_masculino": "45",
        "matricula_femenino": "55",
    }
    base.update(overrides)
    return base


def _context() -> dict[str, str]:
    return {
        "extraction_date": "2026-07-08",
        "ingestion_timestamp": "2026-07-08T10:00:00+00:00",
        "pipeline_run_id": "manual-20260708",
    }


def test_transform_minedu_record_casts_types() -> None:
    transformed = transform_minedu_matricula_secundaria_departamental(_record(), _context())

    assert transformed["anio"] == 2025
    assert transformed["matricula_total"] == 100
    assert transformed["matricula_publica"] == 80


def test_transform_minedu_record_normalizes_region() -> None:
    transformed = transform_minedu_matricula_secundaria_departamental(_record(region="San Martín"), _context())

    assert transformed["region_normalizada"] == "SAN MARTIN"


def test_transform_minedu_record_assigns_grade_order() -> None:
    transformed = transform_minedu_matricula_secundaria_departamental(_record(grado="TERCER_GRADO"), _context())

    assert transformed["grado_orden"] == 3


def test_transform_minedu_record_calculates_percentages() -> None:
    transformed = transform_minedu_matricula_secundaria_departamental(_record(), _context())

    assert transformed["publica_pct"] == Decimal("0.8")
    assert transformed["femenino_pct"] == Decimal("0.55")


def test_transform_minedu_record_handles_zero_total() -> None:
    transformed = transform_minedu_matricula_secundaria_departamental(
        _record(
            matricula_total="0",
            matricula_publica="0",
            matricula_privada="0",
            matricula_urbana="0",
            matricula_rural="0",
            matricula_masculino="0",
            matricula_femenino="0",
        ),
        _context(),
    )

    assert transformed["publica_pct"] is None
    assert transformed["femenino_pct"] is None


def test_transform_minedu_record_preserves_metadata() -> None:
    transformed = transform_minedu_matricula_secundaria_departamental(_record(), _context())

    assert transformed["source_system"] == "MINEDU_ESCALE"
    assert transformed["source_dataset"] == "minedu_matricula_secundaria_departamental"
    assert transformed["extraction_date"] == "2026-07-08"
    assert transformed["pipeline_run_id"] == "manual-20260708"
