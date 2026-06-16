"""
Pruebas unitarias para validar las transformaciones base y la ejecución
del skeleton de Apache Beam / Dataflow.
"""

from __future__ import annotations

from pathlib import Path

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from pipelines.dataflow_bronze_to_silver import ReadCsvDoFn, ReadJsonlDoFn, run
from pipelines.transforms.base import (
    add_technical_metadata,
    clean_text_basic,
    empty_to_none,
    parse_bool_safe,
    parse_int_safe,
    parse_numeric_safe,
)


def test_empty_to_none() -> None:
    """Valida la función empty_to_none."""
    assert empty_to_none("") is None
    assert empty_to_none("   ") is None
    assert empty_to_none(None) is None
    assert empty_to_none("Beca 18") == "Beca 18"
    assert empty_to_none(123) == 123


def test_clean_text_basic() -> None:
    """Valida la función clean_text_basic."""
    assert clean_text_basic("  Beca   18  ") == "Beca 18"
    assert clean_text_basic("Universidad\tNacional") == "Universidad Nacional"
    assert clean_text_basic("   ") is None
    assert clean_text_basic(None) is None
    assert clean_text_basic(123) == "123"


def test_parse_int_safe() -> None:
    """Valida la función parse_int_safe."""
    assert parse_int_safe("10") == 10
    assert parse_int_safe("1,234") == 1234
    assert parse_int_safe("") is None
    assert parse_int_safe("   ") is None
    assert parse_int_safe(None) is None
    assert parse_int_safe("not_a_number") is None
    assert parse_int_safe(45.6) == 45


def test_parse_numeric_safe() -> None:
    """Valida la función parse_numeric_safe en diversos formatos."""
    # Formatos no ambiguos
    assert parse_numeric_safe("1,234.56") == 1234.56
    assert parse_numeric_safe("1.234,56") == 1234.56
    assert parse_numeric_safe("16,43") == 16.43
    assert parse_numeric_safe("16.43") == 16.43
    assert parse_numeric_safe("-1,234.56") == -1234.56
    assert parse_numeric_safe("+16,43") == 16.43
    assert parse_numeric_safe("  $ 1,234.56 ") == 1234.56
    assert parse_numeric_safe(" 50 % ") == 50.0
    
    # Formatos limpios
    assert parse_numeric_safe("") is None
    assert parse_numeric_safe("   ") is None
    assert parse_numeric_safe(None) is None
    
    # Formatos ambiguos (deben retornar None)
    assert parse_numeric_safe("1.234") is None  # ¿1.234 decimal o 1234 entero?
    assert parse_numeric_safe("1,234") is None  # ¿1.234 decimal o 1234 entero?
    assert parse_numeric_safe("12,345") is None
    assert parse_numeric_safe("12.345") is None

    # Múltiples separadores no ambiguos
    assert parse_numeric_safe("1.234.567") == 1234567.0
    assert parse_numeric_safe("1,234,567") == 1234567.0


def test_parse_bool_safe() -> None:
    """Valida la función parse_bool_safe."""
    assert parse_bool_safe("true") is True
    assert parse_bool_safe("si") is True
    assert parse_bool_safe("yes") is True
    assert parse_bool_safe("1") is True
    assert parse_bool_safe(True) is True
    
    assert parse_bool_safe("false") is False
    assert parse_bool_safe("no") is False
    assert parse_bool_safe("0") is False
    assert parse_bool_safe(False) is False
    
    assert parse_bool_safe("maybe") is None
    assert parse_bool_safe("") is None
    assert parse_bool_safe(None) is None


def test_add_technical_metadata() -> None:
    """Valida la función add_technical_metadata."""
    record = {"id": "1", "nombre": "Beca 18"}
    updated = add_technical_metadata(
        record=record,
        source_system="pronabec_reports",
        source_dataset="becarios",
        extraction_date="2026-06-15",
        pipeline_run_id="run_123",
    )
    
    assert updated["id"] == "1"
    assert updated["nombre"] == "Beca 18"
    assert updated["source_system"] == "pronabec_reports"
    assert updated["source_dataset"] == "becarios"
    assert updated["extraction_date"] == "2026-06-15"
    assert updated["pipeline_run_id"] == "run_123"
    assert "ingestion_timestamp" in updated
    # Original dict no debe ser modificado
    assert "source_system" not in record


def test_read_csv_do_fn(tmp_path: Path) -> None:
    """Valida la lectura de CSV usando el DoFn."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text(
        "id,nombre,monto\n"
        "1, Beca 18 ,10\n"
        "2,,20\n",
        encoding="utf-8"
    )
    
    with TestPipeline() as p:
        results = (
            p
            | beam.Create([str(csv_file)])
            | beam.ParDo(ReadCsvDoFn())
        )
        
        expected = [
            {"id": "1", "nombre": " Beca 18 ", "monto": "10"},
            {"id": "2", "nombre": "", "monto": "20"},
        ]
        
        assert_that(results, equal_to(expected))


def test_read_jsonl_do_fn(tmp_path: Path) -> None:
    """Valida la lectura de JSONL usando el DoFn."""
    jsonl_file = tmp_path / "test.jsonl"
    jsonl_file.write_text(
        '{"id": "1", "nombre": "Beca 18", "monto": "10"}\n'
        '{"id": "2", "nombre": "", "monto": "20"}\n',
        encoding="utf-8"
    )
    
    with TestPipeline() as p:
        results = (
            p
            | beam.Create([str(jsonl_file)])
            | beam.ParDo(ReadJsonlDoFn())
        )
        
        expected = [
            {"id": "1", "nombre": "Beca 18", "monto": "10"},
            {"id": "2", "nombre": "", "monto": "20"},
        ]
        
        assert_that(results, equal_to(expected))


def test_local_pipeline_execution_csv(tmp_path: Path) -> None:
    """
    Ejecuta el pipeline local completo con DirectRunner sobre un archivo CSV
    en modo dry-run y valida que termine exitosamente.
    """
    input_file = tmp_path / "data.csv"
    input_file.write_text(
        "id,nombre,monto\n"
        "1, Beca 18 ,10\n",
        encoding="utf-8"
    )
    
    argv = [
        "--source-system", "pronabec_reports",
        "--source-dataset", "report_beca18_universitarios_universidad_anual",
        "--extraction-date", "2026-06-15",
        "--input-path", str(input_file),
        "--input-format", "csv",
        "--output-table", "test-project:silver.test_table",
        "--runner", "DirectRunner",
        "--dry-run",
    ]
    
    # Debe ejecutar y finalizar sin errores
    run(argv)


def test_local_pipeline_execution_jsonl(tmp_path: Path) -> None:
    """
    Ejecuta el pipeline local completo con DirectRunner sobre un archivo JSONL
    en modo dry-run y valida que termine exitosamente.
    """
    input_file = tmp_path / "data.jsonl"
    input_file.write_text(
        '{"id": "1", "nombre": "Beca 18", "monto": "10"}\n',
        encoding="utf-8"
    )
    
    argv = [
        "--source-system", "pronabec_reports",
        "--source-dataset", "report_beca18_universitarios_universidad_anual",
        "--extraction-date", "2026-06-15",
        "--input-path", str(input_file),
        "--input-format", "jsonl",
        "--output-table", "test-project:silver.test_table",
        "--runner", "DirectRunner",
        "--dry-run",
    ]
    
    # Debe ejecutar y finalizar sin errores
    run(argv)
