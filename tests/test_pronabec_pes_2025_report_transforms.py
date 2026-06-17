# -*- coding: utf-8 -*-
"""Pruebas unitarias para validar las transformaciones de reportes de PES 2025 de PRONABEC."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from pipelines.dataflow_bronze_to_silver import run
from pipelines.transforms.pronabec_reports import (
    REPORT_SPECS,
    DOCUMENT_METADATA_FIELDS,
    TECHNICAL_METADATA_FIELDS,
    ReportTransformSpec,
    transform_pronabec_report_record,
)

# Contexto común para las pruebas
CONTEXT = {
    "extraction_date": "2026-06-15",
    "ingestion_timestamp": "2026-06-16T00:00:00+00:00",
    "pipeline_run_id": "test-run",
}

# Rutas del repositorio
REPO_ROOT = Path(__file__).resolve().parents[1]
BRONZE_SCHEMA_DIR = REPO_ROOT / "config" / "schemas" / "bronze"
SILVER_SCHEMA_DIR = REPO_ROOT / "config" / "schemas" / "silver"

# Lista completa de los 21 datasets esperados para PES 2025
EXPECTED_PES_2025_DATASETS = [
    "report_beca18_autoidentificacion_etnica_modalidad_2025",
    "report_beca18_becas_otorgadas_modalidad_anual",
    "report_beca18_colegio_gestion_2025",
    "report_beca18_enp_promedio_caracteristica_2025",
    "report_beca18_enp_promedio_region_2025",
    "report_beca18_lengua_materna_modalidad_2025",
    "report_beca18_migracion_region_acumulada",
    "report_beca18_migracion_region_anual",
    "report_beca18_no_continuaria_sin_beca_caracteristica_2025",
    "report_beca18_padres_nivel_educativo_2025",
    "report_beca18_periodo_ingreso_ies_genero_2025",
    "report_beca18_preparacion_ies_meses_caracteristica_2025",
    "report_beca18_preparacion_ies_tipo_2025",
    "report_beca18_primera_generacion_region",
    "report_beca18_razones_eleccion_carrera_gestion_ies_2025",
    "report_beca18_razones_eleccion_carrera_sexo_2025",
    "report_beca18_razones_eleccion_ies_gestion_2025",
    "report_beca18_region_postulacion_2025",
    "report_beca18_region_postulacion_acumulada",
    "report_beca18_region_postulacion_anual",
    "report_beca18_sexo_anual",
]

def schema_fields(path: Path) -> list[str]:
    # Retorna la lista de nombres de campos en un esquema JSON.
    return [field["name"] for field in json.loads(path.read_text(encoding="utf-8"))]

def silver_fields(source_dataset: str) -> list[str]:
    # Retorna los campos del esquema de Silver para un dataset dado.
    return schema_fields(SILVER_SCHEMA_DIR / f"pronabec_{source_dataset}_schema.json")

def build_dummy_snapshot_record(spec: ReportTransformSpec) -> dict[str, Any]:
    # Genera un registro simulado (dummy) basado en los tipos de campo especificados.
    record = {}
    for field_name, field_type in spec.field_types.items():
        if field_name == "ano_encuesta" or field_name == "ano_convocatoria":
            record[field_name] = "2025"
        elif field_name == "periodo":
            record[field_name] = "2025-I"
        elif field_type == "int":
            record[field_name] = "1,234"
        elif field_type == "percent":
            record[field_name] = "12,5%"
        elif field_type == "numeric":
            record[field_name] = "16,43"
        else:
            record[field_name] = f" VALOR {field_name.upper()} "
    
    # Agregar metadatos de documento opcionales si aplica
    for field_name in spec.metadata_fields:
        if field_name == "source_page":
            record[field_name] = "5"
        else:
            record[field_name] = f"doc_{field_name}"
            
    return record

# Pruebas de Inventario y Conformidad de Schemas

def test_pes_2025_inventory_and_specs() -> None:
    # Valida que todos los 21 datasets tengan esquemas Bronze/Silver y estén en REPORT_SPECS.
    for dataset in EXPECTED_PES_2025_DATASETS:
        bronze_path = BRONZE_SCHEMA_DIR / f"{dataset}_schema.json"
        silver_path = SILVER_SCHEMA_DIR / f"pronabec_{dataset}_schema.json"
        
        assert bronze_path.exists(), f"El esquema Bronze no existe para: {dataset}"
        assert silver_path.exists(), f"El esquema Silver no existe para: {dataset}"
        assert dataset in REPORT_SPECS, f"El dataset no está registrado en REPORT_SPECS: {dataset}"

def test_router_recognizes_all_pes_2025_datasets() -> None:
    # Valida que el transformador reconozca y pueda enrutar cada uno de los 21 datasets.
    for dataset in EXPECTED_PES_2025_DATASETS:
        spec = REPORT_SPECS[dataset]
        if spec.report_type == "annual_wide":
            # Para reportes anuales anchos, se requiere una dimensión y columnas de año.
            record = {
                spec.dimension_columns[0]: " CATEGORIA PRUEBA ",
                "2024": "15",
                "2025 (*)": "20",
                "Total": "35",
            }
        else:
            record = build_dummy_snapshot_record(spec)
            
        rows = transform_pronabec_report_record(dataset, record, CONTEXT)
        assert len(rows) >= 1
        for row in rows:
            # Comprobar que los campos de salida coinciden exactamente con el esquema de Silver.
            assert list(row) == silver_fields(dataset)
            assert row["source_system"] == "pronabec_reports"
            assert row["source_dataset"] == dataset

# Pruebas focalizadas en datasets específicos requeridos

def test_report_beca18_becas_otorgadas_modalidad_anual() -> None:
    # Validaciones específicas para report_beca18_becas_otorgadas_modalidad_anual (snapshot).
    dataset = "report_beca18_becas_otorgadas_modalidad_anual"
    spec = REPORT_SPECS[dataset]
    
    assert spec.report_type == "snapshot"
    
    # Registro simulando datos reales
    record = {
        "modalidad": " Beca 18 Ordinaria ",
        "ano_convocatoria": " 2012 ",
        "becas_otorgadas": " 1,500 ",
        "source_page": "8",
    }
    
    rows = transform_pronabec_report_record(dataset, record, CONTEXT)
    assert len(rows) == 1
    assert rows[0]["modalidad"] == "Beca 18 Ordinaria"
    assert rows[0]["ano_convocatoria"] == 2012
    assert rows[0]["becas_otorgadas"] == 1500
    assert rows[0]["source_page"] == 8

def test_report_beca18_sexo_anual() -> None:
    # Validaciones para report_beca18_sexo_anual (snapshot).
    dataset = "report_beca18_sexo_anual"
    record = {
        "sexo": " Femenino ",
        "ano_convocatoria": " 2012 ",
        "porcentaje_becarios": " 45,2 % ",
    }
    rows = transform_pronabec_report_record(dataset, record, CONTEXT)
    assert len(rows) == 1
    assert rows[0]["sexo"] == "Femenino"
    assert rows[0]["ano_convocatoria"] == 2012
    assert rows[0]["porcentaje_becarios"] == 45.2

def test_report_beca18_region_postulacion_anual_and_acumulada() -> None:
    # Validaciones para report_beca18_region_postulacion_anual (snapshot) y acumulada (snapshot).
    dataset_anual = "report_beca18_region_postulacion_anual"
    record_anual = {
        "grupo_region": " Lima ",
        "ano_convocatoria": " 2012 ",
        "porcentaje_becarios": " 35,6 ",
    }
    rows_anual = transform_pronabec_report_record(dataset_anual, record_anual, CONTEXT)
    assert len(rows_anual) == 1
    assert rows_anual[0]["grupo_region"] == "Lima"
    assert rows_anual[0]["ano_convocatoria"] == 2012
    assert rows_anual[0]["porcentaje_becarios"] == 35.6

    dataset_acum = "report_beca18_region_postulacion_acumulada"
    record_acum = {
        "periodo": "2012-2025",
        "region": " Arequipa ",
        "porcentaje_acumulado": " 12,3% ",
    }
    rows_acum = transform_pronabec_report_record(dataset_acum, record_acum, CONTEXT)
    assert len(rows_acum) == 1
    assert rows_acum[0]["periodo"] == "2012-2025"
    assert rows_acum[0]["region"] == "Arequipa"
    assert rows_acum[0]["porcentaje_acumulado"] == 12.3

def test_report_beca18_migracion_region_anual_and_acumulada() -> None:
    # Validaciones para report_beca18_migracion_region_anual (snapshot) y acumulada (snapshot).
    dataset_anual = "report_beca18_migracion_region_anual"
    record_anual = {
        "ano_convocatoria": "2012",
        "porcentaje_migracion_region": "12,4",
    }
    rows_anual = transform_pronabec_report_record(dataset_anual, record_anual, CONTEXT)
    assert len(rows_anual) == 1
    assert rows_anual[0]["ano_convocatoria"] == 2012
    assert rows_anual[0]["porcentaje_migracion_region"] == 12.4

    dataset_acum = "report_beca18_migracion_region_acumulada"
    record_acum = {
        "periodo": "2012-2025",
        "region": " No Migrante ",
        "tasa_migracion_acumulada": "87,5",
    }
    rows_acum = transform_pronabec_report_record(dataset_acum, record_acum, CONTEXT)
    assert len(rows_acum) == 1
    assert rows_acum[0]["periodo"] == "2012-2025"
    assert rows_acum[0]["region"] == "No Migrante"
    assert rows_acum[0]["tasa_migracion_acumulada"] == 87.5

def test_report_beca18_autoidentificacion_etnica_modalidad_2025() -> None:
    # Validaciones para report_beca18_autoidentificacion_etnica_modalidad_2025.
    dataset = "report_beca18_autoidentificacion_etnica_modalidad_2025"
    record = {
        "ano_encuesta": " 2025 ",
        "autoidentificacion_etnica": " Quechua ",
        "modalidad": " Beca 18 Ordinaria ",
        "porcentaje_becarios": " 45,2 % ",
    }
    rows = transform_pronabec_report_record(dataset, record, CONTEXT)
    assert len(rows) == 1
    assert rows[0]["ano_encuesta"] == 2025
    assert rows[0]["autoidentificacion_etnica"] == "Quechua"
    assert rows[0]["modalidad"] == "Beca 18 Ordinaria"
    assert rows[0]["porcentaje_becarios"] == 45.2

def test_report_beca18_lengua_materna_modalidad_2025() -> None:
    # Validaciones para report_beca18_lengua_materna_modalidad_2025.
    dataset = "report_beca18_lengua_materna_modalidad_2025"
    record = {
        "ano_encuesta": "2025",
        "lengua_materna": " Castellano ",
        "modalidad": " VRAEM ",
        "porcentaje_becarios": "33,3",
    }
    rows = transform_pronabec_report_record(dataset, record, CONTEXT)
    assert len(rows) == 1
    assert rows[0]["lengua_materna"] == "Castellano"
    assert rows[0]["modalidad"] == "VRAEM"
    assert rows[0]["porcentaje_becarios"] == 33.3

def test_report_beca18_colegio_gestion_2025() -> None:
    # Validaciones para report_beca18_colegio_gestion_2025.
    dataset = "report_beca18_colegio_gestion_2025"
    record = {
        "ano_encuesta": "2025",
        "tipo_gestion_colegio": " Privada ",
        "porcentaje_becarios": " 22,12 % ",
    }
    rows = transform_pronabec_report_record(dataset, record, CONTEXT)
    assert len(rows) == 1
    assert rows[0]["tipo_gestion_colegio"] == "Privada"
    assert rows[0]["porcentaje_becarios"] == 22.12

def test_report_beca18_padres_nivel_educativo_2025() -> None:
    # Validaciones para report_beca18_padres_nivel_educativo_2025.
    dataset = "report_beca18_padres_nivel_educativo_2025"
    record = {
        "ano_encuesta": "2025",
        "nivel_educativo_padres": " Primaria Completa ",
        "porcentaje_becarios": " 18,9 ",
    }
    rows = transform_pronabec_report_record(dataset, record, CONTEXT)
    assert len(rows) == 1
    assert rows[0]["nivel_educativo_padres"] == "Primaria Completa"
    assert rows[0]["porcentaje_becarios"] == 18.9

# Validaciones de Casos Borde, Formatos, Totales y Limpieza Técnica en Reportes Anuales Anchos (Wide)

def test_wide_report_skips_totals_completely() -> None:
    # Valida que los reportes anuales anchos omitan las filas y columnas de totales.
    dataset = "report_beca18_universitarios_universidad_anual"
    
    # 1. Caso de fila Total General
    record_total_row = {
        "universidad": " Total general ",
        "anio_2012": "1,000",
        "anio_2013": "2,000",
        "total": "3,000",
    }
    rows = transform_pronabec_report_record(dataset, record_total_row, CONTEXT)
    assert rows == [] # Debe descartar la fila de total completamente
    
    # 2. Fila normal con columna Total
    record_normal = {
        "universidad": " UNIVERSIDAD DE PRUEBA ",
        "anio_2012": "500",
        "total": "500",
    }
    rows = transform_pronabec_report_record(dataset, record_normal, CONTEXT)
    assert len(rows) == 1
    assert rows[0]["ano_convocatoria"] == 2012
    # El diccionario de salida no debe contener campo "total" ni se procesa
    assert "total" not in rows[0]

def test_guion_dash_conversion_rules() -> None:
    # Valida que los guiones '-' se conviertan según corresponda (a 0 para cantidad_becarios y None para porcentaje).
    
    # Caso 1: En wide report con cantidad_becarios (report_beca18_universitarios_universidad_anual)
    rows_wide = transform_pronabec_report_record(
        "report_beca18_universitarios_universidad_anual",
        {"universidad": "UNI", "anio_2012": "-"},
        CONTEXT
    )
    assert rows_wide[0]["cantidad_becarios"] == 0
    
    # Caso 2: En snapshot report con porcentaje (report_beca18_padres_nivel_educativo_2025)
    rows_snap = transform_pronabec_report_record(
        "report_beca18_padres_nivel_educativo_2025",
        {"ano_encuesta": "2025", "nivel_educativo_padres": "Ninguno", "porcentaje_becarios": "-"},
        CONTEXT
    )
    assert rows_snap[0]["porcentaje_becarios"] is None

def test_decimal_comma_and_percent_parsing() -> None:
    # Valida la transformación correcta de decimales con coma y el símbolo de porcentaje.
    rows = transform_pronabec_report_record(
        "report_beca18_colegio_gestion_2025",
        {"ano_encuesta": "2025", "tipo_gestion_colegio": "Pública", "porcentaje_becarios": " 87,65 % "},
        CONTEXT
    )
    assert rows[0]["porcentaje_becarios"] == 87.65

# Prueba de Integración / Pipeline de Transformación de dataflow con DLQ y Summary

def test_pipeline_transformation_integration_with_dlq_and_summary(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Valida que el pipeline bronze -> silver enruta a DLQ y genera métricas en resumen para reportes.
    input_file = tmp_path / "bronze_colegio.csv"
    
    # Escribir un archivo CSV temporal con 1 fila válida y 1 fila inválida
    header = "ano_encuesta,tipo_gestion_colegio,porcentaje_becarios,source_document_file,source_document_title,source_page,source_figure,extraction_method\n"
    valid_line = "2025,Pública,85.4%,report.pdf,Reporte,5,Figura 1,manual_csv\n"
    invalid_line = "AÑO_INVALIDO,Privada,14.6%,report.pdf,Reporte,5,Figura 1,manual_csv\n"
    
    with open(input_file, "w", encoding="utf-8") as f:
        f.write(header)
        f.write(valid_line)
        f.write(invalid_line)
        
    dlq_root = tmp_path / "dlq"
    summary_path = tmp_path / "summary.json"
    
    # Mockear transform_pronabec_report_record para simular falla si el año de encuesta es inválido
    from pipelines.transforms.pronabec_reports import transform_pronabec_report_record as original_transform

    def mock_transform(source_dataset: str, record: dict[str, Any], context: dict[str, Any]) -> list[dict[str, Any]]:
        # El parser CSV de Apache Beam lee los campos como strings.
        ano = record.get("ano_encuesta", "")
        if "INVALIDO" in str(ano):
            raise ValueError("Error de transformación simulado para año inválido")
        return original_transform(source_dataset, record, context)

    monkeypatch.setattr(
        "pipelines.dataflow_bronze_to_silver.transform_pronabec_report_record",
        mock_transform
    )
    
    # Ejecutar el pipeline principal en DirectRunner de forma local
    run([
        "--source-system", "pronabec_reports",
        "--source-dataset", "report_beca18_colegio_gestion_2025",
        "--extraction-date", "2026-06-15",
        "--input-path", str(input_file),
        "--input-format", "csv",
        "--runner", "DirectRunner",
        "--dlq-output-root", str(dlq_root),
        "--summary-output-path", str(summary_path),
        "--dry-run",
    ])
    
    # 1. Verificar que el DLQ tiene el registro inválido
    dlq_file = dlq_root / "pronabec_reports" / "report_beca18_colegio_gestion_2025" / "extraction_date=2026-06-15" / "rejected_records.jsonl"
    assert dlq_file.exists()
    
    dlq_lines = dlq_file.read_text(encoding="utf-8").splitlines()
    assert len(dlq_lines) == 1
    
    rejected = json.loads(dlq_lines[0])
    assert rejected["source_system"] == "pronabec_reports"
    assert rejected["source_dataset"] == "report_beca18_colegio_gestion_2025"
    assert rejected["processing_stage"] == "transform"
    assert rejected["error_code"] == "TRANSFORM_ERROR"
    assert "AÑO_INVALIDO" in str(rejected["raw_record"])
    
    # 2. Verificar el archivo de resumen de procesamiento
    assert summary_path.exists()
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["status"] == "COMPLETED_WITH_REJECTIONS"
    assert summary["records_read"] == 2
    assert summary["records_valid"] == 1
    assert summary["records_rejected"] == 1
