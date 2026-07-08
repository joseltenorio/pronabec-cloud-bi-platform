from __future__ import annotations

import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ML_SCHEMAS_DIR = PROJECT_ROOT / "config" / "schemas" / "ml"


def _load_schema(path: Path) -> list[dict[str, object]]:
    return json.loads(path.read_text(encoding="utf-8"))


def _field_map(schema: list[dict[str, object]]) -> dict[str, dict[str, object]]:
    return {field["name"]: field for field in schema}


def test_ml_schema_files_exist() -> None:
    assert (ML_SCHEMAS_DIR / "dim_region_mapping_schema.json").exists()
    assert (ML_SCHEMAS_DIR / "region_context_features_schema.json").exists()
    assert (ML_SCHEMAS_DIR / "region_priority_scores_schema.json").exists()


def test_dim_region_mapping_schema_contract() -> None:
    schema = _load_schema(ML_SCHEMAS_DIR / "dim_region_mapping_schema.json")
    fields = _field_map(schema)

    expected = {
        "source_region": ("STRING", "REQUIRED"),
        "region_canonical": ("STRING", "REQUIRED"),
        "region_scope": ("STRING", "NULLABLE"),
        "mapping_rule": ("STRING", "NULLABLE"),
        "is_aggregated_region": ("BOOL", "NULLABLE"),
        "notes": ("STRING", "NULLABLE"),
    }
    assert set(fields) == set(expected)
    for field_name, (field_type, field_mode) in expected.items():
        assert fields[field_name]["type"] == field_type
        assert fields[field_name]["mode"] == field_mode


def test_region_context_features_schema_contract() -> None:
    schema = _load_schema(ML_SCHEMAS_DIR / "region_context_features_schema.json")
    fields = _field_map(schema)

    expected_required = {
        "anio": ("INT64", "REQUIRED"),
        "region": ("STRING", "REQUIRED"),
        "region_canonical": ("STRING", "REQUIRED"),
    }
    expected_nullable = {
        "pobreza_monetaria_pct": "FLOAT64",
        "poverty_source_type": "STRING",
        "poblacion_total": "INT64",
        "poblacion_15_24": "INT64",
        "poblacion_15_29": "INT64",
        "poblacion_joven_pct": "FLOAT64",
        "matricula_5to_secundaria": "INT64",
        "matricula_5to_publica": "INT64",
        "matricula_5to_privada": "INT64",
        "matricula_5to_urbana": "INT64",
        "matricula_5to_rural": "INT64",
        "ruralidad_educativa_pct": "FLOAT64",
        "education_source_type": "STRING",
        "internet_acceso_pct": "FLOAT64",
        "brecha_digital_pct": "FLOAT64",
        "internet_source_type": "STRING",
        "feature_completeness_score": "FLOAT64",
        "feature_quality_flag": "STRING",
        "has_synthetic_values": "BOOL",
        "synthetic_fields": "STRING",
        "source_priority": "STRING",
        "created_at": "TIMESTAMP",
    }

    for field_name, (field_type, field_mode) in expected_required.items():
        assert field_name in fields
        assert fields[field_name]["type"] == field_type
        assert fields[field_name]["mode"] == field_mode

    for field_name, field_type in expected_nullable.items():
        assert field_name in fields
        assert fields[field_name]["type"] == field_type
        assert fields[field_name]["mode"] == "NULLABLE"


def test_region_priority_scores_schema_contract() -> None:
    schema = _load_schema(ML_SCHEMAS_DIR / "region_priority_scores_schema.json")
    fields = _field_map(schema)

    expected_required = {
        "anio": ("INT64", "REQUIRED"),
        "region": ("STRING", "REQUIRED"),
        "region_canonical": ("STRING", "REQUIRED"),
    }
    expected_nullable = {
        "pobreza_monetaria_pct": "FLOAT64",
        "poblacion_15_24": "INT64",
        "poblacion_15_29": "INT64",
        "poblacion_joven_pct": "FLOAT64",
        "matricula_5to_secundaria": "INT64",
        "ruralidad_educativa_pct": "FLOAT64",
        "internet_acceso_pct": "FLOAT64",
        "brecha_digital_pct": "FLOAT64",
        "pobreza_score": "FLOAT64",
        "demanda_educativa_score": "FLOAT64",
        "poblacion_joven_score": "FLOAT64",
        "ruralidad_score": "FLOAT64",
        "brecha_digital_score": "FLOAT64",
        "priority_score": "FLOAT64",
        "priority_rank": "INT64",
        "priority_tier": "STRING",
        "score_version": "STRING",
        "score_method": "STRING",
        "feature_completeness_score": "FLOAT64",
        "feature_quality_flag": "STRING",
        "source_priority": "STRING",
        "has_synthetic_values": "BOOL",
        "synthetic_fields": "STRING",
        "created_at": "TIMESTAMP",
    }

    for field_name, (field_type, field_mode) in expected_required.items():
        assert field_name in fields
        assert fields[field_name]["type"] == field_type
        assert fields[field_name]["mode"] == field_mode

    assert set(fields) == set(expected_required) | set(expected_nullable)

    for field_name, field_type in expected_nullable.items():
        assert field_name in fields
        assert fields[field_name]["type"] == field_type
        assert fields[field_name]["mode"] == "NULLABLE"
