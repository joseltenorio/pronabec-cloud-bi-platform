import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SILVER_SCHEMAS_DIR = PROJECT_ROOT / "config" / "schemas" / "silver"

PRONABEC_CANONICAL_DATASETS = [
    "pronabec_convocatorias",
    "pronabec_becarios_pais_estudio",
    "pronabec_colegios_elegibles",
    "pronabec_ubigeo_postulacion",
    "pronabec_report_beca18_universitarios_carrera_anual",
    "pronabec_report_beca18_universitarios_universidad_anual",
]


def load_schema(name: str) -> list[dict]:
    path = SILVER_SCHEMAS_DIR / f"{name}_schema.json"
    assert path.exists(), f"Schema file {path} does not exist"
    return json.loads(path.read_text(encoding="utf-8"))


def test_schema_duplicates() -> None:
    # Validate no duplicate fields exist in any silver schemas
    for schema_file in SILVER_SCHEMAS_DIR.glob("*.json"):
        schema = json.loads(schema_file.read_text(encoding="utf-8"))
        names = [field["name"] for field in schema]
        duplicates = [name for name in set(names) if names.count(name) > 1]
        assert not duplicates, f"Schema {schema_file.name} contains duplicate fields: {duplicates}"


def test_mef_schemas_lack_canonical_fields() -> None:
    # MEF transforms and schemas must be intact and without canonical fields
    for schema_file in SILVER_SCHEMAS_DIR.glob("presupuesto_mef*.json"):
        schema = json.loads(schema_file.read_text(encoding="utf-8"))
        for field in schema:
            assert not field["name"].endswith("_canonical"), (
                f"MEF schema {schema_file.name} should not contain canonical fields, but found {field['name']}"
            )
            assert "_canonical_" not in field["name"], (
                f"MEF schema {schema_file.name} contains canonical metadata field: {field['name']}"
            )


def test_canonical_trio_structure_and_types() -> None:
    expected_fields_per_dataset = {
        "pronabec_convocatorias": ["modalidad", "programa"],
        "pronabec_becarios_pais_estudio": ["modalidad", "pais_estudio", "sexo"],
        "pronabec_colegios_elegibles": ["tipo_gestion_colegio", "nivel_modalidad", "forma_atencion", "ugel"],
        "pronabec_ubigeo_postulacion": ["region"],
        "pronabec_report_beca18_universitarios_carrera_anual": ["carrera_estudio"],
        "pronabec_report_beca18_universitarios_universidad_anual": ["universidad"],
    }

    for dataset, base_fields in expected_fields_per_dataset.items():
        schema = load_schema(dataset)
        field_dict = {f["name"]: f for f in schema}

        for base_field in base_fields:
            # Check original exists
            assert base_field in field_dict, f"Original field {base_field} not found in {dataset}"

            # Trio suffixes
            canonical_name = f"{base_field}_canonical"
            match_method_name = f"{base_field}_canonical_match_method"
            review_req_name = f"{base_field}_canonical_review_required"

            # 1. Trio complete check
            assert canonical_name in field_dict, f"Missing {canonical_name} in {dataset}"
            assert match_method_name in field_dict, f"Missing {match_method_name} in {dataset}"
            assert review_req_name in field_dict, f"Missing {review_req_name} in {dataset}"

            # 2. Types validation (STRING / STRING / BOOLEAN)
            assert field_dict[canonical_name]["type"] == "STRING", (
                f"{canonical_name} in {dataset} must be STRING, got {field_dict[canonical_name]['type']}"
            )
            assert field_dict[match_method_name]["type"] == "STRING", (
                f"{match_method_name} in {dataset} must be STRING, got {field_dict[match_method_name]['type']}"
            )
            assert field_dict[review_req_name]["type"] == "BOOLEAN", (
                f"{review_req_name} in {dataset} must be BOOLEAN, got {field_dict[review_req_name]['type']}"
            )

            # 3. Mode validation (NULLABLE)
            assert field_dict[canonical_name]["mode"] == "NULLABLE", (
                f"{canonical_name} in {dataset} must be NULLABLE, got {field_dict[canonical_name]['mode']}"
            )
            assert field_dict[match_method_name]["mode"] == "NULLABLE", (
                f"{match_method_name} in {dataset} must be NULLABLE, got {field_dict[match_method_name]['mode']}"
            )
            assert field_dict[review_req_name]["mode"] == "NULLABLE", (
                f"{review_req_name} in {dataset} must be NULLABLE, got {field_dict[review_req_name]['mode']}"
            )


def test_no_canonical_on_identifiers_codes_metrics() -> None:
    # Validates absence of canonical fields on identifiers, codes, years, and metrics
    forbidden_canonical_bases = [
        # Identifiers
        "source_row_id",
        "source_system",
        "source_dataset",
        "pipeline_run_id",
        # Codes/years/metrics
        "codigo_ubigeo",
        "provincia",
        "distrito",
        "institucion",
        "institucion_educativa",
        "vacantes",
        "ano_convocatoria",
        "cantidad_becarios",
        "es_anio_preliminar",
        "source_page",
    ]

    for dataset in PRONABEC_CANONICAL_DATASETS:
        schema = load_schema(dataset)
        names = {f["name"] for f in schema}

        for base in forbidden_canonical_bases:
            forbidden_canonical = f"{base}_canonical"
            assert forbidden_canonical not in names, (
                f"Field {forbidden_canonical} should not exist in {dataset} schema"
            )
