import json
from pathlib import Path

from pipelines.transforms.pronabec import (
    transform_pronabec_convocatorias,
    transform_pronabec_becarios_pais_estudio,
    transform_pronabec_colegios_elegibles,
    transform_pronabec_ubigeo_postulacion,
)
from pipelines.transforms.pronabec_reports import (
    transform_pronabec_report_record,
)
from pipelines.transforms.mef import (
    transform_mef_record,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SILVER_SCHEMAS_DIR = PROJECT_ROOT / "config" / "schemas" / "silver"

CONTEXT = {
    "extraction_date": "2026-06-15",
    "ingestion_timestamp": "2026-06-16T00:00:00+00:00",
    "pipeline_run_id": "test-run",
}


def load_schema_fields(name: str) -> set[str]:
    path = SILVER_SCHEMAS_DIR / f"{name}_schema.json"
    assert path.exists(), f"Schema file {path} does not exist"
    schema = json.loads(path.read_text(encoding="utf-8"))
    return {field["name"] for field in schema}


def test_canonical_mapping_convocatorias() -> None:
    # 1. Test matched alias for modalidad and programa
    record_match = {
        "source_row_id": "1",
        "id_convocatoria": "100",
        "codigo_anual": "2026-01",
        "description_conv": "Beca Ordinaria Beca 18",
        "modalidad": "ORDINARIA",
        "programa": "BECA18",
        "vacantes": "50",
    }
    res_match = transform_pronabec_convocatorias(record_match, CONTEXT)
    
    # Original field preserved
    assert res_match["modalidad"] == "ORDINARIA"
    assert res_match["programa"] == "BECA18"
    
    # Canonical mapped
    assert res_match["modalidad_canonical"] == "Ordinaria"
    assert res_match["modalidad_canonical_match_method"] == "manual_alias"
    assert res_match["modalidad_canonical_review_required"] is False
    
    assert res_match["programa_canonical"] == "BECA 18"
    assert res_match["programa_canonical_match_method"] == "manual_alias"
    assert res_match["programa_canonical_review_required"] is False

    # 2. Test no match
    record_no_match = {
        "source_row_id": "2",
        "id_convocatoria": "101",
        "codigo_anual": "2026-02",
        "description_conv": "Other Beca",
        "modalidad": "NON_EXISTENT_MODALIDAD",
        "programa": "NON_EXISTENT_PROGRAMA",
        "vacantes": "10",
    }
    res_no_match = transform_pronabec_convocatorias(record_no_match, CONTEXT)
    
    # Original preserved
    assert res_no_match["modalidad"] == "NON_EXISTENT_MODALIDAD"
    assert res_no_match["programa"] == "NON_EXISTENT_PROGRAMA"
    
    # Canonical is None, match method is None, review required is None
    assert res_no_match["modalidad_canonical"] is None
    assert res_no_match["modalidad_canonical_match_method"] is None
    assert res_no_match["modalidad_canonical_review_required"] is None
    
    assert res_no_match["programa_canonical"] is None
    assert res_no_match["programa_canonical_match_method"] is None
    assert res_no_match["programa_canonical_review_required"] is None


def test_canonical_mapping_becarios() -> None:
    # Test matched alias for modalidad, pais_estudio, and sexo
    record = {
        "source_row_id": "1",
        "convocatoria": "CONV_2026",
        "modalidad": "MODALIDAD ORDINARIA",
        "pais_estudio": "PERU",
        "institucion": "U_NACIONAL",
        "sexo": "F",
    }
    res = transform_pronabec_becarios_pais_estudio(record, CONTEXT)
    
    # Originals preserved
    assert res["modalidad"] == "MODALIDAD ORDINARIA"
    assert res["pais_estudio"] == "PERU"
    assert res["sexo"] == "F"
    
    # Canonicals
    assert res["modalidad_canonical"] == "Ordinaria"
    assert res["modalidad_canonical_match_method"] == "manual_alias"
    assert res["modalidad_canonical_review_required"] is False
    
    assert res["pais_estudio_canonical"] == "PERÚ"
    assert res["pais_estudio_canonical_match_method"] == "manual_alias"
    assert res["pais_estudio_canonical_review_required"] is False
    
    assert res["sexo_canonical"] == "Femenino"
    assert res["sexo_canonical_match_method"] == "manual_alias"
    assert res["sexo_canonical_review_required"] is True  # Review required is True for sexo F in catalog


def test_canonical_mapping_colegios_elegibles() -> None:
    # Test matched tipo_gestion_colegio
    record = {
        "source_row_id": "1",
        "ugel": "UGEL CHACHAPOYAS",
        "institucion_educativa": "COLEGIO UNO",
        "tipo_gestion": "ESTATAL",
        "nivel_modalidad": "Secundaria",
        "forma_atencion": "Escolarizada",
        "distrito": "CHACHAPOYAS",
    }
    res = transform_pronabec_colegios_elegibles(record, CONTEXT)
    
    # Original preserved
    assert res["tipo_gestion_colegio"] == "ESTATAL"
    
    # Canonical
    assert res["tipo_gestion_colegio_canonical"] == "Pública"
    assert res["tipo_gestion_colegio_canonical_match_method"] == "manual_alias"
    assert res["tipo_gestion_colegio_canonical_review_required"] is False
    
    # level/attendance/ugel have no matches in candidates -> None
    assert res["nivel_modalidad_canonical"] is None
    assert res["forma_atencion_canonical"] is None
    assert res["ugel_canonical"] is None


def test_canonical_mapping_ubigeo() -> None:
    record = {
        "source_row_id": "1",
        "region": "LIMA",
        "provincia": "LIMA",
        "distrito": "MIRAFLORES",
        "codigo_ubigeo": "150122",
    }
    res = transform_pronabec_ubigeo_postulacion(record, CONTEXT)
    
    # Original preserved
    assert res["region"] == "LIMA"
    
    # Region canonical is None (not in catalog)
    assert res["region_canonical"] is None
    assert res["region_canonical_match_method"] is None
    assert res["region_canonical_review_required"] is None


def test_canonical_mapping_report_carrera_estudio() -> None:
    # test matched carrera_estudio in Beca 18 Carrera Report
    record = {
        "carrera_estudio": "ARTE & DISEO GRAFICO EMPRESARIAL",
        "2026 (*)": "15",
        "source_document_file": "doc.pdf",
        "source_document_title": "Report",
        "source_publication_url": "http://example.com",
        "source_page": "12",
        "source_table": "Table 1",
        "extraction_method": "camelot",
    }
    # unpivots annual career report
    res_list = transform_pronabec_report_record("report_beca18_universitarios_carrera_anual", record, CONTEXT)
    assert len(res_list) == 1
    res = res_list[0]
    
    # Original preserved
    assert res["carrera_estudio"] == "ARTE & DISEO GRAFICO EMPRESARIAL"
    
    # Canonical mapped
    assert res["carrera_estudio_canonical"] == "ARTE & DISEÑO GRÁFICO EMPRESARIAL"
    assert res["carrera_estudio_canonical_match_method"] == "manual_alias"
    assert res["carrera_estudio_canonical_review_required"] is True


def test_canonical_mapping_report_universidad_safe_fallback() -> None:
    # universidad -> institucion: institucion does not exist in catalog
    record = {
        "universidad": "UNIVERSIDAD NACIONAL MAYOR DE SAN MARCOS",
        "2026 (*)": "100",
        "source_document_file": "doc.pdf",
        "source_document_title": "Report",
        "source_publication_url": "http://example.com",
        "source_page": "12",
        "source_table": "Table 1",
        "extraction_method": "camelot",
    }
    res_list = transform_pronabec_report_record("report_beca18_universitarios_universidad_anual", record, CONTEXT)
    assert len(res_list) == 1
    res = res_list[0]
    
    # Original preserved
    assert res["universidad"] == "UNIVERSIDAD NACIONAL MAYOR DE SAN MARCOS"
    
    # Canonical must be None and not fail since institucion is not in catalog
    assert res["universidad_canonical"] is None
    assert res["universidad_canonical_match_method"] is None
    assert res["universidad_canonical_review_required"] is None


def test_no_fuzzy_matching() -> None:
    # Check that similar spelling is not fuzzy-matched (since fuzzy_matching_enabled is false)
    record = {
        "source_row_id": "1",
        "id_convocatoria": "100",
        "codigo_anual": "2026-01",
        "description_conv": "Beca",
        "modalidad": "ORDINAR",  # missing 'IA'
        "programa": "BECA 188",  # extra '8'
        "vacantes": "50",
    }
    res = transform_pronabec_convocatorias(record, CONTEXT)
    
    assert res["modalidad_canonical"] is None
    assert res["programa_canonical"] is None


def test_no_global_character_rules() -> None:
    # No global translations like N -> Ñ or accents outside explicit aliases
    # Test 'DISEO' -> does it match if it is not explicitly mapped?
    # In the carrera catalog, 'ARTE & DISEO GRAFICO EMPRESARIAL' is mapped explicitly.
    # What about a random word like 'DISENO DE MODAS'? It should NOT canonicalize or translate character N to Ñ.
    record = {
        "carrera_estudio": "DISEO DE INTERIORES",
        "2026 (*)": "5",
        "source_document_file": "doc.pdf",
        "source_document_title": "Report",
        "source_publication_url": "http://example.com",
        "source_page": "12",
        "source_table": "Table 1",
        "extraction_method": "camelot",
    }
    res_list = transform_pronabec_report_record("report_beca18_universitarios_carrera_anual", record, CONTEXT)
    assert len(res_list) == 1
    res = res_list[0]
    
    assert res["carrera_estudio"] == "DISEO DE INTERIORES"
    assert res["carrera_estudio_canonical"] is None


def test_mef_transforms_remain_unaffected() -> None:
    # Verify MEF transforms do not have canonical fields populated
    record = {
        "nro_fila": "1",
        "ano": "2024",
        "codigo_entidad": "123",
        "entidad": "MINISTERIO",
        "pia": "1,000.00",
        "pim": "1,200.00",
        "devengado": "900.00",
    }
    context = {"extraction_date": "2026-06-15", "pipeline_run_id": "mef-run"}
    res = transform_mef_record("presupuesto", record, context)
    
    for key in res.keys():
        assert not key.endswith("_canonical")
        assert "_canonical_" not in key


def test_schema_alignment_of_transformed_records() -> None:
    # Check that keys of output records align exactly with schema definitions
    datasets_to_verify = [
        ("pronabec_convocatorias", transform_pronabec_convocatorias, {
            "source_row_id": "1",
            "id_convocatoria": "1",
            "codigo_anual": "A",
            "description_conv": "B",
            "modalidad": "C",
            "programa": "D",
            "vacantes": "10",
        }),
        ("pronabec_becarios_pais_estudio", transform_pronabec_becarios_pais_estudio, {
            "source_row_id": "1",
            "convocatoria": "A",
            "modalidad": "B",
            "pais_estudio": "C",
            "institucion": "D",
            "sexo": "E",
        }),
        ("pronabec_colegios_elegibles", transform_pronabec_colegios_elegibles, {
            "source_row_id": "1",
            "ugel": "A",
            "institucion_educativa": "B",
            "tipo_gestion": "C",
            "nivel_modalidad": "D",
            "forma_atencion": "E",
            "distrito": "F",
        }),
        ("pronabec_ubigeo_postulacion", transform_pronabec_ubigeo_postulacion, {
            "source_row_id": "1",
            "region": "A",
            "provincia": "B",
            "distrito": "C",
            "codigo_ubigeo": "D",
        }),
    ]

    for name, transform_fn, dummy_record in datasets_to_verify:
        schema_fields = load_schema_fields(name)
        res = transform_fn(dummy_record, CONTEXT)
        assert set(res.keys()) == schema_fields, f"Keys in transformed record for {name} do not align with schema"

    # Verify report schemas too
    report_datasets = [
        ("report_beca18_universitarios_carrera_anual", "pronabec_report_beca18_universitarios_carrera_anual", {
            "carrera_estudio": "ARTE & DISEO GRAFICO EMPRESARIAL",
            "2026 (*)": "15",
            "source_document_file": "doc.pdf",
            "source_document_title": "Report",
            "source_publication_url": "http://example.com",
            "source_page": "12",
            "source_table": "Table 1",
            "extraction_method": "camelot",
        }),
        ("report_beca18_universitarios_universidad_anual", "pronabec_report_beca18_universitarios_universidad_anual", {
            "universidad": "SAN MARCOS",
            "2026 (*)": "100",
            "source_document_file": "doc.pdf",
            "source_document_title": "Report",
            "source_publication_url": "http://example.com",
            "source_page": "12",
            "source_table": "Table 1",
            "extraction_method": "camelot",
        })
    ]

    for bronze_name, silver_name, dummy_record in report_datasets:
        schema_fields = load_schema_fields(silver_name)
        res_list = transform_pronabec_report_record(bronze_name, dummy_record, CONTEXT)
        assert len(res_list) == 1
        res = res_list[0]
        assert set(res.keys()) == schema_fields, f"Keys in transformed report record for {silver_name} do not align with schema"
