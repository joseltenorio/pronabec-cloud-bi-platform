"""Unit tests for the PRONABEC text canonization reference layer."""

from __future__ import annotations

from pathlib import Path
import pytest

from pipelines.common.canonical_mapping import (
    build_alias_index,
    load_canonical_mappings,
    lookup_canonical_value,
    normalize_canonical_key,
)

REAL_MAPPING_PATH = (
    Path(__file__).parent.parent / "config" / "reference" / "pronabec_canonical_mappings.yaml"
)


def test_load_real_catalog() -> None:
    catalog = load_canonical_mappings(REAL_MAPPING_PATH)
    assert catalog["version"] == 1
    assert "domains" in catalog
    domains = catalog["domains"]
    assert "carrera" in domains
    assert "pais" in domains
    assert "programa" in domains
    assert "_alias_index" in catalog


def test_key_normalization() -> None:
    norm = normalize_canonical_key("  Arte & Diseño   Gráfico Empresarial ")
    assert norm == "ARTE DISENO GRAFICO EMPRESARIAL"

    # Check that NINO is not globally converted to NIÑO
    assert normalize_canonical_key("NINO") == "NINO"


def test_lookups_real_catalog() -> None:
    catalog = load_canonical_mappings(REAL_MAPPING_PATH)

    # lookup carrera
    match_carrera = lookup_canonical_value(catalog, "carrera", "ARTE & DISEO GRAFICO EMPRESARIAL")
    assert match_carrera.matched is True
    assert match_carrera.canonical_value == "ARTE & DISEÑO GRÁFICO EMPRESARIAL"
    assert match_carrera.domain == "carrera"
    assert match_carrera.review_required is True
    assert match_carrera.match_method == "manual_alias"
    assert match_carrera.confidence == "high"

    # lookup pais
    match_pais = lookup_canonical_value(catalog, "pais", "PERU")
    assert match_pais.matched is True
    assert match_pais.canonical_value == "PERÚ"
    assert match_pais.domain == "pais"
    assert match_pais.review_required is False

    # lookup programa
    match_prog = lookup_canonical_value(catalog, "programa", "BECA18")
    assert match_prog.matched is True
    assert match_prog.canonical_value == "BECA 18"
    assert match_prog.domain == "programa"
    assert match_prog.review_required is False


def test_no_match_and_unknown_domain() -> None:
    catalog = load_canonical_mappings(REAL_MAPPING_PATH)

    # no match
    match_none = lookup_canonical_value(catalog, "carrera", "CARRERA NO REGISTRADA")
    assert match_none.matched is False
    assert match_none.canonical_value is None

    # unknown domain
    match_unk = lookup_canonical_value(catalog, "non_existent_domain", "PERU")
    assert match_unk.matched is False
    assert match_unk.canonical_value is None

    # empty lookup
    match_empty = lookup_canonical_value(catalog, "pais", "   ")
    assert match_empty.matched is False
    assert match_empty.canonical_value is None


def test_no_fuzzy_and_no_global_tilde_translation() -> None:
    catalog = load_canonical_mappings(REAL_MAPPING_PATH)

    # typo
    match_typo = lookup_canonical_value(catalog, "carrera", "ARTE DISENIO GRAFICO EMPRESARIAL")
    assert match_typo.matched is False

    # NINO to NIÑO check
    match_nino = lookup_canonical_value(catalog, "carrera", "NINO")
    assert match_nino.matched is False

    # check stable indexing
    index1 = build_alias_index(catalog)
    index2 = build_alias_index(catalog)
    assert index1 == index2


def test_catalog_validation_errors(tmp_path: Path) -> None:
    # 1. Missing version
    bad_yaml = tmp_path / "bad1.yaml"
    bad_yaml.write_text("domains: {}", encoding="utf-8")
    with pytest.raises(ValueError, match="Catalog is missing required field: 'version'"):
        load_canonical_mappings(bad_yaml)

    # 2. Missing domains
    bad_yaml = tmp_path / "bad2.yaml"
    bad_yaml.write_text("version: 1", encoding="utf-8")
    with pytest.raises(ValueError, match="Catalog is missing required field: 'domains'"):
        load_canonical_mappings(bad_yaml)

    # 3. Missing fields in mapping
    bad_yaml = tmp_path / "bad3.yaml"
    bad_yaml.write_text(
        """
version: 1
domains:
  carrera:
    - canonical_value: "A"
      aliases: ["B"]
      match_method: "manual_alias"
      confidence: "high"
      review_required: true
""",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="missing required field: 'normalized_key'"):
        load_canonical_mappings(bad_yaml)

    # 4. Invalid match_method
    bad_yaml = tmp_path / "bad4.yaml"
    bad_yaml.write_text(
        """
version: 1
domains:
  carrera:
    - canonical_value: "A"
      aliases: ["B"]
      normalized_key: "A"
      match_method: "invalid_method"
      confidence: "high"
      review_required: true
""",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="Invalid match_method 'invalid_method'"):
        load_canonical_mappings(bad_yaml)

    # 5. Invalid confidence
    bad_yaml = tmp_path / "bad5.yaml"
    bad_yaml.write_text(
        """
version: 1
domains:
  carrera:
    - canonical_value: "A"
      aliases: ["B"]
      normalized_key: "A"
      match_method: "manual_alias"
      confidence: "super_high"
      review_required: true
""",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="Invalid confidence 'super_high'"):
        load_canonical_mappings(bad_yaml)

    # 6. Invalid review_required
    bad_yaml = tmp_path / "bad6.yaml"
    bad_yaml.write_text(
        """
version: 1
domains:
  carrera:
    - canonical_value: "A"
      aliases: ["B"]
      normalized_key: "A"
      match_method: "manual_alias"
      confidence: "high"
      review_required: "yes"
""",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="review_required in domain 'carrera' must be a boolean"):
        load_canonical_mappings(bad_yaml)


def test_duplicate_alias_ambiguity(tmp_path: Path) -> None:
    # Ambiguous alias mapping to different canonical values
    bad_yaml = tmp_path / "ambiguous.yaml"
    bad_yaml.write_text(
        """
version: 1
domains:
  carrera:
    - canonical_value: "Carrera A"
      aliases:
        - "DUPLICADO"
      normalized_key: "CARRERA A"
      match_method: "manual_alias"
      confidence: "high"
      review_required: true
    - canonical_value: "Carrera B"
      aliases:
        - "DUPLICADO"
      normalized_key: "CARRERA B"
      match_method: "manual_alias"
      confidence: "high"
      review_required: true
""",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="Ambiguous alias 'DUPLICADO' in domain 'carrera'"):
        load_canonical_mappings(bad_yaml)
