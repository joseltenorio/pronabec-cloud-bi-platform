from __future__ import annotations

import re
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SQL_PATH = PROJECT_ROOT / "sql" / "ml" / "create_dim_region_mapping.sql"


def test_region_mapping_sql_exists() -> None:
    assert SQL_PATH.exists()


def test_region_mapping_sql_targets_ml_view() -> None:
    content = SQL_PATH.read_text(encoding="utf-8")

    assert "CREATE OR REPLACE VIEW" in content
    assert "{project_id}.{ml_dataset}.dim_region_mapping" in content
    assert "source_region" in content
    assert "region_canonical" in content
    assert "mapping_rule" in content
    assert "is_aggregated_region" in content
    assert "{project_id}.ml" not in content


def test_region_mapping_sql_collapses_lima_and_callao_variants() -> None:
    content = SQL_PATH.read_text(encoding="utf-8")

    assert "LIMA METROPOLITANA" in content
    assert "LIMA PROVINCIAS" in content
    assert "PROV. CONST. DEL CALLAO" in content
    assert "PROVINCIA CONSTITUCIONAL DEL CALLAO" in content

    assert re.search(
        r"STRUCT\('LIMA METROPOLITANA'.*'LIMA' AS region_canonical",
        content,
        re.IGNORECASE | re.DOTALL,
    )
    assert re.search(
        r"STRUCT\('LIMA PROVINCIAS'.*'LIMA' AS region_canonical",
        content,
        re.IGNORECASE | re.DOTALL,
    )
    assert re.search(
        r"STRUCT\('PROV\. CONST\. DEL CALLAO'.*'CALLAO' AS region_canonical",
        content,
        re.IGNORECASE | re.DOTALL,
    )
    assert re.search(
        r"STRUCT\('PROVINCIA CONSTITUCIONAL DEL CALLAO'.*'CALLAO' AS region_canonical",
        content,
        re.IGNORECASE | re.DOTALL,
    )


def test_region_mapping_sql_does_not_promote_variants_as_canonical_regions() -> None:
    content = SQL_PATH.read_text(encoding="utf-8")

    forbidden_patterns = [
        r"region_canonical\s*,\s*'LIMA METROPOLITANA'",
        r"region_canonical\s*,\s*'LIMA PROVINCIAS'",
        r"region_canonical\s*,\s*'PROV\. CONST\. DEL CALLAO'",
        r"region_canonical\s*,\s*'PROVINCIA CONSTITUCIONAL DEL CALLAO'",
    ]
    for pattern in forbidden_patterns:
        assert not re.search(pattern, content, re.IGNORECASE | re.DOTALL)
