from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
MODEL_SQL_PATH = REPO_ROOT / "sql" / "ml" / "create_region_cluster_model.sql"
ASSIGNMENTS_SQL_PATH = REPO_ROOT / "sql" / "ml" / "create_region_cluster_assignments.sql"
PROFILES_SQL_PATH = REPO_ROOT / "sql" / "ml" / "create_region_cluster_profiles.sql"


def _read(path: Path) -> str:
    assert path.exists(), f"Missing SQL file: {path}"
    return path.read_text(encoding="utf-8")


def test_region_cluster_sql_files_exist() -> None:
    assert MODEL_SQL_PATH.exists()
    assert ASSIGNMENTS_SQL_PATH.exists()
    assert PROFILES_SQL_PATH.exists()


def test_region_cluster_model_uses_bigquery_ml_kmeans() -> None:
    content = _read(MODEL_SQL_PATH)

    assert "CREATE OR REPLACE MODEL `{project_id}.{ml_dataset}.model_region_clusters`" in content
    assert "model_type = 'kmeans'" in content
    assert "num_clusters = 4" in content
    assert "standardize_features = TRUE" in content
    assert "kmeans_init_method = 'KMEANS++'" in content
    assert "`{project_id}.{ml_dataset}.region_coverage_features`" in content


def test_region_cluster_assignments_and_profiles() -> None:
    assignments = _read(ASSIGNMENTS_SQL_PATH)
    profiles = _read(PROFILES_SQL_PATH)

    assert "CREATE OR REPLACE VIEW `{project_id}.{ml_dataset}.region_cluster_assignments`" in assignments
    assert "ML.PREDICT" in assignments
    assert "MODEL `{project_id}.{ml_dataset}.model_region_clusters`" in assignments
    assert "`{project_id}.{ml_dataset}.region_coverage_features`" in assignments
    assert "centroid_id" in assignments
    assert "Cluster " in assignments

    assert "CREATE OR REPLACE VIEW `{project_id}.{ml_dataset}.region_cluster_profiles`" in profiles
    assert "`{project_id}.{ml_dataset}.region_cluster_assignments`" in profiles
    assert "regiones_count" in profiles
    assert "avg_priority_score_v2" in profiles


def test_region_cluster_sql_avoids_forbidden_sources() -> None:
    combined = "\n".join(
        _read(path).lower()
        for path in [MODEL_SQL_PATH, ASSIGNMENTS_SQL_PATH, PROFILES_SQL_PATH]
    )

    assert "bronze." not in combined
    assert "presupuesto_mef_departamento" not in combined
    assert "{project_id}.ml" not in combined
    assert "pronabec-cloud-bi-platform" not in combined
    assert "create or replace table" not in combined
    assert "drop" not in combined
