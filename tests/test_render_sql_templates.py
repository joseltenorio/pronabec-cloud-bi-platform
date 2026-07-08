from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from tools.render_sql_templates import DEFAULT_SOURCE_FILES


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_render_sql_templates_default_sources_include_ml() -> None:
    assert "sql/ml/create_dim_region_mapping.sql" in DEFAULT_SOURCE_FILES
    assert "sql/ml/create_region_context_features.sql" in DEFAULT_SOURCE_FILES
    assert "sql/ml/create_region_priority_scores.sql" in DEFAULT_SOURCE_FILES
    assert "sql/ml/create_region_coverage_features.sql" in DEFAULT_SOURCE_FILES
    assert "sql/ml/create_region_priority_scores_v2.sql" in DEFAULT_SOURCE_FILES


def test_render_sql_templates_renders_ml_artifacts(tmp_path: Path) -> None:
    output_dir = tmp_path / "generated"

    completed = subprocess.run(
        [
            sys.executable,
            "tools/render_sql_templates.py",
            "--project-id",
            "test-project",
            "--output-dir",
            str(output_dir),
        ],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "Generated" in completed.stdout

    dim_path = output_dir / "create_dim_region_mapping.rendered.sql"
    features_path = output_dir / "create_region_context_features.rendered.sql"
    priority_path = output_dir / "create_region_priority_scores.rendered.sql"
    coverage_path = output_dir / "create_region_coverage_features.rendered.sql"
    priority_v2_path = output_dir / "create_region_priority_scores_v2.rendered.sql"

    assert dim_path.exists()
    assert features_path.exists()
    assert priority_path.exists()
    assert coverage_path.exists()
    assert priority_v2_path.exists()

    dim_content = dim_path.read_text(encoding="utf-8")
    features_content = features_path.read_text(encoding="utf-8")
    priority_content = priority_path.read_text(encoding="utf-8")
    coverage_content = coverage_path.read_text(encoding="utf-8")
    priority_v2_content = priority_v2_path.read_text(encoding="utf-8")

    for content in [dim_content, features_content, priority_content, coverage_content, priority_v2_content]:
        assert "{project_id}" not in content
        assert "{ml_dataset}" not in content
        assert "{silver_dataset}" not in content

    assert "test-project.ml.dim_region_mapping" in dim_content
    assert "test-project.ml.region_context_features" in features_content
    assert "test-project.ml.dim_region_mapping" in features_content
    assert "test-project.ml.region_context_features" in priority_content
    assert "test-project.ml.region_priority_scores" in priority_content
    assert "test-project.ml.region_context_features" in coverage_content
    assert "test-project.ml.region_priority_scores" in coverage_content
    assert "test-project.ml.region_coverage_features" in priority_v2_content
