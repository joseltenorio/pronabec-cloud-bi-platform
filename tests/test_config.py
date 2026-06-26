from pathlib import Path

import pytest

from pipelines.common.config import (
    ConfigError,
    build_gcs_path,
    get_env_var,
    get_nested_value,
    get_pipeline_settings,
    load_yaml_config,
)


def test_load_yaml_config_reads_valid_yaml(tmp_path: Path) -> None:
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        """
pipeline:
  name: project_cloud_bi_platform
  environment: dev
""",
        encoding="utf-8",
    )

    config = load_yaml_config(config_file)

    assert config["pipeline"]["name"] == "project_cloud_bi_platform"
    assert config["pipeline"]["environment"] == "dev"


def test_load_yaml_config_raises_for_missing_file(tmp_path: Path) -> None:
    missing_file = tmp_path / "missing.yaml"

    with pytest.raises(FileNotFoundError):
        load_yaml_config(missing_file)


def test_load_yaml_config_raises_for_empty_file(tmp_path: Path) -> None:
    config_file = tmp_path / "empty.yaml"
    config_file.write_text("", encoding="utf-8")

    with pytest.raises(ConfigError):
        load_yaml_config(config_file)


def test_get_env_var_returns_default_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MISSING_ENV_VAR", raising=False)

    value = get_env_var("MISSING_ENV_VAR", default="fallback")

    assert value == "fallback"


def test_get_env_var_raises_when_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("REQUIRED_ENV_VAR", raising=False)

    with pytest.raises(ConfigError):
        get_env_var("REQUIRED_ENV_VAR", required=True)


def test_get_nested_value_returns_existing_value() -> None:
    config = {
        "pipeline": {
            "name": "project_cloud_bi_platform",
        }
    }

    value = get_nested_value(config, "pipeline.name", required=True)

    assert value == "project_cloud_bi_platform"


def test_get_nested_value_returns_default_when_missing() -> None:
    config = {"pipeline": {}}

    value = get_nested_value(config, "pipeline.environment", default="dev")

    assert value == "dev"


def test_get_nested_value_raises_when_required_path_missing() -> None:
    config = {"pipeline": {}}

    with pytest.raises(ConfigError):
        get_nested_value(config, "pipeline.name", required=True)


def test_get_pipeline_settings_reads_project_pipeline_config() -> None:
    settings = get_pipeline_settings("config/pipeline.yaml")

    assert settings["pipeline_name"] == "project_cloud_bi_platform"
    assert settings["environment"] == "dev"
    assert settings["timezone"] == "America/Lima"
    assert "bronze" in settings["bigquery_datasets"]
    assert "pronabec_bronze_normalized" in settings["gcs_paths"]
    assert settings["pronabec_reports"]["local_manual_dir"] == "data/manual/pronabec_reports"
    assert settings["pronabec_reports"]["landing_prefix"] == "landing/pronabec_reports"
    assert settings["pronabec_reports"]["bronze_prefix"] == "bronze/pronabec_reports"
    assert settings["pronabec_reports"]["subsets"] == [
        "pes_2025",
        "beca18_universitarios_2012_2026",
    ]


def test_get_pipeline_settings_reads_pronabec_reports_env_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PRONABEC_REPORTS_LOCAL_MANUAL_DIR", "custom/manual")
    monkeypatch.setenv("PRONABEC_REPORTS_LANDING_PREFIX", "landing/pronabec_reports")
    monkeypatch.setenv("PRONABEC_REPORTS_BRONZE_PREFIX", "bronze/pronabec_reports")
    monkeypatch.setenv("PRONABEC_REPORTS_SUBSETS", "pes_2025,beca18_universitarios_2012_2026")

    settings = get_pipeline_settings("config/pipeline.yaml")

    assert settings["pronabec_reports"] == {
        "local_manual_dir": "custom/manual",
        "landing_prefix": "landing/pronabec_reports",
        "bronze_prefix": "bronze/pronabec_reports",
        "subsets": [
            "pes_2025",
            "beca18_universitarios_2012_2026",
        ],
    }


def test_build_gcs_path_renders_template() -> None:
    path = build_gcs_path(
        "bronze/pronabec/{dataset}/extraction_date={extraction_date}/data.jsonl",
        dataset="notas_becarios",
        extraction_date="2026-06-10",
    )

    assert path == "bronze/pronabec/notas_becarios/extraction_date=2026-06-10/data.jsonl"


def test_build_gcs_path_raises_when_value_is_missing() -> None:
    with pytest.raises(ConfigError):
        build_gcs_path(
            "bronze/pronabec/{dataset}/extraction_date={extraction_date}/data.jsonl",
            dataset="notas_becarios",
        )
