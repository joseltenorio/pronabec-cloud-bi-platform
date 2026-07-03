from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
RUNTIME_REQUIREMENTS = REPO_ROOT / "requirements.txt"
WORKER_REQUIREMENTS = REPO_ROOT / "requirements-dataflow-worker.txt"
DEV_REQUIREMENTS = REPO_ROOT / "requirements-dev.txt"
DOCKERFILE_DATAFLOW = REPO_ROOT / "Dockerfile.dataflow"
PYPROJECT = REPO_ROOT / "pyproject.toml"


def _requirements(path: Path) -> set[str]:
    lines = path.read_text(encoding="utf-8").splitlines()
    return {
        line.strip()
        for line in lines
        if line.strip() and not line.strip().startswith("#")
    }


def test_runtime_requirements_do_not_include_dev_or_worker_only_dependencies() -> None:
    requirements = _requirements(RUNTIME_REQUIREMENTS)

    assert "pytest" not in requirements
    assert "ruff" not in requirements
    assert "sqlfluff" not in requirements
    assert "ipykernel" not in requirements
    assert "pandas" not in requirements
    assert "pyarrow" not in requirements
    assert "google-cloud-logging" not in requirements
    assert "loguru" not in requirements
    assert "tenacity" not in requirements
    assert "apache-beam[gcp]==2.74.0" in requirements


def test_dev_requirements_do_not_install_runtime_contract() -> None:
    content = DEV_REQUIREMENTS.read_text(encoding="utf-8")

    assert "-r requirements.txt" not in content
    assert "pytest" in content
    assert "ruff" in content


def test_dataflow_worker_requirements_are_minimal() -> None:
    requirements = _requirements(WORKER_REQUIREMENTS)

    assert requirements == {"ftfy", "pyyaml"}


def test_dataflow_dockerfile_uses_worker_requirements_only() -> None:
    content = DOCKERFILE_DATAFLOW.read_text(encoding="utf-8")

    assert "FROM apache/beam_python3.11_sdk:2.74.0" in content
    assert "requirements-dataflow-worker.txt" in content
    assert "requirements.txt" not in content
    assert "requirements" + "-dataflow.txt" not in content
    assert "setup" + ".py" not in content
    assert "README.md" not in content
    assert "apache-beam[gcp]" not in content


def test_pyproject_packages_all_pipeline_subpackages() -> None:
    content = PYPROJECT.read_text(encoding="utf-8")

    assert 'include = ["pipelines*"]' in content


def test_removed_runtime_dependency_mechanisms_do_not_exist() -> None:
    search_roots = [
        REPO_ROOT / "pipelines",
        REPO_ROOT / "scripts",
        REPO_ROOT / "dags",
        REPO_ROOT / "config",
        REPO_ROOT / "docs",
        REPO_ROOT / "tests",
    ]
    forbidden = [
        "DATAFLOW_" + "SETUP_FILE",
        "--setup" + "-file",
        "DATAFLOW_" + "REQUIREMENTS_FILE",
        "--requirements" + "-file",
        "requirements" + "-dataflow.txt",
    ]

    for root in search_roots:
        for path in root.rglob("*"):
            if path.is_file() and path.suffix in {".py", ".ps1", ".yaml", ".yml", ".md", ".txt"}:
                content = path.read_text(encoding="utf-8")
                for value in forbidden:
                    assert value not in content, f"{value} encontrado en {path}"
