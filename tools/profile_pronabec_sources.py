"""
Ejecuta exploración y profiling para todos los datasets PRONABEC habilitados.

Este script usa:
- tools/probe_sources.py
- tools/profile_sample.py

No sube datos a Google Cloud.
Genera resultados locales en tmp/source_probe/ y tmp/source_profile/.
"""

import argparse
import subprocess
import sys
from pathlib import Path

import yaml


def load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def get_enabled_datasets(config_path: str) -> list[str]:
    config = load_yaml(config_path)
    endpoints = config["pronabec"]["endpoints"]

    return [
        endpoint["name"]
        for endpoint in endpoints
        if endpoint.get("enabled", True)
    ]


def find_latest_sample(dataset_name: str) -> Path | None:
    probe_dir = Path("tmp/source_probe")

    if not probe_dir.exists():
        return None

    samples = sorted(
        probe_dir.glob(f"*{dataset_name}*_sample.jsonl"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )

    return samples[0] if samples else None


def run_command(command: list[str]) -> int:
    print("\n" + "=" * 90)
    print("Ejecutando:", " ".join(command))
    print("=" * 90)

    result = subprocess.run(command, check=False)
    return result.returncode


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ejecuta discovery y profiling para datasets PRONABEC."
    )
    parser.add_argument(
        "--config",
        default="config/endpoints.yaml",
        help="Ruta al archivo config/endpoints.yaml.",
    )
    parser.add_argument(
        "--dataset",
        help="Dataset específico. Si se omite, ejecuta todos los habilitados.",
    )

    args = parser.parse_args()

    if args.dataset:
        datasets = [args.dataset]
    else:
        datasets = get_enabled_datasets(args.config)

    failed = []

    for dataset in datasets:
        print("\n" + "#" * 90)
        print(f"Dataset: {dataset}")
        print("#" * 90)

        probe_code = run_command(
            [
                sys.executable,
                "tools/probe_sources.py",
                "--source",
                "pronabec",
                "--dataset",
                dataset,
                "--config",
                args.config,
            ]
        )

        if probe_code != 0:
            failed.append(dataset)
            continue

        sample = find_latest_sample(dataset)

        if not sample:
            print(f"No se encontró muestra JSONL para {dataset}.")
            failed.append(dataset)
            continue

        profile_code = run_command(
            [
                sys.executable,
                "tools/profile_sample.py",
                "--input",
                str(sample),
            ]
        )

        if profile_code != 0:
            failed.append(dataset)

    print("\n" + "=" * 90)
    print("Resumen de ejecución")
    print("=" * 90)
    print(f"Datasets procesados: {len(datasets)}")
    print(f"Datasets con error: {failed}")


if __name__ == "__main__":
    main()