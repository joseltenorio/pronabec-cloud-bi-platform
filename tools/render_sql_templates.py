import argparse
import os
import re
from pathlib import Path


PROJECT_ID_ENV_VAR = "GCP_PROJECT_ID"

DEFAULT_OUTPUT_DIR = "build/generated/sql"
DEFAULT_SOURCE_FILES = (
    "sql/ddl/create_datasets.sql",
    "sql/ddl/create_audit_tables.sql",
    "sql/ddl/create_gold_views.sql",
    "sql/ml/create_dim_region_mapping.sql",
    "sql/ml/create_region_context_features.sql",
    "sql/ml/create_region_priority_scores.sql",
    "sql/quality/data_quality_checks.sql",
)

PLACEHOLDER_PATTERN = re.compile(r"\{[A-Za-z0-9_]+\}")


def load_dotenv_if_available() -> None:
    """Carga variables desde .env en ejecución local, salvo que se desactive explícitamente."""
    if os.getenv("DISABLE_DOTENV") == "1":
        return

    try:
        from dotenv import load_dotenv
    except ImportError:
        return

    load_dotenv()


def clean_config_value(value: str | None) -> str | None:
    if value is None:
        return None

    value = value.strip()
    return value or None


def resolve_required_config(
    *,
    cli_value: str | None,
    env_var_name: str,
    option_name: str,
    parser: argparse.ArgumentParser,
) -> str:
    value = clean_config_value(cli_value) or clean_config_value(os.getenv(env_var_name))

    if value is None:
        parser.error(
            f"Falta configuración requerida para {option_name}. "
            f"Use {option_name} o defina la variable {env_var_name} en el entorno/.env."
        )

    return value


def resolve_optional_config(
    *,
    cli_value: str | None,
    env_var_name: str,
    default_value: str,
) -> str:
    return (
        clean_config_value(cli_value)
        or clean_config_value(os.getenv(env_var_name))
        or default_value
    )


def build_replacements(
    *,
    project_id: str,
    bronze_dataset: str,
    silver_dataset: str,
    gold_dataset: str,
    audit_dataset: str,
    ml_dataset: str,
) -> dict[str, str]:
    return {
        "your-gcp-project-id": project_id,
        "{project_id}": project_id,
        "{bronze_dataset}": bronze_dataset,
        "{silver_dataset}": silver_dataset,
        "{gold_dataset}": gold_dataset,
        "{audit_dataset}": audit_dataset,
        "{ml_dataset}": ml_dataset,
    }


def render_sql_content(sql: str, replacements: dict[str, str]) -> str:
    rendered = sql

    for placeholder, value in replacements.items():
        rendered = rendered.replace(placeholder, value)

    return rendered


def find_unresolved_placeholders(sql: str) -> list[str]:
    return sorted(set(PLACEHOLDER_PATTERN.findall(sql)))


def rendered_file_name(source_path: Path) -> str:
    return f"{source_path.stem}.rendered.sql"


def render_sql_file(
    *,
    source_path: Path,
    output_dir: Path,
    replacements: dict[str, str],
) -> Path:
    if not source_path.exists():
        raise FileNotFoundError(f"No existe el archivo SQL de entrada: {source_path}")

    sql = source_path.read_text(encoding="utf-8")
    rendered_sql = render_sql_content(sql, replacements)

    if "your-gcp-project-id" in rendered_sql:
        raise ValueError(
            f"El archivo renderizado conserva 'your-gcp-project-id': {source_path}"
        )

    unresolved_placeholders = find_unresolved_placeholders(rendered_sql)
    if unresolved_placeholders:
        joined = ", ".join(unresolved_placeholders)
        raise ValueError(
            f"El archivo renderizado conserva placeholders sin resolver en "
            f"{source_path}: {joined}"
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / rendered_file_name(source_path)
    output_path.write_text(rendered_sql, encoding="utf-8")

    return output_path


def parse_source_files(values: list[str] | None) -> list[Path]:
    if not values:
        return [Path(value) for value in DEFAULT_SOURCE_FILES]

    return [Path(value) for value in values]


def parse_args() -> argparse.Namespace:
    load_dotenv_if_available()

    parser = argparse.ArgumentParser(
        description="Renderiza plantillas SQL de BigQuery reemplazando placeholders por valores de entorno."
    )
    parser.add_argument(
        "--project-id",
        default=None,
        help=f"ID del proyecto GCP. También puede definirse con {PROJECT_ID_ENV_VAR}.",
    )
    parser.add_argument("--bronze-dataset", default=None)
    parser.add_argument("--silver-dataset", default=None)
    parser.add_argument("--gold-dataset", default=None)
    parser.add_argument("--audit-dataset", default=None)
    parser.add_argument("--ml-dataset", default=None)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--source-file",
        action="append",
        default=None,
        help="Archivo SQL fuente a renderizar. Puede declararse múltiples veces.",
    )

    args = parser.parse_args()

    args.project_id = resolve_required_config(
        cli_value=args.project_id,
        env_var_name=PROJECT_ID_ENV_VAR,
        option_name="--project-id",
        parser=parser,
    )
    args.bronze_dataset = resolve_optional_config(
        cli_value=args.bronze_dataset,
        env_var_name="BQ_BRONZE_DATASET",
        default_value="bronze",
    )
    args.silver_dataset = resolve_optional_config(
        cli_value=args.silver_dataset,
        env_var_name="BQ_SILVER_DATASET",
        default_value="silver",
    )
    args.gold_dataset = resolve_optional_config(
        cli_value=args.gold_dataset,
        env_var_name="BQ_GOLD_DATASET",
        default_value="gold",
    )
    args.audit_dataset = resolve_optional_config(
        cli_value=args.audit_dataset,
        env_var_name="BQ_AUDIT_DATASET",
        default_value="audit",
    )
    args.ml_dataset = resolve_optional_config(
        cli_value=args.ml_dataset,
        env_var_name="BQ_ML_DATASET",
        default_value="ml",
    )
    args.source_files = parse_source_files(args.source_file)

    return args


def main() -> None:
    args = parse_args()

    replacements = build_replacements(
        project_id=args.project_id,
        bronze_dataset=args.bronze_dataset,
        silver_dataset=args.silver_dataset,
        gold_dataset=args.gold_dataset,
        audit_dataset=args.audit_dataset,
        ml_dataset=args.ml_dataset,
    )

    output_dir = Path(args.output_dir)
    rendered_paths = []

    for source_file in args.source_files:
        rendered_path = render_sql_file(
            source_path=source_file,
            output_dir=output_dir,
            replacements=replacements,
        )
        rendered_paths.append(rendered_path)

    for rendered_path in rendered_paths:
        print(f"Generated {rendered_path}")


if __name__ == "__main__":
    main()
