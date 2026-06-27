"""Helpers for rendering versioned SQL templates."""

from __future__ import annotations

import re
from pathlib import Path


PLACEHOLDER_PATTERN = re.compile(r"\{[A-Za-z0-9_]+\}")


def render_template(text: str, replacements: dict[str, str]) -> str:
    """Render a template with simple placeholder replacement."""
    rendered = text
    for placeholder, value in replacements.items():
        rendered = rendered.replace(f"{{{placeholder}}}", value)
    return rendered


def find_unresolved_placeholders(text: str) -> list[str]:
    """Return unresolved placeholders still present in rendered text."""
    return sorted(set(PLACEHOLDER_PATTERN.findall(text)))


def split_sql_statements(sql: str) -> list[str]:
    """Split a SQL script into executable statements."""
    statements: list[str] = []
    for raw_statement in sql.split(";"):
        cleaned_lines = [
            line
            for line in raw_statement.splitlines()
            if not line.strip().startswith("--")
        ]
        statement = "\n".join(cleaned_lines).strip()
        if statement:
            statements.append(statement)
    return statements


def load_sql_file(path: str | Path) -> str:
    """Load SQL content from disk using UTF-8."""
    sql_path = Path(path)
    if not sql_path.exists():
        raise FileNotFoundError(f"No existe el archivo SQL: {sql_path}")
    return sql_path.read_text(encoding="utf-8")
