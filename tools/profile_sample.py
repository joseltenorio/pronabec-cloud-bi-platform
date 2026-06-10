"""
Perfilado local de muestras para Project Cloud BI Platform.

Este script analiza archivos generados por tools/probe_sources.py para obtener
un perfil de columnas, nulos, valores vacíos, duplicados, valores frecuentes,
detección básica de tipos, rangos numéricos y fechas.

Uso:
    python tools/profile_sample.py --input tmp/source_probe/notas_becarios_xxx_sample.jsonl
    python tools/profile_sample.py --input tmp/source_probe/archivo.csv
"""

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd


COMMON_NULL_STRINGS = {"", " ", "null", "none", "nan", "NaN", "NULL", "None", "-"}


def read_input(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()

    if suffix == ".jsonl":
        records = []
        with path.open("r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return pd.DataFrame(records)

    if suffix == ".json":
        with path.open("r", encoding="utf-8") as file:
            payload = json.load(file)

        if isinstance(payload, list):
            return pd.DataFrame(payload)

        if isinstance(payload, dict):
            for key in ["data", "rows", "result", "results", "records", "items"]:
                value = payload.get(key)
                if isinstance(value, list):
                    return pd.DataFrame(value)

        return pd.DataFrame([payload])

    if suffix == ".csv":
        return pd.read_csv(path)

    raise ValueError(f"Formato no soportado: {suffix}")


def normalize_nulls(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()

    for column in normalized.columns:
        if normalized[column].dtype == "object":
            normalized[column] = normalized[column].apply(
                lambda value: None
                if isinstance(value, str) and value.strip() in COMMON_NULL_STRINGS
                else value
            )

    return normalized


def try_numeric_profile(series: pd.Series) -> dict[str, Any]:
    non_null = series.dropna().astype(str).str.strip()

    normalized_numeric_text = (
        non_null
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )

    numeric = pd.to_numeric(normalized_numeric_text, errors="coerce")
    valid_numeric = numeric.dropna()

    if len(valid_numeric) == 0:
        return {
            "is_mostly_numeric": False,
            "numeric_valid_count": 0,
            "numeric_invalid_count": int(non_null.shape[0]),
        }

    total_non_null = int(non_null.shape[0])
    valid_count = int(valid_numeric.shape[0])
    invalid_count = total_non_null - valid_count
    valid_ratio = valid_count / total_non_null if total_non_null else 0

    result = {
        "is_mostly_numeric": valid_ratio >= 0.8,
        "numeric_valid_count": valid_count,
        "numeric_invalid_count": invalid_count,
    }

    if valid_ratio >= 0.8:
        result.update(
            {
                "numeric_min": float(valid_numeric.min()),
                "numeric_max": float(valid_numeric.max()),
                "numeric_mean": round(float(valid_numeric.mean()), 4),
            }
        )

    return result

def try_date_profile(column_name: str, series: pd.Series) -> dict[str, Any]:
    date_keywords = ["fecha", "date"]

    if not any(keyword in column_name.lower() for keyword in date_keywords):
        return {
            "is_mostly_date": False,
            "date_valid_count": 0,
            "date_invalid_count": int(series.dropna().shape[0]),
        }

    non_null = series.dropna().astype(str)

    if len(non_null) == 0:
        return {
            "is_mostly_date": False,
            "date_valid_count": 0,
            "date_invalid_count": 0,
        }

    parsed = pd.to_datetime(non_null, errors="coerce", dayfirst=True)
    valid_dates = parsed.dropna()

    valid_count = int(valid_dates.shape[0])
    total_non_null = int(non_null.shape[0])
    invalid_count = total_non_null - valid_count
    valid_ratio = valid_count / total_non_null if total_non_null else 0

    result = {
        "is_mostly_date": valid_ratio >= 0.8,
        "date_valid_count": valid_count,
        "date_invalid_count": invalid_count,
    }

    if valid_ratio >= 0.8:
        result.update(
            {
                "date_min": str(valid_dates.min()),
                "date_max": str(valid_dates.max()),
            }
        )

    return result


def profile_dataframe(df: pd.DataFrame) -> dict:
    df = normalize_nulls(df)

    row_count = len(df)
    column_count = len(df.columns)

    columns = []

    for column in df.columns:
        series = df[column]
        non_null_count = int(series.notna().sum())
        null_count = int(series.isna().sum())
        null_percentage = round((null_count / row_count) * 100, 2) if row_count else 0

        unique_count = int(series.dropna().nunique())
        unique_percentage = round((unique_count / row_count) * 100, 2) if row_count else 0

        sample_values = (
            series.dropna()
            .astype(str)
            .drop_duplicates()
            .head(5)
            .tolist()
        )

        top_values = (
            series.dropna()
            .astype(str)
            .value_counts()
            .head(5)
            .to_dict()
        )

        numeric_profile = try_numeric_profile(series)
        date_profile = try_date_profile(str(column), series)

        columns.append(
            {
                "column_name": str(column),
                "pandas_dtype": str(series.dtype),
                "non_null_count": non_null_count,
                "null_count": null_count,
                "null_percentage": null_percentage,
                "unique_count": unique_count,
                "unique_percentage": unique_percentage,
                "sample_values": sample_values,
                "top_values": top_values,
                **numeric_profile,
                **date_profile,
            }
        )

    duplicated_rows = int(df.duplicated().sum()) if row_count else 0

    high_null_columns = [
        column["column_name"]
        for column in columns
        if column["null_percentage"] >= 50
    ]

    mostly_numeric_columns = [
        column["column_name"]
        for column in columns
        if column["is_mostly_numeric"]
    ]

    mostly_date_columns = [
        column["column_name"]
        for column in columns
        if column["is_mostly_date"]
    ]

    return {
        "row_count": row_count,
        "column_count": column_count,
        "duplicated_rows": duplicated_rows,
        "high_null_columns": high_null_columns,
        "mostly_numeric_columns": mostly_numeric_columns,
        "mostly_date_columns": mostly_date_columns,
        "columns": columns,
    }


def print_profile(profile: dict) -> None:
    print("\nResumen")
    print("=" * 80)
    print(f"Filas: {profile['row_count']}")
    print(f"Columnas: {profile['column_count']}")
    print(f"Filas duplicadas exactas: {profile['duplicated_rows']}")
    print(f"Columnas con >=50% nulos: {profile['high_null_columns']}")
    print(f"Columnas mayormente numéricas: {profile['mostly_numeric_columns']}")
    print(f"Columnas mayormente fecha: {profile['mostly_date_columns']}")

    print("\nColumnas")
    print("=" * 80)

    for column in profile["columns"]:
        print(f"\n- {column['column_name']}")
        print(f"  Tipo pandas: {column['pandas_dtype']}")
        print(f"  No nulos: {column['non_null_count']}")
        print(f"  Nulos: {column['null_count']} ({column['null_percentage']}%)")
        print(f"  Únicos: {column['unique_count']} ({column['unique_percentage']}%)")
        print(f"  Ejemplos: {column['sample_values']}")
        print(f"  Top valores: {column['top_values']}")

        if column["is_mostly_numeric"]:
            print(
                "  Perfil numérico: "
                f"min={column.get('numeric_min')}, "
                f"max={column.get('numeric_max')}, "
                f"mean={column.get('numeric_mean')}"
            )

        if column["is_mostly_date"]:
            print(
                "  Perfil fecha: "
                f"min={column.get('date_min')}, "
                f"max={column.get('date_max')}"
            )


def save_profile(profile: dict, input_path: Path) -> Path:
    output_dir = Path("tmp/source_profile")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"{input_path.stem}_profile.json"
    output_path.write_text(
        json.dumps(profile, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Perfila una muestra local.")
    parser.add_argument("--input", required=True, help="Ruta del archivo a perfilar.")
    args = parser.parse_args()

    input_path = Path(args.input)

    if not input_path.exists():
        raise FileNotFoundError(f"No existe el archivo: {input_path}")

    df = read_input(input_path)
    profile = profile_dataframe(df)
    print_profile(profile)

    output_path = save_profile(profile, input_path)
    print(f"\nPerfil guardado en: {output_path}")


if __name__ == "__main__":
    main()