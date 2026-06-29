"""
Exploración de fuentes para Project Cloud BI Platform.

Este script permite inspeccionar cómo responden los endpoints públicos antes de
implementar los extractores productivos.

No sube datos a Google Cloud.
No reemplaza al pipeline final.
Guarda muestras locales en tmp/source_probe/, carpeta ignorada por Git.
"""

import argparse
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
import yaml


def load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def ensure_output_dir() -> Path:
    output_dir = Path("tmp/source_probe")
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def print_section(title: str) -> None:
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)


def try_parse_json(response: requests.Response) -> Any | None:
    try:
        return response.json()
    except ValueError:
        return None


def build_pronabec_data_url(base_url: str, endpoint_path: str) -> str:
    return f"{base_url.rstrip('/')}/Listar{endpoint_path.strip('/')}"


def find_pronabec_endpoint(config: dict, dataset_name: str) -> dict:
    endpoints = {
        endpoint["name"]: endpoint
        for endpoint in config["pronabec"]["endpoints"]
    }

    if dataset_name not in endpoints:
        available = ", ".join(sorted(endpoints.keys()))
        raise ValueError(f"Dataset no encontrado: {dataset_name}. Disponibles: {available}")

    return endpoints[dataset_name]


def probe_pronabec_page_size(
    dataset: str,
    page: int,
    page_size: int,
    endpoints_config: dict,
    timeout: int = 60,
) -> dict[str, Any]:
    if page <= 0:
        raise ValueError("--page debe ser un entero positivo")
    if page_size <= 0:
        raise ValueError("--page-size debe ser un entero positivo")

    endpoint = find_pronabec_endpoint(endpoints_config, dataset)
    url = build_pronabec_data_url(
        endpoints_config["pronabec"]["base_url"],
        endpoint["path"],
    )
    params = {
        "_search": "false",
        "nd": int(datetime.now().timestamp() * 1000),
        "rows": page_size,
        "page": page,
        "sidx": "NRO_FILA",
        "sord": "asc",
    }

    started_at = time.perf_counter()
    result: dict[str, Any] = {
        "source": "pronabec",
        "dataset": dataset,
        "requested_page": page,
        "requested_page_size": page_size,
        "actual_records_returned": None,
        "total_records": None,
        "total_pages": None,
        "status_code": None,
        "elapsed_seconds": None,
        "server_capped_page_size": False,
    }

    try:
        response = requests.get(url, params=params, timeout=timeout)
        result["status_code"] = response.status_code
        result["elapsed_seconds"] = round(time.perf_counter() - started_at, 3)

        if response.status_code >= 400:
            result["error"] = f"HTTP error {response.status_code}"
            result["url"] = response.url
            return result

        try:
            payload = response.json()
        except ValueError:
            result["error"] = "Respuesta PRONABEC no es JSON valido"
            result["url"] = response.url
            return result

        if not isinstance(payload, dict):
            result["error"] = "Respuesta PRONABEC JSON no es un objeto"
            result["url"] = response.url
            return result

        rows = payload.get("rows")
        if not isinstance(rows, list):
            result["error"] = "Respuesta PRONABEC no contiene rows como lista"
            result["url"] = response.url
            return result

        actual_records_returned = len(rows)
        total_records = _parse_positive_int(payload.get("records"))
        total_pages = _parse_positive_int(payload.get("total"))

        result.update(
            {
                "actual_records_returned": actual_records_returned,
                "total_records": total_records,
                "total_pages": total_pages,
                "url": response.url,
            }
        )
        result["server_capped_page_size"] = (
            page == 1
            and page_size > actual_records_returned
            and total_records is not None
            and total_records > actual_records_returned
        )
        return result

    except requests.RequestException as exc:
        result["elapsed_seconds"] = round(time.perf_counter() - started_at, 3)
        response = getattr(exc, "response", None)
        if response is not None:
            result["status_code"] = response.status_code
        result["error"] = str(exc)
        return result


def _parse_positive_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    if parsed < 0:
        return None
    return parsed


def normalize_jqgrid_row(row: dict, expected_columns: list[str] | None = None) -> dict:
    """
    Convierte una fila jqGrid de PRONABEC desde:
        {"id": "...", "cell": [...]}

    hacia un diccionario tabular usando expected_columns.
    """
    if "cell" not in row or not isinstance(row["cell"], list):
        return row

    cells = row["cell"]
    normalized = {
        "source_row_id": row.get("id"),
    }

    for index, value in enumerate(cells):
        if expected_columns and index < len(expected_columns):
            column_name = expected_columns[index]
        else:
            column_name = f"cell_{index + 1}"

        normalized[column_name] = value

    return normalized


def extract_records_from_payload(
        payload: Any,
        expected_columns: list[str] | None = None,
    ) -> list[dict]:
        """
        Intenta detectar registros tabulares dentro de respuestas JSON comunes.

        Soporta estructuras como:
        - [ {...}, {...} ]
        - { "data": [ {...} ] }
        - { "rows": [ {"id": "...", "cell": [...] } ] }
        - { "result": [ {...} ] }
        - { "records": [ {...} ] }
        """
        if isinstance(payload, list):
            return [
                normalize_jqgrid_row(item, expected_columns)
                for item in payload
                if isinstance(item, dict)
            ]

        if not isinstance(payload, dict):
            return []

        candidate_keys = ["data", "rows", "result", "results", "records", "items"]

        for key in candidate_keys:
            value = payload.get(key)
            if isinstance(value, list):
                return [
                    normalize_jqgrid_row(item, expected_columns)
                    for item in value
                    if isinstance(item, dict)
                ]

        return []

def inspect_json_structure(payload: Any) -> None:
    print(f"Tipo raíz: {type(payload).__name__}")

    if isinstance(payload, dict):
        print(f"Claves raíz: {list(payload.keys())}")

        for key, value in payload.items():
            if isinstance(value, list):
                print(f"- {key}: list[{len(value)}]")
                if value:
                    print(f"  Tipo primer elemento: {type(value[0]).__name__}")
                    if isinstance(value[0], dict):
                        print(f"  Claves primer registro: {list(value[0].keys())}")
            elif isinstance(value, dict):
                print(f"- {key}: dict con claves {list(value.keys())}")
            else:
                print(f"- {key}: {type(value).__name__} = {str(value)[:120]}")

    elif isinstance(payload, list):
        print(f"Cantidad de elementos: {len(payload)}")
        if payload:
            print(f"Tipo primer elemento: {type(payload[0]).__name__}")
            if isinstance(payload[0], dict):
                print(f"Claves primer registro: {list(payload[0].keys())}")


def save_outputs(
    output_dir: Path,
    dataset_name: str,
    response: requests.Response,
    payload: Any | None,
    expected_columns: list[str] | None = None,
) -> None:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    metadata = {
        "dataset": dataset_name,
        "url": response.url,
        "status_code": response.status_code,
        "content_type": response.headers.get("Content-Type"),
        "response_size_bytes": len(response.content),
        "created_at": datetime.now().isoformat(),
    }

    metadata_path = output_dir / f"{dataset_name}_{timestamp}_metadata.json"
    metadata_path.write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    if payload is not None:
        raw_path = output_dir / f"{dataset_name}_{timestamp}_raw.json"
        raw_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        records = extract_records_from_payload(payload, expected_columns)
        if records:
            jsonl_path = output_dir / f"{dataset_name}_{timestamp}_sample.jsonl"
            with jsonl_path.open("w", encoding="utf-8") as file:
                for record in records[:100]:
                    file.write(json.dumps(record, ensure_ascii=False) + "\n")
            print(f"Muestra JSONL guardada en: {jsonl_path}")
    else:
        raw_path = output_dir / f"{dataset_name}_{timestamp}_raw.txt"
        raw_path.write_text(response.text[:20000], encoding="utf-8")

    print(f"Metadata guardada en: {metadata_path}")
    print(f"Respuesta raw guardada en: {raw_path}")


def build_possible_urls(base_url: str, path: str) -> list[tuple[str, dict | None]]:
    """
    Genera variantes de URL para explorar la fuente.

    PRONABEC expone una página HTML en /Dataset/<path>, pero los datos suelen
    obtenerse desde un endpoint jqGrid en /Dataset/Listar<path>.
    """
    base_url = base_url.rstrip("/")
    path = path.strip("/")

    timestamp = int(datetime.now().timestamp() * 1000)

    jqgrid_params_10 = {
        "_search": "false",
        "nd": timestamp,
        "rows": 10,
        "page": 1,
        "sidx": "NRO_FILA",
        "sord": "asc",
    }

    jqgrid_params_100 = {
        "_search": "false",
        "nd": timestamp,
        "rows": 100,
        "page": 1,
        "sidx": "NRO_FILA",
        "sord": "asc",
    }

    return [
        (f"{base_url}/{path}", None),
        (f"{base_url}/{path}", {"page": 1, "rows": 10}),
        (f"{base_url}/{path}", {"page": 1, "rows": 100}),
        (f"{base_url}/Listar{path}", jqgrid_params_10),
        (f"{base_url}/Listar{path}", jqgrid_params_100),
    ]

def probe_pronabec_endpoint(config: dict, dataset_name: str, timeout: int = 60) -> None:
    pronabec_config = config["pronabec"]
    base_url = pronabec_config["base_url"]

    endpoints = {
        endpoint["name"]: endpoint
        for endpoint in pronabec_config["endpoints"]
        if endpoint.get("enabled", True)
    }

    if dataset_name not in endpoints:
        available = ", ".join(endpoints.keys())
        raise ValueError(f"Dataset no encontrado: {dataset_name}. Disponibles: {available}")

    endpoint = endpoints[dataset_name]
    urls = build_possible_urls(base_url, endpoint["path"])
    expected_columns = endpoint.get("expected_columns", [])

    output_dir = ensure_output_dir()

    for url, params in urls:
        print_section(f"Probando PRONABEC endpoint: {dataset_name}")
        print(f"URL: {url}")
        print(f"Params: {params}")

        try:
            response = requests.get(url, params=params, timeout=timeout)
            print(f"Status code: {response.status_code}")
            print(f"Content-Type: {response.headers.get('Content-Type')}")
            print(f"Tamaño respuesta: {len(response.content)} bytes")
            print(f"URL final: {response.url}")

            payload = try_parse_json(response)

            if payload is None:
                print("La respuesta no pudo parsearse como JSON.")
                print("Primeros 500 caracteres:")
                print(response.text[:500])
            else:
                inspect_json_structure(payload)
                records = extract_records_from_payload(payload, expected_columns)
                print(f"Registros detectados: {len(records)}")
                if records:
                    print(f"Columnas detectadas: {list(records[0].keys())}")

            save_outputs(output_dir, dataset_name, response, payload, expected_columns)

        except Exception as exc:
            print(f"Error probando URL: {url}")
            print(str(exc))


def probe_all_pronabec(config_path: str) -> None:
    config = load_yaml(config_path)
    endpoints = config["pronabec"]["endpoints"]

    for endpoint in endpoints:
        if not endpoint.get("enabled", True):
            continue

        try:
            probe_pronabec_endpoint(config, endpoint["name"])
        except Exception as exc:
            print_section(f"Error probando {endpoint['name']}")
            print(str(exc))


def main() -> None:
    parser = argparse.ArgumentParser(description="Explora fuentes públicas del proyecto.")
    parser.add_argument(
        "--config",
        default="config/endpoints.yaml",
        help="Ruta al archivo de configuración de endpoints.",
    )
    parser.add_argument(
        "--source",
        choices=["pronabec"],
        default="pronabec",
        help="Fuente a explorar.",
    )
    parser.add_argument(
        "--dataset",
        help="Dataset definido en config/endpoints.yaml. Si se omite, prueba todos los endpoints habilitados.",
    )
    parser.add_argument(
        "--page",
        type=int,
        default=1,
        help="Pagina jqGrid a solicitar para diagnostico PRONABEC.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        help="Cantidad de filas jqGrid a solicitar mediante el parametro rows.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="Timeout HTTP en segundos.",
    )
    parser.add_argument(
        "--output",
        choices=["text", "json"],
        default="text",
        help="Formato de salida para diagnosticos puntuales.",
    )

    args = parser.parse_args()

    if args.source == "pronabec":
        config = load_yaml(args.config)

        if args.dataset:
            if args.page_size is not None:
                result = probe_pronabec_page_size(
                    dataset=args.dataset,
                    page=args.page,
                    page_size=args.page_size,
                    endpoints_config=config,
                    timeout=args.timeout,
                )
                if args.output == "json":
                    print(json.dumps(result, ensure_ascii=False, indent=2))
                else:
                    print(json.dumps(result, ensure_ascii=False, indent=2))
            else:
                probe_pronabec_endpoint(config, args.dataset, timeout=args.timeout)
        else:
            probe_all_pronabec(args.config)


if __name__ == "__main__":
    main()
