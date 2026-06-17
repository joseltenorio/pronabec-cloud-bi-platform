"""
Ejecutor de reglas de calidad de datos (Data Quality Check Runner) para Project Cloud BI Platform.

Este módulo carga las consultas de calidad de datos en SQL, las parametriza con los
datasets correspondientes, las ejecuta en BigQuery y almacena los resultados en
la tabla de auditoría correspondiente. Soporta un modo dry-run para validación local.
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery

from pipelines.common.audit import generate_run_id
from pipelines.common.logging import log_event, setup_structured_logger

# Configuración del logger estructurado
logger = setup_structured_logger("quality_checks")


def deduce_source_metadata(table_name: str) -> tuple[str, str]:
    """
    Deduce el sistema origen (source_system) y dataset origen (source_dataset)
    a partir del nombre de la tabla de la capa Silver.

    Args:
        table_name: Nombre de la tabla Silver analizada.

    Returns:
        Tupla con (source_system, source_dataset).
    """
    if not table_name or table_name == "unknown":
        return "unknown", "unknown"

    # Caso 1: Familia de reportes PRONABEC (PES 2025 o manuales)
    if table_name.startswith("pronabec_report_"):
        return "pronabec_reports", table_name.replace("pronabec_report_", "")
    
    # Caso 2: Tablas principales seleccionadas de PRONABEC
    if table_name.startswith("pronabec_"):
        return "pronabec", table_name.replace("pronabec_", "")
    
    # Caso 3: Tablas del Ministerio de Economía y Finanzas (MEF)
    if table_name.startswith("presupuesto_mef"):
        dataset = table_name.replace("presupuesto_mef_", "presupuesto_") if "_" in table_name else "presupuesto"
        return "mef", dataset

    return "unknown", "unknown"


def split_sql_queries(sql_content: str) -> list[str]:
    """
    Divide el contenido de un archivo SQL en consultas individuales usando punto y coma (;).
    Ignora líneas comentadas enteras y bloques vacíos.

    Decisión de diseño:
    - Se realiza una separación simple por punto y coma. Se documenta la limitación de que
      no se deben incluir puntos y comas internos en literales de texto dentro del SQL,
      ya que esto segmentaría de forma incorrecta la consulta. Si se requiriera soporte para ello,
      se necesitaría un analizador sintáctico completo de SQL.
    """
    raw_queries = sql_content.split(";")
    queries = []
    
    for q in raw_queries:
        trimmed = q.strip()
        # Filtrar líneas de comentario al inicio para evaluar si hay SQL ejecutable
        sql_lines = [
            line.strip() 
            for line in trimmed.splitlines() 
            if not line.strip().startswith("--")
        ]
        sql_content_clean = "\n".join(sql_lines).strip()
        
        if sql_content_clean and any(c.isalnum() for c in sql_content_clean):
            queries.append(trimmed)
            
    return queries


def run_quality_checks(
    project_id: str,
    silver_dataset: str,
    gold_dataset: str,
    audit_dataset: str,
    checks_file: str,
    pipeline_run_id: str,
    dry_run: bool = False,
    fail_on_error: bool = False,
) -> int:
    """
    Carga y ejecuta las consultas de calidad de datos, y persiste los resultados en Audit.

    Args:
        project_id: ID del proyecto GCP.
        silver_dataset: Nombre del dataset Silver.
        gold_dataset: Nombre del dataset Gold.
        audit_dataset: Nombre del dataset de Auditoría.
        checks_file: Ruta del archivo SQL que contiene los checks de calidad.
        pipeline_run_id: ID de ejecución general del pipeline.
        dry_run: Indica si solo se debe simular la ejecución.
        fail_on_error: Si es True, detiene la ejecución ante fallos críticos de consulta.

    Returns:
        Código de salida (0 exitoso, 1 si hubo fallos de calidad de datos).
    """
    log_event(
        logger, 
        "INFO", 
        "Iniciando runner de calidad de datos.", 
        project_id=project_id,
        silver_dataset=silver_dataset,
        gold_dataset=gold_dataset,
        audit_dataset=audit_dataset,
        checks_file=checks_file,
        pipeline_run_id=pipeline_run_id,
        dry_run=dry_run
    )

    checks_path = Path(checks_file)
    if not checks_path.exists():
        log_event(logger, "ERROR", f"No se encontró el archivo de checks: {checks_file}")
        return 1

    try:
        sql_content = checks_path.read_text(encoding="utf-8")
    except Exception as e:
        log_event(logger, "ERROR", f"Error leyendo el archivo de checks: {str(e)}")
        return 1

    queries = split_sql_queries(sql_content)
    log_event(logger, "INFO", f"Se encontraron {len(queries)} consultas de calidad de datos para procesar.")

    quality_run_id = generate_run_id("quality")
    execution_timestamp = datetime.now(timezone.utc)
    results = []

    # Inicializar cliente BigQuery solo si no es dry-run
    client = None
    if not dry_run:
        client = bigquery.Client(project=project_id)

    had_failures = False

    for idx, query in enumerate(queries):
        # Parametrizar la consulta reemplazando placeholders
        rendered_query = query.format(
            project_id=project_id,
            silver_dataset=silver_dataset,
            gold_dataset=gold_dataset,
            audit_dataset=audit_dataset
        )

        # Extraer metadatos estáticos de la consulta para usar de fallback si falla la ejecución
        check_id = "unknown"
        layer = "unknown"
        table_name = "unknown"
        severity = "ERROR"

        clean_lines = [
            line for line in rendered_query.splitlines() 
            if not line.strip().startswith("--")
        ]
        clean_query = "\n".join(clean_lines)

        check_id_match = re.search(r"'\s*([a-zA-Z0-9_-]+)\s*'\s+AS\s+check_id", clean_query, re.IGNORECASE)
        layer_match = re.search(r"'\s*([a-zA-Z0-9_-]+)\s*'\s+AS\s+layer", clean_query, re.IGNORECASE)
        table_name_match = re.search(r"'\s*([a-zA-Z0-9_-]+)\s*'\s+AS\s+table_name", clean_query, re.IGNORECASE)
        severity_match = re.search(r"'\s*([a-zA-Z0-9_-]+)\s*'\s+AS\s+severity", clean_query, re.IGNORECASE)

        if check_id_match:
            check_id = check_id_match.group(1)
        if layer_match:
            layer = layer_match.group(1)
        if table_name_match:
            table_name = table_name_match.group(1)
        if severity_match:
            severity = severity_match.group(1)

        source_system, source_dataset = deduce_source_metadata(table_name)

        if dry_run:
            log_event(
                logger, 
                "INFO", 
                f"Dry-run: Check [{check_id}] para tabla [{table_name}] simulado correctamente.",
                check_id=check_id,
                table_name=table_name
            )
            results.append({
                "check_id": check_id,
                "layer": layer,
                "table_name": table_name,
                "severity": severity,
                "passed": True,
                "failed_rows": 0,
                "details": "Simulado bajo modo dry-run",
                "source_system": source_system,
                "source_dataset": source_dataset
            })
            continue

        # Ejecutar en BigQuery
        log_event(logger, "INFO", f"Ejecutando check [{check_id}] ({idx + 1}/{len(queries)})...")
        try:
            query_job = client.query(rendered_query)
            rows = list(query_job.result())

            if not rows:
                raise ValueError("La consulta no devolvió filas. Revise la definición del SQL.")

            row = rows[0]
            # Convertir fila a diccionario para compatibilidad con mocks de test
            row_dict = dict(row.items()) if hasattr(row, "items") else dict(row)

            passed = bool(row_dict.get("passed"))
            failed_rows = int(row_dict.get("failed_rows", 0))
            details = str(row_dict.get("details", ""))

            results.append({
                "check_id": check_id,
                "layer": layer,
                "table_name": table_name,
                "severity": severity,
                "passed": passed,
                "failed_rows": failed_rows,
                "details": details,
                "source_system": source_system,
                "source_dataset": source_dataset
            })

            if not passed:
                had_failures = True
                level = "WARNING" if severity == "WARNING" else "ERROR"
                log_event(
                    logger, 
                    level, 
                    f"Check de calidad fallido: [{check_id}]. Registros fallidos: {failed_rows}.",
                    check_id=check_id,
                    failed_rows=failed_rows,
                    details=details
                )
            else:
                log_event(logger, "INFO", f"Check de calidad exitoso: [{check_id}].")

        except Exception as e:
            log_event(logger, "ERROR", f"Error crítico al ejecutar consulta para check [{check_id}]: {str(e)}")
            had_failures = True
            results.append({
                "check_id": check_id,
                "layer": layer,
                "table_name": table_name,
                "severity": severity,
                "passed": False,
                "failed_rows": 1,
                "details": f"Excepción en ejecución SQL: {str(e)}",
                "source_system": source_system,
                "source_dataset": source_dataset
            })
            if fail_on_error:
                raise e

    # Persistir los resultados en la tabla Audit.data_quality_results
    audit_rows = []
    checks_file_name = checks_path.name

    for res in results:
        audit_rows.append({
            "quality_run_id": quality_run_id,
            "pipeline_run_id": pipeline_run_id,
            "execution_timestamp": execution_timestamp.isoformat(),
            "check_id": res["check_id"],
            "layer": res["layer"],
            "table_name": res["table_name"],
            "severity": res["severity"],
            "passed": res["passed"],
            "failed_rows": res["failed_rows"],
            "details": res["details"],
            "query_file": checks_file_name,
            "source_system": res["source_system"],
            "source_dataset": res["source_dataset"]
        })

    if dry_run:
        log_event(logger, "INFO", "Ejecución finalizada (modo dry-run). No se persistieron datos.")
        return 0

    table_ref = f"{project_id}.{audit_dataset}.data_quality_results"
    log_event(logger, "INFO", f"Persistiendo {len(audit_rows)} resultados en tabla de auditoría: {table_ref}...")
    
    try:
        errors = client.insert_rows_json(table_ref, audit_rows)
        if errors:
            log_event(logger, "ERROR", f"Errores al insertar filas en BigQuery Audit: {errors}")
            if fail_on_error:
                raise RuntimeError(f"Error persistiendo auditoría en BigQuery: {errors}")
        else:
            log_event(logger, "INFO", "Resultados de calidad de datos persistidos exitosamente.")
    except Exception as e:
        log_event(logger, "ERROR", f"Excepción al insertar resultados en BigQuery Audit: {str(e)}")
        if fail_on_error:
            raise e

    return 1 if had_failures else 0


def main() -> None:
    """Función de entrada CLI."""
    parser = argparse.ArgumentParser(
        description="Ejecuta consultas de calidad de datos en BigQuery y las audita."
    )
    parser.add_argument(
        "--project-id",
        required=True,
        help="ID del proyecto de Google Cloud."
    )
    parser.add_argument(
        "--silver-dataset",
        default="silver",
        help="Nombre del dataset Silver en BigQuery."
    )
    parser.add_argument(
        "--gold-dataset",
        default="gold",
        help="Nombre del dataset Gold en BigQuery."
    )
    parser.add_argument(
        "--audit-dataset",
        default="audit",
        help="Nombre del dataset de auditoría en BigQuery."
    )
    parser.add_argument(
        "--checks-file",
        default="sql/quality/data_quality_checks.sql",
        help="Ruta al archivo SQL con los checks de calidad."
    )
    parser.add_argument(
        "--pipeline-run-id",
        default="manual-quality-check",
        help="Identificador de la ejecución del pipeline para trazabilidad."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Si se especifica, solo simula y formatea las consultas sin ejecutar BigQuery."
    )
    parser.add_argument(
        "--fail-on-error",
        action="store_true",
        help="Si se especifica, la ejecución lanzará excepciones ante fallos críticos SQL."
    )

    args = parser.parse_args()

    exit_code = run_quality_checks(
        project_id=args.project_id,
        silver_dataset=args.silver_dataset,
        gold_dataset=args.gold_dataset,
        audit_dataset=args.audit_dataset,
        checks_file=args.checks_file,
        pipeline_run_id=args.pipeline_run_id,
        dry_run=args.dry_run,
        fail_on_error=args.fail_on_error
    )

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
