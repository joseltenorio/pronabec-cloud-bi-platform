# Documentación del Sistema de Calidad de Datos (Data Quality) y Auditoría

Este documento detalla el diseño, la estructura, la ejecución y la interpretación del sistema de calidad de datos (Data Quality Checks) implementado sobre la capa Silver de BigQuery para el **Project Cloud BI Platform**.

## 1. Objetivo de la Capa de Calidad de Datos

El objetivo principal de esta capa es validar de manera estructurada y automatizada la integridad y la consistencia de los datos procesados en la capa Silver y (posteriormente) Gold. Al ejecutar estas reglas directamente en BigQuery, nos aseguramos de que el repositorio central de datos analíticos cumpla con los estándares mínimos requeridos antes de ser explotado por las vistas analíticas o paneles de control.

## 2. Relación y Diferencia entre Componentes

Es crucial entender que las validaciones de calidad de datos complementan, pero no reemplazan, los otros mecanismos operativos de la plataforma:

| Componente | Nivel | Propósito Principal | Acción ante Falla |
| :--- | :--- | :--- | :--- |
| **Dead Letter Queue (DLQ)** | Ingesta / Dataflow | Salvaguardar registros malformados o corruptos individuales que no pueden ser parseados o transformados desde Bronze a Silver. | Enruta el registro dañado a un almacenamiento aislado (GCS) y permite que el resto del pipeline continúe sin caerse. |
| **Processing Summary** | Dataflow | Reportar métricas acumuladas de procesamiento al finalizar cada ejecución de Dataflow (filas leídas, escritas y rechazadas). | Informa sobre la salud general de la ingesta batch. |
| **Data Quality Checks** | BigQuery (Post-ingesta) | Evaluar consistencia lógica, reglas de negocio y completitud de las tablas completas ya persistidas en BigQuery. | Registra advertencias (WARNING) o fallos críticos (ERROR). Puede bloquear la generación de Gold. |
| **Audit Persistence** | BigQuery (Transversal) | Almacenar logs e históricos persistentes de calidad y ejecuciones para auditoría y reportería operativa. | Escribe en tablas dedicadas del dataset `audit`. |

## 3. Cobertura de Fuentes de Datos

El sistema de validación cubre los tres sistemas principales del proyecto:

1. **`pronabec`**: Validación de las tablas principales seleccionadas:
   - `pronabec_convocatorias`
   - `pronabec_ubigeo_postulacion`
   - `pronabec_becarios_pais_estudio`
   - `pronabec_colegios_elegibles`
   - `pronabec_beca18_becarios_provincia_2016` (valida exclusión de subtotales/totales regionales, campos obligatorios no nulos y conteos no negativos).
2. **`mef`**: Validación del presupuesto público extraído desde el Ministerio de Economía y Finanzas:
   - `presupuesto_mef`, `presupuesto_mef_producto`, `presupuesto_mef_actividad`, `presupuesto_mef_generica`, `presupuesto_mef_hierarchy` (incluyen reglas de no-negatividad para montos `pia`, `pim`, `devengado` y `avance_porcentaje`).
   - `presupuesto_mef_temporal`, `presupuesto_mef_producto_temporal`, `presupuesto_mef_actividad_temporal`, `presupuesto_mef_generica_temporal` (exentas de validaciones de no-negatividad por diseño de ajustes temporales).
3. **`pronabec_reports`**: Cobertura de reportes agregados que incluye:
   - **Beca 18 Universitarios (2012-2026)**: Tablas de carrera anual y universidad anual.
   - **Reportes documentales**: Cobertura de calidad para los 23 reportes documentales de origen, validando que no estén vacíos y cuenten con metadatos técnicos válidos (`extraction_date`, `pipeline_run_id`).
4. **`ml`**: Cobertura de la base regional predictiva, validando `ml.region_context_features` como features territoriales unificadas.

## 4. Tipos de Checks Implementados

Las consultas versionadas en `sql/quality/data_quality_checks.sql` cubren las siguientes tipologías de reglas:

* **Tablas No Vacías (`not empty`)**: Garantiza que los procesos de carga no hayan quedado en blanco.
* **Nulos en Campos Críticos (`critical nulls`)**: Verifica que campos clave de negocio (ej. IDs, UGEL, institución educativa, tipo de gestión, país de estudio) y metadatos técnicos (`source_system`, `extraction_date`, `pipeline_run_id`) estén presentes.
* **Completitud no crítica (`completeness warnings`)**: Registra como `WARNING` brechas parciales de completitud provenientes de fuentes oficiales cuando no impiden el uso analítico base. En `pronabec_colegios_elegibles`, `distrito`, `nivel_modalidad` y `forma_atencion` se auditan como advertencia; los campos críticos `ugel`, `institucion_educativa` y `tipo_gestion_colegio` se mantienen como `ERROR`.
* **Formatos de Texto (`schema-safe format`)**: Validaciones de estructura de cadenas de texto (ej. código de ubigeo no vacío y longitud apropiada).
* **Rangos de Valores Válidos (`valid ranges`)**: Comprobación de año fiscal lógico (rango 2000-2050) y cantidades métricas no negativas en reportes analíticos de becarios.
* **Consistencia de Campos Canónicos (`canonical consistency`)**: Si un campo de canonización (`carrera_estudio_canonical_match_method` o `universidad_canonical_match_method`) no es nulo, el valor canonizado correspondiente (`*_canonical`) no debe ser nulo.
* **Consistencia Temporal MEF (`temporal consistency`)**: Para tablas temporales del MEF, si el `periodo_tipo` es `MENSUAL`, el campo `mes_numero` debe estar entre 1 y 12. Si es `TRIMESTRAL`, el `trimestre` debe estar entre 1 y 4.
* **Duplicados (`duplicate checks`)**: Control de unicidad basado en la clave natural lógica de negocio (ej. combinación de carrera, año y fecha de extracción).
* **Base ML regional (`ml`)**: Validaciones de unicidad por `anio + region_canonical`, rangos 2012-2025, porcentajes, conteos no negativos y metadata sintética.

## 5. Prácticas y Restricciones de Seguridad (Checks que NO se deben hacer)

Para evitar sesgos o alteraciones incorrectas de datos operacionales, se aplican las siguientes restricciones estrictas en el diseño de los checks:

1. **No prohibir números negativos en el MEF globalmente**: Los cortes mensuales o temporales del MEF pueden incluir ajustes negativos legítimos del devengado. Restringir valores menores a cero de forma ciega distorsionará la auditoría financiera.
2. **No realizar agregaciones (sumas) sobre la jerarquía del MEF (`presupuesto_mef_hierarchy`)**: La tabla de jerarquía representa una estructura jerárquica con niveles redundantes. Sumar estos campos duplicaría de forma masiva el presupuesto total.
3. **No forzar fuzzy matching ni canonización heurística automática dentro de los checks**: Las reglas de calidad solo validan la consistencia de lo ya canonizado por el pipeline, no aplican lógica de transformación.
4. **No utilizar `SELECT *`**: Todas las consultas deben seleccionar campos explícitos para optimizar costos y evitar fallos si el esquema cambia.
5. **No ejecutar operaciones destructivas**: Las consultas de calidad deben ser únicamente de lectura (`SELECT`), sin alterar el estado de BigQuery (`DROP`, `DELETE`, `TRUNCATE`, `UPDATE`, `MERGE`, etc.).

## 6. Estructura de Resultados y Persistencia

Todas las consultas SQL de calidad de datos devuelven un **shape homogéneo**:

* `check_id` (STRING): Identificador único de la regla de calidad.
* `layer` (STRING): Capa de datos evaluada (`silver`, `gold`).
* `table_name` (STRING): Tabla evaluada.
* `severity` (STRING): Severidad del check (`ERROR`, `WARNING`, `INFO`).
* `failed_rows` (INT64): Cantidad de registros que fallan la regla.
* `passed` (BOOL): Indica si el check es exitoso (`TRUE`) o falló (`FALSE`).
* `details` (STRING): Explicación textual detallada del resultado.

### Persistencia en Auditoría

El runner Python recolecta estos resultados, agrega metadata técnica, y los persiste en la tabla de auditoría:

`{project_id}.{audit_dataset}.data_quality_results`

Los campos adicionales agregados por el runner son:
* `quality_run_id`: UUID único de la corrida de calidad.
* `pipeline_run_id`: ID de la ejecución general del pipeline para trazabilidad.
* `execution_timestamp`: Timestamp en UTC del momento de ejecución.
* `query_file`: Nombre del archivo de origen de los checks.
* `source_system`: Sistema fuente deducido (mef, pronabec, pronabec_reports).
* `source_dataset`: Sub-dataset de origen deducido.

## 7. Instrucciones de Ejecución

### Ejecución en Modo Dry-Run (Local)

El modo dry-run permite verificar la sintaxis de las consultas y la separación de las mismas sin requerir credenciales activas de GCP:

```powershell
.venv\Scripts\python -m pipelines.quality_checks `
  --project-id dummy-project-id `
  --dry-run
```

### Ejecución en Cloud (Modo Real)

Para ejecutar las pruebas en la nube y persistir los resultados en BigQuery:

```powershell
.venv\Scripts\python -m pipelines.quality_checks `
  --project-id your-gcp-project-id `
  --silver-dataset silver `
  --gold-dataset gold `
  --audit-dataset audit `
  --checks-file sql/quality/data_quality_checks.sql `
  --pipeline-run-id run_manual_$(Get-Date -Format "yyyyMMdd_HHmmss")
```

Se puede añadir el flag `--fail-on-error` si se desea levantar una excepción y detener procesos posteriores ante cualquier fallo de ejecución SQL.

## 8. Interpretación de Severidades

* **`ERROR`**: Fallo crítico de negocio o integridad (ej. campos críticos nulos, tablas vacías, o inconsistencias de claves). Requiere intervención inmediata del equipo de ingeniería de datos y hace que el runner retorne código de salida `1`.
* **`WARNING`**: Advertencias operativas o lógicas no críticas (ej. completitud territorial/descriptiva parcial o inconsistencias de formatos menores). Se persisten en auditoría y se registran como warning, pero no detienen el flujo si no existen checks `ERROR` fallidos.
* **`INFO`**: Checks informativos o de control general.

### Excepciones documentadas

La tabla `pronabec_colegios_elegibles` puede incluir el registro semántico `ESTUDIOS EN EL EXTRANJERO CONVALIDADOS POR MINEDU`. Este caso no representa un colegio nacional ubicado en una UGEL peruana, por lo que no bloquea el E2E por `ugel` faltante en el check crítico. La completitud territorial/descriptiva asociada sigue auditándose como `WARNING`.

La tabla `pronabec_ubigeo_postulacion` puede incluir registros de postulación en el extranjero. Para los registros con `region` igual a `CHILE`, `COLOMBIA` o `MEXICO`, una `provincia` nula no se considera error porque el registro representa un país extranjero y no una provincia peruana incompleta. `region` y `distrito` siguen siendo obligatorios.

## 9. Relación con Gold y Power BI

* **Relación con Gold final**: Las vistas analíticas Gold deben generarse y consumirse **después** de validar que los checks mínimos sobre Silver pasen sin errores. De lo contrario, las vistas analíticas ocultarían problemas graves de calidad (ej. datos vacíos o duplicados).
* **Relación con Power BI**: Los reportes de Power BI consumen la capa Gold. Sin embargo, ante cualquier discrepancia o comportamiento inesperado de los tableros, el equipo técnico debe revisar el histórico en `audit.data_quality_results` para identificar anomalías de calidad registradas durante la ingesta.

## 10. Limitaciones del Sistema de Calidad

* **Aislamiento en Tests**: La suite de pruebas unitarias locales utiliza mocks del cliente de BigQuery, por lo que no ejecuta sentencias reales en la nube ni requiere credenciales activas de GCP.
* **Reglas de Negocio Complejas**: Los checks están diseñados para consistencia lógica del modelo relacional. No cubren validaciones dinámicas avanzadas de negocio de nivel transaccional profundo.
* **Validación de PES 2025**: Debido a que los reportes de la familia PES 2025 provienen de archivos manuales complejos, es altamente recomendable realizar ejecuciones de validación con datos cargados en un entorno de staging/dry-run en GCP antes de promocionarlos a producción y generar las tablas Gold definitivas.
