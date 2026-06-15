# PRONABEC Silver Review Candidates

This document details candidate PRONABEC datasets that are promoted to the Silver layer with specific warnings, as they contain potentially useful information but suffer from interpretation risks or restricted scope.

## Candidate Selection Criteria

A dataset is classified as a "Candidate" (under review) when:
1. **Limited Temporal or Geographical Coverage**: The dataset represents a historical snapshot or specific subset rather than a complete, ongoing registry.
2. **Aggregated Formats**: The dataset mixes multiple levels of aggregation (e.g. details, regional totals, national totals) in the same file.
3. **Restricted Scope**: Interpretation of its columns requires business domain context to avoid misleading conclusions.

Promoting these datasets to Silver ensures they have structured contracts, but they should not be treated as central analytical facts without reading the associated warnings.

## Candidate Datasets

The following two datasets are classified as candidates:
1. `pronabec_beca18_becarios_provincia_2016`
2. `pronabec_convocatorias_carrera_sede`

---

### 1. pronabec_beca18_becarios_provincia_2016

* **Source Dataset**: `BECARIOS_PROVINCIA` (Bronze)
* **Rationale for Name Change**: The original name (`becarios_provincia`) is too generic, suggesting it covers all years or programs. Analysis reveals it represents a specific historical snapshot of Beca 18 beneficiaries by province (circa 2016). The new name clearly documents its historical scope and program restriction.

#### Columns Kept
* `source_row_id` (INT64 / INTEGER): Technical identifier for lineage.
* `region` (STRING): Department name.
* `provincia` (STRING): Province name.
* `becarios_b18_count` (INT64 / INTEGER): Number of Beca 18 beneficiaries (renamed from `b18_n` / `b18n`).
* `aggregation_scope` (STRING): Scope indicator (e.g., `PROVINCIA`, `REGION_TOTAL`, `NATIONAL_TOTAL`) to identify aggregated rows.

#### Columns Discarded
* `nro_fila`: Removed.
* `b18_pct` / `b18pct`: Dropped because percentages are easily computed from totals in the reporting/BI layer.
* Other program columns (e.g., `permanencia_n`, `ffaa_n`) that had sparse, incomplete, or corrupted records.

#### Risk of Interpretation & Allowed/Restricted Use
* **Allowed Use**: Referencial distribution of Beca 18 scholarship holders by province for the historical snapshot.
* **Restricted Use**: 
  - Do NOT use this table as a complete or active series of all scholarship holders across years.
  - Do NOT compare these totals directly with current year figures as a trend.
  - Do NOT ignore the `aggregation_scope` field, as sum operations will double-count records if totals/subtotals are not filtered.

---

### 2. pronabec_convocatorias_carrera_sede

* **Source Dataset**: `ConvocatoriaPorCarreraSede` (Bronze)
* **Rationale for Promotion**: It lists the eligible offer of educational institutions, campuses, and academic programs by convocatoria. Highly valuable to analyze the educational supply, but must not be confused with actual student enrollment.

#### Columns Kept
* `source_row_id` (INT64 / INTEGER): Technical identifier for lineage.
* `id_convocatoria` (INT64 / INTEGER): Business key to link with `pronabec_convocatorias`.
* `pais_origen` (STRING): Country of the university/institute.
* `nivel_educativo` (STRING): Target education level (e.g. Pregrado).
* `tipo_institucion` (STRING): Institution type.
* `sede` (STRING): Campus name.
* `institucion` (STRING): Institution name.
* `carrera` (STRING): Eligible program name.
* `gestion_ies` (STRING): Management type (Pública/Privada) of the Higher Education Institution (IES) (renamed from `gestion`).
* `ruc` (STRING): Standard business registry number of the IES. Kept as `STRING`.

#### Columns Discarded
* `nro_fila`: Removed.
* `resolucion`, `abreviatura`, `region`, `web`, `representante`, `telefono`, `email`, `fecha_carga`: Removed to focus strictly on program-institution availability.

#### Risk of Interpretation & Allowed/Restricted Use
* **Allowed Use**: Analyzing the eligible offer of programs, locations, and institutions by convocatoria.
* **Restricted Use**: 
  - Do NOT use this table to count active becarios or enrollments. It is a catalog of *eligible offers*, not *actual beneficiaries*.
  - Do NOT assume the list of institutions is exhaustive for all national universities.

---

## Technical Audit & Metadata Fields

These tables contain the standard platform metadata columns:
- `source_system` (STRING): "PRONABEC"
- `source_dataset` (STRING): Origin dataset name.
- `extraction_date` (DATE): Logical extraction date.
- `ingestion_timestamp` (TIMESTAMP): Physical timestamp when written to Silver.
- `pipeline_run_id` (STRING): Unique run ID of the pipeline execution.
