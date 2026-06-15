# PRONABEC Selected Silver Analytical Schemas

This document details the selected PRONABEC datasets approved for promotion to the Silver layer, including the criteria for selection, the schema design, column selection rationale, and quality expectations.

## Objective

The objective of the Silver layer is to expose clean, structured, and relationally consistent datasets. This document records the architectural decisions for selecting high-value analytical datasets from the raw Bronze layer and designing their corresponding Silver schemas.

## Selection Criteria

Datasets from the Bronze layer are selected for promotion to the Silver layer based on the following criteria:
1. **Analytical Value**: The dataset contains key business variables (e.g. geographic distributions, scholarship details, institutional parameters) that directly support analytical models or business intelligence dashboards.
2. **Relational Feasibility**: The dataset contains reliable business keys (e.g. `id_convocatoria`, `codigo_ubigeo`) that allow integration with other tables.
3. **Data Completeness and Coverage**: The dataset contains sufficient historical and current records to provide meaningful insights.

## Approved Datasets

The following four PRONABEC datasets have been approved for the Silver layer:
1. `pronabec_convocatorias`
2. `pronabec_ubigeo_postulacion`
3. `pronabec_becarios_pais_estudio`
4. `pronabec_colegios_elegibles`

---

### 1. pronabec_convocatorias

* **Source Dataset**: `CONVOCATORIA` (Bronze)
* **Analytical Purpose**: Represents the catalog of scholarship convocatorias, including program type, modality, and the number of offered vacantes. It serves as a dimension to filter and slice other fact datasets.

#### Columns Kept
* `source_row_id` (INT64 / INTEGER): Technical identifier for lineage and audit trace back to Bronze.
* `id_convocatoria` (INT64 / INTEGER): Unique business key for the convocatoria.
* `codigo_anual` (STRING): Annual identifier representing the year and phase (e.g., `2021-02`). Kept as `STRING` to support mixed alphanumeric formats.
* `descripcion_convocatoria` (STRING): Detailed description of the convocatoria (renamed from `description_conv`).
* `modalidad` (STRING): Modality of the scholarship.
* `programa` (STRING): Scholarship program name.
* `vacantes` (INT64 / INTEGER): Number of vacancies offered.

#### Columns Discarded
* `nro_fila`: Removed in favor of `source_row_id` to eliminate technical redundancy.
* Other non-essential fields (e.g. intermediate dates) to reduce column clutter.

#### Quality Rules & Minimum Fields
The following fields are strictly required for a row to be loaded:
- `source_row_id`
- `id_convocatoria`
- `codigo_anual`
- `descripcion_convocatoria`
- `modalidad`
- `programa`
- `vacantes`
Rows with null values in any of these columns will be filtered out in the transformation layer.

---

### 2. pronabec_ubigeo_postulacion

* **Source Dataset**: `UBIGEO_POSTULACION` (Bronze)
* **Analytical Purpose**: Serves as a geographical district catalog associated with postulations, mapping regions, provinces, and districts to standard ubigeo codes. It does not measure applicant or scholarship counts directly.

#### Columns Kept
* `source_row_id` (INT64 / INTEGER): Technical identifier for lineage.
* `region` (STRING): Department or region name.
* `provincia` (STRING): Province name.
* `distrito` (STRING): District name.
* `codigo_ubigeo` (STRING): Standard 6-digit territorial key (e.g., `150101` for Lima). Kept as `STRING` to preserve leading zeros.

#### Columns Discarded
* `nro_fila`: Removed.

---

### 3. pronabec_becarios_pais_estudio

* **Source Dataset**: `BECARIOS_PAIS_ESTUDIO` (Bronze)
* **Analytical Purpose**: Tracks the volume of active scholarship holders by their country of study, modality, institution, and sex. Extremely valuable for international academic mobility analysis.

#### Columns Kept
* `source_row_id` (INT64 / INTEGER): Technical identifier for lineage.
* `convocatoria` (STRING): Name of the convocatoria, which can contain compound text (e.g., `BECA PERMANENCIA - CONVOCATORIA 2023`). Kept as raw text.
* `modalidad` (STRING): Modality name.
* `pais_estudio` (STRING): Target country of studies (renamed from `pais de estudio` or other variants).
* `institucion` (STRING): Name of the higher education institution.
* `sexo` (STRING): Gender of the beneficiary.

#### Columns Discarded
* `nro_fila`: Removed.

---

### 4. pronabec_colegios_elegibles

* **Source Dataset**: `COLEGIOS_ELEGIBLES` / `COLEGIOS_HABIBLES` (Bronze)
* **Analytical Purpose**: Tracks educational institutions elegible for PRONABEC scholarship application, categorizing them by local UGEL, management type (public/private), and level/modality.

#### Columns Kept
* `source_row_id` (INT64 / INTEGER): Technical identifier for lineage.
* `ugel` (STRING): Local educational management unit.
* `institucion_educativa` (STRING): Name of the school.
* `tipo_gestion_colegio` (STRING): Management type (renamed from `tipo_gestion`), e.g., `Pública - Sector Educación`.
* `nivel_modalidad` (STRING): Level and modality (renamed from `nivel_modalida` or variants).
* `forma_atencion` (STRING): Attention format.
* `distrito` (STRING): School's district.

#### Columns Discarded
* `nro_fila`: Removed.
* `centro_poblado`, `direccion`, `telefono`, `fecha_carga`: Dropped because they lack direct analytical interest.

---

## Technical Audit & Metadata Fields

Each Silver table includes the standard technical metadata fields for consistency across the data platform:
- `source_system` (STRING): "PRONABEC"
- `source_dataset` (STRING): Origin dataset name.
- `extraction_date` (DATE): Logical extraction date.
- `ingestion_timestamp` (TIMESTAMP): Physical timestamp when written to Silver.
- `pipeline_run_id` (STRING): Unique run ID of the pipeline execution.

## Decisions on Technical Columns

A key architectural decision was made to drop `nro_fila` in favor of `source_row_id`. In Bronze, both fields exist, but in Silver, `source_row_id` acts as the single standard lineage reference. 

## Known Limitations

- **Text Cleanliness**: Fields such as `tipo_gestion_colegio` and `convocatoria` contain raw, mixed-case text with trailing spaces or codes. This will be normalized in future pipeline steps, but the schema keeps them as raw strings in Silver.
- **Geographical Keys**: There are no coordinates or spatial data in these schemas; regional groupings rely strictly on the `codigo_ubigeo` and textual names.
