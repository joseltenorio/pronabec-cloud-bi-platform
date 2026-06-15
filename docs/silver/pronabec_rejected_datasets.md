# PRONABEC Bronze-Only (Rejected) Datasets

This document details the PRONABEC datasets that have been excluded from promotion to the Silver layer and are classified as `BRONZE_ONLY`. 

## Objective

To maintain a clean and reliable analytical repository in Silver, datasets that contain low-value, sparse, or isolated information are rejected. This prevents confusion, prevents the ingestion of uninterpretable data, and ensures the platform is cost-efficient.

## Rejection Criteria

A dataset is classified as `BRONZE_ONLY` and rejected for Silver if it meets any of the following conditions:
1. **Lack of Relational Keys**: The dataset cannot be linked to other core entities (like convocatorias or becarios).
2. **Poor Data Quality or Sparsity**: The dataset is incomplete, has low records count, or has limited temporal coverage.
3. **No Direct Analytical Metric**: The dataset contains metadata or configuration data that does not drive analytical decisions or dashboards.
4. **Interpretation Risk**: Using the dataset could lead to misleading conclusions because the underlying data collection is flawed or biased.

## Rejected Datasets

The following five PRONABEC datasets have been rejected and remain strictly in the Bronze layer:

---

### 1. concepto_pago

* **Decisión**: `BRONZE_ONLY`
* **Technical Reason**: This dataset contains lists of payment concepts and subconcepts, but it does not map them to actual monetary values, student IDs, or convocatorias. It lacks business keys, making it impossible to integrate into any spend or cost dashboard.

---

### 2. notas_becarios

* **Decisión**: `BRONZE_ONLY`
* **Technical Reason**: Although academic performance is a high-value concept, the public data available in this dataset is highly sparse, historical, and lacks context (e.g. grading scales by university, career durations, etc.). Loading this data into Silver would create an illusion of a full performance tracker, which cannot be backed by the data itself.

---

### 3. periodos_academicos

* **Decisión**: `BRONZE_ONLY`
* **Technical Reason**: It functions as a lookup table of academic terms, years, and months. However, it does not add any relational value since convocatorias and other datasets already use standard date types.

---

### 4. nota_promedio_postulante_region

* **Decisión**: `BRONZE_ONLY`
* **Technical Reason**: This dataset is extremely small, incomplete, and represents a static historical snapshot with low statistical significance. It cannot be used to analyze regional educational levels or student quality.

---

### 5. perdida_becas

* **Decisión**: `BRONZE_ONLY`
* **Technical Reason**: The records representing scholarship losses or desertion are incomplete and lack proper denominators (e.g. total enrollment per year). Without a denominator, it is impossible to compute retention or desertion rates. Using this raw count in dashboards would create distorted views of scholarship performance.

---

## Data Preservation in Bronze

Excluding these datasets from Silver does **not** mean they are deleted from the platform. Their extractors remain fully functional, and raw files (`data_raw.json` and `data.jsonl`) will continue to be stored in the GCS Bronze layer under the `extraction_date=YYYY-MM-DD/` prefix for historical trace and audit purposes.
