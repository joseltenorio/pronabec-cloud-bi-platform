# Schemas analíticos Silver seleccionados de PRONABEC

Este documento detalla los datasets PRONABEC aprobados para la capa Silver, incluyendo criterios de selección, diseño lógico, columnas conservadas, columnas descartadas y reglas mínimas de calidad.

## Objetivo

La capa Silver expone datasets estructurados, tipados y consistentes para análisis, modelado y consumo posterior por Gold, Power BI o componentes analíticos. Este documento registra las decisiones de selección de columnas y datasets PRONABEC que sí aportan valor analítico claro.

## Criterios de selección

Los datasets de Bronze se promueven a Silver cuando cumplen criterios como:

1. **Valor analítico**: contienen variables relevantes para análisis de convocatorias, cobertura territorial, becarios, instituciones o elegibilidad.
2. **Viabilidad relacional**: incluyen llaves o atributos que permiten integrarse con otras entidades, como `id_convocatoria`, `codigo_ubigeo` o atributos institucionales.
3. **Cobertura suficiente**: contienen volumen o vigencia suficiente para construir análisis defendibles.
4. **Claridad semántica**: sus registros pueden interpretarse sin generar métricas engañosas.

## Datasets aprobados

Los siguientes datasets PRONABEC fueron aprobados para Silver:

1. `pronabec_convocatorias`
2. `pronabec_ubigeo_postulacion`
3. `pronabec_beca18_becarios_provincia_2016`
4. `pronabec_becarios_pais_estudio`
5. `pronabec_colegios_elegibles`

---

### 1. pronabec_convocatorias

- **Dataset origen**: `CONVOCATORIA` en Bronze.
- **Propósito analítico**: representa el catálogo de convocatorias de becas, incluyendo programa, modalidad y número de vacantes. Puede funcionar como dimensión de referencia para filtrar, agrupar o relacionar otros datasets.

#### Columnas conservadas

- `source_row_id` (`INT64` / `INTEGER`): identificador técnico para trazabilidad hacia Bronze.
- `id_convocatoria` (`INT64` / `INTEGER`): llave de negocio de la convocatoria.
- `codigo_anual` (`STRING`): identificador anual o de fase, por ejemplo `2021-02`. Se conserva como `STRING` por tener formatos mixtos.
- `descripcion_convocatoria` (`STRING`): descripción de la convocatoria, renombrada desde `description_conv`.
- `modalidad` (`STRING`): modalidad de la beca.
- `programa` (`STRING`): nombre del programa de beca.
- `vacantes` (`INT64` / `INTEGER`): número de vacantes ofrecidas.

#### Columnas descartadas

- `nro_fila`: eliminado por redundancia con `source_row_id`.
- Fechas intermedias, límites de edad, resolución y otros campos no seleccionados para el contrato analítico actual.

#### Reglas de calidad y campos mínimos

Para cargar un registro en esta tabla Silver, se consideran campos mínimos:

- `source_row_id`
- `id_convocatoria`
- `codigo_anual`
- `descripcion_convocatoria`
- `modalidad`
- `programa`
- `vacantes`

Los registros con valores nulos en cualquiera de estos campos no deben cargarse a Silver durante la transformación.

---

### 2. pronabec_ubigeo_postulacion

- **Dataset origen**: `UBIGEO_POSTULACION` en Bronze.
- **Propósito analítico**: funciona como catálogo geográfico de distritos asociados a postulaciones. No mide cantidad de postulantes ni cantidad de becarios.

#### Columnas conservadas

- `source_row_id` (`INT64` / `INTEGER`): identificador técnico para trazabilidad hacia Bronze.
- `region` (`STRING`): nombre del departamento o región.
- `provincia` (`STRING`): nombre de la provincia.
- `distrito` (`STRING`): nombre del distrito.
- `codigo_ubigeo` (`STRING`): código territorial estándar. Se conserva como `STRING` para preservar ceros a la izquierda.

#### Columnas descartadas

- `nro_fila`: eliminado por redundancia con `source_row_id`.

---

### 3. pronabec_beca18_becarios_provincia_2016

- **Dataset origen**: `becarios_provincia` en Bronze.
- **Propósito analítico**: expone una fotografía histórica 2016 de beneficiarios Beca 18 a nivel de detalle provincial. Bronze conserva tanto detalle como agregados; Silver conserva únicamente filas provinciales.

#### Columnas conservadas

- `source_row_id` (`INT64` / `INTEGER`): identificador técnico para trazabilidad hacia Bronze.
- `region` (`STRING`): departamento o región del detalle provincial.
- `provincia` (`STRING`): provincia del detalle Beca 18.
- `becarios_b18_count` (`INT64` / `INTEGER`): cantidad de becarios Beca 18, derivada de `b18_n`.
- `source_snapshot_date` (`DATE`): fecha de carga publicada por PRONABEC, derivada de `fecha_carga`.

#### Columnas descartadas y filtrado

- `nro_fila`: eliminado por redundancia con `source_row_id`.
- `b18_pct` y las columnas de otros programas (`permanencia_*`, `bicentenario_*`, `especial_*`, `ffaa_*`, `vraem_*`, `repec_*`, `internacional_*`, `otros_*`) no forman parte del contrato Silver.
- Las filas agregadas se excluyen de Silver cuando `region` o `provincia` vienen vacías, o cuando `provincia` normalizada inicia con `TOTAL`, incluyendo `TOTAL`, `TOTAL DE BENEFICIARIOS` y `TOTAL GLOBAL`.
- `aggregation_scope` no existe en Silver porque los agregados regionales y nacionales permanecen solo en Bronze.

---

### 4. pronabec_becarios_pais_estudio

- **Dataset origen**: `BECARIOS_PAIS_ESTUDIO` en Bronze.
- **Propósito analítico**: permite analizar becarios por convocatoria, modalidad, país de estudio, institución y sexo. Es uno de los datasets más útiles para observar movilidad académica, distribución institucional y composición por sexo.

#### Columnas conservadas

- `source_row_id` (`INT64` / `INTEGER`): identificador técnico para trazabilidad hacia Bronze.
- `convocatoria` (`STRING`): nombre de la convocatoria. Puede contener texto compuesto, por ejemplo `BECA PERMANENCIA - CONVOCATORIA 2023`.
- `modalidad` (`STRING`): modalidad asociada.
- `pais_estudio` (`STRING`): país de estudio, renombrado desde variantes como `pais de estudio`.
- `institucion` (`STRING`): institución de educación superior.
- `sexo` (`STRING`): sexo reportado del beneficiario.

#### Columnas descartadas

- `nro_fila`: eliminado por redundancia con `source_row_id`.
- Cualquier otro campo no seleccionado en el contrato Silver vigente.

---

### 5. pronabec_colegios_elegibles

- **Dataset origen**: `COLEGIOS_ELEGIBLES` / `colegios_habiles` en Bronze.
- **Propósito analítico**: permite analizar instituciones educativas elegibles para postulación a becas, considerando UGEL, distrito, tipo de gestión y modalidad educativa.

#### Columnas conservadas

- `source_row_id` (`INT64` / `INTEGER`): identificador técnico para trazabilidad hacia Bronze.
- `ugel` (`STRING`): Unidad de Gestión Educativa Local.
- `institucion_educativa` (`STRING`): nombre de la institución educativa.
- `tipo_gestion_colegio` (`STRING`): tipo de gestión del colegio, renombrado desde `tipo_gestion`. Puede contener valores como `Pública - Sector Educación`.
- `nivel_modalidad` (`STRING`): nivel o modalidad educativa, renombrado desde variantes como `nivel_modalida`.
- `forma_atencion` (`STRING`): forma de atención educativa.
- `distrito` (`STRING`): distrito donde se ubica la institución educativa.

#### Columnas descartadas

- `nro_fila`: eliminado por redundancia con `source_row_id`.
- `centro_poblado`: descartado por granularidad excesiva para el modelo actual.
- `direccion`: descartado por baja utilidad analítica y posible variabilidad textual.
- `telefono`: descartado por no aportar a métricas analíticas.
- `fecha_carga`: descartado como campo de negocio; la trazabilidad se maneja con metadata técnica estándar.

---

## Campos técnicos y metadata

Cada tabla Silver incluye la metadata técnica estándar de la plataforma:

- `source_system` (`STRING`): sistema origen, por ejemplo `PRONABEC`.
- `source_dataset` (`STRING`): nombre del dataset de origen.
- `extraction_date` (`DATE`): fecha lógica de extracción.
- `ingestion_timestamp` (`TIMESTAMP`): timestamp físico de escritura en Silver.
- `pipeline_run_id` (`STRING`): identificador único de ejecución del pipeline.

## Decisión sobre columnas técnicas

Se conserva `source_row_id` como identificador técnico único de trazabilidad hacia Bronze.

Se elimina `nro_fila` de Silver porque cumple una función similar y no aporta valor adicional para análisis. Esta decisión reduce redundancia y estandariza la trazabilidad de los datasets PRONABEC.

## Limitaciones conocidas

- Los campos textuales como `tipo_gestion_colegio`, `convocatoria`, `modalidad`, `institucion` o `carrera` pueden contener diferencias de escritura, espacios, mayúsculas, acentos o variaciones semánticas. En este contrato se conservan como texto seleccionado, sin canonización avanzada.
- La normalización textual profunda, corrección de encoding, homologación de nombres y fuzzy matching no forman parte de esta definición de schema.
- Las claves geográficas dependen de `codigo_ubigeo` y nombres territoriales. No se incorporan coordenadas ni geometrías espaciales.
- Los Gold futuros no deben depender de `silver.pronabec_convocatorias_carrera_sede`; `convocatorias_carrera_sede` queda Bronze-only en esta versión.
