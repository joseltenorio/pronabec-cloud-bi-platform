# Diccionario de datos

## Propósito

Este documento define el primer borrador del diccionario de datos del proyecto. Su objetivo es establecer los principales datasets, campos esperados y usos analíticos antes de implementar las capas Bronze, Silver y Gold en Google Cloud.

Este diccionario será actualizado después de ejecutar la primera ingesta real y validar los nombres de columnas, tipos de datos, formatos y reglas de calidad.

## Convenciones

| Convención | Descripción                                     |
| ---------- | ----------------------------------------------- |
| Bronze     | Datos crudos almacenados en Cloud Storage       |
| Silver     | Datos limpios y normalizados en BigQuery        |
| Gold       | Vistas o tablas analíticas listas para Power BI |
| STRING     | Texto                                           |
| INTEGER    | Número entero                                   |
| NUMERIC    | Número decimal                                  |
| DATE       | Fecha                                           |
| TIMESTAMP  | Fecha y hora                                    |
| BOOLEAN    | Valor verdadero/falso                           |

---

# 1. Dataset: notas_becarios

## Descripción

Contiene las notas promedio de becarios por semestre académico. Es una fuente clave para analizar rendimiento académico y construir indicadores de riesgo académico o pérdida de beca.

## Tabla Silver esperada

```text
silver.notas_becarios
```

| Campo          | Tipo esperado | Descripción                                    | Uso analítico                  |
| -------------- | ------------- | ---------------------------------------------- | ------------------------------ |
| nro_fila       | INTEGER       | Número de fila de origen                       | Trazabilidad                   |
| codigo_becario | STRING        | Código anonimizado o identificador del becario | Relación con hechos académicos |
| semestre       | STRING        | Semestre académico registrado                  | Análisis temporal académico    |
| ciclo          | STRING        | Ciclo académico del becario                    | Segmentación académica         |
| nota_promedio  | NUMERIC       | Nota promedio del becario                      | Rendimiento y riesgo académico |
| cargado_en     | TIMESTAMP     | Fecha y hora de carga del registro             | Auditoría                      |

## Reglas de calidad iniciales

- `codigo_becario` no debe ser nulo.
- `nota_promedio` debe estar entre 0 y 20.
- `semestre` no debe ser nulo.
- No deberían existir duplicados exactos por becario, semestre y ciclo.

---

# 2. Dataset: perdida_becas

## Descripción

Contiene registros de becarios que perdieron la beca. Permite analizar deserción, motivos de pérdida, distribución territorial e impacto por convocatoria.

## Tabla Silver esperada

```text
silver.perdida_becas
```

| Campo             | Tipo esperado | Descripción                       | Uso analítico              |
| ----------------- | ------------- | --------------------------------- | -------------------------- |
| nro_fila          | INTEGER       | Número de fila de origen          | Trazabilidad               |
| convocatoria      | STRING        | Convocatoria asociada al becario  | Análisis por convocatoria  |
| tipo_beca         | STRING        | Tipo de beca clasificado          | Segmentación por programa  |
| modalidad_beca    | STRING        | Modalidad de beca                 | Análisis por modalidad     |
| departamento      | STRING        | Departamento asociado             | Análisis territorial       |
| motivo_perdida    | STRING        | Motivo de pérdida de beca         | Análisis causal            |
| tipo_resolucion   | STRING        | Tipo de resolución administrativa | Seguimiento administrativo |
| fecha_resolucion  | DATE          | Fecha de resolución               | Análisis temporal          |
| fecha_inicio_beca | DATE          | Fecha de inicio de la beca        | Análisis de permanencia    |
| tipo_ies          | STRING        | Tipo de institución educativa     | Segmentación institucional |
| institucion       | STRING        | Institución de educación superior | Análisis por institución   |
| sede              | STRING        | Sede educativa                    | Segmentación geográfica    |
| carrera           | STRING        | Carrera del becario               | Análisis académico         |
| sexo              | STRING        | Sexo reportado                    | Segmentación demográfica   |
| cargado_en        | TIMESTAMP     | Fecha y hora de carga             | Auditoría                  |

## Reglas de calidad iniciales

- `convocatoria` no debe ser nula.
- `departamento` debe normalizarse contra catálogo territorial.
- `fecha_resolucion` debe convertirse a formato DATE cuando esté disponible.
- `tipo_beca` debe clasificarse de forma controlada.

---

# 3. Dataset: convocatorias

## Descripción

Contiene información histórica sobre convocatorias de becas, programas, modalidades, vacantes, etapas y fechas relevantes del proceso.

## Tabla Silver esperada

```text
silver.convocatorias
```

| Campo                    | Tipo esperado | Descripción                   | Uso analítico             |
| ------------------------ | ------------- | ----------------------------- | ------------------------- |
| nro_fila                 | INTEGER       | Número de fila de origen      | Trazabilidad              |
| id_convocatoria          | INTEGER       | Identificador de convocatoria | Llave de análisis         |
| codigo_convocatoria      | STRING        | Código de convocatoria        | Relación con hechos       |
| nombre_convocatoria      | STRING        | Nombre de la convocatoria     | Descripción del proceso   |
| nombre_programa          | STRING        | Programa asociado             | Clasificación de beca     |
| tipo_beca                | STRING        | Tipo de beca normalizado      | Segmentación BI           |
| modalidad_beca           | STRING        | Modalidad normalizada         | Segmentación BI           |
| vacantes                 | INTEGER       | Cantidad de vacantes          | Análisis de oferta        |
| etapas                   | INTEGER       | Número de etapas              | Análisis operativo        |
| fecha_inicio_postulacion | DATE          | Inicio de postulación         | Análisis temporal         |
| fecha_fin_postulacion    | DATE          | Fin de postulación            | Análisis temporal         |
| fecha_inicio_evaluacion  | DATE          | Inicio de evaluación          | Análisis operativo        |
| fecha_fin_evaluacion     | DATE          | Fin de evaluación             | Análisis operativo        |
| fecha_inicio_vigencia    | DATE          | Inicio de vigencia            | Análisis normativo        |
| fecha_fin_vigencia       | DATE          | Fin de vigencia               | Análisis normativo        |
| edad_minima              | INTEGER       | Edad mínima permitida         | Requisito de convocatoria |
| edad_maxima              | INTEGER       | Edad máxima permitida         | Requisito de convocatoria |
| resolucion               | STRING        | Resolución asociada           | Trazabilidad normativa    |
| cargado_en               | TIMESTAMP     | Fecha y hora de carga         | Auditoría                 |

## Reglas de calidad iniciales

- `id_convocatoria` debe ser único cuando esté disponible.
- `vacantes` no debe ser negativa.
- Las fechas deben tener formato válido.
- `tipo_beca` debe pertenecer a un catálogo controlado.

---

# 4. Dataset: concepto_pago

## Descripción

Contiene conceptos y subconceptos de subvenciones o pagos asociados a becarios.

## Tabla Silver esperada

```text
silver.concepto_pago
```

| Campo            | Tipo esperado | Descripción                | Uso analítico           |
| ---------------- | ------------- | -------------------------- | ----------------------- |
| nro_fila         | INTEGER       | Número de fila de origen   | Trazabilidad            |
| tipo_subvencion  | STRING        | Tipo de subvención         | Segmentación financiera |
| concepto         | STRING        | Concepto de pago           | Análisis de beneficios  |
| subconcepto      | STRING        | Subconcepto de pago        | Detalle financiero      |
| modalidad        | STRING        | Modalidad asociada         | Segmentación            |
| estado           | STRING        | Estado del concepto        | Control operativo       |
| aplica_descuento | STRING        | Indica si aplica descuento | Reglas financieras      |
| cargado_en       | TIMESTAMP     | Fecha y hora de carga      | Auditoría               |

## Reglas de calidad iniciales

- `concepto` no debe ser nulo.
- `tipo_subvencion` debe normalizarse.
- `estado` debe pertenecer a valores controlados cuando sea posible.

---

# 5. Dataset: becarios_provincia

## Descripción

Contiene cantidades y porcentajes de becarios distribuidos por departamento, provincia y tipo de beca.

## Tabla Silver esperada

```text
silver.becarios_provincia
```

| Campo            | Tipo esperado | Descripción                       | Uso analítico             |
| ---------------- | ------------- | --------------------------------- | ------------------------- |
| nro_fila         | INTEGER       | Número de fila de origen          | Trazabilidad              |
| departamento     | STRING        | Departamento                      | Análisis territorial      |
| provincia        | STRING        | Provincia                         | Análisis territorial      |
| b18_n            | INTEGER       | Cantidad de becarios Beca 18      | Cobertura por programa    |
| b18_pct          | NUMERIC       | Porcentaje de Beca 18             | Participación territorial |
| permanencia_n    | INTEGER       | Cantidad de becarios Permanencia  | Cobertura por programa    |
| permanencia_pct  | NUMERIC       | Porcentaje Permanencia            | Participación territorial |
| bicentenario_n   | INTEGER       | Cantidad de becarios Bicentenario | Cobertura por programa    |
| bicentenario_pct | NUMERIC       | Porcentaje Bicentenario           | Participación territorial |
| cargado_en       | TIMESTAMP     | Fecha y hora de carga             | Auditoría                 |

## Reglas de calidad iniciales

- `departamento` y `provincia` no deben ser nulos.
- Cantidades deben ser mayores o iguales a cero.
- Porcentajes deben estar entre 0 y 100.

---

# 6. Dataset: ubigeo_postulacion

## Descripción

Contiene información geográfica de postulación a becas. Se usará para normalizar el análisis territorial.

## Tabla Silver esperada

```text
silver.ubigeo_postulacion
```

| Campo         | Tipo esperado | Descripción              | Uso analítico        |
| ------------- | ------------- | ------------------------ | -------------------- |
| nro_fila      | INTEGER       | Número de fila de origen | Trazabilidad         |
| codigo_ubigeo | STRING        | Código ubigeo            | Llave territorial    |
| departamento  | STRING        | Departamento             | Dimensión geográfica |
| provincia     | STRING        | Provincia                | Dimensión geográfica |
| distrito      | STRING        | Distrito                 | Dimensión geográfica |
| cargado_en    | TIMESTAMP     | Fecha y hora de carga    | Auditoría            |

## Reglas de calidad iniciales

- `codigo_ubigeo` debe mantener ceros a la izquierda si aplica.
- Departamento, provincia y distrito deben normalizarse.
- Deben eliminarse duplicados exactos.

---

# 7. Dataset: periodos_academicos

## Descripción

Contiene periodos académicos asociados a becarios. Se usará como apoyo para la dimensión tiempo académica.

## Tabla Silver esperada

```text
silver.periodos_academicos
```

| Campo            | Tipo esperado | Descripción              | Uso analítico         |
| ---------------- | ------------- | ------------------------ | --------------------- |
| nro_fila         | INTEGER       | Número de fila de origen | Trazabilidad          |
| anio             | INTEGER       | Año académico            | Análisis temporal     |
| mes_numero       | INTEGER       | Número de mes            | Dimensión tiempo      |
| periodo_completo | STRING        | Periodo completo         | Segmentación temporal |
| mes_nombre       | STRING        | Nombre del mes           | Visualización         |
| cargado_en       | TIMESTAMP     | Fecha y hora de carga    | Auditoría             |

## Reglas de calidad iniciales

- `anio` debe estar en un rango válido.
- `mes_numero` debe estar entre 1 y 12.
- `periodo_completo` debe normalizarse.

---

# 8. Dataset: presupuesto_mef

## Descripción

Contiene información presupuestal pública relacionada con PRONABEC. Permite analizar evolución presupuestal, ejecución y avance financiero.

## Tabla Silver esperada

```text
silver.presupuesto_mef
```

| Campo              | Tipo esperado | Descripción                           | Uso analítico               |
| ------------------ | ------------- | ------------------------------------- | --------------------------- |
| ano                | INTEGER       | Año fiscal                            | Análisis temporal           |
| ejecutora_nombre   | STRING        | Nombre de la ejecutora                | Identificación presupuestal |
| pia                | NUMERIC       | Presupuesto Institucional de Apertura | Presupuesto inicial         |
| pim                | NUMERIC       | Presupuesto Institucional Modificado  | Presupuesto actualizado     |
| certificacion      | NUMERIC       | Monto certificado                     | Control presupuestal        |
| compromiso_anual   | NUMERIC       | Compromiso anual                      | Control presupuestal        |
| compromiso_mensual | NUMERIC       | Compromiso mensual                    | Control presupuestal        |
| devengado          | NUMERIC       | Monto devengado                       | Ejecución presupuestal      |
| girado             | NUMERIC       | Monto girado                          | Ejecución financiera        |
| avance_porcentaje  | NUMERIC       | Porcentaje de avance presupuestal     | KPI financiero              |
| saldo_no_ejecutado | NUMERIC       | Diferencia entre PIM y devengado      | KPI financiero              |
| cargado_en         | TIMESTAMP     | Fecha y hora de carga                 | Auditoría                   |

## Reglas de calidad iniciales

- `ano` no debe ser nulo.
- `pia`, `pim`, `devengado` y `girado` deben ser mayores o iguales a cero.
- `avance_porcentaje` debe estar entre 0 y 100.
- `saldo_no_ejecutado` se calcula como `pim - devengado`.

---

# 9. Capas Gold esperadas

Las vistas Gold estarán orientadas al consumo en Power BI.

| Vista Gold                         | Descripción                            | Uso en Power BI              |
| ---------------------------------- | -------------------------------------- | ---------------------------- |
| gold.vw_resumen_ejecutivo          | KPIs principales del proyecto          | Página Resumen Ejecutivo     |
| gold.vw_presupuesto_mef_anual      | Evolución anual del presupuesto        | Página Presupuesto           |
| gold.vw_becarios_por_departamento  | Cobertura territorial de becarios      | Página Cobertura Territorial |
| gold.vw_notas_por_semestre         | Rendimiento académico por periodo      | Página Rendimiento           |
| gold.vw_desercion_por_convocatoria | Pérdida de becas por convocatoria      | Página Deserción             |
| gold.vw_presupuesto_vs_becas       | Relación entre presupuesto y cobertura | Página Análisis Ejecutivo    |

---

# 10. Actualización futura del diccionario

Este diccionario será actualizado después de la primera ejecución de ingesta real. Las siguientes mejoras previstas son:

- Confirmar nombres reales de columnas.
- Confirmar tipos de datos finales en BigQuery.
- Agregar ejemplos de valores.
- Agregar reglas de calidad definitivas.
- Identificar llaves primarias y llaves de relación.
- Documentar tablas Gold definitivas.
- Documentar campos usados por Power BI.
