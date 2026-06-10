# Diccionario de datos

## Propósito

Este documento define el diccionario de datos inicial de Project Cloud BI Platform. Su objetivo es establecer los principales datasets, campos esperados, tipos de datos, reglas de calidad y usos analíticos que serán utilizados en las capas Silver y Gold del proyecto.

El diccionario se enfoca en la estructura analítica esperada para BigQuery. No documenta logs de ejecución ni resultados operativos del profiling; esos elementos pertenecen a la documentación de exploración de fuentes.

## Alcance

Este diccionario cubre los datasets públicos considerados para el análisis de PRONABEC y presupuesto público:

- Notas de becarios.
- Pérdida de becas.
- Convocatorias.
- Conceptos de pago.
- Becarios por provincia.
- Ubigeo de postulación.
- Periodos académicos.
- Presupuesto MEF.
- Vistas Gold esperadas para Power BI.

## Convenciones

| Convención | Descripción                                                               |
| ---------- | ------------------------------------------------------------------------- |
| Bronze     | Datos crudos y normalizados estructuralmente almacenados en Cloud Storage |
| Silver     | Datos limpios, tipados, normalizados y validados en BigQuery              |
| Gold       | Vistas o tablas analíticas listas para consumo en Power BI                |
| STRING     | Texto                                                                     |
| INTEGER    | Número entero                                                             |
| NUMERIC    | Número decimal                                                            |
| DATE       | Fecha                                                                     |
| TIMESTAMP  | Fecha y hora                                                              |
| BOOLEAN    | Valor verdadero/falso                                                     |

## Criterios de modelado aplicados

Los nombres de campos en Silver priorizan claridad analítica, trazabilidad y compatibilidad con consultas SQL en BigQuery.

Cuando una columna proviene directamente de la fuente, se conserva su significado original. Cuando un campo requiere clasificación, cálculo o enriquecimiento, se documenta como campo derivado y debe implementarse en una transformación Silver enriquecida o en una vista Gold.

Criterios aplicados:

- Los identificadores técnicos de origen se conservan cuando aportan trazabilidad.
- Los campos numéricos que llegan como texto deben convertirse explícitamente en Silver.
- Los valores decimales con coma deben normalizarse antes de cargarse como `NUMERIC`.
- Los campos de fecha deben estandarizarse según su granularidad real: fecha completa, año-mes o timestamp.
- Los códigos territoriales, como ubigeo, deben conservarse como `STRING` para evitar pérdida de ceros iniciales.
- Los campos con alta nulidad no deben definirse como obligatorios.
- Los campos derivados, como clasificaciones de tipo de beca o modalidad normalizada, no deben mezclarse con columnas directas de la fuente si todavía no existe una regla de derivación definida.
- Las tablas Silver deben enfocarse en datos limpios, tipados y normalizados.
- Las vistas Gold deben enfocarse en indicadores, agregaciones y consumo analítico.

---

# 1. Dataset: notas_becarios

## Descripción

Contiene notas promedio de becarios por semestre académico. Es una fuente clave para analizar rendimiento académico y construir indicadores de riesgo académico o pérdida de beca.

## Tabla Silver esperada

```text
silver.notas_becarios
```

| Campo          | Tipo esperado | Descripción                                        | Uso analítico                  |
| -------------- | ------------- | -------------------------------------------------- | ------------------------------ |
| source_row_id  | STRING        | Identificador técnico de la fila en la fuente      | Trazabilidad                   |
| nro_fila       | INTEGER       | Número de fila de origen                           | Trazabilidad                   |
| codigo_becario | STRING        | Código o identificador del becario en la fuente    | Relación con hechos académicos |
| semestre       | STRING        | Semestre académico registrado                      | Análisis temporal académico    |
| ciclo          | STRING        | Ciclo académico del becario                        | Segmentación académica         |
| nota_promedio  | NUMERIC       | Nota promedio del becario                          | Rendimiento y riesgo académico |
| fecha_carga    | TIMESTAMP     | Fecha y hora de extracción o carga desde la fuente | Auditoría                      |

## Reglas de calidad iniciales

- `codigo_becario` no debe ser nulo.
- `semestre` no debe ser nulo.
- `nota_promedio` debe convertirse desde texto con coma decimal a `NUMERIC`.
- `nota_promedio` debe validarse en el rango 0 a 20.
- No deberían existir duplicados exactos por `codigo_becario`, `semestre` y `ciclo`.
- `fecha_carga` debe convertirse a `TIMESTAMP` cuando el formato lo permita.

## Consideraciones de transformación

- La fuente puede entregar valores numéricos con coma decimal.
- La conversión de `nota_promedio` debe ocurrir en Silver, no en Bronze.
- Este dataset será una fuente principal para análisis académico y una posible fase de BigQuery ML.

---

# 2. Dataset: perdida_becas

## Descripción

Contiene registros de becarios que perdieron la beca. Permite analizar deserción, motivos de pérdida, distribución territorial, instituciones asociadas y comportamiento por convocatoria.

## Tabla Silver esperada

```text
silver.perdida_becas
```

| Campo             | Tipo esperado | Descripción                                                      | Uso analítico                            |
| ----------------- | ------------- | ---------------------------------------------------------------- | ---------------------------------------- |
| source_row_id     | STRING        | Identificador técnico de la fila en la fuente                    | Trazabilidad                             |
| nro_fila          | INTEGER       | Número de fila de origen                                         | Trazabilidad                             |
| convocatoria      | STRING        | Convocatoria asociada al registro                                | Análisis por convocatoria                |
| departamento      | STRING        | Departamento, región o valor territorial reportado por la fuente | Análisis territorial                     |
| motivo_perdida    | STRING        | Motivo de pérdida de beca                                        | Análisis causal                          |
| tipo_resolucion   | STRING        | Tipo de resolución administrativa, cuando exista                 | Seguimiento administrativo               |
| fecha_resolucion  | DATE          | Fecha o periodo de resolución, cuando esté disponible            | Análisis temporal                        |
| fecha_inicio_beca | DATE          | Fecha o periodo de inicio de la beca                             | Análisis de permanencia                  |
| tipo_ies          | STRING        | Tipo de institución educativa superior                           | Segmentación institucional               |
| institucion       | STRING        | Institución educativa superior                                   | Análisis por institución                 |
| sede              | STRING        | Sede educativa                                                   | Segmentación institucional y territorial |
| carrera           | STRING        | Carrera del becario                                              | Análisis académico                       |
| sexo              | STRING        | Sexo reportado en la fuente                                      | Segmentación demográfica                 |
| fecha_carga       | TIMESTAMP     | Fecha y hora de extracción o carga desde la fuente               | Auditoría                                |

## Reglas de calidad iniciales

- `convocatoria` no debe ser nula.
- `motivo_perdida` no debe ser nulo.
- `departamento` debe normalizarse con cuidado, considerando que puede contener departamentos peruanos o valores internacionales.
- `tipo_resolucion` no debe tratarse como obligatorio.
- `fecha_resolucion` debe convertirse a formato `DATE` cuando esté disponible, pero no debe tratarse como campo obligatorio.
- `fecha_inicio_beca` debe estandarizarse según su granularidad real.
- La clasificación de tipo de beca o modalidad debe tratarse como enriquecimiento derivado, no como campo directo de esta tabla base.

## Consideraciones de transformación

- `tipo_beca` y `modalidad_beca` no se consideran columnas directas de esta tabla Silver base.
- La clasificación de beca puede derivarse posteriormente desde `convocatoria`, `modalidad` o una tabla de referencia.
- Algunos campos administrativos pueden venir vacíos y deben conservarse como nulos válidos.

---

# 3. Dataset: convocatorias

## Descripción

Contiene información histórica sobre convocatorias de becas, programas, modalidades, vacantes, etapas y fechas relevantes del proceso.

## Tabla Silver esperada

```text
silver.convocatorias
```

| Campo                    | Tipo esperado | Descripción                                        | Uso analítico              |
| ------------------------ | ------------- | -------------------------------------------------- | -------------------------- |
| source_row_id            | STRING        | Identificador técnico de la fila en la fuente      | Trazabilidad               |
| nro_fila                 | INTEGER       | Número de fila de origen                           | Trazabilidad               |
| id_convocatoria          | INTEGER       | Identificador de convocatoria                      | Llave de análisis          |
| codigo_convocatoria      | STRING        | Código de convocatoria                             | Relación con hechos        |
| nombre_convocatoria      | STRING        | Nombre de la convocatoria                          | Descripción del proceso    |
| nombre_programa          | STRING        | Programa asociado a la convocatoria                | Clasificación analítica    |
| modalidad                | STRING        | Modalidad reportada por la fuente                  | Segmentación por modalidad |
| vacantes                 | INTEGER       | Cantidad de vacantes                               | Análisis de oferta         |
| etapas                   | INTEGER       | Número de etapas                                   | Análisis operativo         |
| fecha_fin_convocatoria   | DATE          | Fecha de fin de convocatoria                       | Análisis temporal          |
| fecha_inicio_postulacion | DATE          | Fecha de inicio de postulación                     | Análisis temporal          |
| fecha_fin_postulacion    | DATE          | Fecha de fin de postulación                        | Análisis temporal          |
| fecha_inicio_evaluacion  | DATE          | Fecha de inicio de evaluación                      | Análisis operativo         |
| fecha_fin_evaluacion     | DATE          | Fecha de fin de evaluación                         | Análisis operativo         |
| fecha_inicio_vigencia    | DATE          | Fecha de inicio de vigencia                        | Análisis normativo         |
| fecha_fin_vigencia       | DATE          | Fecha de fin de vigencia                           | Análisis normativo         |
| edad_minima              | INTEGER       | Edad mínima permitida                              | Requisito de convocatoria  |
| edad_maxima              | INTEGER       | Edad máxima permitida                              | Requisito de convocatoria  |
| resolucion               | STRING        | Resolución asociada                                | Trazabilidad normativa     |
| fecha_carga              | TIMESTAMP     | Fecha y hora de extracción o carga desde la fuente | Auditoría                  |

## Reglas de calidad iniciales

- `id_convocatoria` debe ser único cuando esté disponible.
- `codigo_convocatoria` no debe ser nulo cuando esté disponible.
- `vacantes` debe ser mayor o igual a cero.
- `etapas` debe ser mayor o igual a cero.
- `edad_minima` y `edad_maxima` deben ser mayores o iguales a cero cuando estén disponibles.
- `edad_maxima` no debe ser menor que `edad_minima` cuando ambos campos existan.
- Las fechas deben convertirse a `DATE` cuando el formato sea válido.
- `modalidad` debe conservarse como valor fuente en Silver.
- La clasificación `tipo_beca_normalizado` puede derivarse posteriormente en una vista Gold o tabla enriquecida.

## Consideraciones de transformación

- `tipo_beca` no se considera campo directo de esta tabla base si no viene explícitamente desde la fuente.
- Los campos de fecha deben manejarse con tolerancia a nulos.
- La clasificación por programa, beca o modalidad debe documentarse cuando se implemente la regla de negocio.

---

# 4. Dataset: concepto_pago

## Descripción

Contiene conceptos y subconceptos de subvenciones o pagos asociados a becarios. Permite analizar beneficios, tipos de subvención y reglas financieras relacionadas con los programas de becas.

## Tabla Silver esperada

```text
silver.concepto_pago
```

| Campo            | Tipo esperado | Descripción                                                  | Uso analítico              |
| ---------------- | ------------- | ------------------------------------------------------------ | -------------------------- |
| source_row_id    | STRING        | Identificador técnico de la fila en la fuente                | Trazabilidad               |
| nro_fila         | INTEGER       | Número de fila de origen                                     | Trazabilidad               |
| tipo_subvencion  | STRING        | Tipo de subvención                                           | Segmentación financiera    |
| concepto         | STRING        | Concepto de pago                                             | Análisis de beneficios     |
| subconcepto      | STRING        | Subconcepto de pago                                          | Detalle financiero         |
| modalidad        | STRING        | Modalidad asociada                                           | Segmentación por modalidad |
| estado           | STRING        | Estado del concepto                                          | Control operativo          |
| aplica_descuento | STRING        | Indicador de aplicación de descuento reportado por la fuente | Reglas financieras         |
| fecha_carga      | TIMESTAMP     | Fecha y hora de extracción o carga desde la fuente           | Auditoría                  |

## Reglas de calidad iniciales

- `concepto` no debe ser nulo.
- `tipo_subvencion` debe normalizarse como texto controlado.
- `estado` debe pertenecer a valores controlados cuando sea posible.
- `aplica_descuento` puede mantenerse como `STRING` en Silver si la fuente no entrega un booleano consistente.
- `fecha_carga` debe convertirse a `TIMESTAMP` cuando el formato lo permita.

## Consideraciones de transformación

- `aplica_descuento` puede derivarse posteriormente a `BOOLEAN` si se confirma una regla estable de conversión.
- `modalidad` debe conservarse como valor fuente en Silver.
- La normalización de conceptos debe realizarse sin perder el texto original.

---

# 5. Dataset: becarios_provincia

## Descripción

Contiene cantidades y porcentajes de becarios distribuidos por departamento, provincia y tipo de beca. Es una fuente clave para análisis territorial y cobertura por programa.

## Tabla Silver esperada

```text
silver.becarios_provincia
```

| Campo             | Tipo esperado | Descripción                                        | Uso analítico             |
| ----------------- | ------------- | -------------------------------------------------- | ------------------------- |
| source_row_id     | STRING        | Identificador técnico de la fila en la fuente      | Trazabilidad              |
| nro_fila          | INTEGER       | Número de fila de origen                           | Trazabilidad              |
| departamento      | STRING        | Departamento                                       | Análisis territorial      |
| provincia         | STRING        | Provincia                                          | Análisis territorial      |
| b18_n             | INTEGER       | Cantidad de becarios Beca 18                       | Cobertura por programa    |
| b18_pct           | NUMERIC       | Porcentaje de Beca 18                              | Participación territorial |
| permanencia_n     | INTEGER       | Cantidad de becarios Permanencia                   | Cobertura por programa    |
| permanencia_pct   | NUMERIC       | Porcentaje Permanencia                             | Participación territorial |
| bicentenario_n    | INTEGER       | Cantidad de becarios Bicentenario                  | Cobertura por programa    |
| bicentenario_pct  | NUMERIC       | Porcentaje Bicentenario                            | Participación territorial |
| especial_n        | INTEGER       | Cantidad de becarios de becas especiales           | Cobertura por programa    |
| especial_pct      | NUMERIC       | Porcentaje de becas especiales                     | Participación territorial |
| ffaa_n            | INTEGER       | Cantidad de becarios FF.AA.                        | Cobertura por programa    |
| ffaa_pct          | NUMERIC       | Porcentaje FF.AA.                                  | Participación territorial |
| vraem_n           | INTEGER       | Cantidad de becarios VRAEM                         | Cobertura por programa    |
| vraem_pct         | NUMERIC       | Porcentaje VRAEM                                   | Participación territorial |
| repec_n           | INTEGER       | Cantidad de becarios REPEC                         | Cobertura por programa    |
| repec_pct         | NUMERIC       | Porcentaje REPEC                                   | Participación territorial |
| internacional_n   | INTEGER       | Cantidad de becarios Internacional                 | Cobertura por programa    |
| internacional_pct | NUMERIC       | Porcentaje Internacional                           | Participación territorial |
| otros_n           | INTEGER       | Cantidad de becarios de otros programas            | Cobertura por programa    |
| otros_pct         | NUMERIC       | Porcentaje de otros programas                      | Participación territorial |
| fecha_carga       | TIMESTAMP     | Fecha y hora de extracción o carga desde la fuente | Auditoría                 |

## Reglas de calidad iniciales

- `departamento` no debe ser nulo.
- `provincia` no debe ser nula.
- Las cantidades deben ser mayores o iguales a cero.
- Los porcentajes deben convertirse desde texto con coma decimal a `NUMERIC`.
- Los porcentajes deben validarse en el rango 0 a 100.
- `fecha_carga` debe convertirse a `TIMESTAMP` cuando el formato lo permita.

## Consideraciones de transformación

- Los campos terminados en `_n` representan cantidades.
- Los campos terminados en `_pct` representan porcentajes.
- En Gold puede evaluarse una versión normalizada en formato largo con columnas `tipo_beca`, `cantidad` y `porcentaje`.

---

# 6. Dataset: ubigeo_postulacion

## Descripción

Contiene información geográfica de postulación a becas. Se usará para normalizar el análisis territorial y construir dimensiones geográficas.

## Tabla Silver esperada

```text
silver.ubigeo_postulacion
```

| Campo         | Tipo esperado | Descripción                                        | Uso analítico        |
| ------------- | ------------- | -------------------------------------------------- | -------------------- |
| source_row_id | STRING        | Identificador técnico de la fila en la fuente      | Trazabilidad         |
| nro_fila      | INTEGER       | Número de fila de origen                           | Trazabilidad         |
| codigo_ubigeo | STRING        | Código ubigeo                                      | Llave territorial    |
| departamento  | STRING        | Departamento                                       | Dimensión geográfica |
| provincia     | STRING        | Provincia                                          | Dimensión geográfica |
| distrito      | STRING        | Distrito                                           | Dimensión geográfica |
| fecha_carga   | TIMESTAMP     | Fecha y hora de extracción o carga desde la fuente | Auditoría            |

## Reglas de calidad iniciales

- `codigo_ubigeo` no debe ser nulo.
- `codigo_ubigeo` debe conservarse como `STRING` para evitar pérdida de ceros iniciales.
- `departamento`, `provincia` y `distrito` deben normalizarse como texto.
- Deben eliminarse duplicados exactos cuando no aporten trazabilidad.
- `fecha_carga` debe convertirse a `TIMESTAMP` cuando el formato lo permita.

## Consideraciones de transformación

- `codigo_ubigeo` no debe convertirse a número.
- Esta tabla puede servir como base para una dimensión territorial.
- Si se requiere mayor consistencia geográfica, puede complementarse con una fuente oficial de ubigeo.

---

# 7. Dataset: periodos_academicos

## Descripción

Contiene periodos académicos asociados a becarios. Se usará como apoyo para construir una dimensión temporal académica.

## Tabla Silver esperada

```text
silver.periodos_academicos
```

| Campo            | Tipo esperado | Descripción                                        | Uso analítico         |
| ---------------- | ------------- | -------------------------------------------------- | --------------------- |
| source_row_id    | STRING        | Identificador técnico de la fila en la fuente      | Trazabilidad          |
| nro_fila         | INTEGER       | Número de fila de origen                           | Trazabilidad          |
| anio             | INTEGER       | Año académico                                      | Análisis temporal     |
| mes_numero       | INTEGER       | Número de mes                                      | Dimensión tiempo      |
| periodo_completo | STRING        | Periodo académico completo o etiqueta temporal     | Segmentación temporal |
| mes_nombre       | STRING        | Nombre del mes                                     | Visualización         |
| fecha_carga      | TIMESTAMP     | Fecha y hora de extracción o carga desde la fuente | Auditoría             |

## Reglas de calidad iniciales

- `anio` debe estar en un rango válido.
- `mes_numero` debe estar entre 1 y 12.
- `periodo_completo` debe tratarse como etiqueta temporal académica, no como fecha.
- `mes_nombre` debe normalizarse como texto.
- `fecha_carga` debe convertirse a `TIMESTAMP` cuando el formato lo permita.

## Consideraciones de transformación

- `anio` debe tratarse como entero.
- `mes_numero` debe tratarse como entero.
- `periodo_completo` puede usarse para segmentación temporal en Power BI.
- Esta tabla puede integrarse con una dimensión calendario si el análisis lo requiere.

---

# 8. Dataset: presupuesto_mef

## Descripción

Contiene información presupuestal pública relacionada con PRONABEC. Permite analizar evolución presupuestal, ejecución, avance financiero y relación entre recursos públicos y cobertura de programas.

## Tabla Silver esperada

```text
silver.presupuesto_mef
```

| Campo              | Tipo esperado | Descripción                             | Uso analítico               |
| ------------------ | ------------- | --------------------------------------- | --------------------------- |
| ano                | INTEGER       | Año fiscal consultado                   | Análisis temporal           |
| ejecutora_nombre   | STRING        | Nombre de la entidad o unidad ejecutora | Identificación presupuestal |
| pia                | NUMERIC       | Presupuesto Institucional de Apertura   | Presupuesto inicial         |
| pim                | NUMERIC       | Presupuesto Institucional Modificado    | Presupuesto actualizado     |
| certificacion      | NUMERIC       | Monto certificado                       | Control presupuestal        |
| compromiso_anual   | NUMERIC       | Compromiso anual                        | Control presupuestal        |
| compromiso_mensual | NUMERIC       | Compromiso mensual                      | Control presupuestal        |
| devengado          | NUMERIC       | Monto devengado                         | Ejecución presupuestal      |
| girado             | NUMERIC       | Monto girado                            | Ejecución financiera        |
| avance_porcentaje  | NUMERIC       | Porcentaje de avance presupuestal       | KPI financiero              |
| saldo_no_ejecutado | NUMERIC       | Diferencia entre PIM y devengado        | KPI financiero              |
| fecha_carga        | TIMESTAMP     | Fecha y hora de carga del registro      | Auditoría                   |

## Reglas de calidad iniciales

- `ano` no debe ser nulo.
- `pia`, `pim`, `devengado` y `girado` deben ser mayores o iguales a cero.
- `avance_porcentaje` debe estar entre 0 y 100.
- `saldo_no_ejecutado` debe calcularse como `pim - devengado`.
- Los montos deben convertirse a `NUMERIC`.
- `fecha_carga` debe convertirse a `TIMESTAMP` cuando el formato lo permita.

## Consideraciones de transformación

- MEF se obtiene mediante scraper controlado.
- El resultado Bronze de MEF será inicialmente tabular.
- Los montos deben normalizarse antes de cargar la tabla Silver.
- El análisis presupuestal no requiere procesamiento en tiempo real.

---

# 9. Campos derivados y enriquecimientos previstos

## `tipo_beca_normalizado`

| Campo                 | Tipo esperado | Descripción                                  | Capa recomendada          |
| --------------------- | ------------- | -------------------------------------------- | ------------------------- |
| tipo_beca_normalizado | STRING        | Clasificación estandarizada del tipo de beca | Silver enriquecido o Gold |

### Regla esperada

Debe derivarse a partir de campos como `convocatoria`, `nombre_programa`, `modalidad` u otra referencia confiable.

Valores esperados preliminares:

- BECA 18
- PERMANENCIA
- BICENTENARIO
- ESPECIAL
- INTERNACIONAL
- OTROS

## `modalidad_normalizada`

| Campo                 | Tipo esperado | Descripción                     | Capa recomendada          |
| --------------------- | ------------- | ------------------------------- | ------------------------- |
| modalidad_normalizada | STRING        | Modalidad de beca estandarizada | Silver enriquecido o Gold |

### Regla esperada

Debe derivarse desde el campo `modalidad` o desde patrones textuales en la convocatoria cuando la modalidad no esté disponible explícitamente.

## `saldo_no_ejecutado`

| Campo              | Tipo esperado | Descripción                      | Capa recomendada |
| ------------------ | ------------- | -------------------------------- | ---------------- |
| saldo_no_ejecutado | NUMERIC       | Diferencia entre PIM y devengado | Silver o Gold    |

### Regla esperada

```text
saldo_no_ejecutado = pim - devengado
```

## `tasa_ejecucion`

| Campo          | Tipo esperado | Descripción                                          | Capa recomendada |
| -------------- | ------------- | ---------------------------------------------------- | ---------------- |
| tasa_ejecucion | NUMERIC       | Porcentaje ejecutado del presupuesto respecto al PIM | Gold             |

### Regla esperada

```text
tasa_ejecucion = devengado / pim
```

Debe controlarse división entre cero.

---

# 10. Capas Gold esperadas

Las vistas Gold estarán orientadas al consumo en Power BI.

| Vista Gold                         | Descripción                                              | Uso en Power BI              |
| ---------------------------------- | -------------------------------------------------------- | ---------------------------- |
| gold.vw_resumen_ejecutivo          | KPIs principales del proyecto                            | Página Resumen Ejecutivo     |
| gold.vw_presupuesto_mef_anual      | Evolución anual del presupuesto                          | Página Presupuesto           |
| gold.vw_becarios_por_departamento  | Cobertura territorial de becarios                        | Página Cobertura Territorial |
| gold.vw_notas_por_semestre         | Rendimiento académico por periodo                        | Página Rendimiento           |
| gold.vw_desercion_por_convocatoria | Pérdida de becas por convocatoria                        | Página Deserción             |
| gold.vw_presupuesto_vs_becas       | Relación entre presupuesto y cobertura                   | Página Análisis Ejecutivo    |
| gold.vw_riesgo_desercion           | Indicadores o predicciones de riesgo académico/deserción | Página Riesgo o ML           |

## Reglas generales para Gold

- Las vistas Gold deben consumir tablas Silver, no archivos Bronze.
- Las métricas deben tener nombres claros y documentados.
- Las agregaciones deben estar diseñadas para facilitar el consumo desde Power BI.
- Las vistas Gold no deben depender de rutas locales ni archivos CSV.
- Las vistas Gold deben evitar lógica de limpieza pesada; esa lógica pertenece a Silver.

---

# 11. Actualización futura del diccionario

Este diccionario será actualizado conforme se implementen las capas Silver, Gold y ML. Las siguientes mejoras previstas son:

- Confirmar tipos finales en BigQuery.
- Agregar llaves primarias y relaciones entre tablas.
- Documentar tablas externas Bronze cuando sean implementadas.
- Documentar tablas Silver reales después de su creación.
- Documentar vistas Gold definitivas.
- Documentar medidas DAX principales usadas en Power BI.
- Documentar variables usadas en BigQuery ML.
- Documentar reglas finales de calidad de datos.
