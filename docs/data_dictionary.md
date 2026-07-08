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
- Colegios hábiles.
- Becarios por país de estudio.
- Convocatorias por carrera y sede.
- Nota promedio del postulante por región.
- Presupuesto MEF.
- Indicadores de contexto regional INEI.
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

Contiene cantidades y porcentajes de becarios distribuidos por departamento, provincia y tipo de beca. En Bronze se conserva completo, incluyendo filas de detalle provincial y filas agregadas regionales o nacionales. En Silver solo se promueve el detalle provincial histórico de Beca 18 2016.

## Tabla Silver esperada

```text
silver.pronabec_beca18_becarios_provincia_2016
```

| Campo | Tipo esperado | Descripción | Uso analítico |
| :--- | :--- | :--- | :--- |
| source_row_id | INTEGER | Identificador técnico de la fila en Bronze | Trazabilidad |
| region | STRING | Región o departamento | Análisis territorial |
| provincia | STRING | Provincia | Análisis territorial |
| becarios_b18_count | INTEGER | Cantidad de becarios Beca 18 derivada de `b18_n` | Cobertura histórica Beca 18 |
| source_snapshot_date | DATE | Fecha de carga fuente derivada de `fecha_carga` | Contexto temporal del snapshot |
| source_system | STRING | Sistema origen | Auditoría |
| source_dataset | STRING | Dataset origen (`becarios_provincia`) | Auditoría |
| extraction_date | DATE | Fecha lógica de extracción | Auditoría |
| ingestion_timestamp | TIMESTAMP | Timestamp de ingesta Silver | Auditoría |
| pipeline_run_id | STRING | Identificador de corrida | Auditoría |

## Reglas de calidad iniciales

- `region` no debe ser nula.
- `provincia` no debe ser nula.
- Las filas con `provincia` igual a `TOTAL`, `TOTAL DE BENEFICIARIOS`, `TOTAL GLOBAL` o que empiecen con `TOTAL` no pasan a Silver.
- `b18_n` se convierte a `INTEGER`; valores vacíos o inválidos quedan como `NULL`.
- `fecha_carga` se convierte desde `DD/MM/YYYY HH:MM:SS` a `DATE` en `source_snapshot_date`.

## Consideraciones de transformación

- `aggregation_scope` no existe en Silver porque los agregados no se conservan.
- Los totales regionales y nacionales permanecen en Bronze para trazabilidad.
- Las columnas de otros programas y porcentajes permanecen en Bronze, pero no forman parte del contrato Silver vigente.

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

# 8. Dataset: colegios_habiles

## Descripción

Contiene el listado de colegios o instituciones educativas de nivel escolar habilitadas en los procesos de postulación de PRONABEC.

## Tabla Silver esperada

```text
silver.colegios_habiles
```

| Campo | Tipo esperado | Descripción | Uso analítico |
| :--- | :--- | :--- | :--- |
| source_row_id | STRING | Identificador técnico de la fila en la fuente | Trazabilidad |
| nro_fila | INTEGER | Número de fila de origen | Trazabilidad |
| ugel | STRING | Unidad de Gestión Educativa Local asociada | Identificación administrativa escolar |
| institucion_educativa | STRING | Nombre de la institución educativa escolar | Clasificación escolar |
| tipo_gestion | STRING | Tipo de gestión del colegio (público, privado) | Segmentación por gestión |
| nivel_modalidad | STRING | Nivel o modalidad educativa de atención | Clasificación de oferta escolar |
| forma_atencion | STRING | Forma de atención (ej. presencial) | Clasificación de oferta escolar |
| centro_poblado | STRING | Centro poblado de ubicación geográfica | Análisis territorial fino |
| distrito | STRING | Distrito de ubicación geográfica | Análisis territorial |
| direccion | STRING | Dirección física de la institución | Trazabilidad de contacto |
| telefono | STRING | Teléfono de contacto de la institución | Contacto administrativo |
| fecha_carga | TIMESTAMP | Fecha y hora de carga reportada por la fuente | Auditoría |

## Reglas de calidad iniciales

- `institucion_educativa` no debe ser nulo.
- `ugel` y `distrito` no deben ser nulos.
- `telefono` no es obligatorio y debe conservarse como texto.
- `fecha_carga` debe convertirse a `TIMESTAMP`.

## Consideraciones de transformación

- En la capa Bronze, todos los campos de este dataset se almacenan como `STRING` y `NULLABLE` para garantizar la trazabilidad y resiliencia ante cambios.
- `telefono` se mantiene como `STRING` para evitar errores con caracteres especiales, guiones o valores no numéricos.
- Los campos textuales pueden presentar espacios en blanco sobrantes que requieren limpieza (trimming).

---

# 9. Dataset: becarios_pais_estudio

## Descripción

Contiene la distribución de becarios clasificados por el país de estudio, modalidad, convocatoria y tipo de institución educativa.

## Tabla Silver esperada

```text
silver.becarios_pais_estudio
```

| Campo | Tipo esperado | Descripción | Uso analítico |
| :--- | :--- | :--- | :--- |
| source_row_id | STRING | Identificador técnico de la fila en la fuente | Trazabilidad |
| nro_fila | INTEGER | Número de fila de origen | Trazabilidad |
| modalidad | STRING | Modalidad de la beca | Segmentación por tipo de beneficio |
| convocatoria | STRING | Convocatoria del proceso de becas | Análisis temporal de convocatorias |
| pais_estudio | STRING | País donde el becario realiza los estudios | Análisis de internacionalización |
| tipo_institucion | STRING | Tipo de institución educativa superior | Clasificación del sector educativo |
| institucion | STRING | Nombre de la institución educativa superior | Análisis de concentración escolar |
| sexo | STRING | Sexo reportado por el becario | Segmentación demográfica |
| fecha_carga | TIMESTAMP | Fecha y hora de carga reportada por la fuente | Auditoría |

## Reglas de calidad iniciales

- `pais_estudio` e `institucion` no deben ser nulos.
- `modalidad` y `convocatoria` no deben ser nulos.
- `fecha_carga` debe convertirse a `TIMESTAMP`.

## Consideraciones de transformación

- En la capa Bronze, todos los campos de este dataset se almacenan como `STRING` y `NULLABLE` para garantizar la trazabilidad y resiliencia ante cambios.
- El campo `sexo` se conserva en su formato fuente y puede requerir estandarización en capas de reporting.

---

# 10. Dataset: convocatorias_carrera_sede

## Descripción

Dataset de alto volumen que contiene las convocatorias de PRONABEC desagregadas por carrera, institución educativa, sede y región. Se conserva en Bronze por trazabilidad, pero no se promueve a Silver en esta versión porque no será usado directamente en Gold/Power BI.

## Alcance en la plataforma

```text
bronze.pronabec_convocatorias_carrera_sede_raw
```

No existe tabla `silver.pronabec_convocatorias_carrera_sede`, schema Silver, transform Silver ni job Dataflow Silver para este dataset.

| Campo | Tipo esperado | Descripción | Uso analítico |
| :--- | :--- | :--- | :--- |
| source_row_id | STRING | Identificador técnico de la fila en la fuente | Trazabilidad |
| nro_fila | INTEGER | Número de fila de origen | Trazabilidad |
| id_convocatoria | INTEGER | Identificador numérico de la convocatoria | Relación con hechos operativos |
| convocatoria | STRING | Nombre de la convocatoria asociada | Descriptivo del proceso |
| pais_origen | STRING | País de origen de la convocatoria o institución | Clasificación territorial |
| nivel_educativo | STRING | Nivel educativo (pregrado, posgrado, técnico) | Clasificación de oferta |
| tipo_institucion | STRING | Tipo de institución superior (universidad, instituto) | Clasificación del sector |
| sede | STRING | Sede física de la institución educativa | Análisis geográfico de sedes |
| institucion | STRING | Nombre de la institución educativa superior | Concentración institucional |
| carrera | STRING | Nombre de la carrera o programa de estudio | Demanda y oferta de especialidades |
| resolucion | STRING | Resolución administrativa que avala la oferta | Trazabilidad legal |
| gestion | STRING | Tipo de gestión de la institución superior (pública, privada) | Segmentación por gestión |
| abreviatura | STRING | Abreviatura o sigla de la institución | Descriptivo analítico |
| region | STRING | Región geográfica donde se ubica la institución | Análisis territorial de oferta |
| web | STRING | Sitio web oficial de la institución | Contacto institucional |
| representante | STRING | Nombre del representante legal o institucional | Trazabilidad legal |
| telefono | STRING | Teléfono de contacto de la sede o institución | Contacto administrativo |
| ruc | STRING | Registro Único de Contribuyentes de la entidad | Trazabilidad fiscal |
| email | STRING | Correo electrónico de contacto institucional | Contacto administrativo |
| fecha_carga | TIMESTAMP | Fecha y hora de carga reportada por la fuente | Auditoría |

## Reglas de calidad iniciales

- En Bronze se preservan los valores fuente como trazabilidad.
- No se aplican reglas Silver en esta versión.
- Los Gold futuros no deben depender de `silver.pronabec_convocatorias_carrera_sede`.

## Consideraciones de transformación

- En la capa Bronze, todos los campos de este dataset se almacenan como `STRING` y `NULLABLE` para garantizar la trazabilidad y resiliencia ante cambios.
- Si en otra versión se requiere oferta académica en Silver, debe definirse un contrato nuevo y pruebas específicas antes de promover este dataset.

---

# 11. Dataset: nota_postulante_region

## Descripción

Contiene las notas promedio obtenidas por los postulantes a becas, segmentadas por región de origen, modalidad y convocatoria.

## Tabla Silver esperada

```text
silver.nota_postulante_region
```

| Campo | Tipo esperado | Descripción | Uso analítico |
| :--- | :--- | :--- | :--- |
| source_row_id | STRING | Identificador técnico de la fila en la fuente | Trazabilidad |
| nro_fila | INTEGER | Número de fila de origen | Trazabilidad |
| region | STRING | Región o departamento de origen del postulante | Cobertura y procedencia territorial |
| nota_promedio | NUMERIC | Nota promedio del postulante en el proceso | Rendimiento y admisión |
| modalidad | STRING | Modalidad del concurso al que postuló | Segmentación por beneficio |
| anio_convocatoria | INTEGER | Año correspondiente al proceso de convocatoria | Análisis temporal |
| tipo_institucion | STRING | Tipo de institución elegida por el postulante | Preferencia del sector educativo |
| institucion_educativa | STRING | Nombre de la institución educativa elegida | Preferencia escolar/universitaria |
| semestre | STRING | Semestre académico del proceso de admisión | Segmentación temporal académica |
| fecha_carga | TIMESTAMP | Fecha y hora de carga reportada por la fuente | Auditoría |

## Reglas de calidad iniciales

- `region` y `nota_promedio` no deben ser nulos.
- `nota_promedio` debe validarse en el rango de 0 a 20.
- `anio_convocatoria` debe convertirse a `INTEGER`.
- `fecha_carga` debe convertirse a `TIMESTAMP`.

## Consideraciones de transformación

- En la capa Bronze, todos los campos de este dataset se almacenan como `STRING` y `NULLABLE` para garantizar la trazabilidad y resiliencia ante cambios.
- `nota_promedio` viene con coma decimal y precisión excesiva (ej. `14,500000`) en Bronze (como `STRING`). Se requiere realizar un reemplazo de la coma por punto decimal y casteo a `NUMERIC` en la transformación.
- `anio_convocatoria` se conserva en Bronze como `STRING` y debe ser transformado a `INTEGER` en la capa Silver.

---

# 12. Dataset: presupuesto_mef

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

- El dataset de presupuesto MEF se obtiene mediante un scraper controlado que lee directamente del portal Consulta Amigable.
- En la capa Bronze, los datos se preservan en formato CSV crudo (`data.csv`), conservando las columnas (`ano`, `ejecutora_nombre`, `pia`, `pim`, `certificacion`, `compromiso_anual`, `compromiso_mensual`, `devengado`, `girado`, `avance_porcentaje`) como valores tabulares conservadores (generalmente representados como texto en el CSV de origen) para garantizar resiliencia en la ingesta.
- El tipado fuerte (ej. conversión a `INTEGER` para el año, `NUMERIC` para montos y porcentajes) y la validación de negocio ocurren rigurosamente en la transformación hacia la capa Silver.
- Los montos deben normalizarse y limpiarse de cualquier carácter extraño (comas, espacios) antes de materializarse en la tabla Silver.
- El análisis presupuestal no requiere procesamiento en tiempo real.

- El análisis presupuestal no requiere procesamiento en tiempo real.

---

## 12.1. Dataset: presupuesto_mef_hierarchy

### Descripción
Representa la jerarquía presupuestal superior, permitiendo contrastar la Unidad Ejecutora PRONABEC contra los totales del Sector Educación, del Pliego M. de Educación y del Nivel de Gobierno Nacional.

### Tabla Silver esperada
`silver.presupuesto_mef_hierarchy`

| Campo | Tipo esperado | Descripción | Uso analítico |
| :--- | :--- | :--- | :--- |
| ano | INTEGER | Año fiscal consultado | Análisis temporal |
| periodo_tipo | STRING | Tipo de periodo (ANUAL, MENSUAL, TRIMESTRAL) | Granularidad temporal |
| periodo_valor | STRING | Valor específico del periodo (ej. 2026) | Granularidad temporal |
| nivel_jerarquia | STRING | Nivel en la jerarquía presupuestal | Análisis jerárquico |
| codigo | STRING | Código del nivel jerárquico | Trazabilidad |
| descripcion | STRING | Descripción del nivel jerárquico | Visualización |
| pia | NUMERIC | Presupuesto Institucional de Apertura | Presupuesto inicial |
| pim | NUMERIC | Presupuesto Institucional Modificado | Presupuesto actualizado |
| certificacion | NUMERIC | Monto certificado acumulado | Control presupuestal |
| compromiso_anual | NUMERIC | Compromiso anual acumulado | Control presupuestal |
| compromiso_mensual | NUMERIC | Compromiso mensual acumulado | Control presupuestal |
| devengado | NUMERIC | Monto devengado acumulado | Ejecución presupuestal |
| girado | NUMERIC | Monto girado acumulado | Ejecución financiera |
| avance_porcentaje | NUMERIC | Porcentaje de avance | KPI de ejecución |
| fecha_carga | TIMESTAMP | Fecha y hora de carga del registro | Auditoría |

### Reglas de calidad iniciales
- `ano` y `nivel_jerarquia` no deben ser nulos.
- Los montos y porcentajes deben convertirse a `NUMERIC`.
- `fecha_carga` debe convertirse a `TIMESTAMP`.

---

## 12.2. Dataset: presupuesto_mef_producto

### Descripción
Permite analizar la asignación y ejecución presupuestal distribuida por productos, actividades de inversión u obras de PRONABEC.

### Tabla Silver esperada
`silver.presupuesto_mef_producto`

| Campo | Tipo esperado | Descripción | Uso analítico |
| :--- | :--- | :--- | :--- |
| ano | INTEGER | Año fiscal consultado | Análisis temporal |
| periodo_tipo | STRING | Tipo de periodo | Granularidad temporal |
| periodo_valor | STRING | Valor del periodo | Granularidad temporal |
| codigo_producto | STRING | Código único del producto/proyecto | Relación de hechos |
| producto_proyecto | STRING | Descripción de producto/proyecto | Clasificación del gasto |
| pia | NUMERIC | Presupuesto de Apertura | Presupuesto inicial |
| pim | NUMERIC | Presupuesto Modificado | Presupuesto actualizado |
| certificacion | NUMERIC | Monto certificado | Control presupuestal |
| compromiso_anual | NUMERIC | Compromiso anual | Control presupuestal |
| compromiso_mensual | NUMERIC | Compromiso mensual | Control presupuestal |
| devengado | NUMERIC | Monto devengado | Ejecución presupuestal |
| girado | NUMERIC | Monto girado | Ejecución financiera |
| avance_porcentaje | NUMERIC | Porcentaje de avance | KPI de ejecución |
| fecha_carga | TIMESTAMP | Fecha y hora de carga | Auditoría |

---

## 12.3. Dataset: presupuesto_mef_generica

### Descripción
Contiene la clasificación del presupuesto según el nivel de genérica de gasto (ej. Bienes y Servicios, Donaciones y Transferencias, etc.).

### Tabla Silver esperada
`silver.presupuesto_mef_generica`

| Campo | Tipo esperado | Descripción | Uso analítico |
| :--- | :--- | :--- | :--- |
| ano | INTEGER | Año fiscal | temporal |
| periodo_tipo | STRING | Tipo de periodo | Granularidad |
| periodo_valor | STRING | Valor del periodo | Granularidad |
| codigo_generica | STRING | Código de la partida genérica de gasto | Relación |
| generica | STRING | Descripción de la genérica | Clasificación contable |
| pia | NUMERIC | Presupuesto de Apertura | Presupuesto inicial |
| pim | NUMERIC | Presupuesto Modificado | Presupuesto modificado |
| certificacion | NUMERIC | Monto certificado | Control |
| compromiso_anual | NUMERIC | Compromiso anual | Control |
| compromiso_mensual | NUMERIC | Compromiso mensual | Control |
| devengado | NUMERIC | Monto devengado | Ejecución |
| girado | NUMERIC | Monto girado | Ejecución |
| avance_porcentaje | NUMERIC | Porcentaje de avance | KPI |
| fecha_carga | TIMESTAMP | Fecha y hora de carga | Auditoría |

---

## 12.4. Dataset: presupuesto_mef_fuente

### Descripción
Clasificación presupuestal según la fuente de financiamiento oficial (ej. Recursos Ordinarios, Recursos Directamente Recaudados).

### Tabla Silver esperada
`silver.presupuesto_mef_fuente`

| Campo | Tipo esperado | Descripción | Uso analítico |
| :--- | :--- | :--- | :--- |
| ano | INTEGER | Año fiscal | temporal |
| periodo_tipo | STRING | Tipo de periodo | Granularidad |
| periodo_valor | STRING | Valor del periodo | Granularidad |
| codigo_fuente | STRING | Código de la fuente de financiamiento | Relación |
| fuente_financiamiento | STRING | Descripción de la fuente | Clasificación financiera |
| pia | NUMERIC | Presupuesto de Apertura | Presupuesto inicial |
| pim | NUMERIC | Presupuesto Modificado | Presupuesto modificado |
| certificacion | NUMERIC | Monto certificado | Control |
| compromiso_anual | NUMERIC | Compromiso anual | Control |
| compromiso_mensual | NUMERIC | Compromiso mensual | Control |
| devengado | NUMERIC | Monto devengado | Ejecución |
| girado | NUMERIC | Monto girado | Ejecución |
| avance_porcentaje | NUMERIC | Porcentaje de avance | KPI |
| fecha_carga | TIMESTAMP | Fecha y hora de carga | Auditoría |

---

## 12.5. Dataset: presupuesto_mef_rubro

### Descripción
Desglose presupuestal según el rubro de gasto bajo el cual se registran las transacciones oficiales.

### Tabla Silver esperada
`silver.presupuesto_mef_rubro`

| Campo | Tipo esperado | Descripción | Uso analítico |
| :--- | :--- | :--- | :--- |
| ano | INTEGER | Año fiscal | temporal |
| periodo_tipo | STRING | Tipo de periodo | Granularidad |
| periodo_valor | STRING | Valor del periodo | Granularidad |
| codigo_rubro | STRING | Código del rubro de gasto | Relación |
| rubro | STRING | Descripción del rubro | Clasificación contable |
| pia | NUMERIC | Presupuesto de Apertura | Presupuesto inicial |
| pim | NUMERIC | Presupuesto Modificado | Presupuesto modificado |
| certificacion | NUMERIC | Monto certificado | Control |
| compromiso_anual | NUMERIC | Compromiso anual | Control |
| compromiso_mensual | NUMERIC | Compromiso mensual | Control |
| devengado | NUMERIC | Monto devengado | Ejecución |
| girado | NUMERIC | Monto girado | Ejecución |
| avance_porcentaje | NUMERIC | Porcentaje de avance | KPI |
| fecha_carga | TIMESTAMP | Fecha y hora de carga | Auditoría |

---

## 12.6. Dataset: presupuesto_mef_departamento

### Descripción
Asignación presupuestal distribuida geográficamente a nivel de departamento/región.

### Tabla Silver esperada
`silver.presupuesto_mef_departamento`

| Campo | Tipo esperado | Descripción | Uso analítico |
| :--- | :--- | :--- | :--- |
| ano | INTEGER | Año fiscal | temporal |
| periodo_tipo | STRING | Tipo de periodo | Granularidad |
| periodo_valor | STRING | Valor del periodo | Granularidad |
| departamento | STRING | Nombre del departamento geográfico | Análisis geográfico |
| pia | NUMERIC | Presupuesto de Apertura | Presupuesto inicial |
| pim | NUMERIC | Presupuesto Modificado | Presupuesto modificado |
| certificacion | NUMERIC | Monto certificado | Control |
| compromiso_anual | NUMERIC | Compromiso anual | Control |
| compromiso_mensual | NUMERIC | Compromiso mensual | Control |
| devengado | NUMERIC | Monto devengado | Ejecución |
| girado | NUMERIC | Monto girado | Ejecución |
| avance_porcentaje | NUMERIC | Porcentaje de avance | KPI |
| fecha_carga | TIMESTAMP | Fecha y hora de carga | Auditoría |

---

## 12.7. Dataset: presupuesto_mef_temporal

### Descripción
Proporciona los cortes de ejecución presupuestal a nivel de trimestre o mes.

### Tabla Silver esperada
`silver.presupuesto_mef_temporal`

| Campo | Tipo esperado | Descripción | Uso analítico |
| :--- | :--- | :--- | :--- |
| ano | INTEGER | Año fiscal | temporal |
| periodo_tipo | STRING | Tipo de periodo (MENSUAL o TRIMESTRAL) | Granularidad |
| periodo_valor | STRING | Valor del periodo (ej. 2026-01) | Granularidad |
| trimestre | STRING | Identificador de trimestre (ej. T1), cuando aplique | Temporal |
| mes_numero | STRING | Número de mes (ej. 01), cuando aplique | Temporal |
| mes_nombre | STRING | Nombre del mes (ej. ENERO), cuando aplique | Temporal |
| pia | NUMERIC | Presupuesto de Apertura | Presupuesto inicial |
| pim | NUMERIC | Presupuesto Modificado | Presupuesto modificado |
| certificacion | NUMERIC | Monto certificado | Control |
| compromiso_anual | NUMERIC | Compromiso anual | Control |
| compromiso_mensual | NUMERIC | Compromiso mensual | Control |
| devengado | NUMERIC | Monto devengado | Ejecución |
| girado | NUMERIC | Monto girado | Ejecución |
| avance_porcentaje | NUMERIC | Porcentaje de avance | KPI |
| fecha_carga | TIMESTAMP | Fecha y hora de carga | Auditoría |

### Consideraciones sobre nulos y valores vacíos
En Bronze se conservan los strings vacíos que vengan del portal de origen sin alterar la respuesta. Durante la transformación Silver, se permitirá que las columnas de montos y avance en filas mensuales de este dataset se mantengan como `NULL` si el origen no devolvió valores acumulados mensuales para las mismas.

---

# 13. Tablas de contexto regional INEI

Las tablas de contexto regional INEI son indicadores Silver tipados cargados desde archivos CSV manuales en `landing/inei_reports`. Son insumos operacionales para futura ingeniería de características regionales y analítica territorial; no son salidas predictivas.

| Tabla | Grano | Campos clave | Métricas |
| --- | --- | --- | --- |
| `silver.inei_population_youth_region` | Año y región | `anio`, `region` | `poblacion_total`, rangos de población joven, `share_15_24_total` |
| `silver.inei_demographic_indicators_region` | Año y región | `anio`, `region` | tasas de natalidad, fecundidad, esperanza de vida, mortalidad infantil, migración y crecimiento |
| `silver.inei_pobreza_departamental` | Año y región | `anio`, `region` | `pobreza_monetaria_pct` |
| `silver.inei_internet_acceso_region` | Año y región | `anio`, `region` | `internet_acceso_pct` |

Todas las tablas Silver INEI incluyen `source_system`, `source_dataset`, `extraction_date`, `ingestion_timestamp` y `pipeline_run_id`.

Las reglas iniciales de calidad validan `anio` y `region` no nulos, rangos numéricos aceptados, valores de población no negativos y claves duplicadas por `(anio, region)`.

---

# 14. Fundación regional ML

La primera capa del módulo predictivo vive en el dataset `ml`, pero no implementa todavía modelos. Su función es normalizar contexto regional y dejar lista la base para scoring y simulación.

## `ml.dim_region_mapping`

| Campo | Tipo esperado | Descripción |
| --- | --- | --- |
| source_region | STRING | Nombre regional observado en la fuente. |
| region_canonical | STRING | Región canónica estandarizada. |
| region_scope | STRING | Alcance territorial de la variante. |
| mapping_rule | STRING | Regla aplicada para consolidar la variante. |
| is_aggregated_region | BOOLEAN | Indica si la región canónica consolida varias variantes. |
| notes | STRING | Observaciones metodológicas o de trazabilidad. |

## `ml.region_context_features`

| Campo | Tipo esperado | Descripción |
| --- | --- | --- |
| anio | INTEGER | Año de referencia. |
| region | STRING | Región canónica de presentación. |
| region_canonical | STRING | Clave canónica unificada. |
| pobreza_monetaria_pct | NUMERIC/FLOAT64 | Incidencia de pobreza monetaria. |
| poblacion_total | INTEGER | Población total regional. |
| poblacion_15_24 | INTEGER | Población joven de 15 a 24 años. |
| poblacion_15_29 | INTEGER | Población joven de 15 a 29 años. |
| poblacion_joven_pct | NUMERIC/FLOAT64 | Participación de población joven. |
| matricula_5to_secundaria | INTEGER | Matrícula total de quinto de secundaria. |
| matricula_5to_publica | INTEGER | Matrícula pública de quinto de secundaria. |
| matricula_5to_privada | INTEGER | Matrícula privada de quinto de secundaria. |
| matricula_5to_urbana | INTEGER | Matrícula urbana de quinto de secundaria. |
| matricula_5to_rural | INTEGER | Matrícula rural de quinto de secundaria. |
| ruralidad_educativa_pct | NUMERIC/FLOAT64 | Porcentaje rural de la matrícula de quinto. |
| internet_acceso_pct | NUMERIC/FLOAT64 | Acceso regional a internet. |
| brecha_digital_pct | NUMERIC/FLOAT64 | Brecha digital estimada. |
| feature_completeness_score | NUMERIC/FLOAT64 | Proporción de campos clave no nulos. |
| feature_quality_flag | STRING | Etiqueta resumida de calidad. |
| has_synthetic_values | BOOLEAN | Indica si se sintetizó algún valor. |
| synthetic_fields | STRING | Lista de campos sintéticos o imputados. |
| source_priority | STRING | Prioridad o combinación de fuentes. |
| created_at | TIMESTAMP | Timestamp de materialización o consulta. |

### Consideración metodológica

`ml.region_context_features` no es un modelo ML. Es una tabla/vista de features regionales construida para que, en la siguiente rama, se puedan derivar scores, escenarios y simulaciones sin volver a rehacer la normalización territorial.

---

# 15. Campos derivados y enriquecimientos previstos

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

# 15. Capas Gold esperadas

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

# 16. Actualización futura del diccionario

Este diccionario será actualizado conforme se implementen las capas Silver, Gold y ML. Las siguientes mejoras previstas son:

- Confirmar tipos finales en BigQuery.
- Agregar llaves primarias y relaciones entre tablas.
- Documentar tablas externas Bronze cuando sean implementadas.
- Documentar tablas Silver reales después de su creación.
- Documentar vistas Gold definitivas.
- Documentar medidas DAX principales usadas en Power BI.
- Documentar variables usadas en BigQuery ML.
- Documentar reglas finales de calidad de datos.
