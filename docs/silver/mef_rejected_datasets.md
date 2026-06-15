# Datasets MEF no promovidos a la capa Silver (BRONZE_ONLY)

## Objetivo

Para mantener la capa Silver limpia, enfocada y optimizada analíticamente, no todas las fuentes o slices extraídos en Bronze se promueven a tablas físicas Silver. Este documento detalla la clasificación de los datasets del Ministerio de Economía y Finanzas (MEF) catalogados como `BRONZE_ONLY` y las justificaciones de negocio y arquitectura detrás de esta decisión.

## Criterios de exclusión

La exclusión de un dataset para su promoción a la capa Silver se rige por los siguientes criterios:

* **Baja variabilidad analítica**: El dataset presenta un valor único o fuertemente dominante para todos los registros del contexto de PRONABEC, lo que no aporta capacidad de segmentación útil.
* **Redundancia conceptual**: La información contenida en el dataset está duplicada o explicada con mayor claridad en otras tablas aprobadas.
* **Riesgo de mala interpretación (Sesgo de datos)**: El dataset presenta dimensiones administrativas que pueden inducir a conclusiones erróneas si se confunden con el comportamiento real de los becarios.
* **Sobregranularidad sin caso de uso**: El nivel de detalle es excesivamente fino para las necesidades de reporte del modelo analítico actual, generando costos de almacenamiento y procesamiento sin retorno de valor.

---

## Datasets catalogados como BRONZE_ONLY

Los siguientes datasets de presupuesto MEF se mantienen exclusivamente en la capa Bronze y no cuentan con contratos Silver:

### 1. Fuente de Financiamiento (`presupuesto_mef_fuente`)
* **Justificación de rechazo**: En el contexto presupuestario de PRONABEC, el financiamiento se concentra casi en su totalidad en una única fuente dominante (`RECURSOS ORDINARIOS`). Al no haber una distribución significativa con otras fuentes (como Recursos Directamente Recaudados o Donaciones), esta dimensión carece de variabilidad analítica y no aporta segmentación de valor para los reportes ejecutivos.
* **Estado**: Se conserva en Bronze para auditoría financiera de ser requerida en el futuro.

### 2. Rubro Presupuestal (`presupuesto_mef_rubro`)
* **Justificación de rechazo**: El rubro contable oficial repite conceptualmente la misma distribución y valores de la Fuente de Financiamiento (mayoritariamente `RECURSOS ORDINARIOS`). Por lo tanto, crear una tabla Silver para rubro es redundante y solo añade complejidad al modelo sin ofrecer nuevas perspectivas.
* **Estado**: Se conserva en Bronze.

### 3. Distribución Geográfica MEF (`presupuesto_mef_departamento`)
* **Justificación de rechazo**: Este slice en el portal Consulta Amigable registra la ubicación presupuestal y administrativa donde se devenga y gestiona el gasto institucional. Al ser PRONABEC una entidad de alcance nacional centralizada, la inmensa mayoría de este gasto se registra en la sede central (departamento de Lima).
  * **Importante**: Utilizar esta tabla para analizar la cobertura territorial del gasto de becas sugeriría erróneamente que casi todo el presupuesto beneficia solo a Lima. La verdadera distribución regional y la procedencia de los becarios de PRONABEC debe obtenerse rigurosamente desde los datasets propios de PRONABEC (ej. `becarios_pais_estudio`, `ubigeo_postulacion`, etc.), no desde la clasificación contable del MEF.
* **Estado**: Se conserva en Bronze para evitar sesgos analíticos graves en reportes cruzados.

### 4. Categoría Presupuestal (`presupuesto_mef_categoria`)
* **Justificación de rechazo**: La categoría presupuestal (que distingue entre Programas Presupuestales, Acciones Centrales y APNOP) ofrece una clasificación macro que resulta redundante frente al análisis detallado por Producto/Proyecto y Actividad Presupuestaria. Para el modelo analítico actual, el nivel de producto y actividad explica de manera mucho más clara y operativa el destino de los recursos.
* **Estado**: Se conserva en Bronze.

### 5. Subgenérica de Gasto (`presupuesto_mef_subgenerica`)
* **Justificación de rechazo**: La subgenérica de gasto desglosa las partidas genéricas a un nivel operativo sumamente fino (ej. separar bienes y servicios en subpartidas muy específicas). Introducir este nivel de detalle en Silver añade sobregranularidad y ruido, cuando para el modelo y los reportes de Power BI actuales basta con el nivel de Genérica de Gasto para categorizar conceptualmente la naturaleza de la ejecución.
* **Estado**: Se conserva en Bronze.

---

## Preservación en Bronze y Gobierno de Datos

Es importante recalcar las implicaciones de la clasificación `BRONZE_ONLY`:

* **No implica pérdida de datos**: Los datos crudos en formato CSV (`data.csv`) para estos slices siguen siendo extraídos diariamente por el scraper y guardados con su partición temporal en el Data Lake.
* **Garantía de auditoría**: Si en el futuro surgiera una justificación de negocio (como una auditoría presupuestaria detallada de tesorería o un cambio estructural en las fuentes de financiamiento de PRONABEC), los datos históricos estarán disponibles en Bronze listos para que se defina su contrato Silver y se despliegue su pipeline en Dataflow.
* **Reducción de costos**: Al no procesar ni materializar estas tablas en Silver, se optimizan los recursos de computación de Dataflow y se reduce el número de tablas a mantener en BigQuery, previniendo el desorden de datos y simplificando el modelo de datos para los usuarios analistas.
