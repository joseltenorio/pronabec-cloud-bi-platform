# Datasets MEF aprobados para la capa Silver

## Objetivo

La capa Silver de la plataforma de datos tiene como objetivo consolidar contratos tipados, limpios y optimizados analíticamente. En el caso de los datos del Ministerio de Economía y Finanzas (MEF), extraídos mediante el scraper controlado de Consulta Amigable, este documento detalla los criterios de diseño, la estructura de datos aprobada y las decisiones de grano para la promoción de estos datasets a la capa Silver.

## Criterios de selección

Los datasets del MEF seleccionados para la capa Silver cumplen con los siguientes principios de arquitectura y modelado analítico:

* **Valor analítico directo**: Aportan información inmediata para responder preguntas clave de negocio sobre el presupuesto y la ejecución presupuestal del Programa Nacional de Becas y Crédito Educativo (PRONABEC).
* **Segmentación de gasto**: Permiten analizar en qué se gasta el presupuesto público de manera jerárquica (gasto por producto, por actividad operativa y por partida genérica).
* **Análisis de tendencias**: Proveen segmentación de tiempo (mes, trimestre y año fiscal) en tablas diseñadas específicamente para evitar duplicidad o malas agregaciones.
* **Estabilidad y consistencia conceptual**: Se seleccionaron dimensiones oficiales y estables, garantizando contratos de datos sólidos.
* **Simplificación y optimización**: Se descartaron etapas transaccionales intermedias o redundantes que añaden ruido al modelo analítico ejecutivo.

## Decisiones generales de tipado

Para asegurar la integridad de las consultas en BigQuery y evitar el tipado laxo (todo como `STRING` en Bronze), se aplican las siguientes reglas de conversión fuerte para Silver:

* **Año fiscal (`ano`)**: Se tipa como `INT64` (o `INTEGER`). Representa un año de ejercicio fiscal y se utiliza para filtros analíticos y particionamiento, no como una fecha temporal completa.
* **Periodos temporales (`periodo_valor`)**: Se mantiene como `STRING` debido a que contiene formatos heterogéneos según el corte temporal (ej. `"2026"`, `"2026-01"`, `"2026-T1"`).
* **Trimestre y Mes (`trimestre`, `mes_numero`)**: Se tipan como `INT64` cuando son aplicables (nulos en cortes anuales).
* **Mes nombre (`mes_nombre`)**: Se conserva como `STRING` para visualización.
* **Códigos identificadores**: Todos los códigos de catálogo y negocio (ej. `codigo_producto`, `codigo_actividad`, `codigo_generica`, `codigo_entidad`) se conservan como `STRING`. Esto preserva el formato original (incluyendo guiones y caracteres de ordenación como `5-23` o códigos con ceros a la izquierda) y evita errores de conversión a entero.
* **Montos analíticos y Porcentaje de avance (`pia`, `pim`, `devengado`, `avance_porcentaje`)**: Se tipan rigurosamente como `NUMERIC` para permitir operaciones matemáticas precisas en consultas y visualizaciones de Power BI.
* **Metadata técnica estándar**: Cada tabla Silver incluye obligatoriamente campos técnicos en modo `REQUIRED` (`source_system`, `source_dataset`, `extraction_date`, `ingestion_timestamp` y `pipeline_run_id`) para asegurar la trazabilidad del linaje de datos.

## Decisión sobre columnas presupuestales y de ejecución

Con el fin de entregar un modelo limpio y eficiente para análisis ejecutivo, se implementaron dos decisiones clave sobre las métricas financieras:

1. **Exclusión de etapas intermedias**: Se excluyen las métricas de `certificacion`, `compromiso_anual`, `compromiso_mensual` y `girado`. Estas fases presupuestales son de carácter administrativo/operativo intermedio, tienen bajo valor para la toma de decisiones ejecutivas en este nivel y su inclusión en cortes temporales intermedios complejiza innecesariamente las visualizaciones.
2. **Exclusión de PIA/PIM en tablas temporales**: En las tablas de corte temporal (mensual/trimestral), se conserva únicamente el `devengado` como métrica de flujo de ejecución. El PIA (Presupuesto Institucional de Apertura) y el PIM (Presupuesto Institucional Modificado) son métricas anuales acumuladas (stock) y no representan flujos mensuales o trimestrales independientes. Incluirlos en filas mensuales induciría a errores graves de sobreestimación del presupuesto en agregaciones libres.

---

## Datasets aprobados

### 1. Presupuesto Base (`presupuesto_mef`)
* **Grano**: Fila única anual por Unidad Ejecutora (PRONABEC).
* **Uso analítico**: Medir el presupuesto institucional y el avance financiero acumulado total de la institución para cada año fiscal.
* **Renombres aplicados**: Los campos de origen Bronze `ejecutora_codigo` y `ejecutora_nombre` se mapean a `codigo_entidad` y `nombre_entidad` en el modelo Silver para estandarización terminológica.

### 2. Corte Temporal General (`presupuesto_mef_temporal`)
* **Grano**: Año fiscal + Periodo temporal (Mensual/Trimestral/Anual).
* **Uso analítico**: Analizar la velocidad de la ejecución financiera (flujo de devengados) a lo largo del año.
* **Nota de calidad**: No se imputan o inventan meses o trimestres ausentes; se preservan únicamente las filas reportadas oficialmente por el portal del MEF.

### 3. Producto Anual (`presupuesto_mef_producto`)
* **Grano**: Año fiscal + Producto/Proyecto presupuestal.
* **Uso analítico**: Identificar qué productos o proyectos concentran las mayores asignaciones y ejecución de recursos al año.

### 4. Producto Temporal (`presupuesto_mef_producto_temporal`)
* **Grano**: Año fiscal + Periodo temporal + Producto/Proyecto.
* **Uso analítico**: Evaluar la tendencia mensual o trimestral de la ejecución del devengado en cada producto presupuestario.

### 5. Actividad Anual (`presupuesto_mef_actividad`)
* **Grano**: Año fiscal + Producto/Proyecto + Actividad/Acción de Inversión/Obra.
* **Uso analítico**: Detalle operativo interno para comprender los subcomponentes del gasto dentro de cada producto presupuestal. Preserva la relación con su producto padre.

### 6. Actividad Temporal (`presupuesto_mef_actividad_temporal`)
* **Grano**: Año fiscal + Periodo temporal + Producto/Proyecto + Actividad.
* **Uso analítico**: Evolución y ritmo temporal del devengado detallado a nivel de actividad presupuestaria.

### 7. Genérica de Gasto Anual (`presupuesto_mef_generica`)
* **Grano**: Año fiscal + Partida genérica de gasto.
* **Uso analítico**: Clasificar y agrupar el presupuesto según la naturaleza económica del gasto (ej. personal, bienes y servicios, etc.).

### 8. Genérica de Gasto Temporal (`presupuesto_mef_generica_temporal`)
* **Grano**: Año fiscal + Periodo temporal + Partida genérica de gasto.
* **Uso analítico**: Evaluar la estacionalidad y comportamiento mensual/trimestral del gasto agrupado por genérica.

### 9. Jerarquía Presupuestal (`presupuesto_mef_hierarchy`)
* **Grano**: Año fiscal + Nivel de jerarquía gubernamental.
* **Uso analítico**: Comparar la ejecución y asignación de PRONABEC contra los niveles de agregación superiores (Gobierno Nacional, Sector Educación, Pliego M. de Educación).
* **Renombres aplicados**: El campo Bronze `codigo` se mapea a `codigo_entidad` y `descripcion` a `nombre_entidad`.
* **Advertencia metodológica**: Esta tabla no debe agruparse de manera consolidada sin aplicar previamente un filtro estricto sobre la columna `nivel_jerarquia`. Al contener registros en diferentes niveles (ej. Sector, Pliego y Unidad Ejecutora a la vez), una agregación simple causará doble o triple conteo de los mismos fondos públicos.

---

## Limitaciones conocidas

* **Transformación pendiente**: La definición de estos esquemas Silver establece los contratos formales para BigQuery. La lógica de transformación física desde la capa Bronze se implementará de manera independiente en la capa de procesamiento (Dataflow / Beam).
* **Dependencia de origen**: La consistencia de los códigos y descripciones depende enteramente de la estabilidad del portal Consulta Amigable del MEF. Cambios en la nomenclatura oficial o en la jerarquía del Ministerio impactarán la normalización posterior.
