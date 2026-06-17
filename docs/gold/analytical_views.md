# Vistas Analíticas de la Capa Gold y Definición de KPIs

Este documento describe la arquitectura, definición de indicadores y las especificaciones de las vistas analíticas expuestas en la capa **Gold** del Cloud BI Platform de PRONABEC.

Estas vistas consolidan los datos limpios y transformados de la capa **Silver** (que a su vez provienen de la capa **Bronze** y las fuentes MEF y reportes de PRONABEC) para su consumo directo por herramientas de visualización como Power BI, asegurando el cumplimiento de las políticas de gobernanza de datos y consistencia analítica.

---

## 1. Definición de KPIs Clave

### A. PIM Total (`pim_total`)
* **Descripción**: Presupuesto Institucional Modificado asignado a la Unidad Ejecutora de PRONABEC para el año fiscal evaluado. Representa el límite financiero de gasto actualizado después de las modificaciones presupuestarias aprobadas a lo largo del año.
* **Fórmula / Lógica**: 
  $$\text{pim\_total} = \sum(\text{pim})$$
  Consolidado a nivel anual a partir de la tabla Silver `presupuesto_mef` para la entidad de PRONABEC.
* **Unidad de Medida**: Soles (PEN).

### B. Total de Becas Otorgadas (`total_becas_otorgadas`)
* **Descripción**: Cantidad total de becas de estudio otorgadas formalmente por PRONABEC a nivel nacional en las distintas convocatorias anuales.
* **Fórmula / Lógica**:
  $$\text{total\_becas\_otorgadas} = \sum(\text{becas\_otorgadas})$$
  Obtenido del reporte anual de becas otorgadas agrupado por modalidad y año de convocatoria (`pronabec_report_beca18_becas_otorgadas_modalidad_anual`).
* **Unidad de Medida**: Cantidad de becas (Unidades).

### C. Gasto Devengado por Beca (`devengado_por_beca`)
* **Descripción**: Indicador de eficiencia del gasto anual que calcula la relación entre el total ejecutado (devengado) por la institución y el total de becas otorgadas en el mismo periodo.
* **Fórmula / Lógica**:
  $$\text{devengado\_por\_beca} = \frac{\sum(\text{devengado})}{\sum(\text{becas\_otorgadas})}$$
  Es un cálculo calculado dinámicamente mediante `SAFE_DIVIDE` para evitar divisiones por cero en la vista `vw_pronabec_becas_vs_presupuesto_anual`.
* **Advertencia Analítica**: Este ratio es de carácter estrictamente ejecutivo y referencial. La ejecución presupuestaria de un año incluye pagos a becarios activos de convocatorias anteriores (gastos multianuales), por lo que no debe interpretarse como el costo unitario exacto de una beca individual en su ciclo de vida completo.

---

## 2. Catálogo de Vistas Gold

A continuación se detallan las vistas creadas en el dataset Gold, organizadas por componente analítico.

### Componente: Resumen Ejecutivo y Presupuesto

#### Vista: `vw_pronabec_resumen_ejecutivo`
* **Propósito**: Proveer métricas de alto nivel consolidadas para la toma de decisiones gerenciales en una única fila de consulta rápida.
* **Granularidad**: Una única fila agregada (resumen institucional general).
* **Tablas Silver de Origen**:
  - `presupuesto_mef`
  - `pronabec_report_beca18_becas_otorgadas_modalidad_anual`
  - `pronabec_convocatorias`
* **Columnas Expuestas**:
  - `fecha_consulta`: Fecha actual del sistema (`CURRENT_DATE()`).
  - `pia_total`: Presupuesto Institucional de Apertura acumulado.
  - `pim_total`: Presupuesto Institucional Modificado acumulado.
  - `devengado_total`: Ejecución devengada acumulada.
  - `avance_presupuestal_pct`: Porcentaje de avance de la ejecución respecto al PIM ($(\text{devengado} / \text{pim}) \times 100$).
  - `total_becas_otorgadas`: Suma total de becas asignadas.
  - `modalidades_atendidas`: Conteo único de modalidades.
  - `convocatorias_registradas`: Número de convocatorias únicas registradas.
  - `vacantes_registradas`: Suma total de vacantes declaradas en las convocatorias.

> [!IMPORTANT]
> Esta vista realiza un `CROSS JOIN` sobre las métricas agregadas de presupuesto, becas y convocatorias. Cada subconsulta (CTE) está diseñada para retornar exactamente una única fila agrupada sin `GROUP BY`, asegurando que no se produzca una explosión de filas o duplicación cartesiana.

---

### Componente: Evolución de Beca 18

#### Vista: `vw_beca18_becas_otorgadas_anual`
* **Propósito**: Analizar la tendencia de becas otorgadas a lo largo de las convocatorias anuales y por modalidad de postulación.
* **Granularidad**: Modalidad de postulación y año de convocatoria.
* **Tablas Silver de Origen**:
  - `pronabec_report_beca18_becas_otorgadas_modalidad_anual`

#### Vista: `vw_beca18_universitarios_carrera_anual`
* **Propósito**: Monitorear la distribución de becarios de modalidad universitaria a nivel de carrera elegida y año. Incluye campos canónicos para normalización textual de carreras.
* **Granularidad**: Carrera, año de convocatoria.
* **Tablas Silver de Origen**:
  - `pronabec_report_beca18_universitarios_carrera_anual`
* **Columnas Expuestas Destacadas**:
  - `carrera_estudio_final`: Carrera de estudio final (`COALESCE` entre el valor canónico normalizado y el original si no hay match).
  - `carrera_estudio_canonical_match_method`: Metodología de normalización aplicada (ej. exact, alias).
  - `carrera_estudio_canonical_review_required`: Flag indicador si el registro requiere revisión manual.

#### Vista: `vw_beca18_universitarios_universidad_anual`
* **Propósito**: Analizar la asignación de becarios universitarios por institución de educación superior (IES) y año.
* **Granularidad**: Universidad (IES), año de convocatoria.
* **Tablas Silver de Origen**:
  - `pronabec_report_beca18_universitarios_universidad_anual`
* **Columnas Expuestas Destacadas**:
  - `universidad_final`: Universidad final (con `COALESCE` sobre el valor canónico).

---

### Componente: Perfil Social y PES 2025

#### Vista: `vw_beca18_perfil_social_indicadores`
* **Propósito**: Consolidar en formato largo ("long format") múltiples dimensiones demográficas, familiares, étnicas, lingüísticas y de preparación educativa de los becarios de Beca 18 para la encuesta de satisfacción y caracterización PES 2025.
* **Granularidad**: Indicador, grupo, categoría, subcategoría y periodo de análisis.
* **Tablas Silver de Origen**:
  - `pronabec_report_beca18_autoidentificacion_etnica_modalidad_2025`
  - `pronabec_report_beca18_lengua_materna_modalidad_2025`
  - `pronabec_report_beca18_sexo_anual`
  - `pronabec_report_beca18_colegio_gestion_2025`
  - `pronabec_report_beca18_padres_nivel_educativo_2025`
  - `pronabec_report_beca18_primera_generacion_region`
  - `pronabec_report_beca18_no_continuaria_sin_beca_caracteristica_2025`
  - `pronabec_report_beca18_preparacion_ies_tipo_2025`
  - `pronabec_report_beca18_preparacion_ies_meses_caracteristica_2025`
  - `pronabec_report_beca18_enp_promedio_caracteristica_2025`
  - `pronabec_report_beca18_enp_promedio_region_2025`
  - `pronabec_report_beca18_periodo_ingreso_ies_genero_2025`

> [!NOTE]
> Esta vista consolida 12 orígenes distintos utilizando operaciones `UNION ALL` para proyectar las métricas sobre una estructura unificada de formato largo. Esto facilita la creación de gráficos dinámicos y tableros interactivos en Power BI sin necesidad de modelar relaciones complejas de uno a muchos o joins anidados fila a fila.

#### Vista: `vw_beca18_region_postulacion`
* **Propósito**: Mostrar la procedencia geográfica de los postulantes y becarios tanto en series anuales, acumuladas como en la encuesta específica de 2025.
* **Granularidad**: Tipo de registro, región y periodo.
* **Tablas Silver de Origen**:
  - `pronabec_report_beca18_region_postulacion_anual`
  - `pronabec_report_beca18_region_postulacion_acumulada`
  - `pronabec_report_beca18_region_postulacion_2025`

#### Vista: `vw_beca18_migracion_region`
* **Propósito**: Analizar la tasa de migración de los becarios desde su región de origen hacia la región de estudio, tanto a nivel anual como acumulado histórico.
* **Granularidad**: Tipo de registro, región y periodo.
* **Tablas Silver de Origen**:
  - `pronabec_report_beca18_migracion_region_anual`
  - `pronabec_report_beca18_migracion_region_acumulada`

#### Vista: `vw_beca18_trayectoria_eleccion`
* **Propósito**: Comparar las motivaciones detrás de la elección de carrera e institución educativa (IES) segmentada por género y tipo de gestión de la institución.
* **Granularidad**: Dimensión de análisis, motivo, segmento de análisis.
* **Tablas Silver de Origen**:
  - `pronabec_report_beca18_razones_eleccion_carrera_gestion_ies_2025`
  - `pronabec_report_beca18_razones_eleccion_carrera_sexo_2025`
  - `pronabec_report_beca18_razones_eleccion_ies_gestion_2025`

---

### Componente: Ejecución Presupuestal MEF

#### Vista: `vw_mef_presupuesto_ejecucion_anual`
* **Propósito**: Mostrar la evolución y estado actual de la ejecución presupuestal institucional de PRONABEC a nivel anual.
* **Granularidad**: Año, entidad ejecutora.
* **Tablas Silver de Origen**:
  - `presupuesto_mef`

#### Vista: `vw_mef_presupuesto_ejecucion_temporal`
* **Propósito**: Analizar la ejecución devengada de forma mensual o trimestral para evaluar la estacionalidad del gasto.
* **Granularidad**: Año, trimestre, mes.
* **Tablas Silver de Origen**:
  - `presupuesto_mef_temporal`

#### Vista: `vw_mef_presupuesto_producto`
* **Propósito**: Monitorear la asignación y ejecución del gasto por producto o proyecto institucional de la cartera de PRONABEC.
* **Granularidad**: Año, producto/proyecto.
* **Tablas Silver de Origen**:
  - `presupuesto_mef_producto`

#### Vista: `vw_mef_presupuesto_actividad`
* **Propósito**: Mayor nivel de detalle del presupuesto MEF evaluando las actividades específicas asociadas a cada producto.
* **Granularidad**: Año, producto, actividad.
* **Tablas Silver de Origen**:
  - `presupuesto_mef_actividad`

#### Vista: `vw_mef_presupuesto_generica`
* **Propósito**: Analizar el gasto de acuerdo a la clasificación económica o genérica de gasto (ej. Personal, Bienes y Servicios, Donaciones y Transferencias).
* **Granularidad**: Año, genérica de gasto.
* **Tablas Silver de Origen**:
  - `presupuesto_mef_generica`

---

### Componente: Cruce Multidimensional

#### Vista: `vw_pronabec_becas_vs_presupuesto_anual`
* **Propósito**: Relacionar de forma analítica el volumen anual de becas otorgadas con el presupuesto institucional devengado y modificado (PIM) de PRONABEC.
* **Granularidad**: Año fiscal.
* **Tablas Silver de Origen**:
  - `pronabec_report_beca18_becas_otorgadas_modalidad_anual`
  - `presupuesto_mef`
* **Columnas Clave Calculadas**:
  - `avance_presupuestal_pct`: Porcentaje general de avance financiero.
  - `devengado_por_beca`: Gasto ejecutado total del año dividido entre el número de becas otorgadas en ese año (ver advertencia analítica en la sección 1).
  - `pim_por_beca`: Presupuesto modificado del año dividido entre las becas otorgadas en el mismo periodo.

---

## 3. Advertencias Analíticas y Reglas de Negocio

> [!WARNING]
> **Consistencia Temporal y Métricas Presupuestarias Obsoletas**:
> - En esta iteración de la capa Gold se han retirado por completo todas las columnas presupuestarias obsoletas que ya no están soportadas en la estructura de presupuesto MEF Silver actual, tales como: `girado`, `certificacion`, `compromiso_anual`, `compromiso_mensual` y `saldo_no_ejecutado`. Todo análisis financiero debe basarse estrictamente en `pia`, `pim` y `devengado`.
> - La tabla Silver de jerarquía (`presupuesto_mef_hierarchy`) se utiliza con fines contextuales; bajo ninguna circunstancia se debe aplicar la función `SUM()` directamente a sus filas, ya que los montos a nivel de cadena de jerarquía duplicarían de forma artificial el valor total ejecutado.

> [!IMPORTANT]
> **Exclusiones de Gobernabilidad**:
> - De acuerdo a las políticas de gobernanza definidas, se han excluido del catálogo de vistas Gold las tablas que pertenecen a ámbitos de revisión técnica preliminar o candidatos, tales como `pronabec_beca18_becarios_provincia_2016` y `pronabec_convocatorias_carrera_sede`. Estas fuentes contienen alcances restringidos y no son aptas para reportería oficial institucional.

> [!NOTE]
> **Gobernanza Textual y Campos Canónicos**:
> - Al construir tableros o reportes, se debe priorizar siempre el uso de los campos con sufijo `_final` o `_canonical` (ej. `carrera_estudio_final`, `universidad_final`) en lugar de sus contrapartes originales sin normalizar. Esto reduce la fragmentación de categorías en los filtros del dashboard debido a variaciones en la ortografía, mayúsculas/minúsculas o abreviaciones en los reportes manuales.
