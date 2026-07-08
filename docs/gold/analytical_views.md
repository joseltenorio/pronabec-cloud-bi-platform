# Modelo Analítico de la Capa Gold (Vistas de Reportería)

Este documento describe la arquitectura de datos, el diseño y el catálogo analítico de la capa Gold del proyecto. La capa Gold provee las vistas analíticas expuestas y optimizadas para el consumo final por parte de Power BI y modelos de inteligencia de negocios.

---

## 1. Objetivo de Gold
La capa Gold convierte los datos integrados y estructurados de la capa Silver en vistas y marts analíticos listos para reportería. Su propósito es responder a preguntas de negocio a nivel ejecutivo y táctico, aplicando agrupaciones y cálculos de indicadores clave (KPIs).

La publicación de estas vistas se ejecuta de forma idempotente desde Cloud Run mediante `pipelines.publish_gold_views`, y su contrato operativo se valida con `pipelines.validate_gold` antes de que Composer permita continuar con calidad y consumo downstream.

**Reglas de oro de la capa Gold:**
- **Consumo controlado de capas curadas:** Las vistas Gold clásicas leen desde Silver (`{project_id}.{silver_dataset}`). Las salidas predictivas se publican desde objetos curados en `ml` y se exponen sin recalcular modelos en Gold.
- **No duplicación de limpieza:** Gold no realiza tareas de limpieza, tipado fuerte, conversión de formatos (guiones, nulos, coma decimal), normalización de columnas anchas o canonización. Ese trabajo pertenece estrictamente a la capa Silver / Dataflow.
- **Grano e integridad:** No se deben realizar joins fila a fila que crucen granularidades incompatibles. Gold unifica indicadores de grano diverso en formato largo (`UNION ALL`) o mediante agregaciones intermedias seguras.

---

## 2. Principios de Diseño
- **Placeholders dinámicos:** Todas las vistas analíticas se crean utilizando placeholders (`{project_id}`, `{silver_dataset}`, `{gold_dataset}`) permitiendo que el script sea agnóstico de entornos y proyectos específicos de GCP.
- **CROSS JOINs controlados:** En las vistas de resumen ejecutivo, se limita el uso de joins cruzados (`CROSS JOIN`) a expresiones CTE que garanticen retornar exactamente una fila agregada, previniendo la multiplicación cartesiana de registros.
- **Campos Canónicos con Fallback:** Para análisis institucionales y de carrera, se priorizan los campos canonizados paralelos de Silver (`universidad_canonical`, `carrera_estudio_canonical`). Si son nulos, se utiliza un fallback seguro al campo original limpio (`COALESCE`).

---

## 3. Catálogo de Vistas Gold

### 1. `vw_pronabec_resumen_ejecutivo`
* **Objetivo:** Ofrecer un consolidado instantáneo de KPIs de alto nivel para la cabecera ejecutiva del dashboard.
* **Fuentes Silver:** `presupuesto_mef`, `pronabec_report_beca18_becas_otorgadas_modalidad_anual`, `pronabec_convocatorias`.
* **Grano:** 1 fila única consolidada (KPIs generales).
* **KPIs calculados:** `pia_total`, `pim_total`, `devengado_total`, `avance_presupuestal_pct`, `total_becas_otorgadas`, `modalidades_atendidas`, `convocatorias_registradas`, `vacantes_registradas`.
* **Uso en Power BI:** Tarjetas ejecutivas principales y KPIs destacados.
* **Advertencias:** Los indicadores corresponden al acumulado general de todas las ejecuciones cargadas en Silver.

### 2. `vw_beca18_becas_otorgadas_anual`
* **Objetivo:** Evolución histórica de la cobertura del programa Beca 18.
* **Fuentes Silver:** `pronabec_report_beca18_becas_otorgadas_modalidad_anual`.
* **Grano:** Año de convocatoria + Modalidad de beca.
* **KPIs calculados:** `becas_otorgadas`.
* **Uso en Power BI:** Gráficos de líneas e históricos de barras sobre cobertura de becas.

### 2b. `vw_beca18_cobertura_territorial_2016`
* **Objetivo:** Analizar la distribución de becarios otorgados a nivel de región y provincia desde la convocatoria 2016.
* **Fuentes Silver:** `pronabec_beca18_becarios_provincia_2016`.
* **Grano:** Región + Provincia + Fecha de snapshot.
* **KPIs calculados:** `becarios_b18_count`, `ranking_nacional_provincia`, `total_region_b18`, `participacion_provincia_region`.
* **Uso en Power BI:** Mapas de calor, tablas de distribución geográfica e indicadores de participación provincial.
* **Filtros:** Incluye filtros defensivos para omitir registros con agregados de totales provinciales.

### 3. `vw_beca18_universitarios_carrera_anual`
* **Objetivo:** Distribución y ranking de becarios según carrera de estudio universitaria.
* **Fuentes Silver:** `pronabec_report_beca18_universitarios_carrera_anual`.
* **Grano:** Año de convocatoria + Carrera de estudio.
* **KPIs calculados:** `cantidad_becarios`.
* **Uso en Power BI:** Matriz de carreras, gráficos de torta/treemap y tops analíticos.
* **Canonización:** Utiliza `carrera_estudio_final` resolviendo el fallback a través de la canonización de carreras en Silver.

### 4. `vw_beca18_universitarios_universidad_anual`
* **Objetivo:** Distribución de becarios según IES (Universidad) receptora.
* **Fuentes Silver:** `pronabec_report_beca18_universitarios_universidad_anual`.
* **Grano:** Año de convocatoria + Universidad.
* **KPIs calculados:** `cantidad_becarios`.
* **Uso en Power BI:** Matriz de universidades, rankings de instituciones públicas vs privadas.
* **Canonización:** Utiliza `universidad_final` resolviendo el fallback a través de la canonización de universidades en Silver.

### 5. `vw_beca18_perfil_social_indicadores`
* **Objetivo:** Estandarizar bajo un formato largo unificado los diversos indicadores sociales provenientes de encuestas de PES 2025. Evita joins destructivos y simplifica el consumo de gráficos categóricos en Power BI.
* **Fuentes Silver:** 
  - `pronabec_report_beca18_autoidentificacion_etnica_modalidad_2025`
  - `pronabec_report_beca18_lengua_materna_modalidad_2025`
  - `pronabec_report_beca18_colegio_gestion_2025`
  - `pronabec_report_beca18_padres_nivel_educativo_2025`
  - `pronabec_report_beca18_primera_generacion_region`
  - `pronabec_report_beca18_no_continuaria_sin_beca_caracteristica_2025`
  - `pronabec_report_beca18_preparacion_ies_tipo_2025`
  - `pronabec_report_beca18_preparacion_ies_meses_caracteristica_2025`
  - `pronabec_report_beca18_sexo_anual`
  - `pronabec_report_beca18_enp_promedio_caracteristica_2025`
  - `pronabec_report_beca18_enp_promedio_region_2025`
  - `pronabec_report_beca18_periodo_ingreso_ies_genero_2025`
* **Grano:** Estandarizado por `indicator_group` + `indicator_name` + `category` + `subcategory` + `period`.
* **KPIs calculados:** `value_percentage` (porcentaje acumulado o tasa), `value_count` (conteos crudos cuando apliquen).
* **Uso en Power BI:** Filtros, gráficos de barras de desglose demográfico y perfiles sociales.
* **Advertencias:** Los registros de PES 2025 representan datos agregados de encuestas y reportes publicados oficiales, no microdatos de estudiantes individuales.

### 6. `vw_beca18_region_postulacion`
* **Objetivo:** Unificación analítica de la cobertura territorial y postulación de becarios.
* **Fuentes Silver:** `pronabec_report_beca18_region_postulacion_anual`, `pronabec_report_beca18_region_postulacion_acumulada`, `pronabec_report_beca18_region_postulacion_2025`.
* **Grano:** Tipo de registro (anual, acumulado o instantáneo) + Región + Periodo.
* **KPIs calculados:** `porcentaje_becarios` (tasas regionales).

### 7. `vw_beca18_migracion_region`
* **Objetivo:** Analizar las tasas de migración estudiantil (becarios que estudian fuera de su región de postulación).
* **Fuentes Silver:** `pronabec_report_beca18_migracion_region_anual`, `pronabec_report_beca18_migracion_region_acumulada`.
* **Grano:** Tipo de registro + Región + Periodo.
* **KPIs calculados:** `tasa_migracion`.

### 8. `vw_beca18_trayectoria_eleccion`
* **Objetivo:** Detallar las razones declaradas por los becarios para elegir su carrera o IES.
* **Fuentes Silver:** `pronabec_report_beca18_razones_eleccion_carrera_gestion_ies_2025`, `pronabec_report_beca18_razones_eleccion_carrera_sexo_2025`, `pronabec_report_beca18_razones_eleccion_ies_gestion_2025`.
* **Grano:** Dimensión de análisis + Motivo + Segmento.
* **KPIs calculados:** `porcentaje_becarios`.

### 9. `vw_mef_presupuesto_ejecucion_anual`
* **Objetivo:** Evaluar la evolución del presupuesto del pliego presupuestal asignado a PRONABEC.
* **Fuentes Silver:** `presupuesto_mef`.
* **Grano:** Año fiscal + Entidad.
* **KPIs calculados:** `pia`, `pim`, `devengado`, `avance_porcentaje`, `avance_calculado_pct`.

### 10. `vw_mef_presupuesto_ejecucion_temporal`
* **Objetivo:** Monitoreo y estacionalidad de la ejecución del gasto a nivel temporal (mensual o trimestral).
* **Fuentes Silver:** `presupuesto_mef_temporal`.
* **Grano:** Año fiscal + Periodo temporal (Mes / Trimestre).
* **KPIs calculados:** `devengado`.

### 11. `vw_mef_presupuesto_producto`
* **Objetivo:** Presupuesto y avance clasificado por producto o proyecto de inversión.
* **Fuentes Silver:** `presupuesto_mef_producto`.
* **Grano:** Año fiscal + Producto/Proyecto.
* **KPIs calculados:** `pia`, `pim`, `devengado`, `avance_calculado_pct`.

### 12. `vw_mef_presupuesto_actividad`
* **Objetivo:** Presupuesto y avance clasificado por actividad operativa.
* **Fuentes Silver:** `presupuesto_mef_actividad`.
* **Grano:** Año fiscal + Producto + Actividad.
* **KPIs calculados:** `pia`, `pim`, `devengado`, `avance_calculado_pct`.

### 13. `vw_mef_presupuesto_generica`
* **Objetivo:** Avance por categoría de genérica de gasto presupuestal (bienes, servicios, personal, etc.).
* **Fuentes Silver:** `presupuesto_mef_generica`.
* **Grano:** Año fiscal + Genérica de gasto.
* **KPIs calculados:** `pia`, `pim`, `devengado`, `avance_calculado_pct`.

### 14. `vw_pronabec_becas_vs_presupuesto_anual`
* **Objetivo:** Relacionar de forma agregada el presupuesto anual institucional frente a las becas otorgadas para propósitos de comparación ejecutiva de eficiencia e inversión.
* **Fuentes Silver:** `presupuesto_mef` (agrupado por año) y `pronabec_report_beca18_becas_otorgadas_modalidad_anual` (agrupado por año).
* **Grano:** Año.
* **KPIs calculados:** `becas_otorgadas_total`, `pia_total`, `pim_total`, `devengado_total`, `avance_presupuestal_pct`, `devengado_por_beca` (referencial), `pim_por_beca` (referencial).
* **Advertencias Metodológicas:** Los ratios `devengado_por_beca` y `pim_por_beca` son indicadores financieros consolidados e indirectos que dividen el presupuesto total ejecutado del pliego entre las becas otorgadas en el año. No representan de forma estricta el costo directo o la subvención individual asignada por becario (el cual requiere análisis transaccional de planillas fuera del alcance de este modelo agregado).

### 15. `vw_pronabec_beca18_resumen_analitico`
* **Objetivo:** Vista resumen ejecutiva que consolida y compara los indicadores principales de reportes Beca 18 y presupuestos ejecutados MEF a nivel anual.
* **Fuentes Silver:** `pronabec_report_beca18_becas_otorgadas_modalidad_anual` (por año) y `presupuesto_mef` (por año).
* **Grano:** Año.
* **KPIs calculados:** `total_becas_otorgadas`, `total_pia`, `total_pim`, `total_devengado`, `avance_presupuestal_pct`, `costo_promedio_por_beca`.

### 16. `vw_predictive_region_priority_scores`
* **Objetivo:** Exponer para Power BI el score regional explicable de prioridad predictiva sin recalcularlo.
* **Fuente curada:** `ml.region_priority_scores`.
* **Grano:** Año + región canónica.
* **KPIs calculados:** `priority_score`, `priority_score_pct`, `priority_rank`, `priority_tier`.
* **Uso en Power BI:** Ranking regional, semáforos de prioridad y mapas temáticos.
* **Advertencias metodológicas:** La vista no es causal ni individual; solo resume contexto regional ponderado.

### 17. `vw_predictive_region_priority_scores_v2`
* **Objetivo:** Exponer el score regional v2 con señales de contexto, cobertura y primera generacion.
* **Fuente curada:** `ml.region_priority_scores_v2`.
* **Grano:** Año + region canonica.
* **Uso en Power BI:** Priorizacion regional, ranking y comparacion de componentes.
* **Advertencias metodologicas:** No es causal ni individual.

### 18. `vw_predictive_region_clusters`
* **Objetivo:** Exponer las asignaciones del modelo KMeans regional sin recalcular `ML.PREDICT` en Gold.
* **Fuente curada:** `ml.region_cluster_assignments`.
* **Grano:** Año + region canonica.
* **Uso en Power BI:** Segmentacion territorial y comparacion de clusters.
* **Advertencias metodologicas:** KMeans es no supervisado. Las etiquetas son genericas y deben interpretarse con perfiles promedio.

### 19. `vw_predictive_region_cluster_profiles`
* **Objetivo:** Publicar promedios por cluster para interpretar los centroides.
* **Fuente curada:** `ml.region_cluster_profiles`.
* **Grano:** Centroide.
* **Uso en Power BI:** Perfilamiento de segmentos regionales.

### 20. `vw_predictive_budget_forecast`
* **Objetivo:** Exponer el forecast presupuestal mensual agregado producido por ARIMA_PLUS.
* **Fuente curada:** `ml.budget_forecast_results`.
* **Grano:** Mes pronosticado.
* **Uso en Power BI:** Escenario referencial de devengado mensual.
* **Advertencias metodologicas:** Es referencial, no causal y no representa compromisos financieros.

---

## 4. Clasificación y Granularidad de MEF
El modelado presupuestal utiliza los facts atómicos de Silver para evitar dobles contabilidades:
- Para el análisis general se consume `vw_mef_presupuesto_ejecucion_anual`.
- Para el análisis por categoría se usan las dimensiones correspondientes (`vw_mef_presupuesto_producto`, `vw_mef_presupuesto_actividad` o `vw_mef_presupuesto_generica`).
- **Uso de Jerarquías:** La tabla `presupuesto_mef_hierarchy` es un contexto de orden y estructuración multinivel. Para evitar duplicaciones financieras catastróficas, las vistas analíticas Gold **no realizan sumatorias agregadas (`SUM`)** directas sobre registros de jerarquía.

---

## 5. Control de Calidad de Datos (Relación con Quality)
Las métricas expuestas en la capa Gold asumen que los controles de calidad en Silver han sido validados:
- Si el motor de auditoría (`audit_data_quality_results`) registra anomalías críticas de calidad en tablas de origen Silver (ej. duplicación de llaves, nulos inconsistentes o fallas de integridad referencial), los usuarios de Power BI deben ser alertados y los indicadores reportados no deben ser considerados definitivos hasta su remediación.

---

## 6. Integración y Conexión en Power BI
- **Acceso:** Power BI debe conectarse exclusivamente al dataset `gold` en BigQuery utilizando la conexión nativa DirectQuery o Import.
- **Prohibición:** Está terminantemente prohibido conectar tableros o reportes directos del usuario de negocio a la capa `bronze` para evitar latencias, lecturas innecesarias e inconsistencias de datos. La capa `silver` sólo se expone para propósitos de desarrollo, pruebas de auditoría técnica o depuración.

## 7. Extensión predictiva regional y presupuestal

La vista `vw_predictive_region_priority_scores_v2` amplía la salida regional existente con cobertura PRONABEC y primera generación. Su fuente curada es `ml.region_priority_scores_v2`, por lo que Gold sigue siendo una capa de exposición y no de recálculo.

Las vistas `vw_predictive_region_clusters`, `vw_predictive_region_cluster_profiles` y `vw_predictive_budget_forecast` siguen el mismo principio: consumen resultados del dataset `ml` y no ejecutan modelos dentro de Gold.
