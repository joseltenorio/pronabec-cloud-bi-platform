# Datasets PRONABEC Bronze-Only

Este documento detalla los datasets PRONABEC que fueron excluidos de la promoción a la capa Silver y quedan clasificados como `BRONZE_ONLY`.

## Objetivo

Mantener una capa Silver limpia, confiable y útil para análisis. Los datasets con bajo valor analítico, información aislada, cobertura limitada o alto riesgo de interpretación se conservan únicamente en Bronze. Esta decisión evita confusión, reduce complejidad innecesaria en los pipelines y protege la calidad del modelo analítico.

## Criterios de exclusión

Un dataset se clasifica como `BRONZE_ONLY` si cumple una o más de las siguientes condiciones:

1. **Falta de llaves relacionales**: no puede vincularse de forma confiable con entidades principales como convocatorias, becarios, instituciones o ubicaciones.
2. **Baja calidad, escasez o cobertura limitada**: presenta pocos registros, información incompleta o cobertura temporal insuficiente.
3. **Ausencia de métrica analítica directa**: contiene metadatos, catálogos aislados o información que no permite construir indicadores útiles.
4. **Riesgo de interpretación**: su uso puede inducir conclusiones incorrectas por limitaciones de origen, falta de denominadores o ambigüedad semántica.

## Datasets clasificados como Bronze-Only

Los siguientes cinco datasets PRONABEC no pasan a Silver y permanecen estrictamente en la capa Bronze:

---

### 1. concepto_pago

- **Decisión**: `BRONZE_ONLY`
- **Motivo técnico y analítico**: este dataset contiene conceptos y subconceptos de pago, pero no los relaciona con montos reales, estudiantes, convocatorias ni periodos analíticos claros. Al no contar con llaves de negocio suficientes, no permite integrarse de forma confiable a un dashboard de gasto, subvenciones o costos.

---

### 2. notas_becarios

- **Decisión**: `BRONZE_ONLY`
- **Motivo técnico y analítico**: aunque el rendimiento académico es un concepto de alto valor, la información pública disponible en este dataset es dispersa, histórica y carece de contexto suficiente, como escalas de calificación por institución, duración de carreras o continuidad completa por becario. Cargarlo a Silver podría crear la impresión equivocada de que existe un seguimiento académico completo y defendible.

---

### 3. periodos_academicos

- **Decisión**: `BRONZE_ONLY`
- **Motivo técnico y analítico**: funciona principalmente como un catálogo de periodos académicos, años y meses. Sin embargo, no aporta una relación analítica clara con las tablas principales, ya que las convocatorias y otros datasets pueden modelarse directamente con campos de fecha o periodo propios.

---

### 4. nota_promedio_postulante_region

- **Decisión**: `BRONZE_ONLY`
- **Motivo técnico y analítico**: es un dataset pequeño, incompleto y con apariencia de fotografía histórica estática. Su baja cobertura limita la significancia estadística y no permite analizar de manera confiable el nivel académico regional ni la calidad de los postulantes.

---

### 5. perdida_becas

- **Decisión**: `BRONZE_ONLY`
- **Motivo técnico y analítico**: aunque conceptualmente podría aportar al análisis de pérdida o deserción de becas, los registros disponibles no permiten interpretar con claridad la cobertura real del fenómeno. Además, no existen denominadores adecuados, como total de becarios por año o total de beneficiarios expuestos al riesgo, por lo que no se pueden calcular tasas de pérdida, retención o deserción de forma confiable. Usar conteos crudos en dashboards generaría una visión distorsionada del desempeño del programa.

---

## Preservación en Bronze

Excluir estos datasets de Silver no significa eliminarlos de la plataforma. Sus datos crudos se conservan en la capa Bronze para trazabilidad, auditoría y revisión histórica.

Los extractores pueden seguir preservando los archivos originales bajo la estructura:

```text
gs://<bucket>/bronze/pronabec/<dataset>/extraction_date=YYYY-MM-DD/data_raw.json
gs://<bucket>/bronze/pronabec/<dataset>/extraction_date=YYYY-MM-DD/data.jsonl
```

La decisión `BRONZE_ONLY` indica únicamente que estos datasets no forman parte del contrato analítico Silver vigente.
