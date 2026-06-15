# Datasets PRONABEC candidatos para Silver

Este documento detalla los datasets PRONABEC que pasan a la capa Silver como candidatos de revisión. Estos datasets contienen información potencialmente útil, pero presentan restricciones de cobertura, riesgos de interpretación o alcance histórico limitado.

## Criterios de selección como candidato

Un dataset se clasifica como candidato cuando cumple una o más de las siguientes condiciones:

1. **Cobertura temporal o geográfica limitada**: representa una fotografía histórica, un subconjunto específico o una cobertura no necesariamente continua.
2. **Formato agregado o mixto**: combina registros de detalle con totales regionales o nacionales dentro del mismo archivo.
3. **Alcance restringido**: requiere contexto de negocio para evitar interpretaciones incorrectas.
4. **Valor analítico condicionado**: puede aportar a análisis exploratorios o dimensiones auxiliares, pero no debe tratarse como tabla central sin restricciones.

Promover estos datasets a Silver permite contar con contratos estructurados, tipos consistentes y trazabilidad técnica. Sin embargo, su uso debe respetar las advertencias documentadas.

## Datasets candidatos

Los siguientes datasets quedan clasificados como candidatos:

1. `pronabec_beca18_becarios_provincia_2016`
2. `pronabec_convocatorias_carrera_sede`

---

### 1. pronabec_beca18_becarios_provincia_2016

- **Dataset origen**: `BECARIOS_PROVINCIA` en Bronze.
- **Motivo del cambio de nombre**: el nombre original `becarios_provincia` es demasiado genérico y podría sugerir que cubre todos los años, programas o convocatorias. El análisis funcional indica que representa una fotografía histórica de beneficiarios de Beca 18 por provincia, aproximadamente asociada al periodo disponible alrededor de 2016. El nuevo nombre explicita su alcance histórico y su restricción al programa Beca 18.

#### Columnas conservadas

- `source_row_id` (`INT64` / `INTEGER`): identificador técnico para trazabilidad hacia Bronze.
- `region` (`STRING`): nombre del departamento o región.
- `provincia` (`STRING`): nombre de la provincia.
- `becarios_b18_count` (`INT64` / `INTEGER`): cantidad de beneficiarios de Beca 18. Campo renombrado desde `b18_n` / `b18n`.
- `aggregation_scope` (`STRING`): indicador del nivel de agregación del registro. Valores esperados: `PROVINCIA`, `REGION_TOTAL`, `NATIONAL_TOTAL`.

#### Columnas descartadas

- `nro_fila`: eliminado por redundancia con `source_row_id`.
- `b18_pct` / `b18pct`: eliminado porque el porcentaje puede calcularse desde conteos y totales en Gold o Power BI.
- Otras columnas de programas o métricas con registros dispersos, incompletos o poco confiables.

#### Riesgos de interpretación y uso permitido

**Uso permitido**:

- Analizar de forma referencial la distribución territorial histórica de beneficiarios de Beca 18 por región y provincia.
- Explorar concentración territorial en una fotografía histórica específica.

**Uso restringido**:

- No usar esta tabla como serie histórica completa.
- No interpretarla como registro actualizado de beneficiarios.
- No comparar directamente sus totales con años recientes como si fueran tendencia.
- No sumar registros sin filtrar `aggregation_scope`, porque se pueden duplicar totales regionales o nacionales.

---

### 2. pronabec_convocatorias_carrera_sede

- **Dataset origen**: `ConvocatoriaPorCarreraSede` en Bronze.
- **Motivo de promoción**: contiene la oferta elegible de instituciones, sedes y carreras por convocatoria. Es útil para analizar disponibilidad educativa asociada a convocatorias, pero no debe confundirse con matrícula real ni con beneficiarios efectivos.

#### Columnas conservadas

- `source_row_id` (`INT64` / `INTEGER`): identificador técnico para trazabilidad hacia Bronze.
- `id_convocatoria` (`INT64` / `INTEGER`): llave de negocio potencial para relacionarse con `pronabec_convocatorias`.
- `pais_origen` (`STRING`): país de origen de la institución educativa.
- `nivel_educativo` (`STRING`): nivel educativo asociado, por ejemplo pregrado.
- `tipo_institucion` (`STRING`): tipo de institución.
- `sede` (`STRING`): sede o campus.
- `institucion` (`STRING`): nombre de la institución educativa superior.
- `carrera` (`STRING`): carrera o programa elegible.
- `gestion_ies` (`STRING`): tipo de gestión de la institución de educación superior, renombrado desde `gestion`.
- `ruc` (`STRING`): número de RUC de la institución. Se conserva como `STRING` para evitar pérdida de formato.

#### Columnas descartadas

- `nro_fila`: eliminado por redundancia con `source_row_id`.
- `resolucion`: descartado por no aportar al análisis estructural de oferta.
- `abreviatura`: descartado por redundancia o baja utilidad analítica.
- `region`: descartado por alta nulidad o baja confiabilidad observada.
- `web`, `representante`, `telefono`, `email`: descartados por no formar parte del modelo analítico.
- `fecha_carga`: descartado como campo de negocio; la trazabilidad técnica se maneja con metadata estándar.

#### Riesgos de interpretación y uso permitido

**Uso permitido**:

- Analizar oferta elegible por convocatoria, institución, sede, carrera, nivel educativo y tipo de gestión.
- Explorar la relación entre convocatorias y oferta académica mediante `id_convocatoria`.
- Construir dimensiones auxiliares de instituciones, sedes o carreras elegibles.

**Uso restringido**:

- No usar esta tabla para contar becarios activos.
- No usarla como matrícula real.
- No interpretarla como distribución de beneficiarios.
- No asumir que la lista representa toda la oferta nacional de educación superior.

---

## Campos técnicos y metadata

Estas tablas incluyen la metadata estándar de la plataforma:

- `source_system` (`STRING`): sistema origen, por ejemplo `PRONABEC`.
- `source_dataset` (`STRING`): nombre del dataset de origen.
- `extraction_date` (`DATE`): fecha lógica de extracción.
- `ingestion_timestamp` (`TIMESTAMP`): timestamp físico de escritura en Silver.
- `pipeline_run_id` (`STRING`): identificador único de ejecución del pipeline.

La metadata permite auditoría, trazabilidad y control operativo sin depender de campos visuales o numeraciones internas del portal.
