# Política de Canonización de Textos PRONABEC

Esta política establece el marco de diseño, la arquitectura y las reglas de negocio aplicadas para canonizar los valores textuales de PRONABEC en la plataforma analítica, garantizando legibilidad en los reportes (usando tildes y Ñ donde corresponda) y total trazabilidad hacia la fuente original.

---

## 1. Objetivo
El objetivo de la canonización es producir valores consistentes, legibles y listos para Power BI (Gold), evitando la fragmentación producida por errores de captura, variantes de codificación o tipografía en los textos de origen. Todo esto se realiza sin perder el valor original y sin aplicar aproximaciones automáticas difusas no supervisadas.

---

## 2. Capas de la Arquitectura Medallion

Para garantizar la consistencia, el tratamiento del texto se divide estrictamente por capas:

* **Bronze (Crudo/Lakehouse):**
  - Conserva la evidencia original bit a bit.
  - No aplica correcciones ni altera ortografía/codificación.
  - No utiliza aliases ni diccionarios.

* **Silver (Limpieza y Tipado):**
  - Realiza la limpieza técnica (remoción de caracteres de control, normalización de espacios, corrección de mojibake general).
  - Incorpora campos canónicos calculados (`canonical_value`) únicamente cuando existe un mapeo explícitamente aprobado en el catálogo de referencia.
  - Almacena de manera trazable el valor original/limpio y el canónico resultante.

* **Gold (Marts Analíticos / Power BI):**
  - Consume los valores canónicos definitivos para los reportes visuales.
  - Muestra la representación ortográfica correcta (tildes, Ñ, mayúsculas/minúsculas de lectura).
  - Impide que las métricas se dupliquen o se agreguen de manera fragmentada en los tableros.

---

## 3. Diferencia entre Clean, Normalized y Canonical

Para cada texto procesado, diferenciamos los siguientes estados:

| Estado | Significado | Ejemplo |
| :--- | :--- | :--- |
| **Original** | Texto tal como vino de la fuente. | `ARTE & DISEO GRAFICO EMPRESARIAL` |
| **Clean** | Limpieza técnica básica (mojibake corregido). | `ARTE & DISEO GRAFICO EMPRESARIAL` |
| **Normalized** | Clave simplificada para matching (sin acentos/Ñ/puntuación). | `ARTE DISENO GRAFICO EMPRESARIAL` |
| **Canonical** | Valor formal legible aprobado para los dashboards. | `ARTE & DISEÑO GRÁFICO EMPRESARIAL` |

---

## 4. Política de Tildes y Ñ

* **Tildes y Ñ en Gold/Silver:** Se deben mostrar correctamente tildes y eñes en los reportes analíticos para preservar la calidad ortográfica y la formalidad institucional.
* **Normalización para matching:** Las tildes y caracteres especiales se remueven únicamente al generar la clave normalized de comparación (`normalized_key`), permitiendo que el matching ignore diferencias ortográficas del origen.
* **No sustituciones globales directas:** No se permite realizar reemplazos ciegos globales (como cambiar cualquier `N` por `Ñ` o cualquier vocal sin tilde a vocal con tilde), ya que induce a errores semánticos masivos. Las correcciones deben estar sustentadas en aliases explícitos en el catálogo.

---

## 5. Alcance de Canonización

### Dominios Permitidos (Textos Categorizados)
Se canonizan los valores textuales de dimensiones categóricas:
- `programa` (ej: Beca 18, Beca Permanencia)
- `modalidad` (ej: Ordinaria, EIB, Vraem)
- `convocatoria` (ej: Beca 18 Ordinaria Convocatoria 2025)
- `carrera` (ej: Ingeniería Civil, Derecho)
- `institucion` (ej: Pontificia Universidad Católica del Perú)
- `pais` (ej: Perú, España)
- `region` / `departamento`
- `sexo` (ej: Femenino, Masculino)
- `tipo_gestion_colegio` (ej: Pública, Privada)
- `nivel_modalidad`
- `forma_atencion`
- `ugel`

### Dominios Excluidos (Códigos y Métricas)
Bajo ninguna circunstancia se canonizan:
- Códigos lógicos e identificadores (`codigo_ubigeo`, `id_convocatoria`, `codigo_anual`, `source_row_id`).
- Fechas, años (`ano`, `periodo_valor`) o marcas temporales.
- Métricas cuantitativas (montos presupuestales, cantidades, porcentajes).
- Códigos presupuestales del MEF (ej: clasificador de gastos, códigos de producto/actividad).

---

## 6. Fuzzy Matching (Coincidencias Difusas)

* **Deshabilitado en pipelines:** El fuzzy matching automático no está activo para los pipelines de producción Batch. Las asignaciones automáticas basadas en distancia de caracteres (Levenshtein, Jaro-Winkler) tienen alta probabilidad de producir falsos positivos.
* **Solo uso consultivo:** A futuro, algoritmos difusos se usarán exclusivamente para sugerir candidatos en herramientas offline de perfilado de datos, no para decidir fusiones lógicas en tiempo de ejecución de producción.

---

## 7. Catálogo de Referencia y Mapeo Canónico

Los mapeos se gobiernan desde el archivo `config/reference/pronabec_canonical_mappings.yaml` estructurado bajo las siguientes reglas:

* **`canonical_value`:** Nombre oficial visual (con tildes y Ñ).
* **`aliases`:** Lista de posibles variantes textuales encontradas en Bronze.
* **`normalized_key`:** Clave normalizada de control para asegurar coincidencia exacta limpia.
* **`match_method`:** Identificador del origen del mapping (valores permitidos: `manual_alias`, `safe_equivalent`, `profile_candidate`).
* **`confidence`:** Nivel de confianza del match (`high`, `medium`, `low`).
* **`review_required`:**
  - `true`: El mapping es temporal o candidato y requiere validación del negocio antes de aplicarse de forma productiva.
  - `false`: El mapping está completamente validado y es seguro para su despliegue automatizado.

### Ejemplo de Configuración YAML
```yaml
domains:
  carrera:
    - canonical_value: "ARTE & DISEÑO GRÁFICO EMPRESARIAL"
      aliases:
        - "ARTE & DISEO GRAFICO EMPRESARIAL"
        - "ARTE & DISEÑO GRAFICO EMPRESARIAL"
        - "ARTE Y DISEÑO GRAFICO EMPRESARIAL"
      normalized_key: "ARTE DISENO GRAFICO EMPRESARIAL"
      match_method: "manual_alias"
      confidence: "high"
      review_required: true
```

---

## 8. Relación con BigQuery y Power BI

* **Esquemas Silver consistentes:** La incorporación de campos canónicos (como `carrera_canonical` o `institucion_canonical`) debe realizarse en la capa Silver antes del despliegue masivo y la escritura en BigQuery. Esto evita migraciones y DDL updates costosos una vez que las tablas ya están pobladas.
* **Power BI:** Los tableros ejecutivos consumirán exclusivamente los campos canónicos para la agregación de métricas y la consistencia visual, permitiendo a su vez auditorías mediante el cruce con el valor limpio original para resolver dudas.

---

## 9. Flujo Futuro de Desarrollo

Los siguientes pasos en el flujo Dataflow consisten en:
1. `feat(dataflow): apply PRONABEC canonical mappings in silver transforms`
2. `test(dataflow): validate PRONABEC canonical mapping application`
3. `feat(dataflow): write silver records to BigQuery`
