# Datasets MINEDU aprobados para la capa Silver

## Dataset seleccionado

### `silver.minedu_matricula_secundaria_departamental`

Grano:

- `anio`
- `codigo_departamento`
- `grado`

Columnas derivadas:

- `region_normalizada`
- `grado_orden`
- `publica_pct`
- `privada_pct`
- `urbana_pct`
- `rural_pct`
- `masculino_pct`
- `femenino_pct`

## Uso esperado

Este dataset Silver deja preparada la fuente MINEDU ESCALE como insumo regional para PRs posteriores de features y analítica predictiva. Quinto grado debe leerse solo como proxy de demanda potencial inmediata y no como demanda real de becas.
