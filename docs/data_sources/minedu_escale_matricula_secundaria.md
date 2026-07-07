# MINEDU ESCALE - Matrícula de Educación Secundaria por Departamento

## Objetivo
Explicar esta fuente como insumo oficial de demanda educativa regional para análisis futuros de cobertura y priorización territorial.

## Fuente
URL base:

`https://escale.minedu.gob.pe/magnitudes-portlet/reporte/cuadro`

Parámetros relevantes:

- `anio`
- `cuadro`
- `dpto`

Parámetros fijos del scraper:

- `forma=U`
- `prov=`
- `dre=`
- `tipo_ambito=ambito-ubigeo`

## Grano
`anio + departamento + grado`

## Cobertura temporal
2012-2025

## Departamentos
25 departamentos/ámbitos usados por ESCALE.

## Columnas extraídas
- total
- pública
- privada
- urbana
- rural
- masculino
- femenino

## Uso analítico
- demanda educativa potencial
- demanda futura por grado
- quinto grado como proxy de potenciales postulantes próximos
- ruralidad educativa
- composición pública/privada

## Limitaciones
- Es matrícula escolar, no postulantes reales a PRONABEC.
- No mide elegibilidad individual.
- No debe interpretarse como demanda real directa de becas.
- Quinto grado es proxy de demanda potencial inmediata.
