# Forecast presupuestal BigQuery ML

## Objetivo

`ml.model_budget_forecast` usa BigQuery ML ARIMA_PLUS para pronosticar la serie mensual agregada de devengado presupuestal. El forecast es referencial y sirve para analitica ejecutiva, no para compromisos financieros ni decisiones automaticas.

## Fuente

- `silver.presupuesto_mef_temporal`

La serie se construye con registros mensuales, `DATE(ano, mes_numero, 1)` como periodo y `SUM(devengado)` como metrica.

## Objetos

- `ml.model_budget_forecast`: modelo ARIMA_PLUS mensual.
- `ml.budget_forecast_results`: vista con `ML.FORECAST` a 12 meses y 80% de confianza.
- `gold.vw_predictive_budget_forecast`: salida Gold para Power BI.

## Interpretacion

El forecast resume tendencias temporales observadas en el devengado mensual agregado. Los intervalos de prediccion expresan incertidumbre estadistica del modelo, no garantias presupuestales.

## Limites

- No hay forecast por producto, actividad, generica ni territorio.
- No hay prediccion individual.
- No hay causalidad.
- El resultado puede variar si cambian historicos, periodicidad o ajustes presupuestales.
