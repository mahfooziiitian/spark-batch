# Spark SQL Analytics

Register Spark DataFrames as temporary views and run standard SQL for
multi-table analytics, aggregations, and reporting.

---

## Register views

```python
forecast_sdf.createOrReplaceTempView("forecasts")
actuals_sdf.createOrReplaceTempView("actuals")
```

---

## Accuracy by group

```sql
SELECT
    f.store,
    ROUND(SQRT(AVG(POWER(f.yhat - a.y, 2))), 2)          AS rmse,
    ROUND(AVG(ABS(f.yhat - a.y)), 2)                      AS mae,
    ROUND(AVG(ABS(f.yhat - a.y) / NULLIF(a.y, 0)) * 100, 2) AS mape_pct
FROM forecasts f
JOIN actuals   a USING (store, ds)
WHERE f.split = 'historical'
GROUP BY f.store
ORDER BY mape_pct
```

---

## Monthly bias report

```sql
SELECT
    store,
    DATE_FORMAT(ds, 'yyyy-MM')                                          AS month,
    ROUND(SUM(yhat), 0)                                                 AS forecast,
    ROUND(SUM(actual), 0)                                               AS actuals,
    ROUND(100.0 * (SUM(yhat) - SUM(actual)) / NULLIF(SUM(actual), 0), 2) AS bias_pct
FROM (
    SELECT f.store, f.ds, f.yhat, a.y AS actual
    FROM   forecasts f
    JOIN   actuals   a USING (store, ds)
    WHERE  f.split = 'historical'
)
GROUP BY store, month
ORDER BY store, month
```

---

## Anomaly detection

Flag days where actual falls outside the prediction interval:

```sql
SELECT
    f.store,
    f.ds,
    a.y         AS actual,
    f.yhat,
    f.yhat_lower,
    f.yhat_upper,
    CASE
        WHEN a.y < f.yhat_lower THEN 'below_lower'
        WHEN a.y > f.yhat_upper THEN 'above_upper'
        ELSE 'normal'
    END AS anomaly_type
FROM forecasts f
JOIN actuals   a USING (store, ds)
WHERE f.split = 'historical'
  AND (a.y < f.yhat_lower OR a.y > f.yhat_upper)
ORDER BY f.store, f.ds
```

---

## Future forecast summary

```sql
SELECT
    store,
    MIN(ds)             AS forecast_from,
    MAX(ds)             AS forecast_to,
    ROUND(AVG(yhat), 2) AS avg_daily_forecast,
    ROUND(SUM(yhat), 0) AS total_forecast
FROM forecasts
WHERE split = 'forecast'
GROUP BY store
ORDER BY store
```

---

## Trend direction per group

```sql
SELECT
    store,
    FIRST_VALUE(trend) OVER w AS trend_start,
    LAST_VALUE(trend)  OVER w AS trend_end,
    LAST_VALUE(trend)  OVER w - FIRST_VALUE(trend) OVER w AS trend_change
FROM forecasts
WHERE split = 'forecast'
WINDOW w AS (PARTITION BY store ORDER BY ds
             ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
GROUP BY store
```

---

## Combining Python API and SQL

You can mix both styles freely:

```python
# Python API for UDF output
forecast_sdf = (
    sdf.repartition(n, "store")
       .groupby("store")
       .applyInPandas(forecast_fn, schema=schema)
)
forecast_sdf.createOrReplaceTempView("forecasts")

# SQL for final analytics
spark.sql("""
    SELECT store, SUM(yhat) AS total_90d_forecast
    FROM   forecasts
    WHERE  split = 'forecast'
    GROUP  BY store
""").show()
```
