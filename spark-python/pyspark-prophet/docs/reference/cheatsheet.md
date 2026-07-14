# API Cheat Sheet

Quick-reference snippets for the most common Prophet + PySpark operations.

---

## Prophet

### Fit & predict

```python
from prophet import Prophet

m = Prophet()
m.fit(df)                                              # df: ds + y
future   = m.make_future_dataframe(periods=90, freq="D")
forecast = m.predict(future)
```

### Growth modes

```python
Prophet(growth="linear")    # default, unbounded
Prophet(growth="logistic")  # needs cap (+ floor) columns in df and future
Prophet(growth="flat")      # no trend, seasonality only
```

### Seasonality

```python
Prophet(yearly_seasonality=True, weekly_seasonality=10, daily_seasonality=False)
m.add_seasonality(name="monthly", period=30.5, fourier_order=5)
```

### Holidays

```python
m = Prophet(holidays=holidays_df, holidays_prior_scale=10)
m.add_country_holidays(country_name="US")
```

### Regressors

```python
m.add_regressor("promo", standardize=True, mode="additive")
# column must exist in both df and future
```

### Cross-validation

```python
from prophet.diagnostics import cross_validation, performance_metrics
df_cv   = cross_validation(m, initial="730 days", period="180 days", horizon="365 days")
df_perf = performance_metrics(df_cv)
```

### Serialise

```python
import pickle
with open("model.pkl", "wb") as f: pickle.dump(m, f)
with open("model.pkl", "rb") as f: m = pickle.load(f)
```

### Posterior samples

```python
raw = m.predictive_samples(future)   # raw["yhat"]: (n_dates, uncertainty_samples)
```

---

## PySpark

### SparkSession

```python
spark = (
    SparkSession.builder
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
```

### applyInPandas

```python
sdf.repartition(n, "key").groupby("key").applyInPandas(fn, schema=result_schema)
```

### Window functions

```python
w = Window.partitionBy("store").orderBy("ds")
F.lag("y", 7).over(w)                                           # lag-7
F.avg("y").over(w.rowsBetween(-(7-1), 0))                       # 7-day trailing mean
F.last("y", ignorenulls=True).over(w.rowsBetween(Window.unboundedPreceding, 0))  # ffill
```

### Parquet write (idempotent)

```python
sdf.write.mode("overwrite").partitionBy("date", "group").parquet(path)
```

### Broadcast join

```python
sdf.join(F.broadcast(small_lookup), on="key", how="left")
```

### IQR outlier cap

```python
quantiles = sdf.groupby("g").agg(
    F.expr("percentile(y, 0.25)").alias("q1"),
    F.expr("percentile(y, 0.75)").alias("q3"),
).withColumn("fence_lo", F.col("q1") - 1.5 * (F.col("q3") - F.col("q1"))) \
 .withColumn("fence_hi", F.col("q3") + 1.5 * (F.col("q3") - F.col("q1")))
```

---

## Forecast output columns

| Column | Description |
|---|---|
| `yhat` | Point forecast |
| `yhat_lower` | Lower credible bound |
| `yhat_upper` | Upper credible bound |
| `trend` | Trend component |
| `yearly` | Yearly seasonality |
| `weekly` | Weekly seasonality |
| `holidays` | Holiday effect (if configured) |
| `<regressor>` | Per-regressor contribution |
