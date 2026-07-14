# Common Pitfalls

Mistakes that are easy to make and hard to debug — with fixes.

---

## Prophet

### ❌ Deleting outlier rows

```python
# Wrong — creates a gap in the time grid
df = df[df["y"] < threshold]
```

```python
# Correct — mask as NaN; Prophet skips NaN but predicts those dates
df.loc[df["y"] > threshold, "y"] = float("nan")
```

---

### ❌ Forgetting `cap` / `floor` in the future DataFrame

```python
# Wrong — logistic model crashes on predict()
m = Prophet(growth="logistic")
m.fit(df_with_cap)
future = m.make_future_dataframe(periods=365)
m.predict(future)   # KeyError: 'cap'
```

```python
# Correct
future["cap"]   = 95.0
future["floor"] = 15.0
m.predict(future)
```

---

### ❌ Using `freq="D"` for hourly data

```python
# Wrong — generates only 7 rows for 7 "days"
future = m.make_future_dataframe(periods=7, freq="D")

# Correct — generates 7 × 24 = 168 hourly rows
future = m.make_future_dataframe(periods=7 * 24, freq="h")
```

---

### ❌ Over-tuning Fourier order on short series

High Fourier order fits training noise as seasonality, producing erratic forecasts:

```python
# Risky on < 2 years of data
m = Prophet(yearly_seasonality=20)

# Safer
m = Prophet(yearly_seasonality=10)
```

---

### ❌ Extrapolating trend without a cap

Linear growth extrapolates the last detected slope indefinitely.
For bounded metrics (revenue %, utilisation), use logistic:

```python
# Prevents runaway 5-year forecast
m = Prophet(growth="logistic")
df["cap"] = 100.0
```

---

## PySpark

### ❌ `repartition(0, col)` — zero partitions

Happens when the eligible group count is zero (e.g., incremental run with no new data):

```python
# Crashes with IllegalArgumentException
sdf.repartition(n_groups, "store")   # if n_groups == 0
```

```python
# Correct — guard before repartition
n_groups = stores_to_run.count()
if n_groups == 0:
    spark.stop(); raise SystemExit(0)
sdf.repartition(n_groups, "store")
```

---

### ❌ Computing eligibility from the incremental slice

```python
# Wrong — filtered slice has 0 rows → n_groups=0 on second run
silver_in = silver_full.filter(F.col("ds") > last_checkpoint)
eligible  = silver_in.groupby("store").agg(F.count("ds").alias("n")).filter(...)
```

```python
# Correct — eligibility always uses FULL history
eligible = silver_full.groupby("store").agg(F.count("ds").alias("n")).filter(...)
```

---

### ❌ Importing Prophet at module level in a UDF

PySpark serialises the UDF function and sends it to workers.
Module-level imports are not guaranteed to be available in the worker environment.

```python
# Risky — may fail on workers
from prophet import Prophet

def forecast_group(group_df):
    m = Prophet()
    ...
```

```python
# Correct — import inside the UDF
def forecast_group(group_df):
    from prophet import Prophet   # imported on each worker independently
    m = Prophet()
    ...
```

---

### ❌ Using `dropDuplicates()` for latest-wins deduplication

`dropDuplicates()` keeps an arbitrary row when duplicates exist — not the latest.

```python
# Wrong — non-deterministic
deduped = sdf.dropDuplicates(["store", "ds"])

# Correct — keeps most-recently ingested row
w = Window.partitionBy("store", "ds").orderBy(F.desc("ingested_at"))
deduped = sdf.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
```

---

### ❌ Forgetting `.dt.date` on the UDF return

Prophet's `ds` column is `datetime64`. Returning it directly maps to Spark `TimestampType`,
not `DateType`, causing a schema mismatch:

```python
# Wrong — returns Timestamp, not Date
return forecast[["store", "ds", "yhat"]]

# Correct
return forecast[["store", "ds", "yhat"]].assign(ds=lambda df: df["ds"].dt.date)
```

---

### ❌ NumPy 2.x installed

Prophet's Stan backend is incompatible with NumPy ≥ 2.0:

```
ImportError: cannot import name 'bool' from 'numpy'
```

Pin to `numpy==1.26.4` in `pyproject.toml`.
