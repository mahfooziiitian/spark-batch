# Distributed Forecasting with PySpark

The canonical pattern: one Prophet model per group, all groups trained in
**parallel** across the Spark cluster using `applyInPandas`.

---

## Why `applyInPandas`?

| Method | Use-case |
|---|---|
| `pandas_udf` (scalar/aggregate) | Row-level or aggregation transformations |
| **`applyInPandas`** | **Group-level operations that consume an entire group at once** — perfect for Prophet |

`applyInPandas` sends each group as a complete `pandas.DataFrame` to a Python
worker, collects the returned `pandas.DataFrame`, and reassembles a Spark DataFrame.
Arrow serialisation makes the transfer fast.

---

## Step-by-step pattern

### 1. Enable Arrow

```python
spark = (
    SparkSession.builder
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .config("spark.sql.execution.arrow.maxRecordsPerBatch", "50000")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
```

### 2. Declare the output schema

Prophet produces new columns not present in the input DataFrame.
You must tell Spark what to expect:

```python
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType

result_schema = StructType([
    StructField("store",      StringType(), False),
    StructField("ds",         DateType(),   False),
    StructField("yhat",       DoubleType(), True),
    StructField("yhat_lower", DoubleType(), True),
    StructField("yhat_upper", DoubleType(), True),
    StructField("trend",      DoubleType(), True),
    StructField("split",      StringType(), False),
])
```

### 3. Write the UDF

```python
def forecast_store(group_df: pd.DataFrame) -> pd.DataFrame:
    from prophet import Prophet           # import inside UDF — each worker is independent

    store = group_df["store"].iloc[0]
    train = group_df[["ds", "y"]].copy()
    train["ds"] = pd.to_datetime(train["ds"])

    if len(train) < 60:                  # guard — return empty on tiny groups
        return pd.DataFrame(columns=[f.name for f in result_schema])

    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=True,
        seasonality_mode="additive",
        interval_width=0.90,
    )
    model.fit(train)

    future   = model.make_future_dataframe(periods=90, include_history=True)
    forecast = model.predict(future)

    last_date = train["ds"].max()
    forecast["split"] = forecast["ds"].apply(
        lambda d: "historical" if d <= last_date else "forecast"
    )
    forecast["store"] = store

    return (
        forecast[["store", "ds", "yhat", "yhat_lower", "yhat_upper", "trend", "split"]]
        .assign(ds=lambda df: df["ds"].dt.date)   # DateType, not Timestamp
    )
```

### 4. Distribute

```python
n_groups = sdf.select("store").distinct().count()

# Guard: repartition(0) raises IllegalArgumentException
if n_groups == 0:
    spark.stop()
    raise SystemExit(0)

forecast_sdf = (
    sdf
    .repartition(n_groups, "store")          # one partition per group
    .groupby("store")
    .applyInPandas(forecast_store, schema=result_schema)
    .cache()                                 # materialise once; reuse below
)

forecast_sdf.show(10)
```

---

## Scaling to thousands of groups

```
Parallelism = min(n_groups, total_executor_cores)
```

For 10,000 SKUs on a 40-core cluster:

```python
spark = SparkSession.builder \
    .config("spark.executor.instances", "20") \
    .config("spark.executor.cores",     "2") \
    .getOrCreate()

sdf.repartition(10_000, "sku").groupby("sku").applyInPandas(fn, schema)
```

!!! tip "Memory per worker"
    Each Prophet model with 3 years of daily data uses ~50–100 MB RAM per worker.
    Set `spark.executor.memory` accordingly (e.g., `4g` for 40 concurrent models).

---

## Writing results

```python
forecast_sdf \
    .write \
    .mode("overwrite") \
    .partitionBy("store") \
    .parquet("/output/forecasts")
```

---

## Accuracy KPIs in Spark SQL

```python
actuals = sdf.withColumnRenamed("y", "actual")

accuracy = (
    forecast_sdf
    .filter(F.col("split") == "historical")
    .join(actuals, on=["store", "ds"])
    .withColumn("abs_pct_err",
        F.abs(F.col("yhat") - F.col("actual")) / F.col("actual") * 100)
    .groupby("store")
    .agg(
        F.sqrt(F.mean(F.pow(F.col("yhat") - F.col("actual"), 2))).alias("rmse"),
        F.mean("abs_pct_err").alias("mape_pct"),
    )
)
accuracy.show()
```

---

## Source file

```
src/02_pyspark_prophet_distributed.py
```
