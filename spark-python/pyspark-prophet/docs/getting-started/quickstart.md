# Quick Start

Get a working Prophet forecast running on PySpark in under 10 minutes.

---

## Single-group forecast (Prophet only)

The simplest Prophet workflow — no Spark required:

```python
import pandas as pd
from prophet import Prophet

# Prophet expects exactly two columns: ds (date) + y (numeric)
df = pd.read_csv(
    "https://raw.githubusercontent.com/facebook/prophet/main/examples/"
    "example_wp_log_peyton_manning.csv"
)

m = Prophet(yearly_seasonality=True, weekly_seasonality=True)
m.fit(df)

future   = m.make_future_dataframe(periods=365)
forecast = m.predict(future)

print(forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail())
m.plot(forecast)
m.plot_components(forecast)
```

---

## Multi-group distributed forecast (PySpark)

Scale to thousands of groups by running one Prophet model per group in parallel:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType
import pandas as pd

spark = (
    SparkSession.builder
    .appName("quick-start-forecast")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .getOrCreate()
)

result_schema = StructType([
    StructField("store",      StringType(), False),
    StructField("ds",         DateType(),   False),
    StructField("yhat",       DoubleType(), True),
    StructField("yhat_lower", DoubleType(), True),
    StructField("yhat_upper", DoubleType(), True),
])

def forecast_store(group_df: pd.DataFrame) -> pd.DataFrame:
    from prophet import Prophet          # (1) always import inside UDF
    store = group_df["store"].iloc[0]
    train = group_df[["ds", "y"]].copy()
    train["ds"] = pd.to_datetime(train["ds"])
    model = Prophet(yearly_seasonality=True, weekly_seasonality=True)
    model.fit(train)
    future   = model.make_future_dataframe(periods=90)
    forecast = model.predict(future)
    forecast["store"] = store
    return forecast[["store", "ds", "yhat", "yhat_lower", "yhat_upper"]] \
               .assign(ds=lambda df: df["ds"].dt.date)   # (2) return as date

sdf       = spark.read.parquet("/path/to/daily_sales")   # store, ds, y
n_stores  = sdf.select("store").distinct().count()

forecast_sdf = (
    sdf
    .repartition(n_stores, "store")     # (3) one partition per group
    .groupby("store")
    .applyInPandas(forecast_store, schema=result_schema)
)

forecast_sdf.write.mode("overwrite").partitionBy("store").parquet("/output/forecasts")
spark.stop()
```

---

## What to read next

| Goal | Page |
|---|---|
| Deep-dive into Prophet | [Prophet Overview](../prophet/overview.md) |
| Understand `applyInPandas` | [Distributed Forecasting](../pyspark/distributed-forecasting.md) |
| Build a full ETL → forecast pipeline | [End-to-End Pipeline](../pipeline/e2e-pipeline.md) |
| Tune a Prophet model | [Cross-Validation](../prophet/cross-validation.md) |
