# PySpark + Prophet

> Distributed time-series forecasting — one Prophet model per group, run in parallel across a Spark cluster.

---

## What this project covers

This repository is a structured set of reference implementations that teach every layer of production-grade distributed forecasting using **Meta Prophet** and **Apache PySpark 3.x**.

```
src/
├── 01_prophet_fundamentals.py        ← Core Prophet API
├── 02_pyspark_prophet_distributed.py ← applyInPandas UDF pattern
├── 03_e2e_pipeline.py                ← ETL → forecast → KPIs
├── 04_tuning_and_serialisation.py    ← Hyperparameter grid search
├── 05_prophet_deep_concepts.py       ← Flat growth, Fourier, sub-daily
├── 06_pyspark_timeseries_features.py ← Lags, rolling, gap-fill, pivots
└── 07_medallion_prophet_pipeline.py  ← Bronze → Silver → Gold pipeline
```

---

## Architecture at a glance

```mermaid
flowchart LR
    A[Raw CSV / Events] -->|Bronze| B[Validate & Ingest]
    B -->|Silver| C[Clean · Dedup · Gap-fill · Features]
    C -->|Gold| D[Prophet UDF per group]
    D --> E[Forecasts + KPIs + Anomalies]
    E --> F[Parquet / Delta Lake]
    F --> G[BI / Dashboard]
```

---

## Core pattern

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from prophet import Prophet

def forecast_group(group_df):           # (1) runs in parallel Spark workers
    model = Prophet(yearly_seasonality=True)
    model.fit(group_df[["ds", "y"]])
    future = model.make_future_dataframe(periods=90)
    return model.predict(future)[["ds", "yhat", "yhat_lower", "yhat_upper"]]

spark = SparkSession.builder \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

(
    sdf
    .repartition(n_groups, "store")     # (2) one partition per group
    .groupby("store")
    .applyInPandas(forecast_group, schema=result_schema)   # (3) fan-out
    .write.parquet("/output/forecasts")
)
```

---

## Stack

| Package   | Version  | Role |
|-----------|----------|------|
| Python    | 3.11     | Runtime |
| PySpark   | 3.5.x    | Distributed compute |
| Prophet   | 1.1.x    | Time-series model |
| NumPy     | 1.26.4   | Numerical arrays (pinned — Prophet requires < 2.0) |
| Pandas    | < 3.0    | In-worker DataFrames |
| PyArrow   | 23.x     | Arrow-accelerated Spark ↔ pandas transfer |
| Plotly    | 5.x      | Interactive forecast charts |

---

## Navigation guide

| If you want to… | Go to |
|---|---|
| Understand Prophet from scratch | [Prophet Overview](prophet/overview.md) |
| Forecast hundreds of groups in Spark | [Distributed Forecasting](pyspark/distributed-forecasting.md) |
| Build a production pipeline | [Medallion Architecture](pipeline/medallion.md) |
| Tune a Prophet model | [Cross-Validation](prophet/cross-validation.md) |
| Engineer time-series features | [Time-Series Features](pyspark/timeseries-features.md) |
| Look up a parameter | [API Cheat Sheet](reference/cheatsheet.md) |
