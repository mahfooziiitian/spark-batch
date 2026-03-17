# GitHub Copilot Instructions — pyspark-prophet

## Project Overview
This project contains reference implementations for distributed time-series
forecasting using **Meta Prophet** and **Apache PySpark 3.x**. The canonical
pattern is: one Prophet model trained per group (store, SKU, region) executed
in parallel across a Spark cluster via `applyInPandas`.

**Stack:** Python 3.11 · PySpark 3.5 · Prophet 1.1 · NumPy 1.26 · Pandas · Poetry

---

## Architecture & File Map

| File | Responsibility |
|---|---|
| `src/01_prophet_fundamentals.py`         | Core Prophet API — growth modes, seasonality, holidays, regressors, CV |
| `src/02_pyspark_prophet_distributed.py`  | `applyInPandas` UDF pattern, Arrow, Spark SQL analytics |
| `src/03_e2e_pipeline.py`                 | ETL → forecast → post-processing pipeline |
| `src/04_tuning_and_serialisation.py`     | Hyperparameter grid search, pickle serialisation |
| `src/05_prophet_deep_concepts.py`        | Flat/logistic growth, Fourier orders, sub-daily, outlier masking |
| `src/06_pyspark_timeseries_features.py`  | Calendar features, lags, rolling stats, gap-fill, pivots |
| `src/07_medallion_prophet_pipeline.py`   | Bronze → Silver → Gold lakehouse pipeline, incremental processing |
| `src/model/data_model.md`               | Domain data model reference |

---

## Coding Conventions

### Python
- **Type hints** on all function signatures (`def fn(x: pd.DataFrame) -> pd.DataFrame`).
- **`from __future__ import annotations`** at the top of every file.
- Keep imports inside `applyInPandas` UDFs (`from prophet import Prophet`) — each
  Spark worker process must import independently.
- Use `np.random.seed()` at module level for reproducibility in synthetic data.
- Format section headers as `# ── SECTION NAME ─────` (em-dashes, trailing fill).
- No inline comments for obvious code; only explain non-obvious logic.

### PySpark
- Always enable Arrow: `.config("spark.sql.execution.arrow.pyspark.enabled", "true")`.
- Set `spark.sql.shuffle.partitions` explicitly — never rely on the default 200.
- Prefer `applyInPandas` over `pandas_udf` for Prophet UDFs (full group control).
- Repartition by the group key before `groupby().applyInPandas()`:
  ```python
  sdf.repartition(n_groups, "group_col").groupby("group_col").applyInPandas(fn, schema)
  ```
- Guard against zero partitions:
  ```python
  n_groups = stores_to_run.count()
  if n_groups == 0:
      spark.stop(); raise SystemExit(0)
  ```
- Use `F.broadcast()` for small lookup tables (< a few MB).
- Use `Window.partitionBy(...).orderBy(...)` for lag/rolling features; never
  sort the full DataFrame for window computations.
- Prefer Spark SQL (`spark.sql("""...""")`) for multi-table analytics — it is
  easier to read and test than deeply chained DataFrame API calls.
- Always call `sdf.cache()` after expensive UDF results that are reused.
- Use `write.mode("overwrite").partitionBy(...)` for idempotent Parquet writes.

### Prophet
- Always declare `result_schema` as a `StructType` before the UDF function.
- The `ds` column must be `DateType` (not `TimestampType`) when returned from
  UDFs: `.assign(ds=lambda df: df["ds"].dt.date)`.
- For logistic growth, always supply `cap` (and optionally `floor`) in BOTH
  the training DataFrame and the future DataFrame.
- For incremental pipelines, **eligibility must be computed from full history**,
  not from the incremental slice.
- Mask outliers with `y = NaN` (not deletion) to preserve the time grid.
- Default seasonality mode: `"additive"` for stable-variance series;
  `"multiplicative"` for revenue/count series that scale with trend.

---

## DataFrame Conventions

### Prophet input
```python
# Required columns
df = pd.DataFrame({"ds": <date_col>, "y": <numeric_col>})
# Optional extras passed as regressors
df["regressor_name"] = ...
```

### `applyInPandas` UDF signature
```python
def forecast_group(group_df: pd.DataFrame) -> pd.DataFrame:
    ...
    return result_df  # columns must exactly match result_schema

sdf.groupby("key").applyInPandas(forecast_group, schema=result_schema)
```

### Medallion layer conventions
| Layer  | Contents | Partition key |
|--------|----------|---------------|
| Bronze | Raw bytes, ingested_at timestamp | `ingest_date` |
| Silver | Clean, deduped, gap-filled, feature-enriched | `group_key` |
| Gold   | Forecasts + KPIs + anomaly flags | `run_date`, `group_key` |

---

## Incremental Processing Pattern
```python
# 1. Eligibility always uses full silver history
eligible = silver_full.groupby("store").agg(F.count("ds").alias("n")).filter(F.col("n") >= MIN_DAYS)

# 2. Incremental filter identifies which stores have new rows
stores_with_new_data = silver_full.filter(F.col("ds") > last_checkpoint).select("store").distinct()

# 3. Intersect — only eligible stores that also have new data
stores_to_run = eligible.join(stores_with_new_data, on="store", how="inner")

# 4. model_input always uses full history (Prophet needs it all)
model_input = silver_full.join(stores_to_run, on="store", how="inner")
```

---

## Dependencies (pyproject.toml)
```toml
pyspark  = "^3.5.1"
prophet  = "^1.1.5"
numpy    = "1.26.4"       # pinned — Prophet requires <2.0
pandas   = (transitive)
plotly   = "^5.22.0"
```
> **NumPy must stay at 1.26.x.** Prophet's Stan backend is incompatible with NumPy 2.x.

---

## Common Pitfalls to Avoid
- Do **not** set `n_changepoints` very high on short series — overfitting.
- Do **not** use `freq="D"` for sub-daily data; use `freq="h"` etc.
- Do **not** delete outlier rows — set `y = NaN` to preserve the time grid.
- Do **not** compute `repartition(n, col)` with `n=0` — Spark throws `IllegalArgumentException`.
- Do **not** apply the incremental date filter before computing eligibility.
- Do **not** use `pkill` or `killall` to stop Spark; call `spark.stop()`.
