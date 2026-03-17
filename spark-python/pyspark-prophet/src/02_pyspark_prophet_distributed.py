"""
Distributed Forecasting — PySpark + Prophet
=============================================
Pattern: one Prophet model trained per group (store, SKU, region, etc.)
         run in parallel across a Spark cluster via pandas_udf.

Covers:
  1.  SparkSession setup
  2.  Synthetic multi-group dataset
  3.  Single-group Prophet baseline (sanity check)
  4.  pandas_udf  — GROUPED_MAP approach (applyInPandas)
  5.  Return forecast + confidence intervals to a Spark DataFrame
  6.  Tuning hints (repartitioning, broadcast, Arrow)
  7.  Writing results to Parquet / Delta
  8.  Reading results & computing accuracy KPIs in Spark SQL
"""

from __future__ import annotations

import pandas as pd
import numpy as np
from datetime import date, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DateType, DoubleType, TimestampType,
)

# ── 1. SPARKSESSION ──────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("prophet-distributed-forecast")
    # Arrow-based columnar serialization — dramatically speeds up
    # pandas ↔ Spark data transfer in pandas_udf calls
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .config("spark.sql.execution.arrow.maxRecordsPerBatch", "50000")
    # Tune shuffle partitions to match your cluster parallelism
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── 2. SYNTHETIC MULTI-STORE SALES DATASET ───────────────────────────────────
# Real-world pattern: one time-series per store/product combination.

np.random.seed(42)
STORES = ["store_A", "store_B", "store_C", "store_D"]
START  = date(2020, 1, 1)
DAYS   = 365 * 3   # 3 years of history


def make_store_series(store_id: str, noise_scale: float = 0.15) -> pd.DataFrame:
    dates = [START + timedelta(days=i) for i in range(DAYS)]
    t     = np.arange(DAYS)
    trend = 100 + 0.05 * t
    yearly = 20 * np.sin(2 * np.pi * t / 365)
    weekly = 10 * np.sin(2 * np.pi * t / 7)
    noise  = np.random.normal(0, noise_scale * trend.mean(), DAYS)
    sales  = trend + yearly + weekly + noise
    return pd.DataFrame({"store": store_id, "ds": dates, "y": sales.clip(0)})


history_pd = pd.concat(
    [make_store_series(s, noise_scale=0.1 * (i + 1)) for i, s in enumerate(STORES)],
    ignore_index=True,
)
print(history_pd.groupby("store").size())

# ── Convert to Spark DataFrame ───────────────────────────────────────────────
schema_history = StructType(
    [
        StructField("store", StringType(),  nullable=False),
        StructField("ds",    DateType(),     nullable=False),
        StructField("y",     DoubleType(),   nullable=False),
    ]
)

sdf = spark.createDataFrame(history_pd, schema=schema_history)
sdf.printSchema()
sdf.show(5)

# ── 3. SINGLE-GROUP SANITY CHECK ─────────────────────────────────────────────

store_a_pd = history_pd[history_pd["store"] == "store_A"][["ds", "y"]].copy()
store_a_pd["ds"] = pd.to_datetime(store_a_pd["ds"])

from prophet import Prophet  # local import to keep top-level clean in UDF

m_check = Prophet(
    yearly_seasonality=True,
    weekly_seasonality=True,
    daily_seasonality=False,
    seasonality_mode="additive",
    interval_width=0.90,
)
m_check.fit(store_a_pd)
future_check = m_check.make_future_dataframe(periods=90)
forecast_check = m_check.predict(future_check)
print(forecast_check[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail(5))

# ── 4. DISTRIBUTED FORECAST VIA applyInPandas ────────────────────────────────
#
# applyInPandas (Spark 3.x):
#   • Groups the Spark DF by a key column(s)
#   • Sends each group as a pandas DataFrame to a Python worker
#   • Collects the returned pandas DataFrames back into a Spark DataFrame
#
# The UDF must declare:
#   - input  schema: implied by the grouped Spark DataFrame
#   - output schema: declared explicitly as a StructType

FORECAST_HORIZON = 90  # days to forecast beyond last known date

result_schema = StructType(
    [
        StructField("store",       StringType(),  nullable=False),
        StructField("ds",          DateType(),    nullable=False),
        StructField("yhat",        DoubleType(),  nullable=True),
        StructField("yhat_lower",  DoubleType(),  nullable=True),
        StructField("yhat_upper",  DoubleType(),  nullable=True),
        StructField("trend",       DoubleType(),  nullable=True),
        StructField("is_forecast", StringType(),  nullable=False),
    ]
)


def forecast_store(group_df: pd.DataFrame) -> pd.DataFrame:
    """Train one Prophet model per store and return the full forecast."""
    store_id = group_df["store"].iloc[0]

    train = group_df[["ds", "y"]].copy()
    train["ds"] = pd.to_datetime(train["ds"])

    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=False,
        seasonality_mode="additive",
        interval_width=0.90,
        changepoint_prior_scale=0.05,
    )
    model.fit(train)

    future = model.make_future_dataframe(periods=FORECAST_HORIZON, include_history=True)
    forecast = model.predict(future)

    last_historical = train["ds"].max()
    forecast["is_forecast"] = forecast["ds"].apply(
        lambda d: "future" if d > last_historical else "historical"
    )
    forecast["store"] = store_id

    return forecast[
        ["store", "ds", "yhat", "yhat_lower", "yhat_upper", "trend", "is_forecast"]
    ].assign(ds=lambda df: df["ds"].dt.date)


# Each group (store) runs in its own Python worker process.
# Number of parallel workers = number of partitions after groupBy repartition.
forecast_sdf = (
    sdf
    .repartition(len(STORES), "store")   # one partition per store → one task per store
    .groupby("store")
    .applyInPandas(forecast_store, schema=result_schema)
)

forecast_sdf.cache()   # materialise once; reuse for analysis below
forecast_sdf.show(10)

# ── 5. SCALE: HUNDREDS OF GROUPS ─────────────────────────────────────────────
#
# For large numbers of groups (e.g., 10 000 SKUs):
#   • Partition by group key so each partition holds exactly one group
#   • Increase spark.executor.cores to run more UDF workers per machine
#   • Prophet is CPU-bound; use processes (not threads) in cross_validation
#   • Broadcast any small lookup tables needed inside the UDF
#
# Example for 10 k SKUs on a 40-core cluster:
#   .config("spark.executor.instances", "20")
#   .config("spark.executor.cores",     "2")
#   → 40 concurrent Prophet tasks
#   .repartition(10_000, "sku")   # one partition per SKU

# ── 6. ACCURACY KPIs IN SPARK SQL ────────────────────────────────────────────
# Join forecast (historical fit) back to actuals for in-sample metrics.

actuals = sdf.withColumnRenamed("y", "actual")

accuracy = (
    forecast_sdf
    .filter(F.col("is_forecast") == "historical")
    .join(actuals, on=["store", "ds"], how="inner")
    .withColumn("error",     F.col("yhat") - F.col("actual"))
    .withColumn("abs_error", F.abs(F.col("error")))
    .withColumn(
        "pct_error",
        F.abs(F.col("error")) / F.col("actual") * 100,
    )
    .groupby("store")
    .agg(
        F.sqrt(F.mean(F.pow(F.col("error"), 2))).alias("rmse"),
        F.mean("abs_error").alias("mae"),
        F.mean("pct_error").alias("mape_pct"),
    )
    .orderBy("store")
)

accuracy.show()

# ── 7. REGISTERING AS A SPARK SQL VIEW ───────────────────────────────────────

forecast_sdf.createOrReplaceTempView("store_forecast")
actuals.createOrReplaceTempView("store_actuals")

spark.sql("""
    SELECT
        f.store,
        f.ds,
        f.yhat,
        f.yhat_lower,
        f.yhat_upper,
        a.actual,
        f.yhat - a.actual AS residual
    FROM store_forecast f
    LEFT JOIN store_actuals a USING (store, ds)
    WHERE f.is_forecast = 'historical'
    ORDER BY f.store, f.ds
""").show(20)

# Future-only rows for downstream planning
spark.sql("""
    SELECT store, ds, ROUND(yhat, 2) AS forecast, is_forecast
    FROM store_forecast
    WHERE is_forecast = 'future'
    ORDER BY store, ds
""").show(20)

# ── 8. WRITING RESULTS ───────────────────────────────────────────────────────

OUTPUT_PATH = "/tmp/store_forecasts"

(
    forecast_sdf
    .write
    .mode("overwrite")
    .partitionBy("store")            # partition Parquet files by store
    .parquet(OUTPUT_PATH)
)

# Read back and verify
spark.read.parquet(OUTPUT_PATH).filter(
    F.col("is_forecast") == "future"
).orderBy("store", "ds").show(10)

# ── 9. ADVANCED UDF — REGRESSORS + HOLIDAYS ──────────────────────────────────
# Pass extra regressor columns alongside ds and y.
# The UDF receives the full group DataFrame including those columns.

result_schema_adv = StructType(
    [
        StructField("store",       StringType(), nullable=False),
        StructField("ds",          DateType(),   nullable=False),
        StructField("yhat",        DoubleType(), nullable=True),
        StructField("yhat_lower",  DoubleType(), nullable=True),
        StructField("yhat_upper",  DoubleType(), nullable=True),
    ]
)


def forecast_with_regressor(group_df: pd.DataFrame) -> pd.DataFrame:
    """Prophet UDF that uses an external 'promo' regressor."""
    store_id = group_df["store"].iloc[0]

    train = group_df[["ds", "y", "promo"]].copy()
    train["ds"] = pd.to_datetime(train["ds"])

    model = Prophet(interval_width=0.90)
    model.add_country_holidays(country_name="US")
    model.add_regressor("promo", standardize=False, mode="additive")
    model.fit(train)

    future = model.make_future_dataframe(periods=FORECAST_HORIZON)
    # For the forecast horizon, assume no promotions (promo=0)
    future["promo"] = 0
    # Merge known historical promo values back for the training period
    future = future.merge(train[["ds", "promo"]], on="ds", how="left", suffixes=("", "_hist"))
    future["promo"] = future["promo_hist"].fillna(0)
    future.drop(columns=["promo_hist"], inplace=True)

    forecast = model.predict(future)
    forecast["store"] = store_id
    return forecast[["store", "ds", "yhat", "yhat_lower", "yhat_upper"]].assign(
        ds=lambda df: df["ds"].dt.date
    )


# Add a synthetic promo column to the Spark DF and run the advanced UDF
sdf_promo = sdf.withColumn(
    "promo",
    (F.dayofweek("ds").isin([1, 7])).cast("double"),  # 1 on weekends
)

forecast_adv = (
    sdf_promo
    .repartition(len(STORES), "store")
    .groupby("store")
    .applyInPandas(forecast_with_regressor, schema=result_schema_adv)
)

forecast_adv.show(10)

# ── CLEAN UP ─────────────────────────────────────────────────────────────────
spark.stop()
