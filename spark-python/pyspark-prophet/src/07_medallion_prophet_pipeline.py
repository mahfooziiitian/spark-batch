"""
Medallion / Lakehouse Architecture — PySpark + Prophet
========================================================
A production-grade, incremental forecasting pipeline using the
Bronze → Silver → Gold layered data model.

Bronze  — raw ingest with schema validation; bad rows quarantined
Silver  — clean, deduplicated, gap-filled, feature-enriched daily series
Gold    — Prophet forecasts, accuracy KPIs, anomaly flags per group

Key concepts:
  • Idempotent, date-partitioned Parquet writes
  • Incremental processing (only re-process new data)
  • Schema validation & reject / quarantine pattern
  • Broadcast-join for small lookup tables
  • Delta-style merge-on-read deduplication
  • Config-driven pipeline (no hard-coded business logic)
"""

from __future__ import annotations

import json
import os
from datetime import date, timedelta

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE CONFIGURATION  (in practice, load from YAML / Databricks widgets)
# ─────────────────────────────────────────────────────────────────────────────
PIPELINE_CONFIG = {
    "base_path":       "/tmp/medallion",
    "forecast_horizon": 90,
    "min_history_days": 180,
    "interval_width":   0.95,
    "changepoint_prior_scale": 0.05,
    "groups":          ["store_A", "store_B", "store_C"],
    "run_date":        str(date.today()),
}

BASE        = PIPELINE_CONFIG["base_path"]
BRONZE_PATH = f"{BASE}/bronze/sales"
SILVER_PATH = f"{BASE}/silver/daily_sales"
GOLD_PATH   = f"{BASE}/gold/forecasts"
REJECT_PATH = f"{BASE}/quarantine/rejected_rows"
CHECKPOINT  = f"{BASE}/_checkpoints/last_processed_date"

# ─────────────────────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("medallion-prophet-pipeline")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def write_parquet(sdf, path: str, partition_by: list[str] | None = None):
    """Idempotent overwrite-partition write."""
    writer = sdf.write.mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(path)


def read_checkpoint(path: str) -> str | None:
    """Read the last-processed date from a tiny JSON checkpoint file."""
    ck_file = f"{path}/checkpoint.json"
    if os.path.exists(ck_file):
        with open(ck_file) as f:
            return json.load(f)["last_processed"]
    return None


def write_checkpoint(path: str, last_date: str):
    os.makedirs(path, exist_ok=True)
    with open(f"{path}/checkpoint.json", "w") as f:
        json.dump({"last_processed": last_date}, f)


# ─────────────────────────────────────────────────────────────────────────────
# STAGE 0 — GENERATE SYNTHETIC RAW DATA  (simulates a landing zone)
# ─────────────────────────────────────────────────────────────────────────────
np.random.seed(11)
START = date(2021, 1, 1)
END   = date(2023, 12, 31)
ALL_DATES = pd.date_range(START, END, freq="D")

raw_rows = []
for store in PIPELINE_CONFIG["groups"]:
    base = {"store_A": 400, "store_B": 600, "store_C": 250}[store]
    for d in ALL_DATES:
        i     = (d.date() - START).days
        y     = (base + 0.06 * i
                 + 50 * np.sin(2 * np.pi * d.dayofyear / 365)
                 + 20 * np.sin(2 * np.pi * d.dayofweek / 7)
                 + np.random.normal(0, base * 0.05))
        # Inject bad rows: negative revenue, null store, wrong date format
        if i == 10:
            raw_rows.append(("", str(d.date()), str(round(y, 2)), "0"))
        elif i == 20:
            raw_rows.append((store, "not-a-date", str(round(y, 2)), "0"))
        elif i == 30:
            raw_rows.append((store, str(d.date()), "-9999", "0"))
        else:
            raw_rows.append((store, str(d.date()), str(round(max(0, y), 2)), "0"))

raw_schema = StructType([
    StructField("store",      StringType(), True),
    StructField("sale_date",  StringType(), True),
    StructField("revenue",    StringType(), True),
    StructField("batch_flag", StringType(), True),
])

raw_sdf = spark.createDataFrame(raw_rows, schema=raw_schema) \
               .withColumn("ingested_at", F.current_timestamp())

# ── BRONZE LAYER ─────────────────────────────────────────────────────────────
# Store raw data exactly as received, partitioned by ingest date.
# NO transformations — preserve original bytes for auditability.

write_parquet(
    raw_sdf.withColumn("ingest_date", F.lit(PIPELINE_CONFIG["run_date"])),
    BRONZE_PATH,
    partition_by=["ingest_date"],
)
print(f"Bronze rows written: {raw_sdf.count()}")

# ── SILVER LAYER ─────────────────────────────────────────────────────────────
# Read bronze, validate, clean, deduplicate, fill gaps.

bronze_sdf = spark.read.parquet(BRONZE_PATH)

# --- 1. Schema validation + type casting ---
typed_sdf = (
    bronze_sdf
    .withColumn("ds",      F.to_date("sale_date", "yyyy-MM-dd"))
    .withColumn("revenue", F.col("revenue").cast(DoubleType()))
)

# --- 2. Row-level validation rules ---
valid_sdf = typed_sdf.filter(
    F.col("store").isNotNull()
    & (F.col("store") != "")
    & F.col("ds").isNotNull()
    & F.col("revenue").isNotNull()
    & (F.col("revenue") >= 0)
)

# --- 3. Quarantine rejected rows ---
rejected_sdf = typed_sdf.subtract(valid_sdf) \
                         .withColumn("rejection_reason",
                             F.when(F.col("store").isNull() | (F.col("store") == ""), "null_store")
                              .when(F.col("ds").isNull(), "invalid_date")
                              .when(F.col("revenue").isNull() | (F.col("revenue") < 0), "invalid_revenue")
                              .otherwise("unknown"))

write_parquet(
    rejected_sdf.withColumn("quarantine_date", F.lit(PIPELINE_CONFIG["run_date"])),
    REJECT_PATH,
    partition_by=["quarantine_date"],
)
print(f"Rejected (quarantined): {rejected_sdf.count()} rows")

# --- 4. Deduplication: keep the latest row per (store, ds) ---
w_dedup = Window.partitionBy("store", "ds").orderBy(F.desc("ingested_at"))

deduped_sdf = (
    valid_sdf
    .withColumn("_rn", F.row_number().over(w_dedup))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
    .select("store", "ds", "revenue")
)

# --- 5. Gap filling: ensure every (store, date) pair exists ---
date_spine = spark.createDataFrame(
    [(s, d.date()) for s in PIPELINE_CONFIG["groups"] for d in ALL_DATES],
    schema=StructType([
        StructField("store", StringType(), False),
        StructField("ds",    DateType(),   False),
    ])
)

w_ff = Window.partitionBy("store").orderBy("ds").rowsBetween(Window.unboundedPreceding, 0)

silver_sdf = (
    date_spine
    .join(deduped_sdf, on=["store", "ds"], how="left")
    .withColumn("y", F.last("revenue", ignorenulls=True).over(w_ff))
    .withColumn("is_imputed", F.col("revenue").isNull().cast(BooleanType()))
    .drop("revenue")
)

# --- 6. Feature enrichment ---
silver_sdf = (
    silver_sdf
    .withColumn("day_of_week",  F.dayofweek("ds"))
    .withColumn("is_weekend",   F.dayofweek("ds").isin([1, 7]).cast(DoubleType()))
    .withColumn("month",        F.month("ds"))
    .withColumn("is_month_end",
        (F.dayofmonth("ds") == F.dayofmonth(F.last_day("ds"))).cast(DoubleType()))
)

write_parquet(silver_sdf, SILVER_PATH, partition_by=["store"])
print(f"Silver rows written: {silver_sdf.count()}")
silver_sdf.show(5)

# ── GOLD LAYER — PROPHET FORECASTS ───────────────────────────────────────────
# Read silver; only process stores that have enough history.

silver_full = spark.read.parquet(SILVER_PATH)

# Eligibility is always based on full history — never the incremental slice.
eligible = (
    silver_full
    .groupby("store")
    .agg(F.count("ds").alias("n_days"))
    .filter(F.col("n_days") >= PIPELINE_CONFIG["min_history_days"])
    .select("store")
)

# Incremental: re-run only stores that have new rows since last checkpoint.
last_processed = read_checkpoint(CHECKPOINT)
if last_processed:
    print(f"Incremental run — processing data after {last_processed}")
    stores_with_new_data = (
        silver_full
        .filter(F.col("ds") > F.lit(last_processed).cast(DateType()))
        .select("store")
        .distinct()
    )
    stores_to_run = eligible.join(stores_with_new_data, on="store", how="inner")
else:
    print("Full (initial) run")
    stores_to_run = eligible

n_groups = stores_to_run.count()
if n_groups == 0:
    print("No stores with new data — skipping forecast. Pipeline complete.")
    write_checkpoint(CHECKPOINT, PIPELINE_CONFIG["run_date"])
    spark.stop()
    raise SystemExit(0)

# model_input always uses the full silver history (Prophet needs all history to fit).
model_input = silver_full.join(stores_to_run, on="store", how="inner")

# ── Gold UDF ─────────────────────────────────────────────────────────────────

gold_schema = StructType([
    StructField("store",      StringType(), False),
    StructField("ds",         DateType(),   False),
    StructField("yhat",       DoubleType(), True),
    StructField("yhat_lower", DoubleType(), True),
    StructField("yhat_upper", DoubleType(), True),
    StructField("trend",      DoubleType(), True),
    StructField("split",      StringType(), False),
    StructField("run_date",   StringType(), False),
])

_HORIZON   = PIPELINE_CONFIG["forecast_horizon"]
_CPS       = PIPELINE_CONFIG["changepoint_prior_scale"]
_IW        = PIPELINE_CONFIG["interval_width"]
_RUN_DATE  = PIPELINE_CONFIG["run_date"]


def gold_forecast(group_df: pd.DataFrame) -> pd.DataFrame:
    from prophet import Prophet

    store = group_df["store"].iloc[0]
    group_df = group_df.sort_values("ds").copy()
    group_df["ds"] = pd.to_datetime(group_df["ds"])

    if len(group_df) < 60:
        return pd.DataFrame(columns=[f.name for f in gold_schema])

    train = group_df[["ds", "y", "is_weekend"]].copy()

    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=False,
        seasonality_mode="additive",
        changepoint_prior_scale=_CPS,
        interval_width=_IW,
    )
    model.add_country_holidays(country_name="US")
    model.add_regressor("is_weekend", standardize=False)
    model.fit(train)

    future = model.make_future_dataframe(periods=_HORIZON, include_history=True)
    future["is_weekend"] = future["ds"].dt.dayofweek.isin([5, 6]).astype(float)
    fc = model.predict(future)

    last_train = train["ds"].max()
    fc["split"] = fc["ds"].apply(
        lambda d: "historical" if d <= last_train else "forecast"
    )
    fc["store"]    = store
    fc["run_date"] = _RUN_DATE

    return fc[["store", "ds", "yhat", "yhat_lower", "yhat_upper", "trend", "split", "run_date"]] \
             .assign(ds=lambda df: df["ds"].dt.date)


n_groups = eligible.count()

gold_sdf = (
    model_input
    .repartition(n_groups, "store")
    .groupby("store")
    .applyInPandas(gold_forecast, schema=gold_schema)
    .cache()
)

# Partition by both run_date (for incremental querying) and store
write_parquet(gold_sdf, GOLD_PATH, partition_by=["run_date", "store"])

# Update checkpoint
write_checkpoint(CHECKPOINT, PIPELINE_CONFIG["run_date"])
print(f"Checkpoint updated → {PIPELINE_CONFIG['run_date']}")

# ── GOLD ANALYTICS ───────────────────────────────────────────────────────────

gold_in = spark.read.parquet(GOLD_PATH)
silver_actuals = spark.read.parquet(SILVER_PATH).withColumnRenamed("y", "actual")

# Accuracy report (historical fit)
accuracy = (
    gold_in
    .filter(F.col("split") == "historical")
    .join(silver_actuals.select("store", "ds", "actual"), on=["store", "ds"], how="inner")
    .withColumn("error",     F.col("yhat") - F.col("actual"))
    .withColumn("abs_error", F.abs("error"))
    .withColumn("pct_error",
        F.when(F.col("actual") > 0, F.abs("error") / F.col("actual") * 100)
         .otherwise(F.lit(None).cast(DoubleType())))
    .groupby("store")
    .agg(
        F.sqrt(F.mean(F.pow("error", 2))).alias("rmse"),
        F.mean("abs_error").alias("mae"),
        F.mean("pct_error").alias("mape_pct"),
        F.expr("percentile(pct_error, 0.5)").alias("mdape_pct"),
    )
)

print("═" * 55)
print("GOLD — accuracy per store")
print("═" * 55)
accuracy.show()

# Anomaly detection: actual outside prediction interval
anomalies = (
    gold_in
    .filter(F.col("split") == "historical")
    .join(silver_actuals.select("store", "ds", "actual"), on=["store", "ds"])
    .filter(
        (F.col("actual") < F.col("yhat_lower"))
        | (F.col("actual") > F.col("yhat_upper"))
    )
    .select("store", "ds", "actual", "yhat", "yhat_lower", "yhat_upper")
    .orderBy("store", "ds")
)

print("GOLD — anomalous days (outside prediction interval):")
anomalies.show(20)

# Future forecast summary
print("GOLD — 30-day forecast summary:")
gold_in.filter(F.col("split") == "forecast") \
       .groupby("store") \
       .agg(
           F.min("ds").alias("forecast_from"),
           F.max("ds").alias("forecast_to"),
           F.round(F.avg("yhat"), 2).alias("avg_daily_forecast"),
           F.round(F.sum("yhat"), 0).alias("total_forecast"),
       ) \
       .show()

# ── SPARK SQL VIEW LAYER (BI / dashboard layer) ───────────────────────────────

gold_in.createOrReplaceTempView("gold_forecasts")
silver_actuals.createOrReplaceTempView("silver_actuals")

spark.sql("""
    SELECT
        g.store,
        DATE_FORMAT(g.ds, 'yyyy-MM')              AS month,
        ROUND(SUM(g.yhat),   0)                   AS forecast_revenue,
        ROUND(SUM(a.actual), 0)                   AS actual_revenue,
        ROUND(
            100.0 * (SUM(g.yhat) - SUM(a.actual))
                   / NULLIF(SUM(a.actual), 0), 2
        )                                          AS bias_pct,
        COUNT(*)                                   AS n_days
    FROM gold_forecasts g
    JOIN silver_actuals a USING (store, ds)
    WHERE g.split = 'historical'
    GROUP BY g.store, month
    ORDER BY g.store, month
""").show(40)

# ── PIPELINE SUMMARY ─────────────────────────────────────────────────────────
print("\n" + "═" * 55)
print("PIPELINE SUMMARY")
print("═" * 55)
print(f"  Run date     : {PIPELINE_CONFIG['run_date']}")
print(f"  Bronze rows  : {spark.read.parquet(BRONZE_PATH).count()}")
print(f"  Silver rows  : {spark.read.parquet(SILVER_PATH).count()}")
print(f"  Rejected rows: {spark.read.parquet(REJECT_PATH).count()}")
print(f"  Gold rows    : {spark.read.parquet(GOLD_PATH).count()}")
print(f"  Stores       : {', '.join(PIPELINE_CONFIG['groups'])}")
print(f"  Horizon      : {PIPELINE_CONFIG['forecast_horizon']} days")
print("═" * 55)

spark.stop()
