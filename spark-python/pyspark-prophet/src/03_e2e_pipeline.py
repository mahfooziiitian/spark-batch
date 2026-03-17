"""
End-to-End Pipeline — PySpark ETL → Prophet → Spark Results
=============================================================
Demonstrates a production-style pipeline:

  Stage 1 — Ingest raw CSV data with PySpark
  Stage 2 — Clean & aggregate to daily grain in Spark SQL
  Stage 3 — Identify groups that have enough history to model
  Stage 4 — Forecast each group with Prophet via applyInPandas
  Stage 5 — Post-process: anomaly flags, residuals, trend extraction
  Stage 6 — Write outputs (Parquet partitioned by group + forecast date)
"""

from __future__ import annotations

import pandas as pd
import numpy as np
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DateType, DoubleType, LongType,
)

# ────────────────────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("e2e-prophet-pipeline")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── STAGE 1: INGEST ──────────────────────────────────────────────────────────
# Simulate a CSV landing zone for multi-region daily transaction data.

np.random.seed(7)
REGIONS = ["NORTH", "SOUTH", "EAST", "WEST"]
START   = date(2021, 1, 1)
END     = date(2023, 12, 31)
DATES   = pd.date_range(START, END, freq="D")


def make_csv_data() -> str:
    rows = []
    for region in REGIONS:
        offset = {"NORTH": 0, "SOUTH": 5, "EAST": -3, "WEST": 8}[region]
        for d in DATES:
            trend  = 500 + 0.08 * (d - DATES[0]).days + offset * 10
            season = 80 * np.sin(2 * np.pi * d.dayofyear / 365)
            weekly = 40 * (1 if d.dayofweek < 5 else -1)
            noise  = np.random.normal(0, 20)
            # deliberately introduce some dirty rows
            revenue = max(0.0, trend + season + weekly + noise)
            qty     = max(0, int(revenue / 12 + np.random.randint(-3, 4)))
            rows.append(f"{region},{d.date()},{revenue:.2f},{qty}")
    # inject NULLs and duplicates for ETL cleaning demo
    rows[5]   = "NORTH,,abc,0"      # bad date + bad revenue
    rows[10]  = rows[9]             # duplicate
    return "region,sale_date,revenue,quantity\n" + "\n".join(rows)


raw_csv = make_csv_data()

raw_schema = StructType(
    [
        StructField("region",    StringType(), True),
        StructField("sale_date", StringType(), True),  # read as string first
        StructField("revenue",   StringType(), True),  # cast after cleaning
        StructField("quantity",  StringType(), True),
    ]
)

raw_sdf = spark.read.csv(
    spark.sparkContext.parallelize(raw_csv.splitlines()),
    schema=raw_schema,
    header=True,
)
print(f"Raw rows: {raw_sdf.count()}")

# ── STAGE 2: CLEAN & AGGREGATE ───────────────────────────────────────────────

clean_sdf = (
    raw_sdf
    # Cast to proper types; nulls appear for unparseable values
    .withColumn("ds",  F.to_date("sale_date", "yyyy-MM-dd"))
    .withColumn("rev", F.col("revenue").cast(DoubleType()))
    .withColumn("qty", F.col("quantity").cast(LongType()))
    # Drop rows missing key fields
    .filter(F.col("ds").isNotNull() & F.col("rev").isNotNull())
    .filter(F.col("rev") >= 0)
    # Deduplicate on natural key
    .dropDuplicates(["region", "ds"])
    .select("region", "ds", "rev", "qty")
)

# Daily grain — already at daily level but aggregate in case of late arrivals
daily_sdf = (
    clean_sdf
    .groupby("region", "ds")
    .agg(
        F.sum("rev").alias("y"),
        F.sum("qty").alias("total_qty"),
    )
    .orderBy("region", "ds")
)

daily_sdf.printSchema()
daily_sdf.show(5)
print(f"Clean daily rows: {daily_sdf.count()}")

# ── STAGE 3: FILTER GROUPS WITH SUFFICIENT HISTORY ──────────────────────────
# Prophet needs at least ~2 seasonal cycles to fit reliably.

MIN_DAYS = 365 * 1  # require at least 1 year

eligible_regions = (
    daily_sdf
    .groupby("region")
    .agg(F.count("ds").alias("n_days"))
    .filter(F.col("n_days") >= MIN_DAYS)
    .select("region")
)

model_input = daily_sdf.join(eligible_regions, on="region", how="inner")
print(f"Groups eligible for modelling: {eligible_regions.count()}")

# ── STAGE 4: PROPHET FORECAST ────────────────────────────────────────────────

HORIZON      = 90
TRAIN_END    = date(2023, 9, 30)  # cutoff; remainder becomes hold-out
CUTOFF_STR   = str(TRAIN_END)

forecast_schema = StructType(
    [
        StructField("region",      StringType(), False),
        StructField("ds",          DateType(),   False),
        StructField("yhat",        DoubleType(), True),
        StructField("yhat_lower",  DoubleType(), True),
        StructField("yhat_upper",  DoubleType(), True),
        StructField("trend",       DoubleType(), True),
        StructField("yearly",      DoubleType(), True),
        StructField("weekly",      DoubleType(), True),
        StructField("split",       StringType(), False),  # train | test | future
    ]
)


def prophet_forecast(group_df: pd.DataFrame) -> pd.DataFrame:
    from prophet import Prophet

    region = group_df["region"].iloc[0]
    group_df = group_df.sort_values("ds")
    group_df["ds"] = pd.to_datetime(group_df["ds"])

    train = group_df[group_df["ds"] <= pd.Timestamp(CUTOFF_STR)][["ds", "y"]]
    hold  = group_df[group_df["ds"] >  pd.Timestamp(CUTOFF_STR)][["ds", "y"]]

    if len(train) < 60:
        return pd.DataFrame(columns=[f.name for f in forecast_schema])

    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=False,
        seasonality_mode="multiplicative",
        changepoint_prior_scale=0.05,
        interval_width=0.95,
    )
    model.add_country_holidays(country_name="US")
    model.fit(train)

    future = model.make_future_dataframe(periods=HORIZON + len(hold), include_history=True)
    forecast = model.predict(future)

    # tag each row's split
    train_dates  = set(train["ds"])
    hold_dates   = set(pd.to_datetime(hold["ds"])) if len(hold) else set()
    last_actual  = group_df["ds"].max()

    def split_label(d):
        if d in train_dates:
            return "train"
        if d in hold_dates:
            return "test"
        return "future"

    forecast["split"]  = forecast["ds"].apply(split_label)
    forecast["region"] = region

    # Only keep the components that exist (Prophet generates them conditionally)
    for col in ["yearly", "weekly"]:
        if col not in forecast.columns:
            forecast[col] = np.nan

    return (
        forecast[["region", "ds", "yhat", "yhat_lower", "yhat_upper",
                   "trend", "yearly", "weekly", "split"]]
        .assign(ds=lambda df: df["ds"].dt.date)
    )


n_regions = eligible_regions.count()

forecast_sdf = (
    model_input
    .repartition(n_regions, "region")
    .groupby("region")
    .applyInPandas(prophet_forecast, schema=forecast_schema)
    .cache()
)

forecast_sdf.show(10)

# ── STAGE 5: POST-PROCESSING ─────────────────────────────────────────────────

# 5a — Join actuals back for residual analysis
results = (
    forecast_sdf
    .join(
        daily_sdf.withColumnRenamed("y", "actual"),
        on=["region", "ds"],
        how="left",
    )
    .withColumn("residual", F.col("yhat") - F.col("actual"))
    .withColumn(
        "pct_error",
        F.when(
            F.col("actual").isNotNull() & (F.col("actual") > 0),
            F.abs(F.col("residual")) / F.col("actual") * 100,
        ).otherwise(F.lit(None).cast(DoubleType()))
    )
)

# 5b — Anomaly flag: actual outside the 95 % prediction interval
anomalies = (
    results
    .filter(F.col("split").isin("train", "test"))
    .withColumn(
        "is_anomaly",
        (F.col("actual") < F.col("yhat_lower"))
        | (F.col("actual") > F.col("yhat_upper")),
    )
    .filter(F.col("is_anomaly"))
    .select("region", "ds", "actual", "yhat", "yhat_lower", "yhat_upper")
)

print("Anomalous days (outside 95 % interval):")
anomalies.orderBy("region", "ds").show(20)

# 5c — Hold-out accuracy KPIs per region
hold_kpis = (
    results
    .filter(F.col("split") == "test")
    .groupby("region")
    .agg(
        F.sqrt(F.mean(F.pow("residual", 2))).alias("rmse"),
        F.mean(F.abs("residual")).alias("mae"),
        F.mean("pct_error").alias("mape_pct"),
        F.expr("percentile(pct_error, 0.5)").alias("mdape_pct"),
    )
)

print("Hold-out accuracy by region:")
hold_kpis.show()

# 5d — Rolling 7-day average of forecast (smoothed signal for dashboards)
w = Window.partitionBy("region").orderBy("ds").rowsBetween(-6, 0)

results_smooth = results.withColumn(
    "yhat_7d_avg", F.avg("yhat").over(w)
)

# ── STAGE 6: WRITE OUTPUTS ───────────────────────────────────────────────────

BASE_PATH      = "/tmp/e2e_forecast"
FORECAST_DATE  = str(date.today())  # partition label for pipeline idempotency

(
    results_smooth
    .withColumn("forecast_date", F.lit(FORECAST_DATE))
    .write
    .mode("overwrite")
    .partitionBy("forecast_date", "region")
    .parquet(f"{BASE_PATH}/forecasts")
)

(
    hold_kpis
    .withColumn("forecast_date", F.lit(FORECAST_DATE))
    .write
    .mode("overwrite")
    .parquet(f"{BASE_PATH}/kpis")
)

# Verify
spark.read.parquet(f"{BASE_PATH}/forecasts").filter(
    F.col("split") == "future"
).orderBy("region", "ds").show(12)

spark.read.parquet(f"{BASE_PATH}/kpis").show()

# ── SPARK SQL ANALYTICS ──────────────────────────────────────────────────────

results_smooth.createOrReplaceTempView("forecasts")
daily_sdf.createOrReplaceTempView("actuals")

spark.sql("""
    SELECT
        region,
        date_format(ds, 'yyyy-MM') AS month,
        ROUND(SUM(yhat), 0)        AS forecast_revenue,
        ROUND(SUM(actual), 0)      AS actual_revenue,
        ROUND(
            100.0 * (SUM(yhat) - SUM(actual)) / NULLIF(SUM(actual), 0),
            2
        )                          AS bias_pct
    FROM forecasts
    WHERE split = 'test'
    GROUP BY region, month
    ORDER BY region, month
""").show(30)

spark.stop()
