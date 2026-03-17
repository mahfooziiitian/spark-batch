"""
PySpark Time-Series Feature Engineering
=========================================
Everything you need to turn raw event/transaction data into a
model-ready daily time-series DataFrame.

  1.  Calendar feature extraction (DOW, month, quarter, ISO-week, flags)
  2.  Lag features  (t-1, t-7, t-28)
  3.  Rolling-window statistics  (mean, std, min, max, median)
  4.  Gap detection & filling  (forward-fill, back-fill, linear interpolation)
  5.  Outlier detection — IQR & z-score capping in Spark
  6.  Multi-resolution rollups  (daily → weekly → monthly)
  7.  Wide ↔ long (pivot / stack) transformations
  8.  Joining external calendar / event tables
  9.  Sessionisation  (group consecutive events into sessions)
  10. Seasonal decomposition proxy  (trend isolation with rolling baseline)
  11. Exporting to Prophet-ready pandas DataFrames
"""

from __future__ import annotations

import pandas as pd
import numpy as np
from datetime import date, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DateType, DoubleType, LongType, IntegerType, BooleanType,
)

# ─────────────────────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("timeseries-feature-engineering")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ─────────────────────────────────────────────────────────────────────────────
# SYNTHETIC DATASET — daily sales for 4 product categories
# ─────────────────────────────────────────────────────────────────────────────
np.random.seed(99)
CATEGORIES = ["electronics", "clothing", "food", "furniture"]
DATES       = pd.date_range("2021-01-01", "2023-12-31", freq="D")

rows = []
for cat in CATEGORIES:
    base   = {"electronics": 500, "clothing": 300, "food": 800, "furniture": 200}[cat]
    for i, d in enumerate(DATES):
        trend  = base + 0.04 * i
        yearly = 60  * np.sin(2 * np.pi * d.dayofyear / 365)
        weekly = 30  * (1 if d.dayofweek < 5 else -0.5)
        noise  = np.random.normal(0, base * 0.05)
        # inject occasional spike (flash sale)
        spike  = base * 0.8 if (d.month == 11 and d.day == 27) else 0  # Black Friday
        rows.append((cat, d.date(), max(0.0, trend + yearly + weekly + noise + spike)))

raw_pd  = pd.DataFrame(rows, columns=["category", "ds", "y"])
raw_sdf = spark.createDataFrame(raw_pd).withColumn("ds", F.col("ds").cast(DateType()))
raw_sdf.printSchema()
raw_sdf.show(5)

# ── 1. CALENDAR FEATURE EXTRACTION ───────────────────────────────────────────
# Extract every useful date component in one withColumn chain.

calendar_sdf = (
    raw_sdf
    .withColumn("year",        F.year("ds"))
    .withColumn("quarter",     F.quarter("ds"))
    .withColumn("month",       F.month("ds"))
    .withColumn("week_of_year",F.weekofyear("ds"))
    .withColumn("day_of_year", F.dayofyear("ds"))
    .withColumn("day_of_month",F.dayofmonth("ds"))
    .withColumn("day_of_week", F.dayofweek("ds"))   # 1=Sun, 7=Sat
    # Flags
    .withColumn("is_weekend",  F.dayofweek("ds").isin([1, 7]).cast(IntegerType()))
    .withColumn("is_month_start", (F.dayofmonth("ds") == 1).cast(IntegerType()))
    .withColumn("is_month_end",
        (F.dayofmonth("ds") == F.dayofmonth(F.last_day("ds"))).cast(IntegerType()))
    .withColumn("is_quarter_start",
        ((F.month("ds").isin([1, 4, 7, 10])) & (F.dayofmonth("ds") == 1))
        .cast(IntegerType()))
    # Day-name string (useful for filtering / display)
    .withColumn("day_name",    F.date_format("ds", "EEEE"))
    # ISO year-week label  e.g. "2023-W42"
    .withColumn("iso_yearweek",
        F.concat(F.year("ds"), F.lit("-W"), F.lpad(F.weekofyear("ds"), 2, "0")))
)

calendar_sdf.select(
    "ds", "year", "quarter", "month", "day_of_week", "is_weekend",
    "is_month_end", "day_name", "iso_yearweek"
).show(7, truncate=False)

# ── 2. LAG FEATURES ──────────────────────────────────────────────────────────
# Lags capture autocorrelation — "what was the value N days ago?"
# Window ordered by ds within each category; `lag(col, N)` looks N rows back.

w_cat = Window.partitionBy("category").orderBy("ds")

lag_sdf = (
    calendar_sdf
    .withColumn("y_lag1",  F.lag("y", 1).over(w_cat))   # yesterday
    .withColumn("y_lag7",  F.lag("y", 7).over(w_cat))   # same day last week
    .withColumn("y_lag28", F.lag("y", 28).over(w_cat))  # same day 4 weeks ago
    .withColumn("y_lag365",F.lag("y", 365).over(w_cat)) # same day last year
    # Week-over-week growth rate
    .withColumn("wow_growth",
        F.when(F.col("y_lag7") > 0,
               (F.col("y") - F.col("y_lag7")) / F.col("y_lag7") * 100
        ).otherwise(F.lit(None).cast(DoubleType())))
)

lag_sdf.select("category", "ds", "y", "y_lag1", "y_lag7", "y_lag28", "wow_growth") \
       .filter(F.col("category") == "food") \
       .orderBy("ds").show(15)

# ── 3. ROLLING-WINDOW STATISTICS ─────────────────────────────────────────────
# rowsBetween(-N+1, 0) defines a trailing N-day window.
# Note: rows with fewer than N predecessors get partial-window stats (NaN for std).

def rolling(w_base, n):
    """Return a window spec for trailing n rows within partition."""
    return w_base.rowsBetween(-(n - 1), 0)


roll_sdf = (
    lag_sdf
    # 7-day rolling
    .withColumn("roll7_mean", F.avg("y").over(rolling(w_cat, 7)))
    .withColumn("roll7_std",  F.stddev("y").over(rolling(w_cat, 7)))
    .withColumn("roll7_min",  F.min("y").over(rolling(w_cat, 7)))
    .withColumn("roll7_max",  F.max("y").over(rolling(w_cat, 7)))
    # 28-day rolling
    .withColumn("roll28_mean",F.avg("y").over(rolling(w_cat, 28)))
    .withColumn("roll28_std", F.stddev("y").over(rolling(w_cat, 28)))
    # Rolling z-score: how many std-devs from the rolling mean?
    .withColumn("zscore_7d",
        F.when(F.col("roll7_std") > 0,
               (F.col("y") - F.col("roll7_mean")) / F.col("roll7_std")
        ).otherwise(F.lit(0.0)))
    # Exponentially weighted moving average approximation via recursive lag trick
    # True EWMA requires a UDF; this is a simple 7-day triangular weight proxy
    .withColumn("ema7_proxy",
        (F.col("y") * 4
         + F.lag("y", 1).over(w_cat) * 3
         + F.lag("y", 2).over(w_cat) * 2
         + F.lag("y", 3).over(w_cat) * 1
        ) / F.lit(10.0))
)

roll_sdf.select(
    "category", "ds", "y", "roll7_mean", "roll7_std", "zscore_7d", "ema7_proxy"
).filter(F.col("category") == "electronics").orderBy("ds").show(20)

# ── 4. GAP DETECTION & FILLING ───────────────────────────────────────────────
# Drop some dates to simulate missing data, then restore the full grid.

# 4a — Create reference date grid per category
all_dates = spark.createDataFrame(
    [(cat, d.date()) for cat in CATEGORIES for d in DATES],
    schema=StructType([
        StructField("category", StringType(), False),
        StructField("ds",       DateType(),   False),
    ])
)

# 4b — Left-join actuals; gaps appear as NULL y
full_grid = all_dates.join(
    roll_sdf.select("category", "ds", "y"),
    on=["category", "ds"],
    how="left",
)

# 4c — Forward-fill: carry the last known value into the gap
#       last(col, ignorenulls=True) with unbounded preceding returns the most
#       recent non-null value seen so far in the window order.
w_ff = Window.partitionBy("category").orderBy("ds") \
             .rowsBetween(Window.unboundedPreceding, 0)

filled_sdf = full_grid.withColumn(
    "y_ffill",
    F.last("y", ignorenulls=True).over(w_ff),
)

# 4d — Back-fill (fill nulls with NEXT known value)
w_bf = Window.partitionBy("category").orderBy("ds") \
             .rowsBetween(0, Window.unboundedFollowing)

filled_sdf = filled_sdf.withColumn(
    "y_bfill",
    F.first("y", ignorenulls=True).over(w_bf),
)

# 4e — Linear interpolation via pandas UDF (Spark has no built-in)
interp_schema = StructType([
    StructField("category", StringType(), False),
    StructField("ds",       DateType(),   False),
    StructField("y",        DoubleType(), True),
    StructField("y_interp", DoubleType(), True),
])


def interpolate_group(group_df: pd.DataFrame) -> pd.DataFrame:
    group_df = group_df.sort_values("ds").copy()
    group_df["y_interp"] = group_df["y"].interpolate(method="linear", limit_direction="both")
    return group_df[["category", "ds", "y", "y_interp"]]


interp_sdf = (
    filled_sdf.select("category", "ds", "y")
    .repartition(len(CATEGORIES), "category")
    .groupby("category")
    .applyInPandas(interpolate_group, schema=interp_schema)
)

interp_sdf.filter(F.col("y").isNull()).show(5)  # show filled gaps

# ── 5. OUTLIER DETECTION & CAPPING ───────────────────────────────────────────
# 5a — IQR-based capping: clip values outside [Q1 - 1.5·IQR, Q3 + 1.5·IQR]

quantiles = (
    roll_sdf
    .groupby("category")
    .agg(
        F.expr("percentile(y, 0.25)").alias("q1"),
        F.expr("percentile(y, 0.75)").alias("q3"),
    )
    .withColumn("iqr",        F.col("q3") - F.col("q1"))
    .withColumn("lower_fence",F.col("q1") - 1.5 * F.col("iqr"))
    .withColumn("upper_fence",F.col("q3") + 1.5 * F.col("iqr"))
)

outlier_sdf = (
    roll_sdf
    .join(quantiles, on="category", how="left")
    .withColumn("is_outlier",
        (F.col("y") < F.col("lower_fence")) | (F.col("y") > F.col("upper_fence")))
    .withColumn("y_capped",
        F.greatest(F.col("lower_fence"),
                   F.least(F.col("upper_fence"), F.col("y"))))
    # For Prophet: set outlier rows to NaN rather than clipping
    .withColumn("y_masked",
        F.when(F.col("is_outlier"), F.lit(None).cast(DoubleType()))
         .otherwise(F.col("y")))
)

print("Outlier counts by category:")
outlier_sdf.groupby("category").agg(F.sum(F.col("is_outlier").cast(LongType())).alias("n_outliers")).show()

# 5b — Z-score based: flag anything > 3 σ from the 28-day rolling mean
zscore_flagged = roll_sdf.withColumn(
    "is_zscore_outlier",
    F.abs(F.col("zscore_7d")) > 3.0,
)

# ── 6. MULTI-RESOLUTION ROLLUPS ───────────────────────────────────────────────

# Daily → Weekly (Sun to Sat)
weekly_sdf = (
    roll_sdf
    .withColumn("week_start",
        F.date_trunc("week", F.col("ds")))   # Monday-aligned by default in Spark
    .groupby("category", "week_start")
    .agg(
        F.sum("y").alias("y_weekly_sum"),
        F.avg("y").alias("y_weekly_avg"),
        F.stddev("y").alias("y_weekly_std"),
        F.max("y").alias("y_weekly_max"),
        F.count("y").alias("n_days"),
    )
    .orderBy("category", "week_start")
)

weekly_sdf.show(8)

# Daily → Monthly
monthly_sdf = (
    roll_sdf
    .withColumn("month_start", F.date_trunc("month", F.col("ds")))
    .groupby("category", "month_start")
    .agg(
        F.sum("y").alias("y_monthly_sum"),
        F.avg("y").alias("y_monthly_avg"),
        F.count("y").alias("n_days"),
    )
    .orderBy("category", "month_start")
)

monthly_sdf.show(8)

# ── 7. WIDE ↔ LONG (PIVOT / STACK) ───────────────────────────────────────────
# Wide: one column per category, one row per date
daily_wide = (
    roll_sdf
    .groupby("ds")
    .pivot("category", CATEGORIES)   # pivots category values into columns
    .agg(F.first("y"))
    .orderBy("ds")
)
daily_wide.show(5)

# Long → Wide for lags: useful for ML feature matrices
lag_wide = (
    roll_sdf
    .select("category", "ds", "y", "y_lag1", "y_lag7", "roll7_mean", "zscore_7d")
    .filter(F.col("category") == "food")
    .orderBy("ds")
)
lag_wide.show(10)

# ── 8. JOINING EXTERNAL CALENDAR / EVENT TABLE ───────────────────────────────
# A small promotions / events table broadcast to all workers (< few MB).
# Using broadcast ensures no shuffle; fast join for small lookup tables.

events_pd = pd.DataFrame({
    "ds":         pd.to_datetime([
        "2021-11-27", "2022-11-26", "2023-11-25",   # Black Fridays
        "2021-12-25", "2022-12-25", "2023-12-25",   # Christmas
        "2021-07-04", "2022-07-04", "2023-07-04",   # Independence Day
    ]).date,
    "event_name": [
        "black_friday", "black_friday", "black_friday",
        "christmas",    "christmas",    "christmas",
        "independance_day", "independance_day", "independance_day",
    ],
    "event_weight": [2.0, 2.0, 2.0, 1.5, 1.5, 1.5, 1.2, 1.2, 1.2],
})

events_sdf = spark.createDataFrame(events_pd) \
                  .withColumn("ds", F.col("ds").cast(DateType()))

enriched_sdf = (
    roll_sdf
    .join(F.broadcast(events_sdf), on="ds", how="left")
    .withColumn("event_name",   F.coalesce("event_name",   F.lit("none")))
    .withColumn("event_weight", F.coalesce("event_weight", F.lit(1.0)))
    # Adjusted y: down-weight event spikes for baseline model training
    .withColumn("y_adjusted", F.col("y") / F.col("event_weight"))
)

enriched_sdf.filter(F.col("event_name") != "none").show(10)

# ── 9. SESSIONISATION ────────────────────────────────────────────────────────
# Group consecutive daily records into "sessions" (periods of activity
# separated by gaps longer than a threshold).
# Common use-case: anomaly campaigns, promotional periods.

GAP_THRESHOLD = 7  # break a session if gap > 7 days

session_sdf = (
    roll_sdf
    .filter(F.col("category") == "food")
    .withColumn("prev_ds",    F.lag("ds", 1).over(w_cat))
    .withColumn("gap_days",   F.datediff("ds", "prev_ds"))
    # New session starts when gap > threshold or at the first row
    .withColumn("is_new_session",
        (F.col("gap_days") > GAP_THRESHOLD) | F.col("prev_ds").isNull())
    .withColumn("session_id",
        F.sum(F.col("is_new_session").cast(LongType())).over(w_cat))
)

session_stats = session_sdf.groupby("category", "session_id").agg(
    F.min("ds").alias("session_start"),
    F.max("ds").alias("session_end"),
    F.count("ds").alias("session_length"),
    F.avg("y").alias("session_avg_y"),
)
session_stats.orderBy("session_id").show(10)

# ── 10. SEASONAL DECOMPOSITION PROXY ─────────────────────────────────────────
# Without Prophet, you can isolate trend using a centred moving average.
# residual = y − trend_ma − weekly_pattern
# This is classic time-series decomposition in Spark.

ma_window = 365   # 365-day centred MA as trend proxy

w_centred = (
    Window.partitionBy("category")
          .orderBy("ds")
          .rowsBetween(-(ma_window // 2), ma_window // 2)
)

decomp_sdf = (
    roll_sdf
    .withColumn("trend_ma365", F.avg("y").over(w_centred))
    .withColumn("detrended",   F.col("y") - F.col("trend_ma365"))
    # Weekly pattern: average detrended value for each DOW
    .withColumn("dow", F.dayofweek("ds"))
)

dow_avg = (
    decomp_sdf
    .groupby("category", "dow")
    .agg(F.avg("detrended").alias("weekly_pattern"))
)

decomp_final = (
    decomp_sdf
    .join(dow_avg, on=["category", "dow"], how="left")
    .withColumn("residual",
        F.col("detrended") - F.col("weekly_pattern"))
)

decomp_final.select(
    "category", "ds", "y", "trend_ma365", "weekly_pattern", "residual"
).filter(F.col("category") == "electronics").orderBy("ds").show(15)

# ── 11. EXPORTING TO PROPHET-READY PANDAS ────────────────────────────────────
# Convert the Spark feature DataFrame back to pandas for a specific group,
# then pass directly to Prophet with regressors.

food_prophet_pd = (
    enriched_sdf
    .filter(F.col("category") == "food")
    .select(
        F.col("ds"),
        F.col("y"),
        F.col("is_weekend").cast(DoubleType()).alias("is_weekend"),
        F.col("event_weight"),
        F.col("roll7_mean").alias("rolling_mean_7d"),
    )
    .orderBy("ds")
    .toPandas()
)

food_prophet_pd["ds"] = pd.to_datetime(food_prophet_pd["ds"])
print("Prophet-ready DataFrame (food):")
print(food_prophet_pd.head(10).to_string(index=False))
print(f"\nShape: {food_prophet_pd.shape}")

# Pass directly to Prophet
from prophet import Prophet

m_food = Prophet(yearly_seasonality=True, weekly_seasonality=False, interval_width=0.95)
m_food.add_regressor("is_weekend",    standardize=False)
m_food.add_regressor("event_weight",  standardize=False)
m_food.add_regressor("rolling_mean_7d", standardize=True)
m_food.fit(food_prophet_pd)

future_food = m_food.make_future_dataframe(periods=90)
future_food["is_weekend"]     = future_food["ds"].dt.dayofweek.isin([5, 6]).astype(float)
future_food["event_weight"]   = 1.0
future_food["rolling_mean_7d"] = food_prophet_pd["y"].rolling(7).mean().iloc[-1]

fc_food = m_food.predict(future_food)
print(fc_food[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail(10).to_string(index=False))

spark.stop()
