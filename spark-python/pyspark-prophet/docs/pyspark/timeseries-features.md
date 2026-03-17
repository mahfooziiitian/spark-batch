# Time-Series Feature Engineering

Build model-ready feature DataFrames from raw daily data entirely within Spark.

---

## Calendar features

Extract every useful date attribute in one chain:

```python
from pyspark.sql import functions as F

sdf = (
    raw_sdf
    .withColumn("year",         F.year("ds"))
    .withColumn("quarter",      F.quarter("ds"))
    .withColumn("month",        F.month("ds"))
    .withColumn("week_of_year", F.weekofyear("ds"))
    .withColumn("day_of_week",  F.dayofweek("ds"))       # 1=Sun, 7=Sat
    .withColumn("day_name",     F.date_format("ds", "EEEE"))
    .withColumn("is_weekend",   F.dayofweek("ds").isin([1, 7]).cast("int"))
    .withColumn("is_month_end",
        (F.dayofmonth("ds") == F.dayofmonth(F.last_day("ds"))).cast("int"))
    .withColumn("iso_yearweek",
        F.concat(F.year("ds"), F.lit("-W"), F.lpad(F.weekofyear("ds"), 2, "0")))
)
```

---

## Lag features

Capture autocorrelation — "what was the value $N$ days ago?"

```python
from pyspark.sql import Window

w = Window.partitionBy("store").orderBy("ds")

sdf = (
    sdf
    .withColumn("y_lag1",   F.lag("y", 1).over(w))    # yesterday
    .withColumn("y_lag7",   F.lag("y", 7).over(w))    # same day last week
    .withColumn("y_lag28",  F.lag("y", 28).over(w))   # 4 weeks ago
    .withColumn("y_lag365", F.lag("y", 365).over(w))  # same day last year
    .withColumn("wow_pct",
        F.when(F.col("y_lag7") > 0,
               (F.col("y") - F.col("y_lag7")) / F.col("y_lag7") * 100
        ).otherwise(None))
)
```

---

## Rolling-window statistics

```python
def trailing(w_base, n):
    return w_base.rowsBetween(-(n - 1), 0)

sdf = (
    sdf
    .withColumn("roll7_mean", F.avg("y").over(trailing(w, 7)))
    .withColumn("roll7_std",  F.stddev("y").over(trailing(w, 7)))
    .withColumn("roll7_min",  F.min("y").over(trailing(w, 7)))
    .withColumn("roll7_max",  F.max("y").over(trailing(w, 7)))
    .withColumn("roll28_mean",F.avg("y").over(trailing(w, 28)))
    # Z-score: how many std-devs from rolling mean?
    .withColumn("zscore_7d",
        F.when(F.col("roll7_std") > 0,
               (F.col("y") - F.col("roll7_mean")) / F.col("roll7_std")
        ).otherwise(0.0))
)
```

---

## Gap detection & filling

### Forward-fill (carry last known value forward)

```python
w_ff = Window.partitionBy("store").orderBy("ds").rowsBetween(Window.unboundedPreceding, 0)
w_bf = Window.partitionBy("store").orderBy("ds").rowsBetween(0, Window.unboundedFollowing)

# Join to a full date spine first to expose gaps as NULLs
full_grid = date_spine.join(sdf, on=["store", "ds"], how="left")

filled = (
    full_grid
    .withColumn("y_ffill", F.last("y", ignorenulls=True).over(w_ff))
    .withColumn("y_bfill", F.first("y", ignorenulls=True).over(w_bf))
)
```

### Linear interpolation (via `applyInPandas`)

```python
def interpolate_group(group_df: pd.DataFrame) -> pd.DataFrame:
    group_df = group_df.sort_values("ds").copy()
    group_df["y_interp"] = group_df["y"].interpolate(method="linear", limit_direction="both")
    return group_df

interp_sdf = (
    sdf
    .repartition(n_groups, "store")
    .groupby("store")
    .applyInPandas(interpolate_group, schema=interp_schema)
)
```

---

## Outlier detection & capping

### IQR fence

```python
quantiles = (
    sdf.groupby("store")
    .agg(
        F.expr("percentile(y, 0.25)").alias("q1"),
        F.expr("percentile(y, 0.75)").alias("q3"),
    )
    .withColumn("iqr",          F.col("q3") - F.col("q1"))
    .withColumn("lower_fence",  F.col("q1") - 1.5 * F.col("iqr"))
    .withColumn("upper_fence",  F.col("q3") + 1.5 * F.col("iqr"))
)

sdf = (
    sdf.join(quantiles, on="store")
    .withColumn("is_outlier",
        (F.col("y") < F.col("lower_fence")) | (F.col("y") > F.col("upper_fence")))
    # For Prophet: mask rather than clip
    .withColumn("y_masked",
        F.when(F.col("is_outlier"), None).otherwise(F.col("y")))
)
```

---

## Multi-resolution rollups

```python
# Daily → Weekly
weekly = (
    sdf
    .withColumn("week_start", F.date_trunc("week", "ds"))
    .groupby("store", "week_start")
    .agg(F.sum("y").alias("y_weekly"), F.avg("y").alias("y_daily_avg"))
)

# Daily → Monthly
monthly = (
    sdf
    .withColumn("month_start", F.date_trunc("month", "ds"))
    .groupby("store", "month_start")
    .agg(F.sum("y").alias("y_monthly"))
)
```

---

## Wide ↔ Long (pivot / stack)

```python
# Long → Wide: one column per store
wide = (
    sdf.groupby("ds")
    .pivot("store", ["store_A", "store_B", "store_C"])
    .agg(F.first("y"))
)
```

---

## Broadcast-join event table

```python
events_sdf = spark.createDataFrame(events_pd).withColumn("ds", F.col("ds").cast(DateType()))

enriched = sdf.join(F.broadcast(events_sdf), on="ds", how="left") \
              .withColumn("event_name",   F.coalesce("event_name",   F.lit("none"))) \
              .withColumn("event_weight", F.coalesce("event_weight", F.lit(1.0)))
```

---

## Source file

```
src/06_pyspark_timeseries_features.py   ← all 11 sections
```
