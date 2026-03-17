---
mode: ask
description: Add a new time-series feature column to a PySpark DataFrame
---

# Add a Time-Series Feature

I need a new feature column added to a PySpark time-series DataFrame in this project.

**DataFrame variable name:** [FILL IN]
**Group key column:** [FILL IN — e.g. `store`, `sku`]
**Date column:** [FILL IN — e.g. `ds` (DateType)]
**Value column:** [FILL IN — e.g. `y` (DoubleType)]
**Feature to add:** [FILL IN — choose one or describe]

Available feature types:
- **Lag** — `y_lagN` (value N days ago within group)
- **Rolling mean/std/min/max** — trailing N-day window
- **Rolling z-score** — `(y - roll_mean) / roll_std`
- **Calendar flag** — `is_weekend`, `is_month_end`, `is_quarter_start`, `day_of_week`
- **WoW / MoM growth rate** — percentage change vs lag7 / lag28
- **Forward-fill gap** — fill nulls with last known value within group
- **Linear interpolation** — `applyInPandas` UDF using `pd.Series.interpolate`

Please generate:
1. The `Window` spec (partitioned by group key, ordered by date column)
2. The `.withColumn(...)` expression using `pyspark.sql.functions`
3. A short comment explaining what the feature captures

Follow the patterns in `src/06_pyspark_timeseries_features.py`.
