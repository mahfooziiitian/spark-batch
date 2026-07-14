---
mode: ask
description: Add a new group-level Prophet forecast to the distributed pipeline
---

# Add a Distributed Prophet Forecast

I need to add a new `applyInPandas` Prophet forecast UDF to this PySpark project.

**Group key column:** [FILL IN — e.g. `sku_id`, `region`]
**Input Spark DataFrame:** [FILL IN — e.g. `silver_sdf` with columns `group_key`, `ds`, `y`]
**Forecast horizon (days):** [FILL IN]
**Extra regressors (if any):** [FILL IN or "none"]

Please generate:
1. A `result_schema` StructType with columns: `group_key`, `ds`, `yhat`, `yhat_lower`, `yhat_upper`, `trend`, `split`, `run_date`
2. A `forecast_group(group_df: pd.DataFrame) -> pd.DataFrame` UDF that:
   - Imports Prophet locally inside the function
   - Guards against groups with < 60 rows
   - Uses `yearly_seasonality=True`, `weekly_seasonality=True`, `seasonality_mode="additive"`
   - Adds any specified regressors
   - Tags rows as `"historical"` or `"forecast"` in a `split` column
   - Returns `ds` as `dt.date`
3. The `.repartition(n_groups, "group_key").groupby(...).applyInPandas(...)` call
4. A guard block that exits cleanly if `n_groups == 0`

Follow the patterns in `src/02_pyspark_prophet_distributed.py`.
