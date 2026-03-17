---
applyTo: "src/**/*.ipynb, src/**/*.py"
---

# PySpark Patterns

## SparkSession
Always configure Arrow and explicit shuffle partitions:
```python
spark = (
    SparkSession.builder
    .appName("app-name")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .config("spark.sql.execution.arrow.maxRecordsPerBatch", "50000")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
```

## applyInPandas UDFs
1. Declare `result_schema` as a `StructType` **before** the function.
2. Repartition to exactly one partition per group **before** `groupby`:
   ```python
   sdf.repartition(n_groups, "group_col").groupby("group_col").applyInPandas(fn, schema)
   ```
3. Guard zero partitions immediately after `.count()`:
   ```python
   if n_groups == 0:
       spark.stop(); raise SystemExit(0)
   ```
4. Return `ds` as `dt.date` (not `Timestamp`) from the UDF:
   ```python
   .assign(ds=lambda df: df["ds"].dt.date)
   ```

## Window Functions
- Always `partitionBy` the group key; never rely on global sort.
- Trailing window:  `.rowsBetween(-(n-1), 0)`
- Forward-fill:     `F.last(col, ignorenulls=True).over(w_unbounded_preceding)`
- Back-fill:        `F.first(col, ignorenulls=True).over(w_unbounded_following)`

## Joins
- Use `F.broadcast(small_sdf)` for lookup tables < a few MB.
- Prefer `on=["col"]` over `on=sdf.col == other.col` to avoid column ambiguity.

## Writes
```python
sdf.write.mode("overwrite").partitionBy("date_col", "group_col").parquet(path)
```

## Caching
Call `.cache()` on any DataFrame that is:
- Returned from an expensive UDF and referenced more than once.
- Used in both the write step and the analytics step.

## Spark SQL
Register views with `createOrReplaceTempView` and write analytics as
multi-line SQL strings — easier to maintain than deeply chained API calls.
