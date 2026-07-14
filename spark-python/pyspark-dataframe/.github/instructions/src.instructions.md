---
applyTo: "src/**/*.py"
---

# PySpark DataFrame — Source Code Instructions

## Module Structure

The `src/` tree is organised by topic:

```
src/
└── data_frame/
    ├── analytical/                # Window functions and pivoting
    │   ├── window_function/
    │   │   ├── aggregate/         # sum, avg, min, max over windows
    │   │   ├── analytical/        # first, last, lead, lag
    │   │   ├── null_option/       # IGNORE NULLS / RESPECT NULLS
    │   │   ├── ranking/           # rank, dense_rank, row_number, ntile
    │   │   ├── specification/     # WindowSpec: partitionBy, orderBy, frames
    │   │   │   └── frame/
    │   │   │       ├── boundary/  # unboundedPreceding / unboundedFollowing
    │   │   │       ├── range_type/
    │   │   │       └── row_type/
    │   │   └── usage/             # real-world patterns (running_total, top_n…)
    │   └── pivoting/              # pivot / unpivot examples
    ├── columns/                   # Column operations, aliases, expressions
    ├── creation/                  # DataFrame creation from tuples, dicts, JSON
    │   ├── dictionary/
    │   ├── lists/
    │   └── tuples/
    ├── etl/                       # End-to-end ETL pipeline examples
    ├── joins/                     # inner, outer, cross, broadcast, self, natural
    │   ├── broadcast/
    │   ├── cross/
    │   ├── inner/
    │   ├── natural/
    │   ├── outer/
    │   │   ├── full/
    │   │   ├── left/
    │   │   └── right/
    │   └── self/
    ├── optimization/              # Caching, skew data, AQE
    │   ├── caching/
    │   └── skew_data/
    ├── schema/                    # StructType definitions, JSON schema parsing
    └── transformation/            # filter, sort, dedup, union, partition, sampling
```

Each source file must be **runnable standalone** with `python src/<topic>/<file>.py`.

## SparkSession

Use the standard env-var pattern:

```python
import os
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("descriptive-job-name")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
```

- `shuffle.partitions = "4"` for all local / example scripts (default 200 is wasteful).

**Prohibited patterns — never do these:**

```python
# ❌ hardcoded Windows or absolute JAVA_HOME
os.environ["JAVA_HOME"] = "E:\\Languages\\java\\jdk\\jdk-11"

# ❌ pinning the driver Python to the current executable
os.environ["PYSPARK_PYTHON"] = sys.executable

# ❌ lowercase alias — conflicts with loop variable f
import pyspark.sql.functions as f
```

`JAVA_HOME`, `PYSPARK_PYTHON`, and `PYSPARK_DRIVER_PYTHON` must come from the host shell
environment (`.env`, `export`, CI secrets) — never set them inside source files.

## Imports

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F          # always alias as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType, BooleanType,
    DateType, TimestampType, ArrayType, MapType,
)
```

Never use `from pyspark.sql.functions import *` or `import pyspark.sql.functions as f`
(lowercase `f` conflicts with common loop variables).

## DataFrame Creation

### From tuples (preferred for compact inline data)

```python
data = [
    (1, "Alice", "North", 999.99),
    (2, "Bob",   "South", 1499.50),
]
schema = ["id", "name", "region", "revenue"]
df = spark.createDataFrame(data, schema)
```

### With explicit StructType (required when nullability or exact types matter)

```python
schema = StructType([
    StructField("id",      IntegerType(), nullable=False),
    StructField("name",    StringType(),  nullable=True),
    StructField("region",  StringType(),  nullable=True),
    StructField("revenue", DoubleType(),  nullable=True),
])
df = spark.createDataFrame(data, schema)
```

### From a list of dicts

```python
data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
df = spark.createDataFrame(data)
```

## Column Operations

Use `F.col()` for column references inside transformations — avoid `df["col"]` or `df.col` in chained expressions:

```python
result = (df
          .withColumn("revenue_eur", F.round(F.col("revenue") * 0.92, 2))
          .withColumnRenamed("name", "customer_name")
          .select("id", "customer_name", "region", "revenue_eur"))
```

### Add multiple columns at once (Spark 3.3+)

```python
df = df.withColumns({
    "revenue_eur": F.round(F.col("revenue") * 0.92, 2),
    "tier":        F.when(F.col("revenue") >= 1000, "Gold").otherwise("Silver"),
})
```

### Rename all columns via toDF()

```python
renamed = df.toDF("order_id", "customer_name", "region", "amount")
```

### Conditional columns

```python
df = df.withColumn(
    "tier",
    F.when(F.col("revenue") >= 1000, "Gold")
     .when(F.col("revenue") >= 500,  "Silver")
     .otherwise("Bronze")
)
```

### Select with expressions

```python
df = df.select(
    "id",
    F.upper(F.col("name")).alias("name_upper"),
    (F.col("revenue") * 1.1).alias("revenue_with_tax"),
)
```

## Joins

### Equi-join (preferred — pass join keys as a list)

```python
result = employees.join(departments, on=["department_id"], how="inner")
```

### Non-equi / complex join condition

```python
result = orders.join(
    discounts,
    on=(F.col("orders.amount") >= F.col("discounts.min_amount")),
    how="left",
)
```

### Broadcast hint (small dimension table)

```python
result = large_df.join(F.broadcast(small_df), on=["key"], how="inner")
```

### Natural join

```python
# Joins on all columns with the same name — no explicit key needed
result = left_df.join(right_df, how="natural")  # selects shared columns once
```

Use sparingly; prefer explicit key lists so the join condition is clear.

### Semi and anti joins

```python
# left_semi — keep left rows that have a match; right columns not returned
matched = orders.join(valid_customers, on=["customer_id"], how="left_semi")

# left_anti — keep left rows that have NO match
orphaned = orders.join(valid_customers, on=["customer_id"], how="left_anti")
```

Supported `how` values: `"inner"`, `"left"` / `"left_outer"`, `"right"` / `"right_outer"`,
`"full"` / `"full_outer"`, `"left_semi"`, `"left_anti"`, `"cross"`, `"natural"`.

### Self-join

Always alias both sides to avoid ambiguous column names:

```python
emp = employees.alias("emp")
mgr = employees.alias("mgr")
result = emp.join(mgr, F.col("emp.manager_id") == F.col("mgr.id"), "left")
```

## Window Functions

### WindowSpec construction

```python
w = (Window
     .partitionBy("region")
     .orderBy(F.asc("date")))
```

### Frame boundaries (rows-based)

```python
w_running = (Window
             .partitionBy("region")
             .orderBy("date")
             .rowsBetween(Window.unboundedPreceding, Window.currentRow))
```

### Frame boundaries (range-based)

```python
w_range = (Window
           .partitionBy("region")
           .orderBy("amount")
           .rangeBetween(-100, 100))
```

### Common window patterns

```python
# Ranking
df = df.withColumn("rank",        F.rank().over(w))
df = df.withColumn("dense_rank",  F.dense_rank().over(w))
df = df.withColumn("row_number",  F.row_number().over(w))

# Running aggregation
df = df.withColumn("running_total",  F.sum("revenue").over(w_running))
df = df.withColumn("moving_avg_7d",  F.avg("revenue").over(w_range))

# Lead / Lag
df = df.withColumn("prev_value", F.lag("revenue",  1, 0).over(w))
df = df.withColumn("next_value", F.lead("revenue", 1, 0).over(w))

# Top-N per group — use row_number then filter
df = (df
      .withColumn("rn", F.row_number().over(w))
      .filter(F.col("rn") <= 3)
      .drop("rn"))
```

## Pivoting

Always specify the pivot values explicitly to avoid a full dataset scan:

```python
pivot_values = ["Q1", "Q2", "Q3", "Q4"]

result = (df
          .groupBy("region")
          .pivot("quarter", pivot_values)
          .agg(F.round(F.sum("revenue"), 2).alias("revenue")))
```

For dynamic pivot (values unknown at write time):

```python
pivot_values = [r[0] for r in df.select("quarter").distinct().collect()]
result = df.groupBy("region").pivot("quarter", pivot_values).agg(F.sum("revenue"))
```

## Aggregations

```python
result = (df
          .filter(F.col("status") == "active")
          .groupBy("region", "category")
          .agg(
              F.round(F.sum("revenue"), 2).alias("total_revenue"),
              F.avg("revenue").alias("avg_revenue"),
              F.countDistinct("customer_id").alias("unique_customers"),
              F.max("revenue").alias("max_revenue"),
          )
          .orderBy(F.desc("total_revenue")))
```

## Null Handling

```python
# Drop rows where any column is null
df = df.dropna()

# Drop rows only when specific columns are null
df = df.dropna(subset=["customer_id", "revenue"])

# Fill nulls with a scalar
df = df.fillna({"revenue": 0.0, "region": "Unknown"})

# Coalesce — first non-null value across columns
df = df.withColumn("label", F.coalesce(F.col("label_override"), F.col("label_default")))

# Null-safe equality (treats null == null as True)
df = df.filter(F.col("region").eqNullSafe("North"))

# Explicit null checks
df = df.filter(F.col("revenue").isNotNull())
df = df.filter(F.col("discount").isNull())
```

## Date and Time Functions

```python
# Parse string to date / timestamp
df = df.withColumn("event_date",  F.to_date(F.col("date_str"),  "yyyy-MM-dd"))
df = df.withColumn("event_ts",    F.to_timestamp(F.col("ts_str"), "yyyy-MM-dd HH:mm:ss"))

# Extract parts
df = df.withColumn("year",   F.year("event_date"))
df = df.withColumn("month",  F.month("event_date"))
df = df.withColumn("day",    F.dayofmonth("event_date"))
df = df.withColumn("dow",    F.dayofweek("event_date"))

# Arithmetic
df = df.withColumn("due_date",   F.date_add("event_date", 30))
df = df.withColumn("days_since", F.datediff(F.current_date(), F.col("event_date")))

# Truncate to month / week
df = df.withColumn("month_start", F.date_trunc("month", F.col("event_ts")))

# Format
df = df.withColumn("formatted", F.date_format(F.col("event_date"), "dd/MM/yyyy"))
```

## String Functions

```python
df = df.withColumn("name_upper",  F.upper(F.col("name")))
df = df.withColumn("name_lower",  F.lower(F.col("name")))
df = df.withColumn("name_trim",   F.trim(F.col("name")))
df = df.withColumn("initial",     F.substring(F.col("name"), 1, 1))

# Concatenate with separator
df = df.withColumn("full_name", F.concat_ws(" ", F.col("first"), F.col("last")))

# Regex extract / replace
df = df.withColumn("domain", F.regexp_extract(F.col("email"), r"@(.+)", 1))
df = df.withColumn("clean",  F.regexp_replace(F.col("text"),  r"\s+", " "))

# Split into array
df = df.withColumn("tags", F.split(F.col("tag_str"), ","))
```

## Reusable Transformations with DataFrame.transform()

Use `DataFrame.transform()` to apply reusable pipeline steps as plain functions:

```python
def add_revenue_tier(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "tier",
        F.when(F.col("revenue") >= 1000, "Gold")
         .when(F.col("revenue") >= 500,  "Silver")
         .otherwise("Bronze"),
    )

def filter_active(df: DataFrame) -> DataFrame:
    return df.filter(F.col("status") == "active")

result = (df
          .transform(filter_active)
          .transform(add_revenue_tier))
```

Import `DataFrame` from `pyspark.sql` when used in type hints.

## Debugging with explain()

```python
# Logical + physical plan summary
df.explain()

# Full plan with all optimisation stages
df.explain(extended=True)

# Cost-based optimizer stats
df.explain(mode="cost")
```

Use `explain()` to verify broadcast hints, join strategies, and partition pruning before
running on a cluster.

## Caching

```python
from pyspark import StorageLevel

# Default (MEMORY_AND_DISK) — use for reused DataFrames
df.cache()

# Explicit storage level
df.persist(StorageLevel.MEMORY_AND_DISK_SER)

# Always unpersist when done
df.unpersist()
```

## Skew Data Optimization

Use AQE skew-join detection instead of manual salting when possible:

```python
spark = (SparkSession.builder
         .config("spark.sql.adaptive.enabled",                             "true")
         .config("spark.sql.adaptive.skewJoin.enabled",                    "true")
         .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor",      "5")
         .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m")
         .getOrCreate())
```

Manual salting pattern (when AQE is insufficient):

```python
SALT_BUCKETS = 10

skewed = skewed_df.withColumn(
    "salted_key",
    F.concat_ws("_", F.col("join_key"), (F.rand() * SALT_BUCKETS).cast("int"))
)
small = small_df.withColumn("salt", F.explode(F.array([F.lit(i) for i in range(SALT_BUCKETS)])))
small = small.withColumn("salted_key", F.concat_ws("_", F.col("join_key"), F.col("salt")))

result = skewed.join(small, on="salted_key", how="inner")
```

## Schema Operations

### Inline StructType (preferred for typed examples)

```python
schema = StructType([
    StructField("order_id",  LongType(),   nullable=False),
    StructField("product",   StringType(), nullable=True),
    StructField("quantity",  IntegerType(), nullable=True),
    StructField("price",     DoubleType(), nullable=True),
])
```

### Parse schema from JSON file

```python
import json

with open("src/schema/schema.json") as fh:
    schema = StructType.fromJson(json.load(fh))
```

## Output

```python
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/tmp/dataframe_output")

# Parquet (preferred)
df.write.mode("overwrite").parquet(OUTPUT_PATH)

# Partitioned Parquet
df.write.mode("overwrite").partitionBy("region", "year_month").parquet(OUTPUT_PATH)

# CSV (only when non-technical audience)
df.coalesce(1).write.mode("overwrite").option("header", "true").csv(OUTPUT_PATH)
```

## Script Entry Point

All standalone scripts must use `if __name__ == "__main__":` and call `spark.stop()`.
Always type-hint the `main()` signature:

```python
from pyspark.sql import DataFrame, SparkSession

def main(spark: SparkSession) -> None:
    df: DataFrame = spark.createDataFrame(...)
    ...

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("example-job")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    main(spark)
    spark.stop()
```
