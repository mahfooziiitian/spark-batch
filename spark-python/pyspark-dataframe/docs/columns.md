# Column Operations

Add, rename, drop, and derive columns using the DataFrame API.
Always use `F.col()` inside transformation chains — avoid `df["col"]` or `df.col`
which are fragile in chained expressions.

## API Quick Reference

| Method | Purpose |
|--------|---------|
| `withColumn(name, expr)` | Add or replace a single column |
| `withColumns({name: expr, …})` | Add or replace multiple columns at once *(Spark 3.3+)* |
| `withColumnRenamed(old, new)` | Rename a single column |
| `toDF(*names)` | Rename all columns positionally |
| `select(*cols)` | Project a subset of columns (or derived expressions) |
| `drop(*cols)` | Remove columns by name |
| `when(cond, val).otherwise(val)` | Conditional column value |
| `F.col(name)` | Reference a column by name safely inside expressions |

## Add a Derived Column

```python
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder
         .appName("column-ops")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

data = [(1, "Alice", "North", 999.99), (2, "Bob", "South", 1499.50)]
df = spark.createDataFrame(data, ["id", "name", "region", "revenue"])

result = (df
          .withColumn("revenue_eur",   F.round(F.col("revenue") * 0.92, 2))  # (1)!
          .withColumn("name_upper",    F.upper(F.col("name")))                # (2)!
          .withColumnRenamed("name",   "customer_name"))                      # (3)!
result.show()
```

1. Multiply then round to 2 decimal places.
2. `F.upper()` is a built-in string function.
3. `withColumnRenamed` is applied after `withColumn` — order matters in chains.

### Run

```bash
python src/data_frame/columns/column_operation.py
```

## Add Multiple Columns at Once

```python
df = df.withColumns({                                          # (1)!
    "revenue_eur": F.round(F.col("revenue") * 0.92, 2),
    "tier":        F.when(F.col("revenue") >= 1000, "Gold")
                    .when(F.col("revenue") >= 500,  "Silver")
                    .otherwise("Bronze"),
})
```

1. A single plan step — more efficient than chaining multiple `withColumn` calls.

## Conditional Columns with when / otherwise

```python
df = df.withColumn(
    "tier",
    F.when(F.col("revenue") >= 1000, "Gold")      # (1)!
     .when(F.col("revenue") >= 500,  "Silver")
     .otherwise("Bronze"),                         # (2)!
)
```

1. Conditions are evaluated top-to-bottom; first match wins.
2. `otherwise` is the catch-all; omitting it produces `null` for unmatched rows.

## Select with Expressions

```python
result = df.select(
    "id",                                                    # (1)!
    F.upper(F.col("name")).alias("name_upper"),              # (2)!
    (F.col("revenue") * 1.1).alias("revenue_with_tax"),      # (3)!
    F.lit("USD").alias("currency"),                          # (4)!
)
```

1. Pass plain strings for columns that need no transformation.
2. `.alias()` renames the derived column.
3. Arithmetic expressions are supported inline.
4. `F.lit()` adds a constant column.

## Drop Columns

```python
df = df.drop("region", "revenue_eur")
```

## Rename All Columns

```python
raw = df.toDF("order_id", "customer_name", "region", "amount")  # (1)!
```

1. Positional — must match the current column count exactly.

## Column Type Casting

```python
df = df.withColumn("id",      F.col("id").cast("string"))
df = df.withColumn("revenue", F.col("revenue").cast("decimal(10,2)"))
```

!!! tip "Use F.col() in chains"
    `df["col"]` binds to a specific DataFrame object and breaks if the DataFrame
    is re-assigned. `F.col("col")` is a pure expression that resolves at execution time.

!!! warning "withColumn creates a new DataFrame"
    DataFrames are immutable. Every `withColumn` call returns a new object —
    always reassign: `df = df.withColumn(...)`.
