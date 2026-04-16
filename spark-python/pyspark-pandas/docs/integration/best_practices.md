# Best Practices & Pitfalls

Guidelines for choosing the right integration approach and avoiding common
mistakes when combining pandas with PySpark.

## Decision Table

| Scenario | Best Choice | Why |
|----------|-------------|-----|
| Small dataset (fits in RAM) | **pandas** | No Spark overhead |
| Big data processing | **Spark DataFrame API** | Full optimizer + pushdown |
| Pandas-like coding on big data | **Pandas API on Spark** | Familiar syntax, Spark execution |
| Custom Python logic at scale | **Pandas UDF** | Vectorized via Arrow |
| Fast pandas ↔ Spark conversion | **Arrow** | Zero-copy columnar transfer |
| ML training on features | **Spark prep → pandas** | Leverage scikit-learn / NumPy |
| Model scoring at scale | **Pandas UDF** | Broadcast weights, parallel scoring |
| Data quality debugging | **Sample → pandas** | Interactive inspection |

## Method Comparison

| Method | Spark Version | Input | Output | Best For |
|--------|--------------|-------|--------|----------|
| `toPandas()` | All | Spark DF | pandas DF | Small data collection |
| `createDataFrame()` | All | pandas DF | Spark DF | Converting small data |
| Pandas API on Spark | 3.2+ | pandas syntax | Distributed DF | Migrating pandas code |
| `mapInPandas` | 3.0+ | `Iterator[pd.DataFrame]` | `Iterator[pd.DataFrame]` | General transforms |
| `applyInPandas` | 3.0+ | `pd.DataFrame` (group) | `pd.DataFrame` | Group-wise operations |
| `cogroup.applyInPandas` | 3.0+ | Two `pd.DataFrame`s | `pd.DataFrame` | Joining grouped data |
| Pandas UDF (Series) | 2.3+ | `pd.Series` | `pd.Series` | Element-wise transforms |
| Row-based UDF | All | Python scalar | Python scalar | Avoid — use pandas UDF |

## Arrow Configuration

Always enable Arrow when working with pandas interop:

```python
spark = (SparkSession.builder
         .config("spark.sql.execution.arrow.pyspark.enabled", "true")   # (1)!
         .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")  # (2)!
         .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")  # (3)!
         .getOrCreate())
```
1. Enables Arrow for `toPandas()` and `createDataFrame(pdf)`.
2. Falls back to non-Arrow if an unsupported type is encountered.
3. Tune batch size for memory vs throughput trade-off.

## Memory Management

### `applyInPandas` and group size

All data for a group is loaded into a single executor's memory. Ensure groups
aren't too large:

```python
# Check group sizes before applying
df.groupBy("entity_id").count().orderBy(F.desc("count")).show(5)
```

!!! warning "Data skew"
    Skewed groups can cause OOM on individual executors.
    Consider repartitioning or salting for highly skewed keys.

### Arrow batch size

Control the batch size for `mapInPandas` and pandas UDFs:

```python
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "5000")
```

## Common Pitfalls

### :x: Calling `toPandas()` on huge data

```python
# BAD — will crash the driver with OutOfMemoryError
pdf = huge_spark_df.toPandas()

# GOOD — aggregate first, then convert
summary = huge_spark_df.groupBy("region").agg(F.sum("amount"))
pdf = summary.toPandas()
```

!!! warning "Rule of thumb"
    If the result has more than **1 million rows**, think twice before calling
    `toPandas()`. Aggregate or sample first.

### :x: Overusing Python UDFs

```python
# BAD — row-at-a-time Python UDF (100x slower)
@udf(returnType=DoubleType())
def slow_square(x):
    return x * x

# GOOD — built-in Spark function
df.withColumn("squared", F.col("value") ** 2)

# OK — pandas UDF when custom logic is truly needed
@pandas_udf(DoubleType())
def fast_custom(s: pd.Series) -> pd.Series:
    return s.apply(complex_business_logic)
```

!!! tip "Prefer built-ins"
    Always check if a Spark built-in function exists before writing a UDF.
    Built-ins run in the JVM and benefit from the Catalyst optimizer.

### :x: Ignoring Arrow config

```python
# BAD — 10-100x slower without Arrow
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
pdf = df.toPandas()  # slow pickle serialization

# GOOD — Arrow enabled
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
pdf = df.toPandas()  # fast columnar transfer
```

### :x: Mixing pandas and pyspark.pandas DataFrames

```python
import pandas as pd
import pyspark.pandas as ps

pdf = pd.DataFrame({"a": [1, 2, 3]})
psdf = ps.DataFrame({"b": [4, 5, 6]})

# BAD — cannot combine directly
# result = pdf + psdf  # TypeError!

# GOOD — convert explicitly
result = ps.from_pandas(pdf) + psdf
```

### :x: Assuming Pandas API on Spark is always fast

```python
# pyspark.pandas is still Spark underneath — some operations trigger shuffles
psdf.sort_values("col")  # full data shuffle

# For performance-critical paths, use Spark DataFrame API
sdf = psdf.to_spark()
sdf.orderBy(F.col("col")).show()
```

!!! note
    The Pandas API on Spark translates operations to Spark plans. Complex chains
    may generate suboptimal plans compared to hand-tuned Spark DataFrame code.

## Performance Tips

### Use `select()` before `toPandas()`

```python
# BAD — transfers all columns
pdf = df.toPandas()

# GOOD — transfer only what you need
pdf = df.select("id", "name", "score").toPandas()
```

### Partition Pandas UDF input wisely

```python
# BAD — default 200 shuffle partitions for small data
df.groupBy("region").applyInPandas(my_func, schema)

# GOOD — reduce partitions for small groups
spark.conf.set("spark.sql.shuffle.partitions", "4")
df.groupBy("region").applyInPandas(my_func, schema)
```

### Cache before multiple `toPandas()` calls

```python
# BAD — recomputes df for each conversion
pdf1 = df.filter(...).toPandas()
pdf2 = df.groupBy(...).agg(...).toPandas()

# GOOD — cache once, convert twice
df.cache()
pdf1 = df.filter(...).toPandas()
pdf2 = df.groupBy(...).agg(...).toPandas()
df.unpersist()
```

## Key Takeaway

PySpark 3 doesn't replace pandas — it **extends** it to big data.

| Need | Tool |
|------|------|
| **Flexibility** | pandas |
| **Scale** | Spark |
| **Both** | Integration patterns |
