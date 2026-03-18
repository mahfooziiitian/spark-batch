# RDD Map

Process `Row` objects via the RDD API — `map`, `flatMap`, `filter`, `mapPartitions`.

```mermaid
graph LR
    DF[DataFrame] -->|".rdd"| RDD[RDD of Row]
    RDD -->|map / flatMap / filter| RDD2[Transformed RDD]
    RDD2 -->|".toDF() / createDataFrame"| DF2[DataFrame]
    style DF fill:#2196f3,color:#fff
    style RDD fill:#ff9800,color:#fff
    style DF2 fill:#4caf50,color:#fff
```

## API Quick Reference

| Method | Input → Output | Use Case |
|--------|---------------|----------|
| `rdd.map(f)` | 1 Row → 1 Row | Transform each row (add/change fields) |
| `rdd.flatMap(f)` | 1 Row → 0..N Rows | Expand one row into multiple rows |
| `rdd.filter(f)` | 1 Row → bool | Keep rows matching a predicate |
| `rdd.mapPartitions(f)` | Iterator[Row] → Iterator[Row] | Batch processing per partition (DB writes, API calls) |
| `rdd.keyBy(f)` | Row → (key, Row) | Create key-value pairs for groupBy/reduceByKey |
| `spark.createDataFrame(rdd, schema)` | RDD → DataFrame | Convert back with explicit schema |

## Worked Examples

### map — Transform Each Row

```python
from pyspark.sql import Row

df = spark.createDataFrame([
    Row(id=1, name="Alice", salary=90000.0),
    Row(id=2, name="Bob",   salary=75000.0),
])

rdd = df.rdd.map(lambda r: Row(
    id=r.id,
    name=r.name.upper(),           # (1)!
    salary=r.salary,
    bonus=r.salary * 0.1,          # (2)!
))
result = spark.createDataFrame(rdd)
result.show()
```

1. Apply arbitrary Python logic to each field.
2. Derive new fields from existing values.

### flatMap — Expand Rows

```python
df = spark.createDataFrame([
    Row(id=1, tags="python,spark,sql"),
    Row(id=2, tags="java,scala"),
])

rdd = df.rdd.flatMap(lambda r: [
    Row(id=r.id, tag=t) for t in r.tags.split(",")   # (1)!
])
result = spark.createDataFrame(rdd)
result.show()
# id=1 → 3 rows, id=2 → 2 rows
```

1. One input row produces multiple output rows — one per tag.

### filter — Keep Matching Rows

```python
high_earners = df.rdd.filter(lambda r: r.salary >= 80000)
spark.createDataFrame(high_earners).show()
```

### mapPartitions — Batch Processing

```python
def enrich_partition(rows):           # (1)!
    import json
    for row in rows:
        d = row.asDict()
        d["name_len"] = len(d["name"])
        yield Row(**d)

result = spark.createDataFrame(df.rdd.mapPartitions(enrich_partition))
result.show()
```

1. `mapPartitions` receives an iterator of all rows in one partition — useful for
   batch operations like database lookups or API calls.

### map → createDataFrame with Schema

```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

schema = StructType([
    StructField("id",     IntegerType()),
    StructField("name",   StringType()),
    StructField("salary", DoubleType()),
    StructField("bonus",  DoubleType()),
])

rdd = df.rdd.map(lambda r: (r.id, r.name, r.salary, r.salary * 0.1))
result = spark.createDataFrame(rdd, schema)   # (1)!
```

1. Using tuples + explicit schema is faster than constructing `Row` objects in `map`.

### Run

```bash
cd spark-python/pyspark-dataframe
python src/data_frame/rows/rdd_map/row_rdd_map.py
```

!!! warning "RDD bypasses the Catalyst optimizer"
    `rdd.map` and `rdd.flatMap` execute Python functions row-by-row in the Python
    interpreter — they cannot benefit from Spark's code generation or predicate
    pushdown. Prefer DataFrame API (`withColumn`, `select`, `explode`) whenever
    possible.

!!! tip "Use mapPartitions for expensive setup"
    When each row needs a shared resource (DB connection, ML model), initialise it
    once in `mapPartitions` rather than in `map` to avoid per-row overhead.

!!! note "Schema from RDD"
    If the RDD contains `Row` objects with named fields, `spark.createDataFrame(rdd)`
    infers the schema automatically. For tuples, always pass an explicit `StructType`.

## Full Source

```python title="src/data_frame/rows/rdd_map/row_rdd_map.py"
--8<-- "data_frame/rows/rdd_map/row_rdd_map.py"
```
