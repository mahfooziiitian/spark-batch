# Dynamic JSON Keys

Handling JSON with variable or unknown keys using `MapType`.

## The Problem

Keys are not fixed — they change across records or over time:

```json
{"id": 1, "metrics": {"cpu": 80, "memory": 65, "disk": 90}}
{"id": 2, "metrics": {"gpu": 40, "network": 100}}
```

!!! failure "Without MapType"
    Spark infers a `StructType` from the union of all keys. New keys in future
    data won't be captured without schema changes.

## Solution: MapType Schema

```python
from pyspark.sql.types import DoubleType, LongType, MapType, StringType, StructField, StructType

schema = StructType(
    [
        StructField("id", LongType(), True),
        StructField("metrics", MapType(StringType(), DoubleType()), True),
    ]
)

df = spark.read.schema(schema).json(path)
```

## Querying Specific Keys

Use bracket notation — returns `null` for missing keys:

```python
from pyspark.sql import functions as F

df.select(
    "id",
    F.col("metrics")["cpu"].alias("cpu"),
    F.col("metrics")["memory"].alias("memory"),
    F.col("metrics")["gpu"].alias("gpu"),
)
```

## Explode Map to Rows

```python
df_exploded = df.select(
    "id",
    F.explode_outer("metrics").alias("metric_name", "metric_value"),
)
```

| id | metric_name | metric_value |
|----|-------------|--------------|
| 1  | cpu         | 80.0         |
| 1  | memory      | 65.0         |
| 1  | disk        | 90.0         |
| 2  | gpu         | 40.0         |
| 2  | network     | 100.0        |

## Discover All Keys

```python
all_keys = (
    df.select(F.explode(F.map_keys(F.col("metrics"))).alias("key"))
    .distinct()
    .orderBy("key")
)
```

## Pivot to Wide Format

```python
df_pivot = (
    df_exploded.groupBy("id")
    .pivot("metric_name")
    .agg(F.first("metric_value"))
)
```

!!! warning "High Cardinality"
    Pivot creates one column per unique key. Filter to known keys first
    if the key space is large.

## Nested Dynamic Keys

For two levels of dynamic keys (e.g., service name → config properties):

```python
schema = StructType([
    StructField("id", LongType(), True),
    StructField("config", MapType(StringType(), MapType(StringType(), StringType())), True),
])

df_services = df.select(
    "id",
    F.explode_outer("config").alias("service_name", "service_config"),
).select(
    "id",
    "service_name",
    F.col("service_config")["host"].alias("host"),
    F.col("service_config")["port"].alias("port"),
)
```

## Full Demo

```python title="examples/06_schema/17_dynamic_json_keys.py"
--8<-- "examples/06_schema/17_dynamic_json_keys.py"
```

## Run

```bash
python examples/06_schema/17_dynamic_json_keys.py
```

## Quick Reference

| Operation | Function |
|-----------|----------|
| Access key | `col("map")["key"]` |
| All keys | `map_keys(col("map"))` |
| All values | `map_values(col("map"))` |
| Key count | `size(col("map"))` |
| Explode to rows | `explode_outer("map")` → (key, value) |
| Filter by key | `col("map")["key"].isNotNull()` |
| Pivot to columns | `groupBy("id").pivot("key").agg(first("value"))` |

!!! success "Best Practice"
    Use `MapType` when keys are dynamic. It adapts to any keys without
    schema changes and supports all map operations natively.
