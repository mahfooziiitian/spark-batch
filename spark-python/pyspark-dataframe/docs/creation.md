# DataFrame Creation

Create a PySpark DataFrame from in-memory data, a structured schema, or a JSON
string. Explicit schemas avoid the extra Spark job that schema inference triggers.

## Creation Methods

| Source | Method | Best For |
|--------|--------|----------|
| List of tuples | `spark.createDataFrame(data, ["col1", …])` | Compact inline data |
| List of dicts | `spark.createDataFrame([{"a": 1}])` | Self-documenting test data |
| Explicit schema | `spark.createDataFrame(data, schema)` | Production; nullability control |
| JSON string | `spark.read.json(spark.sparkContext.parallelize([json_str]))` | Semi-structured ingestion |
| `toDF()` | `rdd.toDF(["col1", …])` | Rename all columns at once |

## From Tuples

```python
import os
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("creation-tuples")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

data = [                              # (1)!
    (1, "Alice", "North", 999.99),
    (2, "Bob",   "South", 1499.50),
    (3, "Carol", "East",  750.00),
]
df = spark.createDataFrame(data, ["id", "name", "region", "revenue"])  # (2)!
df.show()
df.printSchema()
```
1. Plain Python list of tuples — no imports needed.
2. Column names provided as a list; types are inferred from the first row.

### Run

```bash
python src/data_frame/creation/tuples/dataframe_from_list_of_tuples.py
```

## From a List of Dicts

```python
data = [
    {"id": 1, "name": "Alice", "region": "North"},
    {"id": 2, "name": "Bob",   "region": "South"},
]
df = spark.createDataFrame(data)  # (1)!
df.show()
```
1. Keys become column names; missing keys produce `null` values.

## With an Explicit StructType

Always use `StructType` in production — it makes nullability and types unambiguous
and avoids schema-inference overhead.

```python
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)

schema = StructType([                                          # (1)!
    StructField("id",      IntegerType(), nullable=False),    # (2)!
    StructField("name",    StringType(),  nullable=True),
    StructField("region",  StringType(),  nullable=True),
    StructField("revenue", DoubleType(),  nullable=True),
])

data = [(1, "Alice", "North", 999.99), (2, "Bob", "South", 1499.50)]
df = spark.createDataFrame(data, schema)
df.printSchema()
```
1. `StructType` wraps an ordered list of `StructField` descriptors.
2. `nullable=False` tells Spark this column must never hold a `null`.

## From a JSON String

```python
import json

json_records = [
    json.dumps({"id": 1, "name": "Alice", "revenue": 999.99}),
    json.dumps({"id": 2, "name": "Bob",   "revenue": 1499.50}),
]
rdd = spark.sparkContext.parallelize(json_records)
df = spark.read.json(rdd)
df.show()
```

!!! note "Schema inference on JSON"
    `spark.read.json()` infers the schema from a sample of the data.
    For production use, pass an explicit schema:
    ```python
    df = spark.read.schema(my_schema).json(rdd)
    ```

## Rename All Columns with toDF()

```python
raw = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["_c0", "_c1"])
df = raw.toDF("id", "name")  # rename positionally
df.show()
```

## Common Patterns

```python
# Empty DataFrame with a known schema
empty = spark.createDataFrame([], schema)

# From a NamedTuple
from collections import namedtuple
Row = namedtuple("Row", ["id", "name"])
df = spark.createDataFrame([Row(1, "Alice"), Row(2, "Bob")])

# Single-column DataFrame
df = spark.createDataFrame([(i,) for i in range(5)], ["n"])
```

!!! success "When to use explicit schema"
    - Reading from CSV, JSON, or any loosely typed source in production
    - Writing tests that assert exact data types
    - Avoiding Spark's type-inference scan (saves one job)

!!! failure "When NOT to infer schema"
    - Large files — Spark reads a sample to infer types, wasting resources
    - Data with mixed types in the same column — inference silently widens to `StringType`
