# DataFrame Patterns

Working with JSON data through PySpark DataFrames — creating, transforming, nesting, writing, and joining.

## Topics

| Topic | Description | Example |
|-------|-------------|---------|
| [Create DataFrames](create-dataframe.md) | 8 ways to create DataFrames from JSON | `01_create_dataframe.py` |
| [Transformations](transformations.md) | Select, filter, aggregate, sort | `02_transformations.py` |
| [Nested JSON](nested-json.md) | Structs, arrays, maps, flatten | `03_nested_json.py` |
| [Write JSON](write-json.md) | Modes, compression, partitioning | `04_write_json.py` |
| [Pandas Bridge](pandas-bridge.md) | Pandas ↔ Spark, Pandas UDFs | `05_pandas_bridge.py` |
| [Joins](joins.md) | Inner, left, right, full, semi, anti, broadcast | `06_joins.py` |

## Common Patterns

```python
from pyspark.sql import functions as F

# Explode a JSON array column into rows
df.withColumn("item", F.explode("items")).select("item.*")

# Access nested JSON fields with dot notation
df.select("address.city", "address.zipcode")

# Flatten a struct column
df.select("id", "data.*")

# Create from inline JSON
data = [
    '{"name": "Alice", "age": 30}',
    '{"name": "Bob", "age": 25}',
]
df = spark.read.json(spark.sparkContext.parallelize(data))
```

!!! tip "Array vs Object Files"
    - **JSON Lines** (one object per line): Use `spark.read.json()` directly — this is the default.
    - **JSON Array** (top-level `[...]`): Use `spark.read.option("multiline", "true").json()`.

## Run

```bash
python examples/03_dataframe/01_create_dataframe.py
python examples/03_dataframe/02_transformations.py
python examples/03_dataframe/03_nested_json.py
```
