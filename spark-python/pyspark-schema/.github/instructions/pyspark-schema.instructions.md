---
applyTo: "src/**/*.py"
---

# PySpark Schema Code Instructions

## SparkSession

Every script must be environment-agnostic via the `SPARK_MASTER` env var:

```python
import os
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("descriptive-job-name")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
```

Always call `spark.stop()` at the end of standalone scripts.

## Imports

```python
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F          # always alias as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType, FloatType,
    BooleanType, TimestampType, DateType, BinaryType,
    ArrayType, MapType, DecimalType,
)
```

Never use `from pyspark.sql import *` or `from pyspark.sql.functions import *`.

## Environment Variables

```python
INPUT_PATH  = os.environ.get("INPUT_PATH",  "/tmp/schema_input")
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/tmp/schema_output")
```

When `INPUT_PATH` points to a missing file, fall back to in-memory sample data
so scripts run locally without any external dependencies.

---

## Schema Definition Patterns

### 1 — StructField list (most explicit)

```python
schema = StructType([
    StructField("id",         LongType(),      nullable=False),
    StructField("name",       StringType(),    nullable=True),
    StructField("score",      DoubleType(),    nullable=True),
    StructField("created_at", TimestampType(), nullable=True),
])
```

Always set `nullable` explicitly — never rely on the default.

### 2 — Builder (`.add()` chain)

```python
schema = (StructType()
          .add("id",         LongType(),      nullable=False)
          .add("name",       StringType(),    nullable=True)
          .add("score",      DoubleType(),    nullable=True)
          .add("created_at", TimestampType(), nullable=True))
```

Equivalent to pattern 1; prefer when building schemas incrementally.

### 3 — DDL string / `fromDDL`

```python
schema = StructType.fromDDL(
    "id BIGINT NOT NULL, name STRING, score DOUBLE, created_at TIMESTAMP"
)
```

Prefer for schemas sourced from a Hive metastore or configuration file.

### 4 — JSON round-trip

```python
import json

schema_json = schema.json()                    # serialise to JSON string
schema_back = StructType.fromJson(json.loads(schema_json))  # deserialise
```

Use for schema registry integration or storing schemas in object storage.

---

## Complex Type Patterns

### Nested struct

```python
address_schema = StructType([
    StructField("street", StringType(), nullable=True),
    StructField("city",   StringType(), nullable=True),
    StructField("zip",    StringType(), nullable=True),
])

person_schema = StructType([
    StructField("id",      LongType(),     nullable=False),
    StructField("name",    StringType(),   nullable=True),
    StructField("address", address_schema, nullable=True),
])
```

### Array of primitives

```python
from pyspark.sql.types import ArrayType

schema = StructType([
    StructField("id",   LongType(),                nullable=False),
    StructField("tags", ArrayType(StringType()),   nullable=True),
])
```

### Array of structs

```python
item_schema = StructType([
    StructField("sku",      StringType(),  nullable=False),
    StructField("quantity", IntegerType(), nullable=True),
])

order_schema = StructType([
    StructField("order_id", LongType(),            nullable=False),
    StructField("items",    ArrayType(item_schema), nullable=True),
])
```

### Map type

```python
from pyspark.sql.types import MapType

schema = StructType([
    StructField("id",         LongType(),                           nullable=False),
    StructField("properties", MapType(StringType(), StringType()),  nullable=True),
])
```

---

## Schema Introspection

```python
df.printSchema()                   # human-readable tree
df.schema                          # StructType object
df.schema.json()                   # JSON string
df.schema.simpleString()           # compact DDL-like string
df.dtypes                          # list of (name, type_string) tuples
[f.name for f in df.schema.fields] # column name list
```

### Column existence check

```python
from pyspark.sql.utils import AnalysisException

def has_column(df: DataFrame, col: str) -> bool:
    try:
        df[col]
        return True
    except AnalysisException:
        return False
```

Only use `AnalysisException` for the existence-check pattern above — not for
general error handling.

---

## Schema Validation

### Enforce expected schema

```python
def assert_schema(df: DataFrame, expected: StructType) -> None:
    actual_fields = {f.name: f.dataType for f in df.schema.fields}
    for field in expected.fields:
        assert field.name in actual_fields, f"Missing column: {field.name}"
        assert actual_fields[field.name] == field.dataType, (
            f"Type mismatch on '{field.name}': "
            f"expected {field.dataType}, got {actual_fields[field.name]}"
        )
```

### Cast to enforce schema

```python
def cast_to_schema(df: DataFrame, schema: StructType) -> DataFrame:
    return df.select([
        F.col(f.name).cast(f.dataType).alias(f.name)
        for f in schema.fields
    ])
```

---

## Schema Evolution

### Parquet — merge on read

```python
df = (spark.read
      .option("mergeSchema", "true")
      .parquet(INPUT_PATH))
```

### Delta Lake — merge on write

```python
(df.write
   .format("delta")
   .option("mergeSchema", "true")
   .mode("append")
   .save(OUTPUT_PATH))
```

---

## Schema Parsing

Use `_parse_datatype_string` (internal Spark helper) to convert a DDL type
string to a `DataType` object:

```python
from pyspark.sql.types import _parse_datatype_string

dtype = _parse_datatype_string("array<struct<id:bigint,name:string>>")
```

Prefix the name with `_` to signal it is an internal API that may change
between Spark versions.

---

## Output

- Parquet is the preferred format: `df.write.mode("overwrite").parquet(path)`
- Use `df.count()` for row counts — never `len(df.collect())`.
- Print schema diagnostics with `df.printSchema()` before writing in examples.
