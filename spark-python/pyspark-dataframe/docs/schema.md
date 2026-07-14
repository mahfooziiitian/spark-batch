# Schema

Define, inspect, evolve, and parse DataFrame schemas using `StructType` and
`StructField`. Explicit schemas are safer and faster than schema inference in production.

## Data Type Reference

| PySpark Type | Python / SQL | Notes |
|-------------|-------------|-------|
| `ByteType()` | `int8` / `TINYINT` | −128 to 127 |
| `ShortType()` | `int16` / `SMALLINT` | |
| `IntegerType()` | `int32` / `INT` | |
| `LongType()` | `int64` / `BIGINT` | Default for Python `int` |
| `FloatType()` | `float32` / `FLOAT` | |
| `DoubleType()` | `float64` / `DOUBLE` | Default for Python `float` |
| `DecimalType(p,s)` | `DECIMAL(p,s)` | Exact; use for monetary values |
| `StringType()` | `str` / `STRING` | |
| `BooleanType()` | `bool` / `BOOLEAN` | |
| `DateType()` | `datetime.date` / `DATE` | No time component |
| `TimestampType()` | `datetime.datetime` / `TIMESTAMP` | Microsecond precision, UTC |
| `ArrayType(T)` | `list` | `ArrayType(IntegerType())` |
| `MapType(K, V)` | `dict` | `MapType(StringType(), DoubleType())` |
| `StructType([…])` | Nested struct | Recursively embeds fields |

## Define a Schema

```python
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, DoubleType, DateType, BooleanType,
)

spark = (SparkSession.builder
         .appName("schema")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("order_id",    LongType(),    nullable=False),  # (1)!
    StructField("customer_id", LongType(),    nullable=False),
    StructField("product",     StringType(),  nullable=True),
    StructField("quantity",    LongType(),    nullable=True),
    StructField("price",       DoubleType(),  nullable=True),
    StructField("order_date",  DateType(),    nullable=True),
    StructField("is_returned", BooleanType(), nullable=True),
])

data = [(1001, 1, "Widget", 3, 9.99, None, False)]
df = spark.createDataFrame(data, schema)
df.printSchema()
df.show()
```
1. `nullable=False` — Spark will reject rows where this field is `null`.

### Run

```bash
python src/data_frame/schema/print_schema_json.py
```

## Nested and Complex Types

```python
from pyspark.sql.types import ArrayType, MapType

nested_schema = StructType([
    StructField("order_id", LongType(),   nullable=False),
    StructField("tags",     ArrayType(StringType()),            nullable=True),  # (1)!
    StructField("metadata", MapType(StringType(), StringType()), nullable=True), # (2)!
    StructField("address",  StructType([                                         # (3)!
        StructField("city",    StringType(), nullable=True),
        StructField("country", StringType(), nullable=True),
    ]), nullable=True),
])
```
1. `ArrayType` wraps the element type.
2. `MapType` takes key type then value type.
3. `StructType` can be nested to any depth.

## Introspect a Schema

```python
# Human-readable tree
df.printSchema()

# List of (name, type_string) tuples
print(df.dtypes)

# Programmatic access
for field in df.schema.fields:
    print(f"{field.name}: {field.dataType} nullable={field.nullable}")

# Check a single column
from pyspark.sql.types import DoubleType
assert df.schema["price"].dataType == DoubleType()
assert df.schema["order_id"].nullable is False
```

## Load Schema from JSON

Store the schema as JSON and load it at runtime — useful for versioned schemas in
source control:

```python
import json

# Serialise
schema_json = df.schema.json()

# Parse back
with open("src/data_frame/schema/schema.json") as fh:
    schema = StructType.fromJson(json.load(fh))
```

### Run

```bash
python src/data_frame/schema/parse_json_schema.py
```

## DDL Schema String

```python
# Create schema from a DDL string — concise for simple schemas
schema = spark.createDataFrame([], "id BIGINT NOT NULL, name STRING, price DOUBLE").schema
```

!!! tip "Prefer StructType over DDL strings for complex schemas"
    DDL strings are concise but lose Python type-checking. Use `StructType` when
    the schema has nested types, custom nullability, or metadata fields.

!!! note "nullable is advisory in PySpark"
    Setting `nullable=False` in `StructType` documents intent but does not cause
    Spark to enforce the constraint at runtime for most operations. Use
    explicit `isNotNull` filters or Delta Lake constraints for enforcement.
