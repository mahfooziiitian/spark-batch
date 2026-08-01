# Inferring Schema from JSON String

Using `schema_of_json()` for quick schema discovery and production-ready strategies.

## Quick Start

```python
from pyspark.sql import functions as F

sample = '{"id": 1, "name": "Alice", "amount": 10.5}'
schema_ddl = spark.range(1).select(
    F.schema_of_json(F.lit(sample)).alias("schema")
).collect()[0]["schema"]

# Result: STRUCT<amount: DOUBLE, id: BIGINT, name: STRING>
df = spark.read.schema(schema_ddl).json(path)
```

## The Limitation

`schema_of_json()` only sees **one sample**:

```python
sample1 = '{"id": 1, "name": "Alice"}'
# Schema: STRUCT<id: BIGINT, name: STRING>

sample2 = '{"id": 2, "name": "Bob", "email": "bob@co.com", "tags": ["vip"]}'
# Schema: STRUCT<email: STRING, id: BIGINT, name: STRING, tags: ARRAY<STRING>>
```

!!! warning "Single Sample Is Not Enough"
    Optional fields, type variations, and rare structures will be missed.
    Never use a single `schema_of_json()` call as your production schema.

## Production Approach: Multiple Samples

Collect diverse samples and merge their schemas:

```python
from pyspark.sql.types import StructType
from pys_json.schema import merge_schemas

samples = [
    '{"id": 1, "name": "Alice", "amount": 10.5}',
    '{"id": 2, "name": "Bob", "email": "bob@co.com"}',
    '{"id": 3, "name": "Charlie", "tags": ["vip"], "active": true}',
]

schemas = []
for s in samples:
    ddl = spark.range(1).select(F.schema_of_json(F.lit(s)).alias("s")).collect()[0]["s"]
    schemas.append(StructType.fromDDL(ddl))

merged = merge_schemas(*schemas)
df = spark.read.schema(merged).json(path)
```

## Combine with `from_json` for String Columns

Parse JSON string columns (e.g., from Kafka) using a derived schema:

```python
payload_sample = '{"user": "Alice", "action": "click", "ts": 1700000000}'
payload_ddl = spark.range(1).select(
    F.schema_of_json(F.lit(payload_sample)).alias("s")
).collect()[0]["s"]

payload_schema = StructType.fromDDL(payload_ddl)
df_parsed = df.select(
    "event_id",
    F.from_json(F.col("payload"), payload_schema).alias("data"),
)
```

## Type Options

Pass options to control inference behavior:

```python
# Default: decimals inferred as DOUBLE
F.schema_of_json(F.lit('{"price": 19.99}'))
# → STRUCT<price: DOUBLE>

# With prefersDecimal: inferred as DECIMAL
F.schema_of_json(F.lit('{"price": 19.99}'), {"prefersDecimal": "true"})
# → STRUCT<price: DECIMAL(4,2)>
```

## Recommended Workflow

| Step | Action | Purpose |
|------|--------|---------|
| 1. Prototype | `schema_of_json(lit(sample))` | Quick discovery |
| 2. Validate | Collect 5–10 diverse samples | Cover optional fields |
| 3. Merge | `merge_schemas(*parsed_schemas)` | Union of all fields |
| 4. Harden | `StructType(...)` in code | Stable production schema |
| 5. Detect | PERMISSIVE + `_corrupt_record` | Catch violations at runtime |

!!! tip
    Use `schema_of_json` during development to bootstrap your schema,
    then hardcode the `StructType` definition in production for stability.

## Full Demo

```python title="examples/06_schema/18_infer_schema_from_string.py"
--8<-- "examples/06_schema/18_infer_schema_from_string.py"
```

## Run

```bash
python examples/06_schema/18_infer_schema_from_string.py
```
