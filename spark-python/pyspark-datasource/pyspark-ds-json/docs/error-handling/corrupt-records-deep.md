# Corrupt Records — Deep Dive

Production patterns for handling malformed JSON with PERMISSIVE mode.

## The Three Modes

| Mode | Behavior | Use Case |
|------|----------|----------|
| `PERMISSIVE` | Bad records → `_corrupt_record` column | **Production** (default) |
| `DROPMALFORMED` | Silently drops bad records | Quick exploration only |
| `FAILFAST` | Throws exception on first bad record | Validation jobs |

## Correct PERMISSIVE Setup

```python
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("_corrupt_record", StringType(), True),  # ← CRITICAL
])

df = (
    spark.read
    .schema(schema)
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .json(path)
    .cache()  # ← REQUIRED before querying _corrupt_record
)

good_df = df.filter(df._corrupt_record.isNull())
bad_df = df.filter(df._corrupt_record.isNotNull())
```

!!! failure "Critical: `_corrupt_record` Must Be in Schema"
    Without it in the schema, bad records become rows of **all nulls** —
    indistinguishable from legitimately null records. You lose visibility
    into data quality silently.

!!! warning "Cache Required"
    Spark raises `UNSUPPORTED_FEATURE.QUERY_ONLY_CORRUPT_RECORD_COLUMN` if you
    query `_corrupt_record` without caching first. Always `.cache()` after read.

## What Gets Flagged as Corrupt

| Input | Result |
|-------|--------|
| `{"id": 1, "name": "A"}` | ✓ Parsed normally |
| `{"id": 2, "name": "B"` | → `_corrupt_record` (truncated) |
| `NOT JSON AT ALL` | → `_corrupt_record` (not JSON) |
| `""` (empty line) | → `_corrupt_record` (empty) |
| `{"id": "WRONG_TYPE", "name": "A"}` | → `_corrupt_record` (type mismatch) |
| `{"extra": "x", "id": 1, "name": "A"}` | ✓ Extra fields ignored |

## Error Classification

```python
from pyspark.sql import functions as F

df_classified = bad_df.withColumn(
    "error_type",
    F.when(F.trim(F.col("_corrupt_record")) == "", "empty_line")
    .when(~F.trim(F.col("_corrupt_record")).startsWith("{"), "not_json")
    .when(~F.col("_corrupt_record").contains("}"), "truncated_json")
    .otherwise("parse_error"),
)
```

## Production Pattern

```python
# Read with corrupt record handling
df = (
    spark.read.schema(schema)
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .json(path)
    .cache()
)

# Route good records → silver
df.filter(col("_corrupt_record").isNull()) \
    .select("id", "name") \
    .write.parquet("/silver/events")

# Route bad records → quarantine
df.filter(col("_corrupt_record").isNotNull()) \
    .select("_corrupt_record") \
    .write.parquet("/quarantine/events")

# Monitor
bad_ratio = bad_count / total_count
if bad_ratio > 0.05:
    alert("Corrupt record rate exceeded 5%!")
```

## Full Demo

```python title="examples/05_error_handling/04_corrupt_records_deep.py"
--8<-- "examples/05_error_handling/04_corrupt_records_deep.py"
```

## Run

```bash
python examples/05_error_handling/04_corrupt_records_deep.py
```

!!! success "Three Rules"
    1. **Always** include `_corrupt_record` in your schema
    2. **Always** `.cache()` before filtering on `_corrupt_record`
    3. **Always** route bad records to quarantine — never silently drop them
