# Optional Fields and Sparse JSON

Handling JSON where not all fields exist in every record, and distinguishing
between missing, null, empty, and present values.

## Basic Behavior

Spark fills missing fields with `null`:

```json
{"id": 1, "email": "a@test.com"}
{"id": 2, "phone": "9999999999"}
{"id": 3}
```

```python
schema = "id BIGINT, email STRING, phone STRING"
df = spark.read.schema(schema).json(path)
# id=3 has email=null, phone=null
```

## The Ambiguity Problem

After parsing, these four cases are **indistinguishable**:

| JSON | Parsed value | Meaning |
|------|-------------|---------|
| `{"id": 1, "email": "a@t.com"}` | `"a@t.com"` | Field present with value |
| `{"id": 2, "email": null}` | `null` | Explicit null |
| `{"id": 3, "email": ""}` | `""` | Empty string |
| `{"id": 4}` | `null` | Field missing entirely |

!!! warning
    Cases 2 and 4 both become `null` after parsing — cannot be distinguished
    without the raw JSON.

## Solution: Raw JSON + Validation Flags

```python
from pyspark.sql import functions as F

# Read as text to preserve raw
raw_df = spark.read.text(path).withColumnRenamed("value", "raw_json")

df_validated = raw_df.withColumn(
    "email_raw", F.get_json_object(F.col("raw_json"), "$.email")
).withColumn(
    "email_status",
    F.when(~F.col("raw_json").contains('"email"'), "missing")
    .when(F.col("email_raw").isNull(), "explicit_null")
    .when(F.col("email_raw") == "", "empty")
    .otherwise("present"),
)
```

## Default Values

### Simple: `coalesce`

```python
df.select(
    "id",
    F.coalesce(F.col("email"), F.lit("unknown@placeholder.com")).alias("email"),
)
```

### Smart: conditional based on status

```python
F.when(F.col("email_status") == "present", F.col("email_raw"))
.when(F.col("email_status") == "empty", F.lit("[EMPTY]"))
.when(F.col("email_status") == "explicit_null", F.lit("[NULL]"))
.when(F.col("email_status") == "missing", F.lit("[NOT_PROVIDED]"))
```

## Sparsity Analysis

Understand which fields are actually populated:

```python
total = df.count()
for col_name in df.columns:
    non_null = df.filter(F.col(col_name).isNotNull()).count()
    print(f"{col_name}: {non_null}/{total} ({non_null/total*100:.0f}%)")
```

## Full Demo

```python title="examples/06_schema/26_optional_fields.py"
--8<-- "examples/06_schema/26_optional_fields.py"
```

## Run

```bash
python examples/06_schema/26_optional_fields.py
```

## Decision Guide

| Need | Approach |
|------|----------|
| Just fill nulls with defaults | `coalesce()` |
| Know if field existed in source | Keep `raw_json` + `contains()` check |
| Distinguish null vs missing vs empty | Validation flags (status column) |
| Understand data completeness | Sparsity analysis per field |
| Preserve unexpected fields | Wide schema or raw JSON retention |

!!! success "Best Practice"
    For most pipelines: use a wide schema that includes all known optional fields.
    For data quality monitoring: keep raw JSON and compute validation flags.
