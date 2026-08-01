# Composition Keywords — allOf, anyOf, oneOf

Handle JSON Schema composition keywords as **validation patterns** in Spark,
since Spark has no native union type.

## Quick Reference

| JSON Schema | Spark Rule | Description |
|-------------|-----------|-------------|
| `allOf` | Merge schemas | Merge all fields; validate all constraints |
| `anyOf` | `match_count >= 1` | At least one branch must match |
| `oneOf` | `match_count == 1` | Exactly one branch must match |
| Primitive union | Read as `STRING` | Cast and validate in Silver layer |
| Polymorphic objects | Discriminator field | Route by `event_type` / `payment_type` |

## allOf — Schema Merging

`allOf` means **all** sub-schemas must independently validate.

```json
{
  "allOf": [
    {"properties": {"event_id": {"type": "string"}, "event_time": {"type": "string"}}, "required": ["event_id", "event_time"]},
    {"properties": {"source_system": {"type": "string"}}, "required": ["source_system"]}
  ]
}
```

**Spark:** Merge into one StructType with all required fields:

```python
schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_time", StringType(), False),
    StructField("source_system", StringType(), False),
])
```

!!! warning "Conflict Detection"
    If sub-schemas define the same field with different types (e.g., `amount`
    as both `string` and `number`), the merged schema is unsatisfiable.
    Use the widest type (`STRING`) and validate downstream.

## oneOf — Exactly One Match

Use a **discriminator field** (e.g., `payment_type`, `event_type`) for routing.

```python
from pyspark.sql.functions import col, from_json, get_json_object, when, lit, expr

card_schema = "payment_type STRING, card_number STRING, expiry STRING"
bank_schema = "payment_type STRING, account_number STRING, ifsc STRING"

# Extract discriminator
classified_df = raw_df.withColumn(
    "payment_type", get_json_object(col("raw_json"), "$.payment_type")
)

# Parse each branch conditionally
parsed_df = (
    classified_df
    .withColumn("card_parsed",
        when(col("payment_type") == "card", from_json(col("raw_json"), card_schema)))
    .withColumn("bank_parsed",
        when(col("payment_type") == "bank", from_json(col("raw_json"), bank_schema)))
)

# Enforce oneOf: exactly one match
validated_df = parsed_df.withColumn(
    "match_count",
    expr("int(card_parsed is not null) + int(bank_parsed is not null)")
).withColumn(
    "oneof_status",
    when(col("match_count") == 1, lit("VALID"))
    .when(col("match_count") == 0, lit("NO_MATCH"))
    .otherwise(lit("MULTIPLE_MATCH"))
)
```

## anyOf — At Least One Match

```python
validated_df = df.withColumn(
    "match_count",
    expr("int(email is not null) + int(phone is not null)")
).withColumn(
    "anyof_status",
    when(col("match_count") >= 1, lit("VALID"))
    .otherwise(lit("NO_MATCH"))
)
```

!!! tip "Key Difference"
    - `oneOf` → `match_count == 1`
    - `anyOf` → `match_count >= 1`

## Primitive Union Types

JSON Schema `anyOf: [{type: string}, {type: integer}]` has no Spark equivalent.

```python
# Read as STRING (safest)
df = spark.read.schema("customer_id STRING").json(path)

# Classify and cast in Silver
df2 = df.withColumn(
    "customer_id_long",
    when(col("customer_id").rlike("^[0-9]+$"), col("customer_id").cast("long"))
)
```

## Polymorphic Events — Envelope Pattern

```python
# Extract envelope
envelope_df = raw_df.select(
    col("raw_json"),
    get_json_object(col("raw_json"), "$.event_type").alias("event_type"),
    get_json_object(col("raw_json"), "$.payload").alias("payload_raw"),
)

# Branch-specific parsing
parsed_df = (
    envelope_df
    .withColumn("user_payload",
        when(col("event_type") == "user_created", from_json(col("payload_raw"), user_schema)))
    .withColumn("order_payload",
        when(col("event_type") == "order_created", from_json(col("payload_raw"), order_schema)))
)
```

## Reusable Utility

```python
def apply_oneof(df, json_col: str, branches: dict[str, str]):
    result_df = df
    for name, schema in branches.items():
        result_df = result_df.withColumn(
            f"{name}_parsed", from_json(col(json_col), schema))

    match_expr = " + ".join(f"int({n}_parsed is not null)" for n in branches)
    return result_df.withColumn(
        "match_count", expr(match_expr)
    ).withColumn(
        "oneof_status",
        when(col("match_count") == 1, lit("VALID"))
        .when(col("match_count") == 0, lit("NO_MATCH"))
        .otherwise(lit("MULTIPLE_MATCH"))
    )
```

## Production Architecture

```
Bronze: raw_json STRING, source_file, ingestion_time
Silver: discriminator, branch_payload, validation_status, validation_error
Gold:   event_id, event_type, business_columns, processed_at
```

!!! success "Enterprise Recommendation"
    1. Keep JSON Schema as source of truth in Git
    2. Resolve `$ref`, `allOf`, `oneOf`, `anyOf` before generating Spark schemas
    3. Require discriminator field for `oneOf` wherever possible
    4. Store `raw_json` always for reprocessing
    5. Store `validation_status` and `validation_error`
    6. Quarantine invalid records

## Full Demo

```python title="examples/06_schema/31_composition_keywords.py"
--8<-- "examples/06_schema/31_composition_keywords.py"
```

## Run

```bash
python examples/06_schema/31_composition_keywords.py
```
