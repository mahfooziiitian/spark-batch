# Validation & Comparison

## Schema Validation

Validate that a DataFrame conforms to an expected schema — checking column
presence, types, and nullable flags.

```python
def validate_schema(df: DataFrame, expected: StructType) -> list[str]:
    errors: list[str] = []
    actual = {f.name: f for f in df.schema.fields}
    for field in expected.fields:
        if field.name not in actual:
            errors.append(f"Missing column: '{field.name}'")
        elif actual[field.name].dataType != field.dataType:
            errors.append(f"Type mismatch on '{field.name}': ...")
    return errors
```

An empty return list means the DataFrame is valid.

## Cast to Schema

When types don't match, `cast_to_schema` coerces each column — rows where
casting fails become `null`.

```python
def cast_to_schema(df: DataFrame, schema: StructType) -> DataFrame:
    return df.select([
        F.col(f.name).cast(f.dataType).alias(f.name)
        for f in schema.fields
    ])
```

```python title="src/arrays/pyspark_array_schema_validate.py"
--8<-- "src/arrays/pyspark_array_schema_validate.py"
```

## Schema Comparison (`schema_diff`)

Compare two schemas to find structural differences:

```python
diff = schema_diff(SCHEMA_V1, SCHEMA_V2)
# {
#   "missing_in_b":     [],
#   "extra_in_b":       ["region"],
#   "type_mismatches":  [],
#   "nullable_changes": []
# }
```

| Key | Meaning |
| --- | ------- |
| `missing_in_b` | Fields in schema A absent from schema B |
| `extra_in_b` | Fields in schema B absent from schema A |
| `type_mismatches` | Same name, different `DataType` |
| `nullable_changes` | Same name and type, different `nullable` |

## Compatibility Check

```python
is_backward_compatible(reader_schema, writer_schema)
```

Returns `True` when a reader using `reader_schema` can safely consume data
written with `writer_schema`:

- Non-nullable reader fields missing from writer → **incompatible**
- Nullable reader fields missing from writer → fill with `null` (OK)
- Any type mismatch → **incompatible**
- Extra writer fields are silently ignored

```python title="src/schema_comparison.py"
--8<-- "src/schema_comparison.py"
```

## Run

```bash
SPARK_MASTER=local[*] python src/arrays/pyspark_array_schema_validate.py
SPARK_MASTER=local[*] python src/schema_comparison.py
```

## Key Points

- Run `validate_schema` as a pipeline guard immediately after reading source data.
- Use `schema_diff` in CI to catch schema changes between pipeline versions.
- `cast_to_schema` is a soft enforce — inspect null counts after casting to detect data loss.
