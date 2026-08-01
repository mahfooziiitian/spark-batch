# Array of Struct vs Struct of Array

Two common JSON layouts that look similar logically but require different processing strategies.

## The Two Layouts

=== "Array of Struct (Preferred)"

    ```json
    {
      "id": 1,
      "items": [
        {"sku": "A", "qty": 2},
        {"sku": "B", "qty": 3}
      ]
    }
    ```

    ```python
    schema = "id BIGINT, items ARRAY<STRUCT<sku: STRING, qty: INT>>"
    ```

=== "Struct of Array (Columnar)"

    ```json
    {
      "id": 1,
      "items": {
        "sku": ["A", "B"],
        "qty": [2, 3]
      }
    }
    ```

    ```python
    schema = "id BIGINT, items STRUCT<sku: ARRAY<STRING>, qty: ARRAY<INT>>"
    ```

## Why It Matters

| Aspect | Array of Struct | Struct of Array |
|--------|----------------|-----------------|
| Flatten with | `explode_outer()` | `arrays_zip()` + `explode_outer()` |
| Element access | `item.sku`, `item.qty` | `items.sku[i]`, `items.qty[i]` |
| Correlation | Automatic (fields co-located) | Manual (must zip by position) |
| Analytics | ✓ Natural for Spark | Requires conversion |
| API responses | Less common | Common in columnar APIs |

## Flattening Array of Struct

Straightforward — each array element is a complete record:

```python
from pyspark.sql import functions as F

df_flat = df.select(
    "id",
    F.explode_outer("items").alias("item"),
).select(
    "id",
    F.col("item.sku").alias("sku"),
    F.col("item.qty").alias("qty"),
)
```

## Flattening Struct of Array

### Option 1: `arrays_zip` (recommended)

```python
df_flat = df.select(
    "id",
    F.explode_outer(
        F.arrays_zip(F.col("items.sku"), F.col("items.qty"))
    ).alias("zipped"),
).select(
    "id",
    F.col("zipped.sku").alias("sku"),
    F.col("zipped.qty").alias("qty"),
)
```

### Option 2: `posexplode` + join

```python
df_sku = df.select("id", F.posexplode_outer(F.col("items.sku")).alias("pos", "sku"))
df_qty = df.select("id", F.posexplode_outer(F.col("items.qty")).alias("pos", "qty"))
df_flat = df_sku.join(df_qty, on=["id", "pos"]).select("id", "sku", "qty")
```

!!! tip "Prefer `arrays_zip`"
    `arrays_zip` is simpler, avoids a shuffle join, and handles mismatched
    array lengths by filling with null.

## Converting Between Layouts

### Struct of Array → Array of Struct

```python
df_converted = df.select(
    "id",
    F.arrays_zip(F.col("items.sku"), F.col("items.qty")).alias("items"),
)
```

### Array of Struct → Struct of Array

```python
df_converted = df.select(
    "id",
    F.struct(
        F.transform(F.col("items"), lambda x: x.sku).alias("sku"),
        F.transform(F.col("items"), lambda x: x.qty).alias("qty"),
    ).alias("items"),
)
```

## Full Demo

```python title="examples/06_schema/16_array_struct_layouts.py"
--8<-- "examples/06_schema/16_array_struct_layouts.py"
```

## Run

```bash
python examples/06_schema/16_array_struct_layouts.py
```

!!! success "Recommendation"
    Prefer **Array of Struct** for analytics pipelines. When you receive
    Struct-of-Array data from external APIs, convert with `arrays_zip()`
    before further processing.
