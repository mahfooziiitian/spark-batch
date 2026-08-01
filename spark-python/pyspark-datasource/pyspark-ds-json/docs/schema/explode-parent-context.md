# Exploding Arrays with Parent Context

Preserving parent-level fields when flattening nested arrays.

## The Mistake

```python
# WRONG — loses customer_id!
df.select(F.explode_outer("orders").alias("order"))

# Result: only order fields, no way to link back to customer
```

## The Correct Pattern

Always include parent columns in the same `select()`:

```python
from pyspark.sql import functions as F

orders_df = df.select(
    F.col("customer_id"),
    F.col("name"),
    F.explode_outer("orders").alias("order"),
).select(
    F.col("customer_id"),
    F.col("name"),
    F.col("order.order_id").alias("order_id"),
    F.col("order.amount").alias("amount"),
)
```

## `inline_outer` — Direct Struct Expansion

For `ARRAY<STRUCT<...>>`, `inline_outer` expands struct fields directly:

```python
df.select("customer_id", "name", F.inline_outer(F.col("orders")))
# Result columns: customer_id, name, order_id, amount
```

## `posexplode` — Preserve Array Index

```python
df.select(
    "customer_id",
    F.posexplode_outer("orders").alias("idx", "order"),
)
```

Useful for ordering or joining back by position.

## Multi-Level Explosion

Carry parent fields through **every** level:

```python
# Level 1: store → departments
df_depts = df.select(
    "store",  # ← parent context
    F.explode_outer("departments").alias("dept"),
)

# Level 2: store + department → products
df_products = df_depts.select(
    "store",           # ← carried from level 1
    F.col("dept.dept").alias("department"),  # ← from level 1
    F.explode_outer("dept.products").alias("product"),
).select(
    "store", "department",  # ← both parents preserved
    F.col("product.sku").alias("sku"),
    F.col("product.price").alias("price"),
)
```

## Aggregate Back to Parent Level

After processing at the child level, aggregate back:

```python
df_agg = orders_df.groupBy("customer_id", "name").agg(
    F.count("order_id").alias("order_count"),
    F.sum("amount").alias("total_amount"),
)
```

## SQL Equivalent

```sql
SELECT customer_id, name, order_entry.order_id, order_entry.amount
FROM customers
LATERAL VIEW OUTER explode(orders) AS order_entry
```

## Full Demo

```python title="examples/06_schema/25_explode_parent_context.py"
--8<-- "examples/06_schema/25_explode_parent_context.py"
```

## Run

```bash
python examples/06_schema/25_explode_parent_context.py
```

## Quick Reference

| Function | Returns | Use When |
|----------|---------|----------|
| `explode_outer` | One row per element (null for empty) | Default choice |
| `posexplode_outer` | (index, element) per row | Need array position |
| `inline_outer` | Struct fields as columns directly | Array of Struct |
| `LATERAL VIEW` | SQL equivalent of explode | Spark SQL queries |

!!! success "Golden Rule"
    If a parent field isn't visible in your exploded result,
    you lost context. Always include parent columns in the
    `select()` that contains the `explode`.
