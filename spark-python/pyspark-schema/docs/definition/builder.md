# Builder Pattern

Build a `StructType` incrementally using the fluent `.add()` chain. Each call
returns the same `StructType`, so calls can be chained or spread across multiple
statements.

## Typed vs Untyped

The `.add()` method accepts either a **DDL string** (untyped) or a **DataType
object** (typed). Both produce identical schemas.

```python
# Untyped (DDL strings)
schema = (StructType()
          .add("order_id",   "long",    nullable=False)
          .add("customer",   "string",  nullable=True)
          .add("amount",     "double",  nullable=True))

# Typed (DataType objects)
schema = (StructType()
          .add("order_id",   LongType(),   nullable=False)
          .add("customer",   StringType(), nullable=True)
          .add("amount",     DoubleType(), nullable=True))
```

!!! note
    Both produce the same `simpleString` and pass `schema == other_schema`.

## When to Use

!!! success "Good fit"
    - Building a schema programmatically from a list or config.
    - Adding fields conditionally based on runtime flags.

!!! failure "Not suitable"
    - Schemas defined once at module level — prefer the StructField list for clarity.

## Code

```python title="src/definition/schema_definition_builder.py"
--8<-- "src/definition/schema_definition_builder.py"
```

## Run

```bash
SPARK_MASTER=local[*] python src/definition/schema_definition_builder.py
```

## Key Points

- `nullable` defaults to `True` — pass it explicitly in `.add()` as well.
- The return value of `.add()` is the same `StructType` object (mutated in-place).
- Untyped strings follow Hive DDL syntax: `"long"`, `"string"`, `"double"`, etc.
