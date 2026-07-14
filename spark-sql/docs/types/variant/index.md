# :material-code-json: VARIANT Data Type

!!! info "Spark 4.0"
    The VARIANT data type is new in Apache Spark 4.0.

The **VARIANT** type stores semi-structured data (JSON-like) without requiring a
fixed schema. Values are stored in an optimized binary format and queried using
field-extraction operators.

---

## :material-pin: Creating VARIANT Values

```sql
-- From a JSON string
SELECT parse_json('{"name": "Alice", "age": 30}') AS v;

-- From multiple values
SELECT parse_json('{"items": [1, 2, 3], "nested": {"key": "val"}}') AS v;
```

---

## :material-key: Field Extraction

### Dot Notation (`:` operator)

```sql
-- Extract a field (returns VARIANT)
SELECT v:name FROM events;

-- Nested access
SELECT v:metadata.version FROM events;

-- Array indexing
SELECT v:items[0] FROM events;
SELECT v:items[0].id FROM events;
```

### Type Casting (`::`)

```sql
-- Cast extracted field to a concrete type
SELECT v:price::DECIMAL(10,2) FROM products;
SELECT v:name::STRING FROM products;
SELECT v:active::BOOLEAN FROM products;
```

### Bracket Notation

Use brackets for fields with special characters or dots in the name:

```sql
SELECT v:['field-name']::STRING FROM events;
SELECT v:['field.with.dots']::INT FROM events;
```

---

## :material-function: VARIANT Functions

| Function | Description |
|----------|-------------|
| `parse_json(str)` | Parse JSON string into VARIANT |
| `variant_get(v, path, type)` | Extract field with explicit type |
| `typeof(v)` | Runtime type name (`'string'`, `'array'`, etc.) |
| `is_variant_null(v)` | Check for SQL NULL vs JSON null |
| `schema_of_variant(v)` | Infer schema from VARIANT value |
| `to_variant_object(...)` | Build VARIANT from key-value pairs |

---

## :material-code-tags: Examples

### Basic Usage

```sql
CREATE TEMP VIEW product_data AS
SELECT parse_json('{
    "item": [
        {"model": "basic", "price": 6.12},
        {"model": "pro",   "price": 9.99}
    ]
}') AS doc;

-- Extract nested values
SELECT
    doc:item[0].model::STRING AS model,
    doc:item[0].price::DOUBLE AS price
FROM product_data;
```

### Type Checking

```sql
SELECT
    typeof(v:name)   AS name_type,   -- 'string'
    typeof(v:scores) AS scores_type  -- 'array'
FROM events;
```

### NULL Handling

```sql
-- SQL NULL (field doesn't exist)
SELECT isnull(v:missing_field) FROM events;       -- true

-- Coalesce with default
SELECT coalesce(v:optional::STRING, 'N/A') FROM events;
```

### Exploding VARIANT Arrays

```sql
-- Cast VARIANT array to typed array, then explode
SELECT explode(CAST(v:scores AS ARRAY<INT>)) AS score
FROM events;
```

### Table with VARIANT Column

```sql
CREATE TABLE raw_events (
    event_id   BIGINT,
    event_time TIMESTAMP,
    payload    VARIANT
) USING PARQUET;

INSERT INTO raw_events VALUES
    (1, current_timestamp(), parse_json('{"type": "click", "x": 100, "y": 200}')),
    (2, current_timestamp(), parse_json('{"type": "scroll", "offset": 450}'));

-- Query without schema definition
SELECT
    event_id,
    payload:type::STRING AS event_type
FROM raw_events;
```

---

## :material-compare-horizontal: VARIANT vs Struct/Map

| Feature | VARIANT | Struct | Map |
|---------|---------|--------|-----|
| Schema required | No | Yes (fixed) | Partial (key/value types) |
| Nested access | `:` dot notation | `.` dot notation | `[]` bracket |
| Mixed types | Yes | No | No |
| Schema evolution | Automatic | ALTER TABLE | Automatic |
| Performance | Good (binary format) | Best (columnar) | Good |
| Best for | Unknown/evolving schemas | Known fixed schemas | Key-value data |
