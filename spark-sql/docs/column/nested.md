# :material-code-json: Struct & Nested Columns

Spark SQL supports complex column types — `STRUCT`, `ARRAY`, and `MAP` — that allow
structured sub-fields, ordered lists, and key-value pairs to be stored in a single
column. Dot-notation and bracket-notation provide access to nested values.

---

## :material-code-tags: Syntax

```sql
-- Access a struct field
SELECT address.city, address.country FROM customers;

-- Access a nested struct field
SELECT contact.address.postcode FROM customers;

-- Array element by index (0-based)
SELECT tags[0] AS first_tag FROM articles;

-- Map value by key
SELECT metadata['source'] AS source FROM events;

-- Build a struct
SELECT STRUCT(city, country, postcode) AS address FROM locations;

-- Build an array
SELECT ARRAY(tag1, tag2, tag3) AS tags FROM raw_articles;

-- Build a map
SELECT MAP('key1', val1, 'key2', val2) AS kv FROM metrics;

-- Explode array into rows
SELECT id, EXPLODE(tags) AS tag FROM articles;

-- Inline (explode array of structs into columns)
SELECT id, INLINE(items) FROM orders;
```

---

## :material-information-outline: Behavior

1. Dot-notation (`struct_col.field`) accesses a named field inside a `STRUCT` type — case-insensitive by default in Spark SQL.
2. Bracket-notation (`array_col[n]`) is **0-based** — `array_col[0]` is the first element; out-of-bounds returns `NULL`.
3. `EXPLODE(array_col)` generates one row per array element — multiply rows if the array has N elements.
4. `EXPLODE_OUTER(array_col)` behaves like `EXPLODE` but keeps rows where the array is `NULL` or empty (emitting one row with `NULL`).
5. `POSEXPLODE(array_col)` produces both the position (0-based index) and the value.
6. `LATERAL VIEW EXPLODE(...)` is the legacy syntax; `EXPLODE` in the `SELECT` list works in Spark 3.x.
7. Parquet, ORC, and Delta store nested types natively — Spark reads and writes them without flattening.

---

## :material-flask-outline: Practical Examples

### Access struct fields

```sql
-- customers.address is STRUCT<city:STRING, state:STRING, country:STRING, postcode:STRING>
SELECT
    customer_id,
    address.city        AS city,
    address.country     AS country,
    address.postcode    AS postcode
FROM customers;
```

### Filter on a nested struct field

```sql
SELECT customer_id, address.city
FROM customers
WHERE address.country = 'GB'
  AND address.postcode LIKE 'SW%';
```

### Build a struct from columns

```sql
SELECT
    order_id,
    STRUCT(
        customer_id         AS id,
        customer_name       AS name,
        customer_email      AS email
    )                       AS customer_info
FROM orders;
```

### Array element access

```sql
-- events.tags is ARRAY<STRING>
SELECT
    event_id,
    tags[0]             AS primary_tag,
    tags[1]             AS secondary_tag,
    SIZE(tags)          AS tag_count
FROM events;
```

### Filter on array membership

```sql
SELECT * FROM articles
WHERE ARRAY_CONTAINS(tags, 'spark');
```

### Explode array into rows

```sql
SELECT event_id, tag
FROM events
LATERAL VIEW EXPLODE(tags) AS tag
WHERE tag LIKE 'spark%';

-- Spark 3.x inline syntax
SELECT event_id, EXPLODE(tags) AS tag
FROM events;
```

### POSEXPLODE — array with index

```sql
SELECT
    order_id,
    pos         AS line_position,
    item.sku    AS sku,
    item.qty    AS quantity
FROM orders
LATERAL VIEW POSEXPLODE(line_items) AS pos, item;
```

### EXPLODE_OUTER — keep rows with empty arrays

```sql
-- Keep orders that have no tags (empty array → one NULL row)
SELECT order_id, EXPLODE_OUTER(tags) AS tag
FROM orders;
```

### Map access

```sql
-- metadata is MAP<STRING, STRING>
SELECT
    event_id,
    metadata['source']          AS source,
    metadata['campaign']        AS campaign,
    MAP_KEYS(metadata)          AS all_keys,
    MAP_VALUES(metadata)        AS all_values
FROM events;
```

### Explode a map into key-value rows

```sql
SELECT event_id, key, value
FROM events
LATERAL VIEW EXPLODE(metadata) AS key, value;
```

### Nested struct — multi-level access

```sql
-- orders.customer is STRUCT<id:BIGINT, contact:STRUCT<email:STRING, phone:STRING>>
SELECT
    order_id,
    customer.id                 AS customer_id,
    customer.contact.email      AS email,
    customer.contact.phone      AS phone
FROM orders;
```

### Flatten nested struct for export

```sql
SELECT
    order_id,
    customer.id                 AS customer_id,
    customer.contact.email      AS customer_email,
    customer.contact.phone      AS customer_phone,
    shipping.address.city       AS ship_city,
    shipping.address.country    AS ship_country
FROM orders;
```

### Collect rows back into an array (GROUP BY)

```sql
SELECT
    customer_id,
    COLLECT_LIST(product_id)    AS purchased_products,
    COLLECT_SET(category)       AS unique_categories,
    SIZE(COLLECT_SET(category)) AS distinct_category_count
FROM order_lines
GROUP BY customer_id;
```

---

## :material-lightbulb-outline: When to Use

| Scenario | Pattern |
|----------|---------|
| Access a field in a struct column | `struct_col.field_name` |
| Filter on a nested field | `WHERE struct_col.field = value` |
| Flatten array column to rows | `EXPLODE(array_col)` |
| Flatten with index | `POSEXPLODE(array_col)` |
| Keep rows with empty arrays | `EXPLODE_OUTER(array_col)` |
| Check array membership | `ARRAY_CONTAINS(array_col, val)` |
| Access map by key | `map_col['key']` |
| Flatten map to key-value rows | `LATERAL VIEW EXPLODE(map_col) AS k, v` |
| Aggregate rows into an array | `COLLECT_LIST(col)` / `COLLECT_SET(col)` |
