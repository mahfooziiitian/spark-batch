# :material-code-json: Struct & Nested Columns

Spark SQL supports complex column types — `STRUCT`, `ARRAY`, and `MAP` — that allow
structured sub-fields, ordered lists, and key-value pairs to be stored in a single
column. Dot-notation and bracket-notation provide access to nested values.

______________________________________________________________________

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

______________________________________________________________________

## :material-information-outline: Behavior

1. Dot-notation (`struct_col.field`) accesses a named field inside a `STRUCT` type — case-insensitive by default in Spark SQL.
2. Bracket-notation (`array_col[n]`) is **0-based** — `array_col[0]` is the first element; out-of-bounds returns `NULL`.
3. `EXPLODE(array_col)` generates one row per array element — multiply rows if the array has N elements.
4. `EXPLODE_OUTER(array_col)` behaves like `EXPLODE` but keeps rows where the array is `NULL` or empty (emitting one row with `NULL`).
5. `POSEXPLODE(array_col)` produces both the position (0-based index) and the value.
6. `LATERAL VIEW EXPLODE(...)` is the legacy syntax; `EXPLODE` in the `SELECT` list works in Spark 3.x.
7. Parquet, ORC, and Delta store nested types natively — Spark reads and writes them without flattening.

______________________________________________________________________

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

### End-to-End: Nested JSON Payload → Rows, Columns, and Per-Row Totals

Real nested data usually starts life as a raw JSON string column, not an already-typed
`STRUCT`/`ARRAY`. The full pipeline is `FROM_JSON` to parse, then whichever of
`EXPLODE`/`INLINE`/`TRANSFORM` fits what you need next:

```sql
-- raw_orders.payload is a STRING column holding one JSON object per row, e.g.
-- {"customer_id": 501, "items": [{"sku": "A1", "qty": 2, "price": 9.99}, ...]}
WITH parsed AS (
    SELECT
        order_id,
        FROM_JSON(
            payload,
            'customer_id INT, items ARRAY<STRUCT<sku STRING, qty INT, price DOUBLE>>'
        ) AS data
    FROM raw_orders
)
SELECT order_id, data.customer_id, data.items
FROM parsed;
```

`FROM_JSON` needs an explicit schema (or `SCHEMA_OF_JSON`/`schema_of_json` inferred from
a sample) — see the [JSON Functions reference](../../functions/structure/json.md) for
parse modes and schema inference. Once `data.items` is a real `ARRAY<STRUCT<...>>`,
reach for:

```sql
-- EXPLODE: one row per line item, columns stay nested (item.sku, item.qty, ...)
SELECT order_id, data.customer_id, EXPLODE(data.items) AS item
FROM parsed;

-- INLINE: one row per line item, struct fields spread into their own columns directly
SELECT order_id, data.customer_id, INLINE(data.items)
FROM parsed;

-- TRANSFORM: per-row array of derived values, with NO row explosion at all
SELECT
    order_id,
    TRANSFORM(data.items, item -> ROUND(item.qty * item.price, 2)) AS line_totals,
    AGGREGATE(
        TRANSFORM(data.items, item -> item.qty * item.price),
        CAST(0.0 AS DOUBLE), (acc, x) -> acc + x
    ) AS order_total
FROM parsed;
```

??? success "Expected output — EXPLODE vs INLINE vs TRANSFORM"

    `EXPLODE` (one row per item, nested `item` struct):

    | order_id | customer_id | item           |
    | -------- | ----------- | -------------- |
    | 1        | 501         | {A1, 2, 9.99}  |
    | 1        | 501         | {B2, 1, 19.99} |
    | 2        | 502         | {C3, 5, 4.5}   |

    `INLINE` (one row per item, fields already spread into columns):

    | order_id | customer_id | sku | qty | price |
    | -------- | ----------- | --- | --- | ----- |
    | 1        | 501         | A1  | 2   | 9.99  |
    | 1        | 501         | B2  | 1   | 19.99 |
    | 2        | 502         | C3  | 5   | 4.5   |

    `TRANSFORM` (still one row per order — no explosion):

    | order_id | line_totals    | order_total |
    | -------- | -------------- | ----------- |
    | 1        | [19.98, 19.99] | 39.97       |
    | 2        | [22.5]         | 22.5        |

Choosing between them: use `EXPLODE` when downstream logic needs one row per element
and wants to keep the struct intact for later access; use `INLINE` when you want that
same one-row-per-element output with the struct fields already flattened to columns
(skips writing `item.field` everywhere); use `TRANSFORM` (plus `AGGREGATE`/`FILTER` as
needed) when you want a value **per original row**, not per array element — it avoids
the explode-then-reaggregate round trip entirely.

### Two Levels of Nesting: `customer.orders[].items[]`

Real documents rarely stop at one array. A typical order-capture payload nests an
array of orders inside a customer object, and each order itself holds an array of line
items:

```json
{
  "customer": {"id": 1001, "name": "A"},
  "orders": [
    {"id": 1, "items": [{"sku": "X1", "qty": 2, "price": 10.0}, {"sku": "X2", "qty": 1, "price": 5.0}]},
    {"id": 2, "items": [{"sku": "X3", "qty": 3, "price": 7.5}]}
  ]
}
```

```sql
CREATE OR REPLACE TEMP VIEW parsed AS
SELECT FROM_JSON(
    raw,
    'STRUCT<customer: STRUCT<id: INT, name: STRING>,
            orders: ARRAY<STRUCT<id: INT,
                                  items: ARRAY<STRUCT<sku: STRING, qty: INT, price: DOUBLE>>>>>'
) AS doc
FROM raw_events;
```

This single view is the starting point for every function in the "nested SQL"
toolbox below — verified end to end against Spark 4.2.

**`POSEXPLODE` — flatten to order grain with position:**

```sql
SELECT doc.customer.id AS customer_id, pos AS order_seq, order_struct.id AS order_id
FROM parsed
LATERAL VIEW POSEXPLODE(doc.orders) AS pos, order_struct;
```

| customer_id | order_seq | order_id |
| ----------- | --------- | -------- |
| 1001        | 0         | 1        |
| 1001        | 1         | 2        |
| 1002        | 0         | 3        |

**`EXPLODE` + `INLINE` chained — flatten straight to item grain:**

```sql
SELECT doc.customer.id AS customer_id, order_struct.id AS order_id, sku, qty, price
FROM parsed
LATERAL VIEW EXPLODE(doc.orders) AS order_struct
LATERAL VIEW INLINE(order_struct.items) AS sku, qty, price;
```

| customer_id | order_id | sku | qty | price |
| ----------- | -------- | --- | --- | ----- |
| 1001        | 1        | X1  | 2   | 10.0  |
| 1001        | 1        | X2  | 1   | 5.0   |
| 1001        | 2        | X3  | 3   | 7.5   |
| 1002        | 3        | X1  | 0   | 10.0  |

Two `LATERAL VIEW` generators are required because there are two nested arrays —
`EXPLODE` unnests `orders`, then `INLINE` unnests each order's `items` in the same
step it spreads the struct fields into columns.

**`TRANSFORM` + `AGGREGATE` — per-order totals, no row explosion at all:**

```sql
SELECT
    doc.customer.id AS customer_id,
    TRANSFORM(
        doc.orders,
        o -> AGGREGATE(o.items, CAST(0.0 AS DOUBLE), (acc, i) -> acc + i.qty * i.price)
    ) AS order_totals
FROM parsed;
```

| customer_id | order_totals |
| ----------- | ------------ |
| 1001        | [25.0, 22.5] |
| 1002        | [0.0]        |

`TRANSFORM` iterates the outer `orders` array; the inner `AGGREGATE` folds each
order's `items` array down to a single total — a nested lambda, one per array level,
staying at customer grain the entire time.

**`FILTER` + `EXISTS` — find orders with a data-quality problem, without exploding:**

```sql
SELECT
    doc.customer.id AS customer_id,
    -- FILTER: keep only the orders that contain at least one zero-quantity item
    FILTER(doc.orders, o -> EXISTS(o.items, i -> i.qty = 0)) AS orders_with_zero_qty_item,
    -- EXISTS: does this customer have ANY such order at all? (single boolean)
    EXISTS(doc.orders, o -> EXISTS(o.items, i -> i.qty = 0)) AS has_zero_qty_item
FROM parsed;
```

| customer_id | orders_with_zero_qty_item | has_zero_qty_item |
| ----------- | ------------------------- | ----------------- |
| 1001        | []                        | false             |
| 1002        | \[{3, [{X1, 0, 10.0}]}\]  | true              |

`EXISTS` nested two levels deep (order → item) is a single expression tree — no join,
no subquery, no correlation limit (unlike the nested-`NOT EXISTS` SQL subquery
correlation limit described in
[Relational Division](../../patterns/aggregation/relational-division.md); HOFs on
arrays don't share that restriction because they never leave the row).

**`ZIP_WITH` — pair each order's total with its 1-based position:**

```sql
WITH totals AS (
    SELECT
        doc.customer.id AS customer_id,
        TRANSFORM(doc.orders, o -> AGGREGATE(o.items, CAST(0.0 AS DOUBLE), (acc, i) -> acc + i.qty * i.price)) AS order_totals
    FROM parsed
)
SELECT
    customer_id,
    ZIP_WITH(order_totals, SEQUENCE(1, SIZE(order_totals)), (val, idx) -> CONCAT('order#', idx, '=', val)) AS labeled
FROM totals;
```

| customer_id | labeled                      |
| ----------- | ---------------------------- |
| 1001        | [order#1=25.0, order#2=22.5] |
| 1002        | [order#1=0.0]                |

**`FLATTEN` — collapse `ARRAY<ARRAY<item>>` to `ARRAY<item>` after a nested `TRANSFORM`:**

```sql
SELECT
    doc.customer.id AS customer_id,
    FLATTEN(TRANSFORM(doc.orders, o -> o.items)) AS all_items
FROM parsed;
```

| customer_id | all_items                             |
| ----------- | ------------------------------------- |
| 1001        | [{X1,2,10.0}, {X2,1,5.0}, {X3,3,7.5}] |
| 1002        | [{X1,0,10.0}]                         |

`TRANSFORM(doc.orders, o -> o.items)` produces one `items` array per order — an
`ARRAY<ARRAY<item>>` — and `FLATTEN` merges that one level down to a single
`ARRAY<item>`, without ever exploding a row.

#### Diagnosing the Two Styles: `EXPLAIN FORMATTED`

Verified against Spark 4.2 — the functional (HOF-only) form for `has_zero_qty_item`
compiles to a single `Project`, no row-generating operator at all:

```text
== Physical Plan ==
Project (3)
+- Project (2)
   +- * Scan ExistingRDD (1)

(3) Project
Output [2]: [doc.customer.id AS customer_id, exists(doc.orders,
    lambdafunction(exists(lambda o.items, lambdafunction((lambda i.qty = 0), lambda i)), lambda o)) AS has_zero_qty_item]
```

The `EXPLODE` + `INLINE` form for the item-grain flatten compiles to **two chained
`Generate` operators** — one per array level — plus a `Filter` that Catalyst pushed
down into the `FROM_JSON` projection:

```text
== Physical Plan ==
* Project (7)
+- * Generate (6)   -- INLINE(order.items): unnest inner array + spread struct fields
   +- * Generate (5)   -- EXPLODE(doc.orders): unnest outer array
      +- * Project (4)
         +- Project (3)
            +- Filter (2)   -- size(orders) > 0 AND orders IS NOT NULL, pushed down
               +- * Scan ExistingRDD (1)
```

!!! tip "Rule of thumb: HOFs stay at document grain, generators multiply rows"

    If the final answer is naturally **one row per document** (a total, a flag, a
    filtered sub-array), reach for `TRANSFORM`/`FILTER`/`EXISTS`/`AGGREGATE`/`ZIP_WITH`/
    `FLATTEN` — Catalyst never materializes a `Generate` stage and row count never
    changes. If the final answer needs **one row per array element** (a fact table,
    something to `GROUP BY` per item), `EXPLODE`/`POSEXPLODE`/`INLINE` are correct and
    unavoidable — just be aware each nested array level adds its own `Generate`
    operator, and row count multiplies by every array's length at every level.

______________________________________________________________________

## :material-lightbulb-outline: When to Use

| Scenario                                                | Pattern                                                                                           |
| ------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| Access a field in a struct column                       | `struct_col.field_name`                                                                           |
| Filter on a nested field                                | `WHERE struct_col.field = value`                                                                  |
| Flatten array column to rows                            | `EXPLODE(array_col)`                                                                              |
| Flatten with index                                      | `POSEXPLODE(array_col)`                                                                           |
| Keep rows with empty arrays                             | `EXPLODE_OUTER(array_col)`                                                                        |
| Check array membership                                  | `ARRAY_CONTAINS(array_col, val)`                                                                  |
| Access map by key                                       | `map_col['key']`                                                                                  |
| Flatten map to key-value rows                           | `LATERAL VIEW EXPLODE(map_col) AS k, v`                                                           |
| Aggregate rows into an array                            | `COLLECT_LIST(col)` / `COLLECT_SET(col)`                                                          |
| Parse a raw JSON string into typed struct/array columns | `FROM_JSON(col, schema)` — see [JSON Functions](../../functions/structure/json.md)                |
| Explode array-of-structs directly into flat columns     | `INLINE(array_col)` instead of `EXPLODE` + `item.field`                                           |
| Derive a per-row array value without exploding rows     | `TRANSFORM(array_col, x -> expr)` (optionally with `AGGREGATE`/`FILTER`)                          |
| Keep only matching elements, still one row per document | `FILTER(array_col, x -> condition)`                                                               |
| Test "does any element match?" without exploding        | `EXISTS(array_col, x -> condition)` — `false` for empty arrays, unlike `FORALL`                   |
| Pair two same-length arrays element-by-element          | `ZIP_WITH(array1, array2, (a, b) -> expr)`                                                        |
| Collapse `ARRAY<ARRAY<T>>` into `ARRAY<T>`              | `FLATTEN(array_of_arrays)`                                                                        |
| Nested arrays (array-of-structs containing an array)    | `TRANSFORM`/`FILTER`/`EXISTS` nested one lambda per level — no join or subquery correlation limit |
