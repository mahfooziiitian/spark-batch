# :material-swap-horizontal-bold: Casting Columns

Casting converts a column from one data type to another. Spark SQL provides
`CAST` (strict), `TRY_CAST` (null-on-failure), and implicit coercion for compatible types.

---

## :material-code-tags: Syntax

```sql
-- Explicit cast
CAST(expression AS target_type)

-- Safe cast — returns NULL instead of error on failure
TRY_CAST(expression AS target_type)

-- Double-colon shorthand (Databricks / Spark 3.x)
expression::target_type
```

Common target types: `INT`, `BIGINT`, `DOUBLE`, `DECIMAL(p, s)`, `STRING`, `DATE`,
`TIMESTAMP`, `BOOLEAN`, `ARRAY<T>`, `MAP<K,V>`, `STRUCT<...>`.

---

## :material-information-outline: Behavior

1. `CAST` raises a runtime error on invalid conversions when `spark.sql.ansi.enabled = true`; in non-ANSI mode it returns `NULL` silently.
2. `TRY_CAST` always returns `NULL` on failure regardless of ANSI mode — use it when the source data is dirty and failures are expected.
3. Casting a `DOUBLE` to `INT` **truncates** (not rounds): `CAST(3.9 AS INT) = 3`.
4. Casting `STRING` to `DATE` requires ISO format `'yyyy-MM-dd'` by default; other formats need `TO_DATE(col, 'format')` instead.
5. Spark promotes types automatically in arithmetic expressions (`INT + DOUBLE → DOUBLE`) — explicit casts are only needed to force a specific output type.
6. Casting to `DECIMAL(p, s)` may silently truncate digits if precision/scale is too narrow when ANSI is off.

---

## :material-flask-outline: Practical Examples

### Numeric casts

```sql
SELECT
    CAST(price_str    AS DECIMAL(18, 2))  AS price,
    CAST(quantity_str AS INT)             AS quantity,
    CAST(revenue      AS BIGINT)          AS revenue_int,
    CAST(score        AS DOUBLE)          AS score_dbl
FROM raw_products;
```

### String to date / timestamp

```sql
SELECT
    CAST('2024-06-01'             AS DATE)      AS order_date,
    CAST('2024-06-01 14:30:00'    AS TIMESTAMP) AS event_ts,
    TO_DATE('01/06/2024', 'dd/MM/yyyy')         AS formatted_date,   -- non-ISO
    TO_TIMESTAMP('01-06-2024 14:30', 'dd-MM-yyyy HH:mm') AS formatted_ts
FROM orders;
```

### Date / timestamp to string

```sql
SELECT
    order_date,
    CAST(order_date AS STRING)                          AS date_str,     -- 'yyyy-MM-dd'
    DATE_FORMAT(order_date, 'dd/MM/yyyy')               AS uk_date,
    DATE_FORMAT(event_ts,   'yyyy-MM-dd HH:mm:ss')      AS ts_str
FROM orders;
```

### TRY_CAST for dirty data

```sql
-- raw_amount is STRING; some rows contain 'N/A' or empty strings
SELECT
    order_id,
    TRY_CAST(raw_amount AS DECIMAL(18, 2)) AS amount,   -- NULL for non-numeric rows
    COALESCE(TRY_CAST(raw_amount AS DECIMAL(18, 2)), 0) AS amount_safe
FROM raw_orders;
```

### Cast in a filter (avoid — prefer typed columns)

```sql
-- BAD: Cast in WHERE disables predicate pushdown
SELECT * FROM orders WHERE CAST(order_id AS STRING) = '1001';

-- GOOD: Compare with the correct type
SELECT * FROM orders WHERE order_id = 1001;
```

### Cast for arithmetic type control

```sql
SELECT
    total_items,
    items_returned,
    -- Integer division without CAST would still return DOUBLE in Spark
    -- but explicit CAST makes intent clear
    CAST(items_returned AS DOUBLE) / NULLIF(total_items, 0) * 100 AS return_rate_pct
FROM inventory_summary;
```

### DECIMAL precision and scale

```sql
-- Avoid truncation: use sufficient precision and scale
SELECT
    CAST(revenue AS DECIMAL(20, 4)) AS revenue_precise,
    CAST(3.14159 AS DECIMAL(5, 2))  AS pi_truncated    -- becomes 3.14
FROM metrics;
```

### Boolean cast

```sql
SELECT
    CAST(is_active  AS INT)     AS is_active_int,   -- TRUE→1, FALSE→0
    CAST(1          AS BOOLEAN) AS flag_true,        -- 1→TRUE
    CAST('true'     AS BOOLEAN) AS from_string,      -- 'true'→TRUE
    CAST('yes'      AS BOOLEAN) AS from_yes           -- NULL in ANSI mode
FROM user_flags;
```

### Cast in INSERT — ensure schema alignment

```sql
INSERT INTO fact_orders
SELECT
    CAST(order_id   AS BIGINT)         AS order_id,
    CAST(customer_id AS BIGINT)        AS customer_id,
    CAST(order_date  AS DATE)          AS order_date,
    CAST(amount      AS DECIMAL(18,2)) AS amount,
    TRIM(UPPER(status))                AS status
FROM raw_staging;
```

### Double-colon shorthand (Databricks)

```sql
SELECT
    price_str::DECIMAL(18, 2)  AS price,
    qty_str::INT               AS quantity,
    event_date_str::DATE       AS event_date
FROM raw_events;
```

---

## :material-swap-horizontal: CAST vs TRY_CAST

| Aspect | `CAST` | `TRY_CAST` |
|--------|--------|-----------|
| On invalid input (ANSI mode) | Raises error | Returns `NULL` |
| On invalid input (non-ANSI) | Returns `NULL` | Returns `NULL` |
| Use case | Clean, trusted source data | Dirty or external data |
| NULL input | Returns `NULL` | Returns `NULL` |

---

## :material-lightbulb-outline: When to Use

| Scenario | Pattern |
|----------|---------|
| Convert string column to numeric | `CAST(col AS DECIMAL(18,2))` |
| Parse ISO date string | `CAST(col AS DATE)` |
| Parse non-ISO date | `TO_DATE(col, 'dd/MM/yyyy')` |
| Dirty data with invalid values | `TRY_CAST(col AS type)` |
| Ensure schema match before INSERT | `CAST` all columns to target types |
| Avoid predicate pushdown penalty | Cast target literal, not the column |

!!! warning "CAST in WHERE disables pushdown"
    `WHERE CAST(string_col AS INT) = 5` prevents the filter from being pushed to the
    Parquet/Delta reader. Store data in the correct type or compare using `string_col = '5'`.
