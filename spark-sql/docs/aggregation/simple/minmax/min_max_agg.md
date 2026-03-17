# MIN / MAX

`MIN` and `MAX` return the smallest or largest non-NULL value in a group, supporting numbers, strings, dates, and timestamps.

---

## 📌 Syntax

```sql
MIN(expr)
MAX(expr)
MIN(expr) FILTER (WHERE condition)
MAX(expr) FILTER (WHERE condition)
```

| Variant | Description |
|---------|-------------|
| `MIN(expr)` | Returns the smallest non-NULL value |
| `MAX(expr)` | Returns the largest non-NULL value |
| `MIN/MAX(expr) FILTER (WHERE ...)` | MIN / MAX scoped to rows matching the condition |

---

## 🔍 Behavior

1. **NULL ignored** — both functions skip `NULL` values; if all values in a group are `NULL` the result is `NULL`.
2. **Any orderable type** — works on `NUMERIC`, `STRING` (lexicographic order), `DATE`, `TIMESTAMP`, `DECIMAL`, and any type that supports comparison operators.
3. **String ordering** — `MIN` / `MAX` on `STRING` uses lexicographic order; `'9' > '10'` because `'9' > '1'` character-by-character. Zero-pad numbers stored as strings to get numeric ordering.
4. **FILTER clause** — `MAX(amount) FILTER (WHERE region = 'East')` computes the max only for East rows without a subquery.
5. **GREATEST / LEAST** — these are *row-level* functions, not aggregates; `GREATEST(a, b, c)` returns the largest value across *columns within a single row*, while `MAX(col)` aggregates across *rows*.
6. **argmin / argmax with STRUCT** — wrap the extreme column as the first field of a `STRUCT` inside `MAX` / `MIN`: `MAX(STRUCT(amount, order_id, region))` returns the struct whose `amount` is highest because Spark compares structs field-by-field left to right.

---

## 🧪 Practical Examples

### Setup

```sql
CREATE TABLE sales (
    order_id   BIGINT,
    region     STRING,
    product    STRING,
    amount     DOUBLE,
    order_date DATE
);

INSERT INTO sales VALUES
    (1, 'East',  'Widget',  120.00, DATE '2024-01-15'),
    (2, 'West',  'Gadget',  340.00, DATE '2024-01-15'),
    (3, 'East',  'Widget',   80.00, DATE '2024-02-10'),
    (4, 'North', 'Gadget',  210.00, DATE '2024-02-10'),
    (5, 'West',  'Widget',  150.00, DATE '2024-03-05'),
    (6, 'East',  'Gadget',  450.00, DATE '2024-03-05'),
    (7, 'North', 'Widget',   90.00, DATE '2024-03-20'),
    (8, 'West',  'Gadget',  270.00, DATE '2024-03-20'),
    (9, 'East',  'Widget',  NULL,   DATE '2024-03-25');  -- NULL amount
```

### 1 — Basic MIN / MAX (global)

```sql
SELECT
    MIN(amount) AS min_sale,
    MAX(amount) AS max_sale,
    MAX(amount) - MIN(amount) AS range_sale
FROM sales;
-- Result:
-- min_sale | max_sale | range_sale
-- ---------|----------|----------
-- 80.0     | 450.0    | 370.0
-- (row 9 with NULL amount is ignored)
```

### 2 — Grouped MIN / MAX

```sql
SELECT
    region,
    MIN(amount) AS min_sale,
    MAX(amount) AS max_sale
FROM sales
GROUP BY region
ORDER BY region;
-- Result:
-- region | min_sale | max_sale
-- --------|----------|--------
-- East    | 80.0     | 450.0
-- North   | 90.0     | 210.0
-- West    | 150.0    | 340.0
```

### 3 — MIN / MAX on strings and dates

```sql
SELECT
    MIN(region)     AS first_region_alpha,   -- lexicographic minimum
    MAX(region)     AS last_region_alpha,    -- lexicographic maximum
    MIN(order_date) AS earliest_order,
    MAX(order_date) AS latest_order
FROM sales;
-- Result:
-- first_region_alpha | last_region_alpha | earliest_order | latest_order
-- -------------------|-------------------|----------------|-------------
-- East               | West              | 2024-01-15     | 2024-03-25
```

### 4 — MIN / MAX with FILTER

```sql
SELECT
    MAX(amount)                                    AS overall_max,
    MAX(amount) FILTER (WHERE region = 'East')     AS east_max,
    MAX(amount) FILTER (WHERE product = 'Widget')  AS widget_max,
    MIN(amount) FILTER (WHERE amount > 100)        AS min_above_100
FROM sales;
-- Result:
-- overall_max | east_max | widget_max | min_above_100
-- ------------|----------|------------|-------------
-- 450.0       | 450.0    | 150.0      | 120.0
```

### 5 — GREATEST / LEAST for row-level comparisons

```sql
-- GREATEST / LEAST operate across columns within the same row — not across rows
SELECT
    order_id,
    amount,
    GREATEST(amount, 120.0)  AS at_least_120,   -- floor
    LEAST(amount, 200.0)     AS capped_at_200    -- ceiling
FROM sales
WHERE amount IS NOT NULL
ORDER BY order_id;
-- Note: GREATEST / LEAST return NULL if any argument is NULL
```

### 6 — argmax: retrieve the whole row for the maximum value

```sql
-- Find the order with the highest amount per region
SELECT
    region,
    MAX(STRUCT(amount, order_id, product, order_date)).amount     AS max_amount,
    MAX(STRUCT(amount, order_id, product, order_date)).order_id   AS order_id,
    MAX(STRUCT(amount, order_id, product, order_date)).product    AS product,
    MAX(STRUCT(amount, order_id, product, order_date)).order_date AS order_date
FROM sales
WHERE amount IS NOT NULL
GROUP BY region
ORDER BY region;
-- Spark compares STRUCTs field-by-field (first field = amount),
-- so MAX selects the struct whose amount is largest.
-- Result:
-- region | max_amount | order_id | product | order_date
-- --------|------------|----------|---------|------------
-- East    | 450.0      | 6        | Gadget  | 2024-03-05
-- North   | 210.0      | 4        | Gadget  | 2024-02-10
-- West    | 340.0      | 2        | Gadget  | 2024-01-15
```

### 7 — argmin: retrieve the whole row for the minimum value

```sql
SELECT
    region,
    MIN(STRUCT(amount, order_id, product)).amount   AS min_amount,
    MIN(STRUCT(amount, order_id, product)).order_id AS order_id,
    MIN(STRUCT(amount, order_id, product)).product  AS product
FROM sales
WHERE amount IS NOT NULL
GROUP BY region
ORDER BY region;
-- Result:
-- region | min_amount | order_id | product
-- --------|------------|----------|---------
-- East    | 80.0       | 3        | Widget
-- North   | 90.0       | 7        | Widget
-- West    | 150.0      | 5        | Widget
```

---

## 🧠 When to Use

| Scenario | Recommended Pattern |
|----------|---------------------|
| Global or per-group extremes | `MIN(col)` / `MAX(col)` |
| Date range of a dataset | `MIN(date_col)` / `MAX(date_col)` |
| Conditional extremes (subset of rows) | `MIN/MAX(col) FILTER (WHERE ...)` |
| Largest / smallest value across columns in a row | `GREATEST(col1, col2)` / `LEAST(col1, col2)` |
| Full row associated with the min/max value (argmin/argmax) | `MIN/MAX(STRUCT(key_col, other_cols...))` |
| Running min/max over time | `MIN/MAX(col) OVER (ORDER BY ...)` |
