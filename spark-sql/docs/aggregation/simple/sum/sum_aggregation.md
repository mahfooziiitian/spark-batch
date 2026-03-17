# SUM

`SUM` returns the total of all non-NULL numeric values in a group.

---

## 📌 Syntax

```sql
SUM(expr)
SUM(DISTINCT expr)
SUM(expr) FILTER (WHERE condition)
```

| Variant | Description |
|---------|-------------|
| `SUM(expr)` | Sum of all non-NULL values |
| `SUM(DISTINCT expr)` | Sum of unique non-NULL values only |
| `SUM(expr) FILTER (WHERE ...)` | Sum only rows matching the condition |

---

## 🔍 Behavior

1. **NULL ignored** — `NULL` values are silently excluded; `SUM` over an all-NULL column returns `NULL`, not `0`.
2. **Return type** — `BIGINT` input returns `BIGINT`; `DOUBLE` / `FLOAT` input returns `DOUBLE`; `DECIMAL` input preserves precision and scale.
3. **BIGINT overflow** — summing very large `INTEGER` or `BIGINT` columns can overflow silently in Spark SQL; cast to `DECIMAL` or `DOUBLE` when totals may exceed `2⁶³ − 1`.
4. **DISTINCT semantics** — `SUM(DISTINCT col)` deduplicates values before summing; useful when rows are duplicated by a join.
5. **Empty group** — `SUM` over zero rows (after a `WHERE` that matches nothing) returns `NULL`.
6. **Window function** — `SUM` is also available as a window function with `OVER (...)` for running totals; see the Window Functions section.

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
    (8, 'West',  'Gadget',  270.00, DATE '2024-03-20');
```

### 1 — Basic SUM (grand total)

```sql
SELECT SUM(amount) AS grand_total
FROM sales;
-- Result:
-- grand_total
-- -----------
-- 1710.0
```

### 2 — Grouped SUM

```sql
SELECT
    region,
    SUM(amount) AS total_sales
FROM sales
GROUP BY region
ORDER BY total_sales DESC;
-- Result:
-- region | total_sales
-- --------|------------
-- West    | 760.0
-- East    | 650.0
-- North   | 300.0
```

### 3 — SUM with FILTER clause

```sql
SELECT
    region,
    SUM(amount)                                   AS total_sales,
    SUM(amount) FILTER (WHERE product = 'Widget') AS widget_sales,
    SUM(amount) FILTER (WHERE product = 'Gadget') AS gadget_sales
FROM sales
GROUP BY region
ORDER BY region;
-- Result:
-- region | total_sales | widget_sales | gadget_sales
-- --------|-------------|--------------|-------------
-- East    | 650.0       | 200.0        | 450.0
-- North   | 300.0       | 90.0         | 210.0
-- West    | 760.0       | 150.0        | 610.0
```

### 4 — SUM(DISTINCT) to avoid double-counting

```sql
-- Simulate a join that duplicates rows
WITH expanded AS (
    SELECT 'East' AS region, 100.00 AS amount
    UNION ALL SELECT 'East', 100.00   -- duplicate
    UNION ALL SELECT 'East', 200.00
)
SELECT
    SUM(amount)          AS sum_all,       -- counts duplicates: 400.0
    SUM(DISTINCT amount) AS sum_distinct   -- deduplicates:      300.0
FROM expanded
WHERE region = 'East';
```

### 5 — SUM with CASE for pivot-style conditional aggregation

```sql
SELECT
    product,
    SUM(CASE WHEN region = 'East'  THEN amount ELSE 0 END) AS east_sales,
    SUM(CASE WHEN region = 'West'  THEN amount ELSE 0 END) AS west_sales,
    SUM(CASE WHEN region = 'North' THEN amount ELSE 0 END) AS north_sales
FROM sales
GROUP BY product;
-- Result:
-- product | east_sales | west_sales | north_sales
-- ---------|------------|------------|------------
-- Widget   | 200.0      | 150.0      | 90.0
-- Gadget   | 450.0      | 610.0      | 210.0
```

### 6 — Running total with SUM as a window function

```sql
SELECT
    order_id,
    order_date,
    amount,
    SUM(amount) OVER (
        ORDER BY order_date, order_id
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total
FROM sales
ORDER BY order_date, order_id;
-- Result:
-- order_id | order_date | amount | running_total
-- ----------|------------|--------|-------------
-- 1         | 2024-01-15 | 120.0  | 120.0
-- 2         | 2024-01-15 | 340.0  | 460.0
-- 3         | 2024-02-10 |  80.0  | 540.0
-- ...
```

---

## 🧠 When to Use

| Scenario | Recommended Pattern |
|----------|---------------------|
| Grand total of a column | `SUM(col)` |
| Per-group totals | `GROUP BY ... SUM(col)` |
| Conditional totals per group | `SUM(col) FILTER (WHERE ...)` |
| Avoid double-counting from joins | `SUM(DISTINCT col)` |
| Pivot-style column totals | `SUM(CASE WHEN ... THEN col ELSE 0 END)` |
| Cumulative / running total | `SUM(col) OVER (ORDER BY ...)` |
