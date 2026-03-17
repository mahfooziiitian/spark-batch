# GROUP BY

`GROUP BY` groups rows that share the same values across one or more columns so that aggregate functions operate on each group independently.

---

## 📌 Syntax

```sql
SELECT col1 [, col2, ...], agg_func(expr) [AS alias]
FROM   table_name
[WHERE filter_condition]
GROUP BY col1 [, col2, ...]
[HAVING group_filter]
[ORDER BY col1 [ASC | DESC]];
```

| Clause | Description |
|--------|-------------|
| `col1, col2, ...` | One or more grouping columns; every non-aggregated `SELECT` column must appear here |
| `agg_func(expr)` | Any aggregate: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, etc. |
| `HAVING` | Filters groups *after* aggregation (analogous to `WHERE` but for groups) |
| `ORDER BY` | Sorts the final grouped output |

---

## 🔍 Behavior

1. **Non-aggregated columns rule** — every column in `SELECT` that is not inside an aggregate must appear in `GROUP BY`; Spark raises an analysis error otherwise.
2. **NULL grouping** — all `NULL` values in a grouping column are treated as equal and placed in one group together.
3. **HAVING vs WHERE** — `WHERE` filters rows *before* grouping; `HAVING` filters groups *after* aggregation and may reference aggregate expressions or their aliases.
4. **Execution order** — `FROM → WHERE → GROUP BY → HAVING → SELECT → ORDER BY`.
5. **Expression grouping** — you can group by an expression such as `YEAR(order_date)` directly, not only plain column references.
6. **Empty table** — `GROUP BY` on an empty table returns zero rows; aggregate functions return `NULL` (except `COUNT`, which returns `0`).

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

### 1 — Single-column grouping

```sql
SELECT
    region,
    COUNT(*)     AS order_count,
    SUM(amount)  AS total_sales,
    AVG(amount)  AS avg_sale
FROM sales
GROUP BY region;
-- Result:
-- region | order_count | total_sales | avg_sale
-- --------|-------------|-------------|----------
-- East    | 3           | 650.0       | 216.67
-- West    | 3           | 760.0       | 253.33
-- North   | 2           | 300.0       | 150.0
```

### 2 — Multi-column grouping

```sql
SELECT
    region,
    product,
    SUM(amount)  AS total_sales,
    MIN(amount)  AS min_sale,
    MAX(amount)  AS max_sale
FROM sales
GROUP BY region, product
ORDER BY region, product;
-- Result:
-- region | product | total_sales | min_sale | max_sale
-- --------|---------|-------------|----------|--------
-- East    | Gadget  | 450.0       | 450.0    | 450.0
-- East    | Widget  | 200.0       | 80.0     | 120.0
-- North   | Gadget  | 210.0       | 210.0    | 210.0
-- North   | Widget  | 90.0        | 90.0     | 90.0
-- West    | Gadget  | 610.0       | 270.0    | 340.0
-- West    | Widget  | 150.0       | 150.0    | 150.0
```

### 3 — Grouping by an expression (derived column)

```sql
SELECT
    YEAR(order_date)  AS sale_year,
    MONTH(order_date) AS sale_month,
    SUM(amount)       AS monthly_total
FROM sales
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY sale_year, sale_month;
-- Result:
-- sale_year | sale_month | monthly_total
-- ----------|------------|-------------
-- 2024      | 1          | 460.0
-- 2024      | 2          | 290.0
-- 2024      | 3          | 960.0
```

### 4 — Filtering groups with HAVING

```sql
SELECT
    region,
    SUM(amount) AS total_sales
FROM sales
GROUP BY region
HAVING SUM(amount) > 500
ORDER BY total_sales DESC;
-- Result:
-- region | total_sales
-- --------|------------
-- West    | 760.0
-- East    | 650.0
```

### 5 — Aggregate with FILTER clause

```sql
SELECT
    product,
    SUM(amount)                                   AS total_sales,
    SUM(amount) FILTER (WHERE region = 'East')    AS east_sales,
    SUM(amount) FILTER (WHERE region = 'West')    AS west_sales
FROM sales
GROUP BY product;
-- Result:
-- product | total_sales | east_sales | west_sales
-- ---------|-------------|------------|----------
-- Widget   | 440.0       | 200.0      | 150.0
-- Gadget   | 1270.0      | 450.0      | 610.0
```

### 6 — GROUP BY with a CASE expression

```sql
SELECT
    CASE
        WHEN amount < 150  THEN 'Low'
        WHEN amount < 300  THEN 'Medium'
        ELSE                    'High'
    END         AS sales_tier,
    COUNT(*)    AS order_count,
    SUM(amount) AS tier_total
FROM sales
GROUP BY
    CASE
        WHEN amount < 150  THEN 'Low'
        WHEN amount < 300  THEN 'Medium'
        ELSE                    'High'
    END
ORDER BY tier_total DESC;
-- Result:
-- sales_tier | order_count | tier_total
-- -----------|-------------|----------
-- High       | 3           | 1060.0
-- Medium     | 3           | 620.0
-- Low        | 2           | 170.0
```

---

## 🧠 When to Use

| Scenario | Recommended Pattern |
|----------|---------------------|
| Aggregate by one dimension | `GROUP BY single_col` |
| Aggregate by multiple dimensions | `GROUP BY col1, col2` |
| Filter aggregated groups | `GROUP BY ... HAVING condition` |
| Group by a computed value | `GROUP BY expression` (e.g., `YEAR(date)`) |
| Partial aggregation per condition | `agg() FILTER (WHERE ...)` |
| Hierarchical subtotals (year → month) | [`ROLLUP`](rollup.md) |
| All combination subtotals | [`CUBE`](cube.md) |
| Custom grouping combinations | [`GROUPING SETS`](group_set.md) |
