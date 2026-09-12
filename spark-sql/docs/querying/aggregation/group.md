# :material-group: GROUP BY

`GROUP BY` partitions rows by one or more keys so aggregate functions run independently for each distinct key combination.

______________________________________________________________________

## :material-sitemap: Overview

```mermaid
graph LR
    A[Input rows] --> B[Group identical keys]
    B --> C[Aggregate each group]
    C --> D[Return one row per key]
```

### :material-animation-play: Interactive Visualization — Grouping Flow

<div id="viz-group-by-flow" class="ts-viz"></div>

The visualization contrasts row-level filtering with post-aggregation filtering so `WHERE` and `HAVING` are easier to distinguish.

______________________________________________________________________

## :material-pin: Syntax

```sql
SELECT col1 [, col2, ...], agg_func(expr) [AS alias]
FROM table_name
[WHERE filter_condition]
GROUP BY col1 [, col2, ...]
[HAVING group_filter]
[ORDER BY ...];
```

______________________________________________________________________

## :material-magnify: Verified behavior

1. Every non-aggregated expression in `SELECT` must also appear in the `GROUP BY` list, or Spark raises an analysis error.
2. `WHERE` filters rows before grouping; `HAVING` filters aggregated groups after grouping.
3. `NULL` keys are grouped together, so all `NULL` values for the same grouping position land in one group.
4. Grouping by expressions is valid: `GROUP BY YEAR(order_date), MONTH(order_date)` works directly.
5. `GROUP BY` over an empty relation returns zero rows; a scalar aggregate without `GROUP BY` returns one row instead.

______________________________________________________________________

## :material-flask-outline: Practical examples

### Setup

```sql
CREATE OR REPLACE TEMP VIEW sales AS
SELECT * FROM VALUES
    (1, 'East',  'Widget', 120.00, DATE '2024-01-15'),
    (2, 'West',  'Gadget', 340.00, DATE '2024-01-15'),
    (3, 'East',  'Widget',  80.00, DATE '2024-02-10'),
    (4, 'North', 'Gadget', 210.00, DATE '2024-02-10'),
    (5, 'West',  'Widget', 150.00, DATE '2024-03-05'),
    (6, 'East',  'Gadget', 450.00, DATE '2024-03-05'),
    (7, 'North', 'Widget',  90.00, DATE '2024-03-20'),
    (8, 'West',  'Gadget', 270.00, DATE '2024-03-20')
AS sales(order_id, region, product, amount, order_date);
```

### One row per region

```sql
SELECT
    region,
    COUNT(*) AS order_count,
    SUM(amount) AS total_sales,
    ROUND(AVG(amount), 2) AS avg_sale
FROM sales
GROUP BY region
ORDER BY region;
```

Verified result:

| region | order_count | total_sales | avg_sale |
| ------ | ----------- | ----------- | -------- |
| East   | 3           | 650.0       | 216.67   |
| North  | 2           | 300.0       | 150.0    |
| West   | 3           | 760.0       | 253.33   |

### Grouping by two keys

```sql
SELECT
    region,
    product,
    SUM(amount) AS total_sales,
    MIN(amount) AS min_sale,
    MAX(amount) AS max_sale
FROM sales
GROUP BY region, product
ORDER BY region, product;
```

### `WHERE` before grouping, `HAVING` after grouping

```sql
SELECT
    region,
    SUM(amount) AS total_sales
FROM sales
WHERE amount >= 100
GROUP BY region
HAVING SUM(amount) > 500
ORDER BY total_sales DESC;
```

### Grouping by an expression

```sql
SELECT
    YEAR(order_date) AS sale_year,
    MONTH(order_date) AS sale_month,
    SUM(amount) AS monthly_total
FROM sales
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY sale_year, sale_month;
```

### Real `NULL` values form their own group

```sql
SELECT
    region,
    COUNT(*) AS rows_in_group
FROM (
    SELECT * FROM sales
    UNION ALL
    SELECT 9, NULL, 'Widget', 50.00, DATE '2024-03-25'
) src
GROUP BY region
ORDER BY region NULLS LAST;
```

In PySpark 4.2, the extra row produces a separate `region = NULL` group; it does not disappear.

______________________________________________________________________

## :material-brain: When to use

| Scenario                         | Pattern                                                                                     |
| -------------------------------- | ------------------------------------------------------------------------------------------- |
| Aggregate by one key             | `GROUP BY single_col`                                                                       |
| Aggregate by multiple keys       | `GROUP BY col1, col2`                                                                       |
| Filter groups by aggregate value | `HAVING`                                                                                    |
| Group by derived buckets         | `GROUP BY CASE ...` or `GROUP BY expression`                                                |
| Need subtotals as extra rows     | [`ROLLUP`](olap/rollup.md), [`CUBE`](olap/cube.md), or [`GROUPING SETS`](olap/group-set.md) |
