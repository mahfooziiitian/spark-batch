# :material-table-pivot: PIVOT with `CASE`

Manual pivoting with conditional aggregates works anywhere Spark SQL works. It is verbose, but it gives full control over output column names, multiple measures, and missing-value behavior.

______________________________________________________________________

## :material-sitemap: Overview

```mermaid
graph LR
    A[Long rows] --> B[CASE per target bucket]
    B --> C[Aggregate each conditional expression]
    C --> D[Wide output columns]
```

### :material-animation-play: Interactive Visualization — Conditional Pivot Cells

<div id="viz-pivot-case-cells" class="ts-viz"></div>

Each output column is just an aggregate over rows whose `CASE` expression matched the target bucket.

______________________________________________________________________

## :material-pin: Pattern

```sql
SELECT
    group_col,
    agg_func(CASE WHEN pivot_col = 'value1' THEN measure_col END) AS value1_alias,
    agg_func(CASE WHEN pivot_col = 'value2' THEN measure_col END) AS value2_alias
FROM table_name
GROUP BY group_col;
```

______________________________________________________________________

## :material-magnify: Verified behavior

1. `SUM(CASE WHEN ... THEN value END)` returns `NULL` when a group has no matching rows.
2. `SUM(CASE WHEN ... THEN value ELSE 0 END)` returns `0` instead, which is often more presentation-friendly.
3. `COUNT(CASE WHEN ... THEN 1 END)` counts only matching rows because `COUNT` ignores `NULL`.
4. This pattern supports multiple aggregates and fully custom aliases without the naming rules used by native `PIVOT`.

______________________________________________________________________

## :material-flask-outline: Practical examples

### Setup

```sql
CREATE OR REPLACE TEMP VIEW sales AS
SELECT * FROM VALUES
    (1,  2023, 'Jan', 'East',  'Laptop',  5, 5000),
    (2,  2023, 'Jan', 'East',  'Monitor', 10, 2000),
    (3,  2023, 'Jan', 'West',  'Laptop',  3, 3000),
    (4,  2023, 'Feb', 'West',  'Monitor', 7, 1400),
    (5,  2023, 'Feb', 'South', 'Laptop',  4, 4000),
    (6,  2023, 'Feb', 'South', 'Monitor', 6, 1200),
    (7,  2024, 'Jan', 'East',  'Laptop',  6, 6000),
    (8,  2024, 'Jan', 'West',  'Laptop',  5, 5000),
    (9,  2024, 'Feb', 'South', 'Monitor', 8, 1600),
    (10, 2024, 'Feb', 'East',  'Monitor', 7, 1400)
AS sales(sale_id, yr, month, region, product, quantity, revenue);
```

### Revenue by region per year

```sql
SELECT
    yr,
    SUM(CASE WHEN region = 'East' THEN revenue ELSE 0 END) AS east_revenue,
    SUM(CASE WHEN region = 'West' THEN revenue ELSE 0 END) AS west_revenue,
    SUM(CASE WHEN region = 'South' THEN revenue ELSE 0 END) AS south_revenue,
    SUM(revenue) AS total_revenue
FROM sales
GROUP BY yr
ORDER BY yr;
```

Verified PySpark 4.2 result:

| yr   | east_revenue | west_revenue | south_revenue | total_revenue |
| ---- | ------------ | ------------ | ------------- | ------------- |
| 2023 | 7000         | 4400         | 5200          | 16600         |
| 2024 | 7400         | 5000         | 1600          | 14000         |

### Multiple measures per product

```sql
SELECT
    yr,
    SUM(CASE WHEN product = 'Laptop' THEN quantity END) AS laptop_qty,
    SUM(CASE WHEN product = 'Monitor' THEN quantity END) AS monitor_qty,
    SUM(CASE WHEN product = 'Laptop' THEN revenue END) AS laptop_revenue,
    SUM(CASE WHEN product = 'Monitor' THEN revenue END) AS monitor_revenue
FROM sales
GROUP BY yr
ORDER BY yr;
```

### Conditional counts

```sql
SELECT
    yr,
    COUNT(CASE WHEN revenue >= 4000 THEN 1 END) AS high_value_orders,
    COUNT(CASE WHEN revenue < 4000 THEN 1 END) AS standard_orders
FROM sales
GROUP BY yr
ORDER BY yr;
```

______________________________________________________________________

## :material-brain: When to use

| Scenario                             | Recommended pattern        |
| ------------------------------------ | -------------------------- |
| Portable, fully explicit pivot logic | `CASE` + aggregate         |
| Need custom output column names      | `CASE` + aggregate         |
| Need a static pivot with less code   | [`PIVOT`](spark.md)        |
| Need only one conditional measure    | `agg() FILTER (WHERE ...)` |
