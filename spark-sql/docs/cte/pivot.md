# :material-table-pivot: CTE for Pivot and Unpivot

CTEs make manual pivot and unpivot queries readable by separating the aggregation from
the column-shaping step. Spark SQL also provides a built-in `PIVOT` clause which can
be combined with a CTE to pre-filter or pre-aggregate before pivoting.

---

## :material-code-tags: Syntax

```sql
-- Built-in PIVOT
SELECT *
FROM (
    SELECT grouping_col, pivot_col, measure_col
    FROM source_table
)
PIVOT (
    AGG_FUNCTION(measure_col)
    FOR pivot_col IN ('val1' AS alias1, 'val2' AS alias2, ...)
);

-- Manual pivot with CASE WHEN (CTE approach)
WITH base AS (
    SELECT grouping_col, pivot_col, measure_col
    FROM source_table
)
SELECT
    grouping_col,
    SUM(CASE WHEN pivot_col = 'val1' THEN measure_col END) AS alias1,
    SUM(CASE WHEN pivot_col = 'val2' THEN measure_col END) AS alias2
FROM base
GROUP BY grouping_col;

-- UNPIVOT (wide → long)
SELECT id, attribute, value
FROM wide_table
UNPIVOT (value FOR attribute IN (col1, col2, col3));
```

---

## :material-information-outline: Behavior

1. Spark's built-in `PIVOT` clause is equivalent to a manual `CASE WHEN` pivot — both produce the same physical plan.
2. `PIVOT` requires the pivot column values to be **known at query-write time**. For dynamic pivots (values discovered at runtime), use a CTE to aggregate first, then reshape with `CASE WHEN`.
3. `UNPIVOT` (Spark 3.4+) converts wide-format columns into rows. Earlier versions require a manual `UNION ALL` approach.
4. For large datasets, pre-aggregating in a CTE before pivoting reduces the number of rows the `PIVOT`/`CASE WHEN` clause must process.
5. `NULL` pivot cells indicate that no rows matched that combination — use `COALESCE(..., 0)` for numeric measures to replace `NULL` with zero.

---

## :material-flask-outline: Practical Examples

### Built-in PIVOT: monthly sales by region

```sql
WITH monthly_sales AS (
    SELECT
        region,
        DATE_FORMAT(order_date, 'yyyy-MM') AS month,
        amount
    FROM sales
    WHERE order_date BETWEEN '2024-01-01' AND '2024-03-31'
)
SELECT *
FROM monthly_sales
PIVOT (
    SUM(amount)
    FOR month IN (
        '2024-01' AS jan_2024,
        '2024-02' AS feb_2024,
        '2024-03' AS mar_2024
    )
);
-- Result:
-- | region | jan_2024 | feb_2024 | mar_2024 |
-- |--------|----------|----------|----------|
-- | EU     | 42000.00 | 38500.00 | 51000.00 |
-- | US     | 61000.00 | 57200.00 | 63800.00 |
```

### Manual CASE WHEN pivot (zero-fill NULLs)

```sql
WITH base AS (
    SELECT
        customer_id,
        category,
        amount
    FROM orders
    JOIN products USING (product_id)
    WHERE order_date >= '2024-01-01'
)
SELECT
    customer_id,
    COALESCE(SUM(CASE WHEN category = 'Electronics' THEN amount END), 0) AS electronics,
    COALESCE(SUM(CASE WHEN category = 'Books'       THEN amount END), 0) AS books,
    COALESCE(SUM(CASE WHEN category = 'Clothing'    THEN amount END), 0) AS clothing,
    COALESCE(SUM(CASE WHEN category = 'Home'        THEN amount END), 0) AS home
FROM base
GROUP BY customer_id
ORDER BY electronics DESC;
```

### Multi-metric pivot (sum + count together)

```sql
WITH base AS (
    SELECT region, status, amount
    FROM orders
    WHERE order_date >= '2024-01-01'
)
SELECT
    region,
    SUM(CASE WHEN status = 'COMPLETED' THEN amount  ELSE 0 END) AS completed_revenue,
    SUM(CASE WHEN status = 'CANCELLED' THEN amount  ELSE 0 END) AS cancelled_revenue,
    SUM(CASE WHEN status = 'COMPLETED' THEN 1       ELSE 0 END) AS completed_count,
    SUM(CASE WHEN status = 'CANCELLED' THEN 1       ELSE 0 END) AS cancelled_count
FROM base
GROUP BY region;
```

### Percent-of-total pivot

```sql
WITH category_totals AS (
    SELECT
        region,
        category,
        SUM(amount) AS category_revenue
    FROM orders JOIN products USING (product_id)
    GROUP BY region, category
),
region_totals AS (
    SELECT region, SUM(category_revenue) AS total_revenue
    FROM category_totals
    GROUP BY region
)
SELECT
    ct.region,
    ROUND(SUM(CASE WHEN category = 'Electronics' THEN category_revenue END)
          / rt.total_revenue * 100, 1) AS electronics_pct,
    ROUND(SUM(CASE WHEN category = 'Books'       THEN category_revenue END)
          / rt.total_revenue * 100, 1) AS books_pct,
    ROUND(SUM(CASE WHEN category = 'Clothing'    THEN category_revenue END)
          / rt.total_revenue * 100, 1) AS clothing_pct
FROM category_totals AS ct
JOIN region_totals   AS rt USING (region)
GROUP BY ct.region, rt.total_revenue;
```

### UNPIVOT: wide monthly columns → long format (Spark 3.4+)

```sql
-- Source: monthly_budget(dept, jan_budget, feb_budget, mar_budget)
SELECT dept, month, budget
FROM monthly_budget
UNPIVOT (
    budget FOR month IN (
        jan_budget AS 'January',
        feb_budget AS 'February',
        mar_budget AS 'March'
    )
)
ORDER BY dept, month;
-- Result:
-- | dept  | month    | budget   |
-- |-------|----------|----------|
-- | Sales | January  | 50000.00 |
-- | Sales | February | 48000.00 |
-- | Sales | March    | 52000.00 |
```

### Manual UNPIVOT with UNION ALL (pre-Spark 3.4)

```sql
WITH wide AS (
    SELECT dept, jan_budget, feb_budget, mar_budget
    FROM monthly_budget
)
SELECT dept, 'January'  AS month, jan_budget AS budget FROM wide
UNION ALL
SELECT dept, 'February' AS month, feb_budget AS budget FROM wide
UNION ALL
SELECT dept, 'March'    AS month, mar_budget AS budget FROM wide
ORDER BY dept, month;
```

### Pivot then aggregate across pivoted columns

```sql
WITH pivoted AS (
    SELECT *
    FROM (SELECT region, category, amount FROM orders JOIN products USING (product_id))
    PIVOT (
        SUM(amount)
        FOR category IN ('Electronics' AS electronics, 'Books' AS books, 'Clothing' AS clothing)
    )
)
SELECT
    region,
    COALESCE(electronics, 0) + COALESCE(books, 0) + COALESCE(clothing, 0) AS total_revenue,
    COALESCE(electronics, 0) AS electronics,
    COALESCE(books, 0)       AS books,
    COALESCE(clothing, 0)    AS clothing
FROM pivoted
ORDER BY total_revenue DESC;
```

---

## :material-swap-horizontal: PIVOT vs Manual CASE WHEN

| Aspect | Built-in `PIVOT` | Manual `CASE WHEN` |
|--------|-----------------|-------------------|
| Readability | Concise | Verbose for many columns |
| Dynamic values | Not supported — must be literals | Not supported without dynamic SQL |
| Multiple aggregations | One per `PIVOT` clause | Unlimited `CASE WHEN` expressions |
| NULL handling | Returns NULL for missing combos | Add `COALESCE` per column |
| Spark version | All versions | All versions |

---

## :material-lightbulb-outline: When to Use

| Scenario | Pattern |
|----------|---------|
| Report with months/quarters as columns | `PIVOT` with literal month values |
| Multiple metrics per pivot column | Manual `CASE WHEN` pivot |
| Convert wide table to long for analysis | `UNPIVOT` (Spark 3.4+) or `UNION ALL` |
| Percentage share across categories | Multi-CTE: totals → percent pivot |
| Pivot a pre-filtered / pre-aggregated result | CTE to prepare data, then `PIVOT` |

!!! tip "Pre-aggregate before PIVOT"
    If the source table is large, use a CTE to `GROUP BY` the pivot dimensions first.
    This reduces the number of rows the `PIVOT` clause processes and avoids shuffling
    raw data through the reshape step.
