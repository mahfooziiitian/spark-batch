# :material-table-pivot: CTE for Pivot and Unpivot

CTEs make reshaping queries easier to read because they let you separate filtering and aggregation from the final column layout. In Spark 4.2, you can combine CTEs with built-in `PIVOT`, manual `CASE WHEN` pivoting, and `UNPIVOT`.

### :material-animation-play: Interactive Visualization — Rows to Columns and Back

<div id="viz-cte-pivot-reshape" class="ts-viz"></div>

Flip between pivot and unpivot to see how the same data changes shape. The visualization matches verified Spark 4.2 behavior, including fixed output columns for `PIVOT` and row expansion for `UNPIVOT`.

______________________________________________________________________

## :material-code-tags: Syntax

```sql
WITH base AS (
    SELECT grouping_col, pivot_col, measure_col
    FROM source_table
)
SELECT *
FROM base
PIVOT (
    SUM(measure_col)
    FOR pivot_col IN ('val1' AS alias1, 'val2' AS alias2)
);
```

```sql
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
```

```sql
SELECT id, attribute, value
FROM wide_table
UNPIVOT (
    value FOR attribute IN (col1 AS attr1, col2 AS attr2, col3 AS attr3)
);
```

______________________________________________________________________

## :material-information-outline: Behavior

1. **`PIVOT` and manual `CASE WHEN` solve the same problem, but not with the same plan node** — in Spark 4.2, `EXPLAIN` showed `PIVOT` using `pivotfirst`, while the manual version aggregated the `CASE` expressions directly.
2. **Output columns must still be known when you write the query** — whether you use `PIVOT` or `CASE WHEN`, truly dynamic pivots require generating SQL text first.
3. **`UNPIVOT` works in Spark 4.2** — aliases in the `IN (...)` list are bare identifiers such as `jan_budget AS January`, not quoted strings.
4. **A prep CTE is often the performance win** — pre-filtering or pre-aggregating in a CTE reduces the rows the reshape step must process.
5. **Missing combinations remain `NULL` unless you fill them** — use `COALESCE` when downstream consumers want zeros instead of nulls.

______________________________________________________________________

## :material-flask-outline: Practical Examples

### Built-in `PIVOT`: monthly sales by region

```sql
WITH monthly_sales AS (
    SELECT *
    FROM VALUES
        ('EU', '2024-01', 10),
        ('EU', '2024-02', 20),
        ('US', '2024-01', 30)
    AS t(region, month, amount)
)
SELECT *
FROM monthly_sales
PIVOT (
    SUM(amount)
    FOR month IN (
        '2024-01' AS jan_2024,
        '2024-02' AS feb_2024
    )
)
ORDER BY region;
```

### Manual pivot with `CASE WHEN`

```sql
WITH base AS (
    SELECT customer_id, category, amount
    FROM orders
    JOIN products USING (product_id)
    WHERE order_date >= DATE '2024-01-01'
)
SELECT
    customer_id,
    COALESCE(SUM(CASE WHEN category = 'Electronics' THEN amount END), 0) AS electronics,
    COALESCE(SUM(CASE WHEN category = 'Books' THEN amount END), 0) AS books,
    COALESCE(SUM(CASE WHEN category = 'Clothing' THEN amount END), 0) AS clothing,
    COALESCE(SUM(CASE WHEN category = 'Home' THEN amount END), 0) AS home
FROM base
GROUP BY customer_id
ORDER BY electronics DESC;
```

### Multi-metric pivot with one prep CTE

```sql
WITH base AS (
    SELECT region, status, amount
    FROM orders
    WHERE order_date >= DATE '2024-01-01'
)
SELECT
    region,
    SUM(CASE WHEN status = 'COMPLETED' THEN amount ELSE 0 END) AS completed_revenue,
    SUM(CASE WHEN status = 'CANCELLED' THEN amount ELSE 0 END) AS cancelled_revenue,
    SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed_count,
    SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_count
FROM base
GROUP BY region;
```

### `UNPIVOT`: wide monthly columns to long rows

```sql
SELECT dept, month, budget
FROM monthly_budget
UNPIVOT (
    budget FOR month IN (
        jan_budget AS January,
        feb_budget AS February,
        mar_budget AS March
    )
)
ORDER BY dept, month;
```

### Manual unpivot with `UNION ALL`

```sql
WITH wide AS (
    SELECT dept, jan_budget, feb_budget, mar_budget
    FROM monthly_budget
)
SELECT dept, 'January' AS month, jan_budget AS budget FROM wide
UNION ALL
SELECT dept, 'February' AS month, feb_budget AS budget FROM wide
UNION ALL
SELECT dept, 'March' AS month, mar_budget AS budget FROM wide;
```

______________________________________________________________________

## :material-swap-horizontal: `PIVOT` vs Manual `CASE WHEN`

| Aspect                       | Built-in `PIVOT`               | Manual `CASE WHEN`                     |
| ---------------------------- | ------------------------------ | -------------------------------------- |
| Readability                  | Concise for one metric         | More verbose                           |
| Planner behavior             | Uses `pivotfirst` in Spark 4.2 | Direct aggregate of `CASE` expressions |
| Multiple custom metrics      | Less flexible                  | Very flexible                          |
| Missing cells                | `NULL` by default              | Control with `COALESCE`                |
| Best partner for a prep step | CTE                            | CTE                                    |

______________________________________________________________________

## :material-lightbulb-outline: When to Use

| Scenario                                    | Pattern                   |
| ------------------------------------------- | ------------------------- |
| A fixed list of categories or months        | `PIVOT`                   |
| Many custom measures in one output row      | Manual `CASE WHEN`        |
| Wide table back to long analytics form      | `UNPIVOT`                 |
| Large raw source that needs shrinking first | Prep CTE + reshape        |
| Dynamic pivot values discovered at runtime  | Generate SQL, then run it |
