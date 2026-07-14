# :material-filter-plus-outline: Subquery in HAVING

A subquery in the `HAVING` clause filters aggregated groups by comparing a group-level
aggregate against a value produced by another query. It is the cleanest way to express
"keep groups whose aggregate exceeds some dynamically computed threshold."

---

## :material-code-tags: Syntax

```sql
SELECT group_col, AGG(measure)
FROM table
[WHERE row_filter]
GROUP BY group_col
HAVING AGG(measure) > (SELECT threshold_expression FROM ...);
```

The subquery in `HAVING` follows the same rules as a scalar subquery — it must return
at most one row and one column.

---

## :material-information-outline: Behavior

1. The `HAVING` subquery is evaluated **once** (when uncorrelated) and the result is used as a constant threshold for all groups.
2. When the `HAVING` subquery is **correlated** (references the outer `GROUP BY` key), it is evaluated once per group — Catalyst attempts to decorrelate it into a join.
3. `HAVING` filters operate on aggregated rows, so the subquery threshold is compared against the **aggregate result**, not individual row values.
4. Combining `HAVING AGG(...) > (subquery)` with a `WHERE` on the same column is valid — `WHERE` filters rows before aggregation; `HAVING` filters groups after.

---

## :material-flask-outline: Practical Examples

### Groups above the global average

```sql
SELECT
    customer_id,
    SUM(amount) AS total_spent
FROM orders
GROUP BY customer_id
HAVING SUM(amount) > (SELECT AVG(amount) FROM orders);
-- Keeps customers whose lifetime spend exceeds the average single-order amount
```

### Regions above the median revenue

```sql
SELECT
    region,
    SUM(amount) AS regional_revenue
FROM orders
WHERE order_date >= '2024-01-01'
GROUP BY region
HAVING SUM(amount) > (
    SELECT PERCENTILE(region_total, 0.5)
    FROM (
        SELECT region, SUM(amount) AS region_total
        FROM orders
        WHERE order_date >= '2024-01-01'
        GROUP BY region
    ) AS rt
);
```

### Groups that exceed a config threshold

```sql
-- Threshold is stored in a config table — no hardcoded value
SELECT
    warehouse_id,
    SUM(units_sold) AS total_units
FROM daily_inventory
GROUP BY warehouse_id
HAVING SUM(units_sold) > (
    SELECT CAST(config_value AS INT)
    FROM app_config
    WHERE config_key = 'high_volume_warehouse_threshold'
);
```

### Categories with revenue above last year's top category

```sql
SELECT
    category,
    SUM(amount) AS this_year_revenue
FROM orders JOIN products USING (product_id)
WHERE YEAR(order_date) = 2024
GROUP BY category
HAVING SUM(amount) > (
    SELECT MAX(SUM(amount))
    FROM orders JOIN products USING (product_id)
    WHERE YEAR(order_date) = 2023
    GROUP BY category
);
```

### Correlated HAVING: groups beating their own previous period

```sql
SELECT
    region,
    SUM(amount) AS current_revenue
FROM orders
WHERE order_date BETWEEN '2024-04-01' AND '2024-06-30'
GROUP BY region
HAVING SUM(amount) > (
    SELECT SUM(amount)
    FROM orders AS prev
    WHERE prev.region = orders.region        -- correlated on outer GROUP BY key
      AND prev.order_date BETWEEN '2024-01-01' AND '2024-03-31'
);
```

### Count filter: groups with more than average group size

```sql
SELECT
    department,
    COUNT(*) AS headcount
FROM employees
GROUP BY department
HAVING COUNT(*) > (
    SELECT AVG(dept_size)
    FROM (
        SELECT department, COUNT(*) AS dept_size
        FROM employees
        GROUP BY department
    ) AS dept_counts
);
```

### Multiple HAVING conditions, one with a subquery

```sql
SELECT
    product_id,
    SUM(quantity)   AS units_sold,
    SUM(revenue)    AS total_revenue
FROM order_lines
GROUP BY product_id
HAVING SUM(quantity) >= 100                            -- literal threshold
   AND SUM(revenue)  > (SELECT AVG(revenue)            -- dynamic subquery threshold
                         FROM (SELECT product_id, SUM(revenue) AS revenue
                               FROM order_lines
                               GROUP BY product_id) AS prt);
```

---

## :material-swap-horizontal: HAVING Subquery vs CTE Approach

```sql
-- HAVING subquery: concise but subquery is hidden at the bottom
SELECT region, SUM(amount) AS total
FROM orders
GROUP BY region
HAVING SUM(amount) > (SELECT AVG(total) FROM (SELECT region, SUM(amount) AS total FROM orders GROUP BY region) t);

-- CTE approach: same logic, more readable
WITH region_totals AS (
    SELECT region, SUM(amount) AS total
    FROM orders
    GROUP BY region
),
avg_total AS (
    SELECT AVG(total) AS threshold FROM region_totals
)
SELECT region, total
FROM region_totals
WHERE total > (SELECT threshold FROM avg_total);
```

Use the CTE approach for complex thresholds — it separates concerns and is easier to test.

---

## :material-lightbulb-outline: When to Use HAVING Subqueries

| Scenario | Pattern |
|----------|---------|
| Filter groups above the global average | `HAVING AGG > (SELECT AVG(col) FROM ...)` |
| Filter groups above a stored threshold | `HAVING AGG > (SELECT value FROM config WHERE key = '...')` |
| Groups beating a prior period | Correlated `HAVING` subquery |
| Complex threshold (median, percentile) | Nested subquery in `HAVING` or CTE |
| Multiple threshold conditions | Combine literal `HAVING` and subquery `HAVING` |

!!! tip "Use a CTE for complex thresholds"
    When the threshold subquery itself requires aggregation or joins, extract it into a
    CTE. A `CROSS JOIN` against the threshold CTE is more readable than deeply nested
    subqueries inside `HAVING`.
