# :material-filter-variant: HAVING Clause

`HAVING` filters **groups** after aggregation. Use it when you need to apply a
condition to aggregated results (e.g., `COUNT`, `SUM`, `AVG`).

---

## :material-view-grid: In This Section

| Page | What You Will Learn |
|------|---------------------|
| [WHERE vs HAVING](having_vs_where.md) | Execution order, performance impact, common mistakes |
| [FILTER Modifier](having_filter.md) | Conditional aggregation with `FILTER (WHERE ...)` |
| [Advanced HAVING](having_advanced.md) | ROLLUP / CUBE / GROUPING SETS, QUALIFY, multi-condition |
| [HAVING Patterns](having_patterns.md) | Top-N groups, ratio filters, config-driven thresholds |

!!! note "HAVING with subqueries"
    For subqueries inside a HAVING clause see [Subquery in HAVING](../subquery/having_subquery.md).

---

## :material-sitemap: Execution Order

```mermaid
graph LR
    A[All Rows] --> B["FROM / JOIN"]
    B --> C["WHERE (row filter)"]
    C --> D["GROUP BY"]
    D --> E["HAVING (group filter)"]
    E --> F["SELECT / ORDER BY"]
    F --> G[Result]
```

---

## :material-code-tags: Syntax

```sql
SELECT   group_cols, aggregate_exprs
FROM     table
[WHERE   row_predicate]
GROUP BY group_cols
HAVING   aggregate_predicate
[ORDER BY ...]
[LIMIT n];
```

---

## :material-table: WHERE vs HAVING vs FILTER

| Clause | Applies To | Timing | Example |
|--------|------------|--------|---------|
| `WHERE` | Individual rows | Before aggregation | `WHERE status = 'shipped'` |
| `HAVING` | Aggregated groups | After aggregation | `HAVING SUM(amount) > 1000` |
| `FILTER` | A single aggregate | During aggregation | `SUM(amount) FILTER (WHERE status='shipped')` |

---

## :material-flask-outline: Practical Examples

### Filter Groups by Count

```sql
SELECT region, COUNT(*) AS total_orders
FROM orders
GROUP BY region
HAVING COUNT(*) > 100;
```

### Filter by Aggregate Sum

```sql
SELECT customer_id, SUM(amount) AS total_spent
FROM orders
GROUP BY customer_id
HAVING SUM(amount) > 5000;
```

### Combine WHERE + HAVING

```sql
-- WHERE filters rows first, then HAVING filters the grouped result
SELECT product, AVG(price) AS avg_price
FROM sales
WHERE sale_date >= '2024-01-01'
GROUP BY product
HAVING AVG(price) > 100;
```

### Filter by Aggregate Ratio Using FILTER

```sql
SELECT region,
       SUM(amount)                                        AS total_sales,
       SUM(amount) FILTER (WHERE status = 'shipped')     AS shipped_sales
FROM orders
GROUP BY region
HAVING SUM(amount) FILTER (WHERE status = 'shipped')
     / NULLIF(SUM(amount), 0) > 0.9;
```

### Multiple Conditions

```sql
SELECT warehouse_id,
       COUNT(*)         AS order_count,
       SUM(units)       AS total_units
FROM shipments
WHERE ship_date >= '2024-01-01'
GROUP BY warehouse_id
HAVING COUNT(*)   > 50
   AND SUM(units) > 1000;
```

### Named Aggregate in HAVING (Databricks / Spark 3.4+)

```sql
-- Alias defined in SELECT can be referenced in HAVING in Databricks
SELECT
    region,
    SUM(amount) AS total_revenue
FROM orders
GROUP BY region
HAVING total_revenue > 100000;
```

---

## :material-brain: When to Use

| Scenario | Recommended Pattern |
|----------|---------------------|
| Remove groups with small counts | `HAVING COUNT(*) > n` |
| Keep only high-value segments | `HAVING SUM(amount) > threshold` |
| Ratio / proportion filter | `HAVING shipped / NULLIF(total, 0) > 0.9` |
| Dynamic threshold from another table | HAVING with scalar subquery |
| Row-level filtering | Use `WHERE` instead — it runs earlier and is cheaper |

---

## :material-magnify: Behavior Notes

1. `HAVING` is evaluated **after** `GROUP BY` — it cannot reference non-aggregated columns that are not in `GROUP BY`.
2. Prefer `WHERE` for row-level predicates so fewer rows enter the aggregation step.
3. `NULLIF(denominator, 0)` prevents division-by-zero in ratio conditions.
4. In Databricks Runtime, SELECT aliases are visible in `HAVING` — in standard SQL they are not.
5. `HAVING` without `GROUP BY` treats the entire table as a single group.

---

## :material-lightbulb-outline: Quick Anti-Pattern Check

```sql
-- Wrong: filtering a non-aggregate in HAVING instead of WHERE
SELECT product, SUM(amount) FROM sales
GROUP BY product
HAVING product LIKE 'Electronics%';  -- works but wasteful

-- Correct: push non-aggregate filters to WHERE
SELECT product, SUM(amount) FROM sales
WHERE product LIKE 'Electronics%'
GROUP BY product;
```
