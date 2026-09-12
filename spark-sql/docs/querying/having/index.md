# :material-filter-variant: HAVING Clause

`HAVING` filters grouped results after aggregation. In PySpark 4.2, it can reference aggregate expressions, aggregate aliases, grouped-expression aliases, and scalar subqueries.

______________________________________________________________________

### :material-animation-play: Interactive Visualization — HAVING Pipeline Explorer

<div id="viz-having-overview" class="ts-viz"></div>

Move the predicate between `WHERE` and `HAVING` to see whether Spark removes rows before aggregation or removes groups after aggregation. The counts match the logical pipeline Spark 4.2 executes.

<script src="../../assets/js/querying-having-viz.js"></script>

______________________________________________________________________

## :material-view-grid: In This Section

| Page                                  | What You Will Learn                                                         |
| ------------------------------------- | --------------------------------------------------------------------------- |
| [WHERE vs HAVING](having_vs_where.md) | Execution order, plan shape, and performance implications                   |
| [FILTER Modifier](having_filter.md)   | Conditional aggregation with `FILTER (WHERE ...)` and how it differs        |
| [Advanced HAVING](having_advanced.md) | `ROLLUP`, `CUBE`, `GROUPING SETS`, `QUALIFY`, aliases, and single-group use |
| [HAVING Patterns](having_patterns.md) | Thresholds, ratios, top-N groups, and scalar-subquery filters               |

!!! note "Spark 4.2 behaviors verified"

    The examples and behavior notes on this page were checked against PySpark 4.2 execution rather than copied from prose assumptions.

______________________________________________________________________

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

`WHERE` reduces rows before Spark builds groups. `HAVING` evaluates after the grouped aggregates exist, so it can filter on `SUM`, `COUNT`, `AVG`, and similar expressions.

______________________________________________________________________

## :material-code-tags: Syntax

```sql
SELECT
    group_cols,
    aggregate_exprs
FROM table
[WHERE row_predicate]
[GROUP BY group_cols]
[HAVING aggregate_predicate]
[ORDER BY ...]
[LIMIT n];
```

______________________________________________________________________

## :material-table: WHERE vs HAVING vs FILTER

| Clause   | Applies To         | Timing             | Example                                       |
| -------- | ------------------ | ------------------ | --------------------------------------------- |
| `WHERE`  | Individual rows    | Before aggregation | `WHERE status = 'shipped'`                    |
| `HAVING` | Aggregated groups  | After aggregation  | `HAVING SUM(amount) > 1000`                   |
| `FILTER` | A single aggregate | During aggregation | `SUM(amount) FILTER (WHERE status='shipped')` |

______________________________________________________________________

## :material-flask-outline: Practical Examples

### Filtering groups by an aggregate

```sql
SELECT
    region,
    COUNT(*) AS total_orders
FROM orders
GROUP BY region
HAVING COUNT(*) > 100;
```

### Combining `WHERE` and `HAVING`

```sql
SELECT
    product,
    AVG(price) AS avg_price
FROM sales
WHERE sale_date >= '2024-01-01'
GROUP BY product
HAVING AVG(price) > 100;
```

### Aggregate used in `HAVING` but omitted from `SELECT`

```sql
SELECT
    region
FROM orders
GROUP BY region
HAVING SUM(amount) > 5000;
```

Spark 4.2 accepts this pattern: the aggregate can appear only in `HAVING`.

### Scalar subquery inside `HAVING`

```sql
SELECT
    region,
    SUM(amount) AS total_revenue
FROM orders
GROUP BY region
HAVING SUM(amount) > (
    SELECT AVG(region_total)
    FROM (
        SELECT region, SUM(amount) AS region_total
        FROM orders
        GROUP BY region
    ) t
);
```

!!! note "More on subqueries"

    For dedicated subquery examples, see [Subquery in HAVING](../subquery/having-subquery.md).

______________________________________________________________________

## :material-magnify: Behavior Notes

1. In Spark 4.2, `HAVING` is implemented as a filter above the aggregate in the optimized plan.
2. Spark 4.2 allows `HAVING` to reference an aggregate that is not projected in the `SELECT` list.
3. Spark 4.2 also resolves `SELECT` aliases in `HAVING`, including grouped-expression aliases such as `SELECT upper(region) AS region_key ... HAVING region_key = 'APAC'`.
4. `HAVING` cannot reference a raw column that is neither grouped nor aggregated.
5. Without `GROUP BY`, Spark treats the input as a single group only when the projection is aggregate-safe; aggregate-only output works, but raw columns do not.

______________________________________________________________________

## :material-lightbulb-outline: Quick Anti-Pattern Check

```sql
-- Wasteful: this predicate belongs in WHERE
SELECT
    product,
    SUM(amount) AS total_amount
FROM sales
GROUP BY product
HAVING product LIKE 'Electronics%';

-- Better: filter rows before the aggregate
SELECT
    product,
    SUM(amount) AS total_amount
FROM sales
WHERE product LIKE 'Electronics%'
GROUP BY product;
```
