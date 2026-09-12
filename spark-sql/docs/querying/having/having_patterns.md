# :material-lightbulb-on: HAVING Patterns

These patterns focus on practical post-aggregation filtering: thresholds, scalar subqueries, top-N cutoffs, and group-level comparisons that are awkward to express earlier in the pipeline.

______________________________________________________________________

### :material-animation-play: Interactive Visualization — Threshold Pattern Explorer

<div id="viz-having-patterns" class="ts-viz"></div>

The slider acts like a configurable `HAVING` threshold. It shows which grouped totals survive a literal threshold and how that differs from a threshold computed from other grouped results.

<script src="../../assets/js/querying-having-viz.js"></script>

______________________________________________________________________

## :material-format-list-numbered: Top-N Groups

`LIMIT` does the final row trimming, but `HAVING` is still useful for removing trivial groups first.

```sql
SELECT
    customer_id,
    SUM(amount) AS lifetime_spend
FROM orders
GROUP BY customer_id
HAVING SUM(amount) > 0
ORDER BY lifetime_spend DESC
LIMIT 5;
```

```sql
SELECT
    product_id,
    COUNT(*) AS order_count,
    SUM(amount) AS total_revenue
FROM order_lines
GROUP BY product_id
HAVING COUNT(*) >= 50
ORDER BY order_count DESC
LIMIT 10;
```

______________________________________________________________________

## :material-database-cog-outline: Scalar-Subquery Thresholds

Spark 4.2 allows a scalar subquery inside `HAVING`.

```sql
SELECT
    region,
    SUM(amount) AS total_revenue
FROM orders
GROUP BY region
HAVING SUM(amount) > (
    SELECT AVG(region_total)
    FROM (
        SELECT
            region,
            SUM(amount) AS region_total
        FROM orders
        GROUP BY region
    ) t
);
```

This pattern is useful when the cutoff depends on other grouped results rather than a fixed literal.

### Config-driven threshold table

```sql
SELECT
    warehouse_id,
    SUM(units_shipped) AS total_units
FROM shipments
WHERE ship_date >= '2024-01-01'
GROUP BY warehouse_id
HAVING SUM(units_shipped) > (
    SELECT CAST(config_value AS INT)
    FROM app_config
    WHERE config_key = 'high_volume_warehouse_threshold'
);
```

______________________________________________________________________

## :material-percent: Ratio Filters

```sql
SELECT
    region,
    COUNT(*) AS total_orders,
    COUNT(*) FILTER (WHERE status = 'returned') AS returned_orders,
    COUNT(*) FILTER (WHERE status = 'returned')
        / NULLIF(COUNT(*), 0) AS return_rate
FROM orders
GROUP BY region
HAVING COUNT(*) FILTER (WHERE status = 'returned')
    / NULLIF(COUNT(*), 0) > 0.05;
```

`FILTER` builds the numerator, while `HAVING` decides whether the completed ratio is acceptable.

______________________________________________________________________

## :material-calendar-range: Period Comparison

```sql
SELECT
    customer_id,
    SUM(amount) FILTER (WHERE YEAR(order_date) = 2023) AS spend_2023,
    SUM(amount) FILTER (WHERE YEAR(order_date) = 2024) AS spend_2024
FROM orders
GROUP BY customer_id
HAVING SUM(amount) FILTER (WHERE YEAR(order_date) = 2024)
    > SUM(amount) FILTER (WHERE YEAR(order_date) = 2023);
```

This pattern keeps only the groups whose aggregate improved between periods.

______________________________________________________________________

## :material-account-group: Two-Step Thresholds with a CTE

Complex thresholds are often easier to read when you separate aggregation from final filtering.

```sql
WITH region_totals AS (
    SELECT
        region,
        SUM(amount) AS total
    FROM orders
    GROUP BY region
),
global_avg AS (
    SELECT AVG(total) AS avg_total
    FROM region_totals
)
SELECT
    rt.region,
    rt.total
FROM region_totals rt
CROSS JOIN global_avg ga
WHERE rt.total > ga.avg_total;
```

Use direct `HAVING` when the threshold logic is short. Switch to a CTE when the threshold itself needs its own query stages.

______________________________________________________________________

## :material-magnify: Pattern Notes

1. `HAVING` is ideal when the filter depends on aggregates from the current group.
2. Scalar subqueries in `HAVING` are supported in Spark 4.2, including nested grouped subqueries.
3. `ORDER BY ... LIMIT` and `HAVING` often work together: `HAVING` removes weak groups, then `ORDER BY` ranks the survivors.
4. For complex threshold derivation, a CTE plus outer `WHERE` is usually easier to debug than a deeply nested `HAVING`.
