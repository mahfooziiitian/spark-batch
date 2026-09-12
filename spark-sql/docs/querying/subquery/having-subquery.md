# :material-filter-plus-outline: Subquery in HAVING

A subquery in `HAVING` lets you compare each aggregated group with another computed threshold. In Spark 4.2 it follows scalar-subquery rules: one column, and at execution time no more than one row.

<script src="../../assets/js/querying-subquery-viz.js"></script>

### :material-animation-play: Interactive Visualization — Group Total vs Dynamic Threshold

<div id="viz-subquery-having-threshold" class="ts-viz"></div>

The chart shows how `HAVING` first forms groups, then compares each group aggregate with a threshold coming from another query.

______________________________________________________________________

## :material-code-tags: Basic Form

```sql
SELECT customer, SUM(amount) AS total_spent
FROM orders
GROUP BY customer
HAVING SUM(amount) > (SELECT AVG(amount) FROM orders);
```

______________________________________________________________________

## :material-information-outline: Verified Spark 4.2 Behavior

1. An uncorrelated `HAVING` subquery is evaluated once and used as a constant threshold.
2. A correlated `HAVING` subquery can reference the outer grouped key and may be decorrelated into a join.
3. The comparison happens **after** aggregation, so `HAVING` filters groups, not rows.
4. Nested aggregates such as `MAX(SUM(amount))` in the same subquery level are invalid in Spark 4.2; you must add another subquery layer.

______________________________________________________________________

## :material-flask-outline: Verified Examples

### Uncorrelated threshold

```sql
SELECT customer, SUM(amount) AS total_spent
FROM orders
GROUP BY customer
HAVING SUM(amount) > (SELECT AVG(amount) FROM orders)
ORDER BY total_spent DESC;
```

Verified result in PySpark 4.2: `Alice` and `Bob`.

### Correlated group comparison

```sql
SELECT region, SUM(amount) AS current_revenue
FROM orders cur
WHERE order_date BETWEEN DATE'2024-04-01' AND DATE'2024-06-30'
GROUP BY region
HAVING SUM(amount) > (
    SELECT SUM(amount)
    FROM orders prev
    WHERE prev.region = cur.region
      AND prev.order_date BETWEEN DATE'2024-01-01' AND DATE'2024-03-31'
);
```

This pattern executed successfully in Spark 4.2.

### Invalid nested aggregate, and the fix

This fails:

```sql
SELECT region, SUM(amount) AS this_year_revenue
FROM orders
GROUP BY region
HAVING SUM(amount) > (
    SELECT MAX(SUM(amount))
    FROM orders
    GROUP BY region
);
```

Spark 4.2 raises `NESTED_AGGREGATE_FUNCTION`.

Rewrite it with another subquery layer:

```sql
SELECT region, SUM(amount) AS this_year_revenue
FROM orders
GROUP BY region
HAVING SUM(amount) > (
    SELECT MAX(region_total)
    FROM (
        SELECT region, SUM(amount) AS region_total
        FROM orders
        GROUP BY region
    ) t
);
```

______________________________________________________________________

## :material-lightbulb-outline: When to Use

| Scenario                                   | Recommendation               |
| ------------------------------------------ | ---------------------------- |
| Compare each group to one global threshold | Great fit                    |
| Compare each group to a prior-period value | Correlated `HAVING`          |
| Threshold query is itself multi-step       | Use a nested subquery or CTE |
| Need only row-level filtering              | Use `WHERE`, not `HAVING`    |
