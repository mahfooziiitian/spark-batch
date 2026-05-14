# :material-lightbulb-on: HAVING Patterns

Reusable HAVING patterns for common analytical problems.

---

## :material-format-list-numbered: Top-N Groups

Return only the top N groups by an aggregate measure.

```sql
-- Top 5 customers by lifetime spend
SELECT
    customer_id,
    SUM(amount) AS lifetime_spend
FROM orders
GROUP BY customer_id
HAVING SUM(amount) > 0          -- exclude zero-spend groups
ORDER BY lifetime_spend DESC
LIMIT 5;
```

```sql
-- Top 10 products by order count, excluding low-volume products
SELECT
    product_id,
    COUNT(*)        AS order_count,
    SUM(amount)     AS total_revenue
FROM order_lines
GROUP BY product_id
HAVING COUNT(*) >= 50
ORDER BY order_count DESC
LIMIT 10;
```

---

## :material-percent: Ratio / Proportion Filters

Keep only groups where the ratio between two aggregates meets a threshold.

```sql
-- Regions where the return rate exceeds 5 %
SELECT
    region,
    COUNT(*)                                             AS total_orders,
    COUNT(*) FILTER (WHERE status = 'returned')          AS returned_orders,
    COUNT(*) FILTER (WHERE status = 'returned')
        / NULLIF(COUNT(*), 0)                            AS return_rate
FROM orders
GROUP BY region
HAVING COUNT(*) FILTER (WHERE status = 'returned')
     / NULLIF(COUNT(*), 0) > 0.05;

-- Products with a margin below 20 %
SELECT
    product_id,
    SUM(revenue - cost)     AS total_margin,
    SUM(revenue)            AS total_revenue
FROM sales
GROUP BY product_id
HAVING SUM(revenue - cost) / NULLIF(SUM(revenue), 0) < 0.2;
```

---

## :material-database-cog-outline: Config-Driven Thresholds

Drive the HAVING threshold from a config table rather than a hardcoded literal.

```sql
-- Threshold stored in a config table
SELECT
    warehouse_id,
    SUM(units_shipped)  AS total_units
FROM shipments
WHERE ship_date >= '2024-01-01'
GROUP BY warehouse_id
HAVING SUM(units_shipped) > (
    SELECT CAST(config_value AS INT)
    FROM app_config
    WHERE config_key = 'high_volume_warehouse_threshold'
);
```

```sql
-- Multiple thresholds via a lookup
WITH thresholds AS (
    SELECT
        region,
        revenue_target
    FROM region_targets
    WHERE target_year = 2024
)
SELECT
    o.region,
    SUM(o.amount)   AS total_revenue
FROM orders o
JOIN thresholds t ON o.region = t.region
GROUP BY o.region
HAVING SUM(o.amount) >= MAX(t.revenue_target);  -- per-region target
```

---

## :material-account-group: Segment Identification

Label or filter customer segments based on aggregate behaviour.

```sql
-- Flag customers by spend tier
WITH customer_totals AS (
    SELECT
        customer_id,
        SUM(amount) AS total_spent
    FROM orders
    GROUP BY customer_id
    HAVING SUM(amount) > 0   -- exclude zero-spend
)
SELECT
    customer_id,
    total_spent,
    CASE
        WHEN total_spent >= 10000 THEN 'Platinum'
        WHEN total_spent >= 5000  THEN 'Gold'
        WHEN total_spent >= 1000  THEN 'Silver'
        ELSE                           'Bronze'
    END AS tier
FROM customer_totals
ORDER BY total_spent DESC;
```

---

## :material-calendar-range: Period Comparison

Keep groups that improved (or declined) between two periods.

```sql
-- Customers whose spend grew from 2023 to 2024
SELECT
    customer_id,
    SUM(amount) FILTER (WHERE YEAR(order_date) = 2023)  AS spend_2023,
    SUM(amount) FILTER (WHERE YEAR(order_date) = 2024)  AS spend_2024
FROM orders
GROUP BY customer_id
HAVING SUM(amount) FILTER (WHERE YEAR(order_date) = 2024)
     > SUM(amount) FILTER (WHERE YEAR(order_date) = 2023);
```

---

## :material-alert-circle: Outlier Detection

Surface groups that deviate significantly from the mean.

```sql
-- Regions with revenue more than 2 standard deviations above the mean
WITH region_revenue AS (
    SELECT region, SUM(amount) AS revenue
    FROM orders
    GROUP BY region
),
stats AS (
    SELECT AVG(revenue) AS avg_rev, STDDEV(revenue) AS std_rev
    FROM region_revenue
)
SELECT r.region, r.revenue
FROM region_revenue r
CROSS JOIN stats s
WHERE r.revenue > s.avg_rev + 2 * s.std_rev;
```

```sql
-- Warehouses with an unusually high error rate (HAVING version)
SELECT
    warehouse_id,
    COUNT(*)                                              AS total_picks,
    COUNT(*) FILTER (WHERE pick_error = true)             AS error_picks
FROM warehouse_picks
GROUP BY warehouse_id
HAVING COUNT(*) FILTER (WHERE pick_error = true)
     / NULLIF(COUNT(*), 0) > (
           SELECT AVG(err_rate) + 2 * STDDEV(err_rate)
           FROM (
               SELECT COUNT(*) FILTER (WHERE pick_error = true)
                    / NULLIF(COUNT(*), 0) AS err_rate
               FROM warehouse_picks
               GROUP BY warehouse_id
           ) AS wh_rates
       );
```

---

## :material-swap-horizontal: HAVING vs CTE for Complex Thresholds

```sql
-- HAVING with nested subquery: works but hard to read
SELECT region, SUM(amount) AS total
FROM orders
GROUP BY region
HAVING SUM(amount) > (
    SELECT AVG(region_total)
    FROM (SELECT region, SUM(amount) AS region_total FROM orders GROUP BY region) t
);

-- CTE equivalent: easier to read, test, and extend
WITH region_totals AS (
    SELECT region, SUM(amount) AS total
    FROM orders
    GROUP BY region
),
global_avg AS (
    SELECT AVG(total) AS avg_total FROM region_totals
)
SELECT rt.region, rt.total
FROM region_totals rt
CROSS JOIN global_avg ga
WHERE rt.total > ga.avg_total;
```

!!! tip
    For simple scalar thresholds, a `HAVING` subquery is fine.
    For complex thresholds (median, percentile, multi-step), prefer a CTE — it keeps the logic readable and testable.
