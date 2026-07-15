# :material-flask-outline: View Examples

Real-world patterns for building views in Spark SQL and Databricks.

---

## :material-layers: Pattern 1 — Layered (Medallion) Views

Build views at each layer of a medallion architecture so downstream consumers
never query raw tables directly.

```sql
-- Bronze: raw ingestion view (schema enforcement)
CREATE OR REPLACE VIEW bronze.v_raw_orders AS
SELECT
    order_id,
    customer_id,
    CAST(amount  AS DECIMAL(18, 2)) AS amount,
    CAST(order_date AS DATE)        AS order_date,
    region,
    _ingest_ts
FROM bronze.raw_orders_landing;

-- Silver: cleaned and enriched view
CREATE OR REPLACE VIEW silver.v_orders AS
SELECT
    o.order_id,
    o.customer_id,
    c.name                          AS customer_name,
    c.tier,
    LOWER(TRIM(o.region))           AS region,
    o.amount,
    o.order_date,
    DATE_TRUNC('month', o.order_date) AS order_month
FROM bronze.v_raw_orders o
JOIN silver.customers c ON o.customer_id = c.customer_id
WHERE o.order_id IS NOT NULL;

-- Gold: business-level aggregation view
CREATE OR REPLACE VIEW gold.v_monthly_revenue AS
SELECT
    order_month,
    region,
    tier,
    COUNT(*)        AS order_count,
    SUM(amount)     AS total_revenue,
    AVG(amount)     AS avg_order_value
FROM silver.v_orders
GROUP BY order_month, region, tier;
```

---

## :material-shield-lock: Pattern 2 — Security / Row-Level View

Row-level filtering without Unity Catalog row filters — works on any metastore.

```sql
-- Each sales rep sees only their own region's orders
CREATE OR REPLACE VIEW sales_portal.v_my_orders AS
SELECT
    order_id,
    customer_id,
    amount,
    order_date,
    region
FROM sales.orders
WHERE region = (
    SELECT region FROM sales.reps WHERE email = current_user()
);
```

---

## :material-eye-off: Pattern 3 — Column Masking View

Hide PII for users without admin privileges.

```sql
CREATE OR REPLACE VIEW analytics.v_customers_safe AS
SELECT
    customer_id,
    tier,
    region,
    -- Mask email unless the querying user is an admin
    CASE
        WHEN is_member('admin_group') THEN email
        ELSE CONCAT(LEFT(email, 2), '****@****.com')
    END AS email,
    -- Mask phone to last 4 digits
    CASE
        WHEN is_member('admin_group') THEN phone
        ELSE CONCAT('***-***-', RIGHT(phone, 4))
    END AS phone
FROM analytics.customers;

GRANT SELECT ON VIEW analytics.v_customers_safe TO `role:analysts`;
```

---

## :material-chart-bar: Pattern 4 — Running Totals and Window Views

Encapsulate window logic in a view so reports are always consistent.

```sql
CREATE OR REPLACE VIEW reporting.v_cumulative_sales AS
SELECT
    order_date,
    region,
    daily_revenue,
    SUM(daily_revenue) OVER (
        PARTITION BY region
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_revenue,
    ROUND(
        100.0 * daily_revenue /
        SUM(daily_revenue) OVER (PARTITION BY region),
        2
    ) AS pct_of_region_total
FROM (
    SELECT
        order_date,
        region,
        SUM(amount) AS daily_revenue
    FROM sales.orders
    GROUP BY order_date, region
);
```

---

## :material-cached: Pattern 5 — Materialized View for Dashboard KPIs

Heavy aggregation computed once, queried thousands of times.

```sql
CREATE OR REPLACE MATERIALIZED VIEW main.reporting.mv_exec_dashboard
COMMENT 'Executive KPIs — refreshed daily at 01:00 UTC'
SCHEDULE REFRESH CRON '0 1 * * *' AT TIME ZONE 'UTC'
AS
SELECT
    DATE_TRUNC('month', order_date)   AS month,
    region,
    COUNT(DISTINCT customer_id)       AS unique_buyers,
    COUNT(*)                          AS orders,
    SUM(amount)                       AS gmv,
    AVG(amount)                       AS aov,
    SUM(amount) / COUNT(DISTINCT customer_id) AS revenue_per_buyer
FROM main.sales.orders
WHERE order_date >= '2022-01-01'
GROUP BY 1, 2;

-- Fast dashboard query — hits Delta, no re-aggregation
SELECT month, region, gmv, unique_buyers
FROM main.reporting.mv_exec_dashboard
WHERE month >= '2024-01-01'
ORDER BY month DESC, gmv DESC;
```

---

## :material-pipe: Pattern 6 — Reusable CTE-Style View

Replace long repeated CTEs with a permanent view to keep reports DRY.

```sql
-- Define the "spine" once
CREATE OR REPLACE VIEW analytics.v_date_spine AS
SELECT explode(sequence(
    DATE '2020-01-01',
    CURRENT_DATE,
    INTERVAL 1 DAY
)) AS calendar_date;

-- Use it in any query
SELECT
    d.calendar_date,
    COALESCE(SUM(o.amount), 0) AS revenue
FROM analytics.v_date_spine d
LEFT JOIN sales.orders o ON o.order_date = d.calendar_date
GROUP BY d.calendar_date
ORDER BY d.calendar_date;
```

---

## :material-swap-horizontal: Pattern 7 — Backward-Compatible Rename View

Rename a table without breaking existing queries by keeping the old name as a view.

```sql
-- Old table: sales.order_facts  →  New table: sales.orders
-- Create compatibility view so legacy queries still work
CREATE OR REPLACE VIEW sales.order_facts AS
SELECT * FROM sales.orders;
```

---

## :material-refresh: Pattern 8 — Temp View Pipeline

Chain temp views as pipeline stages within a single job — no data written to disk.

```sql
-- Stage 1: load and filter
CREATE OR REPLACE TEMP VIEW stage_raw AS
SELECT * FROM events WHERE event_date = CURRENT_DATE;

-- Stage 2: parse nested JSON
CREATE OR REPLACE TEMP VIEW stage_parsed AS
SELECT
    event_id,
    event_type,
    get_json_object(payload, '$.user_id')   AS user_id,
    get_json_object(payload, '$.session_id') AS session_id
FROM stage_raw;

-- Stage 3: sessionise
CREATE OR REPLACE TEMP VIEW stage_sessions AS
SELECT
    user_id,
    session_id,
    MIN(event_id) AS first_event,
    COUNT(*)      AS event_count
FROM stage_parsed
GROUP BY user_id, session_id;

-- Final output
SELECT * FROM stage_sessions WHERE event_count > 1;
```
