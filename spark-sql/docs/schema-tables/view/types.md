# :material-eye-settings: View Types

---

## :material-clock-fast: 1. Temporary View

Exists only for the current Spark session. Automatically dropped when the session ends.
Ideal for intermediate transformations in a single notebook or job.

```sql
-- Create (or replace if already exists)
CREATE OR REPLACE TEMP VIEW cleaned_orders AS
SELECT
    order_id,
    LOWER(TRIM(region))  AS region,
    CAST(amount AS DECIMAL(18, 2)) AS amount,
    order_date
FROM raw_orders
WHERE order_id IS NOT NULL;

-- Use in subsequent queries
SELECT region, SUM(amount) AS total
FROM cleaned_orders
GROUP BY region;

-- Drop explicitly (optional — session end drops it automatically)
DROP VIEW IF EXISTS cleaned_orders;
```

!!! tip "Caching a temp view"
    Pair with `CACHE TABLE cleaned_orders` to avoid recomputing the same
    transformation multiple times in the same job.

---

## :material-earth: 2. Global Temporary View

Scoped to the **Spark application** (not just one session). All sessions within
the same `SparkContext` can read it. Stored under the special `global_temp` database.

```sql
-- Create — requires GLOBAL TEMP keyword
CREATE OR REPLACE GLOBAL TEMP VIEW global_products AS
SELECT product_id, name, category, is_published
FROM products
WHERE is_published = true;

-- Always query with global_temp. prefix
SELECT * FROM global_temp.global_products WHERE category = 'electronics';

-- Drop
DROP VIEW IF EXISTS global_temp.global_products;
```

!!! warning "Not cross-cluster"
    Global temp views are NOT shared across different Databricks clusters or
    different Spark applications. Use a permanent view or Delta table for that.

---

## :material-database: 3. Permanent View

Persisted in the metastore (Hive Metastore or Unity Catalog). Visible to all users
with the appropriate privilege. Does not store data.

```sql
-- Hive Metastore / classic catalog
CREATE OR REPLACE VIEW analytics.active_customers AS
SELECT customer_id, name, region, tier
FROM analytics.customers
WHERE status = 'active';

-- Unity Catalog (three-level namespace)
CREATE OR REPLACE VIEW main.sales.premium_customers AS
SELECT customer_id, name, region
FROM main.sales.customers
WHERE tier = 'platinum' AND status = 'active';

-- Describe
DESCRIBE TABLE EXTENDED main.sales.premium_customers;
SHOW CREATE TABLE main.sales.premium_customers;

-- Alter the defining query
ALTER VIEW main.sales.premium_customers AS
SELECT customer_id, name, region, loyalty_score
FROM main.sales.customers
WHERE tier IN ('gold', 'platinum') AND status = 'active';

-- Drop
DROP VIEW IF EXISTS main.sales.premium_customers;
```

### Granting access (Unity Catalog)

```sql
-- Grant SELECT on the view; users do NOT need direct table access
GRANT SELECT ON VIEW main.sales.premium_customers TO `analyst@company.com`;
GRANT SELECT ON VIEW main.sales.premium_customers TO `role:data_analysts`;
```

---

## :material-cached: 4. Materialized View

A Materialized View (MV) precomputes and physically stores the query result as a
Delta table. Reads are fast because no re-computation is needed. Available in
**Databricks SQL** and Unity Catalog environments.

```sql
-- Create (Unity Catalog required)
CREATE MATERIALIZED VIEW main.reporting.mv_daily_sales AS
SELECT
    order_date,
    region,
    SUM(amount)  AS total_sales,
    COUNT(*)     AS order_count
FROM main.sales.orders
GROUP BY order_date, region;

-- Query — reads pre-computed Delta data, very fast
SELECT region, SUM(total_sales)
FROM main.reporting.mv_daily_sales
WHERE order_date >= '2024-01-01'
GROUP BY region;

-- Manual refresh (re-runs the query and overwrites the stored result)
REFRESH MATERIALIZED VIEW main.reporting.mv_daily_sales;

-- Describe
DESCRIBE TABLE EXTENDED main.reporting.mv_daily_sales;

-- Drop
DROP MATERIALIZED VIEW IF EXISTS main.reporting.mv_daily_sales;
```

### Refresh Modes

| Mode | How | When |
|------|-----|------|
| Manual | `REFRESH MATERIALIZED VIEW mv` | On demand |
| Scheduled | Databricks SQL serverless pipeline | Cron or trigger |
| Continuous (DLT) | Delta Live Tables pipeline | Near-real-time |

### Materialized View Limitations

| Limitation | Detail |
|------------|--------|
| No DML | Cannot `INSERT`/`UPDATE`/`DELETE` — refresh only |
| Schema lock | Dropping a source column invalidates the MV |
| Storage cost | Result stored as Delta table — billed separately |
| Not for streaming | Use Delta Live Tables for continuous ingestion |
| No OPTIMIZE hint | Run `OPTIMIZE` on the underlying Delta table if needed |

---

## :material-compare: All Types Side by Side

| Aspect | Temp | Global Temp | Permanent | Materialized |
|--------|:----:|:-----------:|:---------:|:------------:|
| Session lifetime | Yes | App lifetime | Forever | Forever |
| Stored in catalog | No | No (global_temp) | Yes | Yes |
| Data stored | No | No | No | Yes (Delta) |
| Cross-session | No | Yes (same app) | Yes | Yes |
| Cross-cluster | No | No | Yes | Yes |
| Fast repeated reads | No | No | No | Yes |
| Supports GRANT | No | No | Yes | Yes |
| Refresh needed | — | — | — | Yes |
