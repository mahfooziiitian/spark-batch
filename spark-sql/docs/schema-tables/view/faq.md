# :material-frequently-asked-questions: FAQ

Common errors and troubleshooting for Spark SQL views.

---

## :material-alert: 1. "Table or view not found"

**Symptom:** `AnalysisException: Table or view not found: my_view`

**Causes and fixes:**

| Cause | Fix |
|-------|-----|
| Temp view used in different session | Recreate in current session, or promote to global temp / permanent |
| Global temp view queried without prefix | Use `SELECT * FROM global_temp.my_view` |
| Permanent view in different database | Run `USE my_database;` first, or qualify: `my_database.my_view` |
| Cluster restarted — temp view lost | Temp views are in-memory; recreate via init script or job setup step |

```sql
-- Always qualify permanent views
SELECT * FROM analytics.active_customers;

-- Always use global_temp. prefix
SELECT * FROM global_temp.shared_products;
```

---

## :material-table-column: 2. View fails after schema change

**Symptom:** `AnalysisException: cannot resolve 'old_col' given input columns`

Views use **late binding** — the query is re-parsed at query time.
If a base column was renamed or dropped, the view breaks.

```sql
-- Fix: redefine the view with the updated column name
ALTER VIEW analytics.customer_summary AS
SELECT customer_id, full_name AS name, region  -- updated column reference
FROM analytics.customers
WHERE status = 'active';

-- Verify
SHOW CREATE TABLE analytics.customer_summary;
```

!!! tip "Defensive alias pattern"
    Always alias columns in views instead of relying on base column names.
    This makes future renames less breaking.

---

## :material-lock: 3. Permission denied on view

**Symptom:** `PERMISSION_DENIED: User does not have SELECT privilege on VIEW`

In Unity Catalog, querying a view requires `SELECT` on the **view** only.
The underlying table privilege is **not** required when the view owner has access.

```sql
-- Grant on the view (not on the base table)
GRANT SELECT ON VIEW main.reporting.daily_revenue TO `analyst@company.com`;

-- Verify grants
SHOW GRANTS ON VIEW main.reporting.daily_revenue;
```

!!! warning "Hive Metastore difference"
    On a Hive Metastore (non-Unity Catalog), users need `SELECT` on both
    the view **and** all underlying tables.

---

## :material-speedometer-slow: 4. View query is slow

**Symptom:** Querying a view takes as long as the full base table query.

Views re-execute their underlying query every time. They are not cached.

```sql
-- Option A: cache the view for the session
CACHE TABLE analytics.customer_summary;

-- Option B: promote to a Materialized View (Databricks SQL / Unity Catalog)
CREATE OR REPLACE MATERIALIZED VIEW main.reporting.mv_customer_summary
SCHEDULE REFRESH CRON '0 2 * * *' AT TIME ZONE 'UTC'
AS
SELECT customer_id, region, SUM(amount) AS ltv
FROM main.sales.orders
GROUP BY customer_id, region;

-- Option C: materialise to Delta table for full control
CREATE OR REPLACE TABLE analytics.customer_summary_snapshot
USING DELTA AS
SELECT * FROM analytics.customer_summary;
```

---

## :material-refresh-auto: 5. Materialized View data is stale

**Symptom:** `SELECT` returns old data; new source rows not reflected.

Materialized Views do not auto-refresh unless a schedule is set.

```sql
-- Manual refresh
REFRESH MATERIALIZED VIEW main.reporting.mv_daily_sales;

-- Check last refresh time
DESCRIBE TABLE EXTENDED main.reporting.mv_daily_sales;
-- Look for: "Last Modified Time"

-- Add a schedule if not already set
ALTER MATERIALIZED VIEW main.reporting.mv_daily_sales
  SCHEDULE REFRESH CRON '0 3 * * *' AT TIME ZONE 'UTC';
```

---

## :material-eye-off: 6. `SELECT *` from view returns unexpected columns

**Symptom:** Extra or missing columns when using `SELECT *`.

Views use late binding. If the base table schema changed (columns added),
`SELECT *` in the view expands at query time to the **current** base schema.

```sql
-- Explicit column list prevents surprises
CREATE OR REPLACE VIEW analytics.safe_orders AS
SELECT order_id, customer_id, amount, region, order_date  -- explicit list
FROM orders;

-- Confirm schema
DESCRIBE analytics.safe_orders;
```

---

## :material-tools: 7. OPTIMIZE / VACUUM fails on a view

**Symptom:** `AnalysisException: OPTIMIZE does not support view`

`OPTIMIZE`, `VACUUM`, `Z-ORDER`, and other Delta commands operate on **physical tables**,
not views.

```sql
-- Run commands on the base table, not the view
OPTIMIZE main.sales.orders ZORDER BY (region, order_date);
VACUUM main.sales.orders RETAIN 168 HOURS;

-- For a Materialized View: the underlying Delta table is at:
-- system.information_schema.materialized_views → storage_path
OPTIMIZE delta.`/path/to/mv_storage`;
```

---

## :material-sync-alert: 8. Global temp view disappeared

**Symptom:** `Table or view not found: global_temp.my_view` after cluster restart.

Global temp views are scoped to the **Spark application**. When a cluster restarts,
the `SparkContext` is new and all global temp views are gone.

```sql
-- For cross-cluster sharing, use a permanent view instead
CREATE OR REPLACE VIEW analytics.shared_products AS
SELECT product_id, name, category FROM products WHERE is_published = true;
```
