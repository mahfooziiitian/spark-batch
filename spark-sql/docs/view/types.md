# View Types

## Session Temporary view

1. Exist only in the current Spark session (or notebook).
2. Useful for intermediate transformations without persisting data.
3. Disappear when the session ends.

```sql
-- Create temporary view from a query
CREATE OR REPLACE TEMP VIEW temp_orders AS
SELECT order_id, customer_id, total_amount
FROM orders
WHERE status = 'shipped';

-- Query it like a table
SELECT * FROM temp_orders;
```

## Global Temporary Views

1. Persist for the lifetime of the `Spark application` (shared across notebooks & clusters if connected to same app).
2. Stored in the special `global_temp` database.
3. Require fully qualified naming: `global_temp.view_name`.

```sql
CREATE OR REPLACE GLOBAL TEMP VIEW global_orders AS
SELECT order_id, total_amount
FROM orders;

SELECT * FROM global_temp.global_orders;
```

## Permanent Views

1. Saved in the `Databricks metastore` (Unity Catalog or Hive metastore).
2. Persist until explicitly dropped.
3. Behave like database objects — visible to users with proper permissions.

```sql
-- Create a permanent view in a database
CREATE OR REPLACE VIEW sales_db.shipped_orders AS
SELECT order_id, customer_id, total_amount
FROM sales_db.orders
WHERE status = 'shipped';

SELECT * FROM sales_db.shipped_orders;
```

## Materialized View

In Databricks, a Materialized View (MV) is like a view + cached results — the query is precomputed and stored physically so that reading from it is much faster than re-running the underlying SQL every time.

Runs the query once (on creation or refresh) and stores the results in the metastore as a Delta table under the hood.

### When to Use a Materialized View

If you need:

1. Faster performance
2. Automatic refresh
3. Precomputed aggregations

### Syntax

```sql
-- Create materialized view
CREATE MATERIALIZED VIEW my_catalog.my_schema.mv_sales
AS
SELECT region, SUM(total) AS total_sales
FROM my_catalog.my_schema.orders
GROUP BY region;

-- Query it like a table
SELECT * FROM my_catalog.my_schema.mv_sales;

-- Refresh it manually
REFRESH MATERIALIZED VIEW my_catalog.my_schema.mv_sales;

-- Drop it
DROP MATERIALIZED VIEW my_catalog.my_schema.mv_sales;
```

### View Metadata in Databricks

You can inspect views in system tables or via:

```sql
DESCRIBE VIEW EXTENDED my_view;
SHOW CREATE TABLE my_view;
```

### Refresh Behavior

Automatic refresh can be configured (with Databricks SQL Warehouse or a scheduled job).

Refresh rewrites the data, replacing it with the latest results.

### Advantages

1. Performance: Queries on the MV are just reading a Delta table — much faster.
2. Lower Compute Costs: Avoids re-running heavy aggregation/joins.
3. Consistency: All users see the same precomputed snapshot.

### Limitations

1. No DML: Can't INSERT/UPDATE into MV directly — update the source table and refresh.
2. Storage Cost: You pay for storing MV results separately from the source table.
3. Schema Lock: If the MV references columns that are dropped/renamed in source tables, it becomes invalid.
4. Not for Streaming: Materialized views are not designed for continuous streaming ingestion.
