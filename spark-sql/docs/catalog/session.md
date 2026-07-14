# :material-clock-time-four: Session Catalog

The **session catalog** is Spark's built-in in-memory catalog that holds
**temporary views** and **temporary functions**. Objects in the session catalog
exist only for the lifetime of the current Spark session.

---

## :material-compare: Session vs Global Temp vs Permanent Objects

| Object type | Scope | Persisted? | Namespace |
|-------------|-------|:----------:|-----------|
| Temp view | Current session | No | (none — no prefix) |
| Global temp view | All sessions on same cluster | No | `global_temp.view_name` |
| Permanent view | Any session | Yes | `db.view_name` |
| Temp function | Current session | No | (none) |
| Permanent function | Any session | Yes | `db.function_name` |

---

## :material-eye: Temporary Views

### Create / Replace

```sql
-- Session-scoped temp view
CREATE OR REPLACE TEMP VIEW recent_orders AS
SELECT *
FROM orders
WHERE order_date >= current_date() - INTERVAL 7 DAYS;

-- Query it
SELECT region, SUM(amount) AS total
FROM recent_orders
GROUP BY region;
```

### Drop

```sql
DROP VIEW IF EXISTS recent_orders;
```

### Multi-step Notebook Pipeline

```sql
-- Step 1: clean raw data
CREATE OR REPLACE TEMP VIEW clean_events AS
SELECT
    event_id,
    LOWER(TRIM(event_type)) AS event_type,
    CAST(ts AS TIMESTAMP)   AS event_ts
FROM raw_events
WHERE event_id IS NOT NULL;

-- Step 2: aggregate
CREATE OR REPLACE TEMP VIEW hourly_counts AS
SELECT
    DATE_TRUNC('hour', event_ts) AS hour,
    event_type,
    COUNT(*)                     AS cnt
FROM clean_events
GROUP BY 1, 2;

-- Step 3: final result
SELECT * FROM hourly_counts ORDER BY hour, cnt DESC;
```

---

## :material-earth: Global Temporary Views

Global temp views are shared across **all sessions on the same cluster** but
are still not persisted to disk. They live in the special `global_temp` database.

```sql
-- Create a global temp view (available to all sessions on this cluster)
CREATE OR REPLACE GLOBAL TEMP VIEW shared_lookup AS
SELECT code, description FROM reference_data;

-- Access from any session on the same cluster
SELECT * FROM global_temp.shared_lookup WHERE code = 'US';

-- Drop it
DROP VIEW IF EXISTS global_temp.shared_lookup;
```

!!! note "Scope of global_temp"
    Global temp views survive across sessions but disappear when the Spark
    application (driver) stops. They are not persisted to a metastore.

---

## :material-function-variant: Temporary Functions

Register a temporary SQL function for the current session:

```sql
-- Register a simple inline expression as a function
CREATE OR REPLACE TEMP FUNCTION cents_to_dollars(cents BIGINT)
RETURNS DOUBLE
RETURN cents / 100.0;

-- Use it
SELECT order_id, cents_to_dollars(amount_cents) AS amount_usd
FROM orders;

-- List session functions
SHOW USER FUNCTIONS;

-- Drop
DROP FUNCTION IF EXISTS cents_to_dollars;
```

---

## :material-console: Introspection Commands

```sql
-- List all temp views in the session
SHOW VIEWS IN global_temp;   -- global temp views
SHOW VIEWS;                   -- temp + permanent views in current DB

-- Check if a temp view exists
SHOW TABLES LIKE 'recent_orders';

-- Inspect the definition
DESCRIBE TABLE recent_orders;

-- See the underlying SQL
SHOW CREATE TABLE recent_orders;
```

---

## :material-magnify: Behavior Notes

1. **Shadowing** — a temp view with the same name as a permanent table takes priority in the current session.
2. **No metastore writes** — temp views never touch Hive Metastore or Unity Catalog.
3. **Not shared** — different notebooks/sessions on the same cluster each have their own temp view namespace (except `global_temp`).
4. **CACHE TABLE** — `CACHE TABLE view_name` materialises a temp view to memory for repeated queries.

```sql
-- Cache for repeated aggregation
CACHE TABLE recent_orders;
SELECT COUNT(*) FROM recent_orders;   -- served from memory
UNCACHE TABLE recent_orders;
```

---

## :material-brain: When to Use

| Scenario | Recommendation |
|----------|----------------|
| Intermediate ETL steps | `CREATE OR REPLACE TEMP VIEW` |
| Sharing a reference dataset across notebook cells | `GLOBAL TEMP VIEW` |
| Custom helper logic in SQL | `CREATE TEMP FUNCTION` |
| Production persistent objects | Use permanent tables/views instead |
| Repeated expensive query | `CACHE TABLE` the temp view |
