# :material-console: Session Management

`SET`, `RESET`, and `SET -v` are the SQL commands for inspecting and changing
session-level Spark configuration. Understanding their scope prevents unexpected
behaviour when settings bleed between queries.

---

## :material-code-tags: Syntax

```sql
-- Set a session-level config
SET key = value;

-- Read the current value of a key
SET key;

-- List ALL settings and their current values
SET -v;

-- Reset a key to its cluster/default value
RESET key;

-- Reset ALL session-level overrides
RESET;
```

---

## :material-information-outline: Behavior

1. `SET key = value` overrides the setting **for the current SparkSession only**. Other sessions are unaffected.
2. `RESET key` removes the session override — the setting reverts to the cluster-level or built-in default.
3. `RESET` (no argument) removes **all** session overrides at once.
4. `SET -v` returns every known Spark/Hadoop configuration key, its current value, and its meaning — output can be thousands of rows.
5. **Static settings** (e.g., `spark.executor.memory`, `spark.executor.cores`) cannot be changed with `SET` after the SparkContext starts — they must be set at cluster startup.
6. Custom user-defined properties (any `key = value` pair) can also be stored with `SET` and read back — they persist only for the session.

---

## :material-flask-outline: Practical Examples

### Read the current value of a setting

```sql
SET spark.sql.shuffle.partitions;
-- Result:
-- | key                              | value |
-- |----------------------------------|-------|
-- | spark.sql.shuffle.partitions     | 200   |
```

### Change and verify a setting

```sql
SET spark.sql.shuffle.partitions = 50;
SET spark.sql.shuffle.partitions;
-- Result: 50
```

### Reset a single setting

```sql
SET spark.sql.shuffle.partitions = 50;
-- ... run queries ...
RESET spark.sql.shuffle.partitions;

SET spark.sql.shuffle.partitions;
-- Result: 200  (back to default)
```

### Reset all session overrides

```sql
SET spark.sql.shuffle.partitions           = 50;
SET spark.sql.autoBroadcastJoinThreshold   = 209715200;
SET spark.sql.adaptive.enabled             = false;

-- Bulk reset
RESET;

-- Verify — all back to defaults
SET spark.sql.shuffle.partitions;           -- 200
SET spark.sql.autoBroadcastJoinThreshold;   -- 10485760
SET spark.sql.adaptive.enabled;             -- true
```

### List all settings (filtered)

```sql
-- SET -v returns all settings; use a derived table to filter in SQL
SELECT key, value
FROM (
    VALUES
        ('spark.sql.shuffle.partitions',         current_setting('spark.sql.shuffle.partitions')),
        ('spark.sql.adaptive.enabled',           current_setting('spark.sql.adaptive.enabled')),
        ('spark.sql.autoBroadcastJoinThreshold', current_setting('spark.sql.autoBroadcastJoinThreshold'))
) AS t(key, value);

-- Or in Databricks notebooks, run SET -v and inspect the output table
SET -v;
```

### Store a custom session variable

```sql
-- Use SET to store a custom key (not a Spark setting)
SET my.pipeline.run_date = 2024-06-01;

-- Read it back in a query
SELECT *
FROM orders
WHERE order_date = '${my.pipeline.run_date}';  -- Databricks variable substitution
```

### Per-query config with reset pattern

```sql
-- Save → set → query → reset pattern for safe per-query tuning
SET spark.sql.shuffle.partitions = 10;
SET spark.sql.autoBroadcastJoinThreshold = -1;

SELECT region, SUM(amount)
FROM small_daily_rollup
GROUP BY region;

RESET spark.sql.shuffle.partitions;
RESET spark.sql.autoBroadcastJoinThreshold;
```

### Check if ANSI mode is active

```sql
SET spark.sql.ansi.enabled;
-- Returns true or false
```

### Inspect static settings (read-only via SET)

```sql
-- These CAN be read but NOT changed via SET
SET spark.executor.memory;
SET spark.executor.cores;
SET spark.driver.memory;
```

---

## :material-swap-horizontal: SET Scope Comparison

| Scope | How to set | When it applies | Survives session end? |
|-------|-----------|----------------|----------------------|
| Built-in default | Compiled into Spark | Always (lowest priority) | Yes |
| Cluster config | `spark-defaults.conf` / cluster UI | All sessions on the cluster | Yes |
| Session override | `SET key = value` (SQL) | Current session only | No |
| Query hint | `/*+ BROADCAST(t) */` | Single query | No |

---

## :material-lightbulb-outline: When to Use Session Commands

| Scenario | Command |
|----------|---------|
| Temporarily tune a setting for one query | `SET key = value` … query … `RESET key` |
| Check current effective value | `SET key` |
| Bulk-reset after a tuning session | `RESET` |
| Audit all active settings | `SET -v` |
| Store a run-date or parameter for variable substitution | `SET my.key = value` |

!!! tip "Always RESET after per-query tuning"
    In long-running sessions (notebooks, interactive SQL editors), always `RESET` after
    per-query overrides. Leaving a `shuffle.partitions = 10` override in place will
    silently under-parallelise all subsequent queries in the session.
