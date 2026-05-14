# :material-table-minus: DELETE

`DELETE` removes rows from a Delta Lake table that match a given condition.
Like `UPDATE`, it requires a transactional table format — standard Hive or
Parquet tables do not support row-level deletes.

---

## :material-pin: Syntax

```sql
DELETE FROM table_name
[WHERE condition];
```

| Clause | Purpose |
|--------|---------|
| `WHERE` | Identifies rows to remove. **Omitting WHERE deletes all rows.** |

---

## :material-magnify: Behavior

1. **File rewrite** — Delta does not delete individual rows in-place. Instead it
   rewrites the affected data files without the deleted rows and updates the
   transaction log.
2. **Predicate push-down** — The `WHERE` clause is pushed into the scan;
   data files that cannot contain matching rows are skipped entirely.
3. **Partition pruning** — If the predicate references partition columns, only
   files in matching partitions are read.
4. **Atomicity** — The delete is all-or-nothing. A failure rolls back the
   transaction.
5. **VACUUM dependency** — Deleted data files are not physically removed until
   you run `VACUUM`. Until then, time-travel queries can still access them.

---

## :material-flask-outline: Practical Examples

### Delete by Condition

```sql
DELETE FROM orders
WHERE order_date < '2022-01-01';
```

### Delete with a Subquery

```sql
DELETE FROM customers
WHERE customer_id NOT IN (
    SELECT DISTINCT customer_id
    FROM orders
    WHERE order_date >= '2023-01-01'
);
-- Remove customers with no orders in 2023+
```

### Delete with EXISTS

```sql
DELETE FROM inventory
WHERE EXISTS (
    SELECT 1 FROM discontinued_products dp
    WHERE dp.product_id = inventory.product_id
);
```

### Delete All Rows (Equivalent to TRUNCATE)

```sql
DELETE FROM staging_events;
-- Removes every row; table and schema remain intact
```

### Delete by Partition Value

```sql
DELETE FROM logs
WHERE event_date = '2024-01-15'
  AND level = 'DEBUG';
-- Prunes to a single partition, then filters within it
```

---

## :material-table-minus: DELETE vs TRUNCATE vs DROP

| Operation | Rows Removed | Table Remains | Transaction Log | Time Travel |
|-----------|:------------:|:-------------:|:---------------:|:-----------:|
| `DELETE FROM t` (no WHERE) | All | :material-check-circle-outline: | :material-check-circle-outline: Entry added | :material-check-circle-outline: |
| `TRUNCATE TABLE t` | All | :material-check-circle-outline: | :material-close-circle-outline: Resets | :material-close-circle-outline: |
| `DROP TABLE t` | All | :material-close-circle-outline: | :material-close-circle-outline: Removed | :material-close-circle-outline: |

---

## :material-brain: When to Use

| Scenario | Recommended Approach |
|----------|---------------------|
| Remove expired / aged-out data | `DELETE ... WHERE date < threshold` |
| Remove orphaned child rows | `DELETE ... WHERE id NOT IN (subquery)` |
| Purge a staging table between loads | `DELETE FROM staging` or `TRUNCATE` |
| Conditional upsert with deletes | Use `MERGE INTO` with `WHEN MATCHED ... DELETE` |
| Physically reclaim disk space | Run `VACUUM` after deleting |

---

> **See also:** [MERGE INTO](merge.md) supports a `DELETE` action inside the
> `WHEN MATCHED` clause for conditional row removal during upserts.

---

## :material-database-remove: Advanced Patterns

### GDPR right-to-erasure (hard delete)

```sql
-- Step 1: delete matching rows
DELETE FROM customers
WHERE customer_id IN (
    SELECT customer_id FROM erasure_requests WHERE fulfilled = FALSE
);

-- Step 2: mark requests as fulfilled
UPDATE erasure_requests
SET fulfilled = TRUE, fulfilled_at = current_timestamp()
WHERE fulfilled = FALSE;

-- Step 3: reclaim disk space (removes old data files)
VACUUM customers RETAIN 0 HOURS;
-- Note: disable retention check first:
-- SET spark.databricks.delta.retentionDurationCheck.enabled = false;
```

### Age-out data beyond retention window

```sql
DELETE FROM event_logs
WHERE event_date < date_sub(current_date(), 90);

-- Then compact + reclaim space
OPTIMIZE event_logs;
VACUUM event_logs RETAIN 168 HOURS;
```

### Conditional delete from CDC feed

```sql
-- CDC records with op_type = 'D' should be deleted from the target
DELETE FROM orders
WHERE order_id IN (
    SELECT order_id FROM cdc_feed WHERE op_type = 'D'
);
```

### Delete orphaned child rows

```sql
DELETE FROM order_items
WHERE order_id NOT IN (
    SELECT order_id FROM orders WHERE order_id IS NOT NULL
);
```

### Delete duplicate rows (keep latest)

```sql
-- Keep only the row with the highest updated_at per key
DELETE FROM events
WHERE event_id NOT IN (
    SELECT event_id FROM (
        SELECT event_id,
               ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY updated_at DESC) AS rn
        FROM events
    )
    WHERE rn = 1
);
```

---

## :material-recycle: After Deleting — Maintenance Checklist

```sql
-- 1. Verify row count
SELECT COUNT(*) FROM my_table;

-- 2. Compact the newly sparse files
OPTIMIZE my_table;

-- 3. Z-ORDER if filtered by a specific column
OPTIMIZE my_table ZORDER BY (customer_id);

-- 4. Physically remove old file versions
VACUUM my_table RETAIN 168 HOURS;

-- 5. Check Delta history
DESCRIBE HISTORY my_table;
```

!!! warning "VACUUM before confirming GDPR deletion"
    Deleted rows remain in old Parquet files on disk until `VACUUM` runs.
    For right-to-erasure compliance, run `VACUUM RETAIN 0 HOURS` after confirming
    all downstream pipelines and consumers have been notified.

---

## :material-speedometer: Performance Tips

| Tip | Reason |
|-----|--------|
| Include partition column in `WHERE` | Partition pruning — only matching files rewritten |
| Use `NOT EXISTS` instead of `NOT IN` | Avoids NULL trap; often better plan |
| Delete then `OPTIMIZE` | Compacts the many small files left after deletion |
| Batch large deletes by date range | Prevents one huge rewrite transaction |
| Prefer `TRUNCATE` for full-table wipe | Much faster than `DELETE FROM table` |
