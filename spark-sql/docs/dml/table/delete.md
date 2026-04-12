# :material-table-minus: DELETE

`DELETE` removes rows from a Delta Lake table that match a given condition.
Like `UPDATE`, it requires a transactional table format — standard Hive or
Parquet tables do not support row-level deletes.

---

## 📌 Syntax

```sql
DELETE FROM table_name
[WHERE condition];
```

| Clause | Purpose |
|--------|---------|
| `WHERE` | Identifies rows to remove. **Omitting WHERE deletes all rows.** |

---

## 🔍 Behavior

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

## 🧪 Practical Examples

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
| `DELETE FROM t` (no WHERE) | All | ✅ | ✅ Entry added | ✅ |
| `TRUNCATE TABLE t` | All | ✅ | ❌ Resets | ❌ |
| `DROP TABLE t` | All | ❌ | ❌ Removed | ❌ |

---

## 🧠 When to Use

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
