# :material-table-edit: UPDATE

!!! note "[Databricks] Delta Lake Required"
    `UPDATE` requires Delta tables. Not supported on Hive, Parquet, or CSV tables.

`UPDATE` modifies existing rows in a Delta Lake table. It is **not** supported
on Hive, Parquet, or CSV tables — only Delta (and Iceberg/Hudi with their
respective connectors).

---

## :material-pin: Syntax

```sql
UPDATE table_name
SET col1 = expr1 [, col2 = expr2, ...]
[WHERE condition];
```

| Clause | Purpose |
|--------|---------|
| `SET` | Assigns new values to one or more columns |
| `WHERE` | Restricts which rows are updated (omit to update **all** rows) |

---

## :material-magnify: Behavior

1. **Transactional** — The update is atomic; either all matching rows change or
   none do.
2. **Predicate push-down** — Spark pushes the `WHERE` clause into the scan,
   so only relevant data files are rewritten.
3. **Column expressions** — The `SET` clause accepts any valid SQL expression,
   including references to other columns, functions, and CASE expressions.
4. **No cross-table SET** — You cannot reference another table in `SET`
   directly. Use `MERGE INTO` for correlated updates.
5. **Schema evolution** — `UPDATE` cannot add new columns. Use `ALTER TABLE`
   first if needed.

---

## :material-flask-outline: Practical Examples

### Simple Update

```sql
UPDATE customers
SET loyalty_tier = 'Gold'
WHERE total_spend > 10000;
```

### Update Multiple Columns

```sql
UPDATE orders
SET status      = 'cancelled',
    cancelled_at = current_timestamp(),
    cancelled_by = 'system'
WHERE status = 'pending'
  AND order_date < date_sub(current_date(), 30);
```

### Conditional Update with CASE

```sql
UPDATE products
SET price = CASE
              WHEN category = 'Electronics' THEN price * 0.90
              WHEN category = 'Clothing'    THEN price * 0.85
              ELSE price
            END
WHERE on_sale = true;
```

### Update with a Subquery Filter

```sql
UPDATE inventory
SET reorder_flag = true
WHERE product_id IN (
    SELECT product_id
    FROM sales
    GROUP BY product_id
    HAVING SUM(quantity) > 1000
);
```

### Update Nested Fields (Delta)

```sql
UPDATE events
SET payload.status = 'processed'
WHERE payload.status = 'raw';
```

---

## :material-brain: When to Use

| Scenario | Recommended Approach |
|----------|---------------------|
| Fix incorrect values | `UPDATE ... SET ... WHERE` |
| Bulk recalculate a column | `UPDATE ... SET col = expr` (no WHERE) |
| Correlated update from another table | Use `MERGE INTO` instead |
| Backfill a new column with defaults | `UPDATE ... SET new_col = default_val` |
| Conditional logic per row | `UPDATE ... SET col = CASE ... END` |

---

> **See also:** [MERGE INTO](merge.md) for combining `UPDATE`, `INSERT`, and
> `DELETE` in a single atomic statement.

---

## :material-database-arrow-up: Advanced Patterns

### Correlated update via scalar subquery

```sql
-- Backfill total_orders on customers table from the orders table
UPDATE customers AS c
SET total_orders = (
    SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.customer_id
)
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id);
```

### Update from a staging join (MERGE preferred, but subquery works)

```sql
UPDATE products
SET price      = s.new_price,
    updated_at = current_timestamp()
WHERE product_id IN (
    SELECT product_id FROM price_updates s
    WHERE s.effective_date = current_date()
);
```

### Soft-delete pattern (mark instead of remove)

```sql
UPDATE customers
SET is_deleted   = TRUE,
    deleted_at   = current_timestamp(),
    deleted_by   = 'gdpr_request'
WHERE customer_id IN (
    SELECT customer_id FROM gdpr_erasure_requests
    WHERE processed = FALSE
);

-- Mark the requests as handled
UPDATE gdpr_erasure_requests
SET processed = TRUE, processed_at = current_timestamp()
WHERE processed = FALSE;
```

### Backfill a new column after ALTER TABLE

```sql
ALTER TABLE orders ADD COLUMNS (order_year INT);

UPDATE orders
SET order_year = YEAR(order_date)
WHERE order_year IS NULL;
```

### Partial string update using string functions

```sql
UPDATE events
SET event_url = REGEXP_REPLACE(event_url, '^http://', 'https://')
WHERE event_url LIKE 'http://%';
```

---

## :material-compare: UPDATE vs MERGE

| Scenario | Use `UPDATE` | Use `MERGE` |
|----------|:------------:|:-----------:|
| Fix values in the same table | :material-check: | Overkill |
| Apply changes from another table | Via subquery (limited) | :material-check: |
| Insert missing rows alongside update | :material-close: | :material-check: |
| Delete some rows while updating others | :material-close: | :material-check: |
| Complex conditional per-row logic | CASE in SET | :material-check: |

---

## :material-speedometer: Performance Tips

| Tip | Reason |
|-----|--------|
| Filter with partition columns in `WHERE` | Enables partition pruning — rewrites fewer files |
| Avoid full-table updates without a WHERE | Every data file gets rewritten |
| Z-ORDER the table by update key | Clusters related rows into fewer files |
| Run `OPTIMIZE` after large updates | Compacts the newly written small files |
| Use `MERGE` for cross-table updates | More expressive and often better optimised |
