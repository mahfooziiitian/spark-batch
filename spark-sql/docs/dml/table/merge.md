# :material-merge: MERGE INTO (Upsert)

`MERGE INTO` performs an atomic **upsert** — a single statement that can
`INSERT`, `UPDATE`, and `DELETE` rows by comparing a target table against a
source. It is the most powerful DML statement in Delta Lake.

### :material-sitemap: Overview

```mermaid
graph LR
    A[Source Table] --> B{MERGE INTO Target}
    B -->|Row matched| C["WHEN MATCHED: UPDATE / DELETE"]
    B -->|No match in target| D["WHEN NOT MATCHED: INSERT"]
    B -->|No match in source| E["WHEN NOT MATCHED BY SOURCE: DELETE"]
```

---

## 📌 Syntax

```sql
MERGE INTO target_table [AS target_alias]
USING source_table_or_query [AS source_alias]
ON merge_condition
[WHEN MATCHED [AND condition] THEN
    UPDATE SET col1 = expr1, col2 = expr2, ...]
[WHEN MATCHED [AND condition] THEN DELETE]
[WHEN NOT MATCHED [AND condition] THEN
    INSERT (col1, col2, ...) VALUES (expr1, expr2, ...)]
[WHEN NOT MATCHED BY SOURCE [AND condition] THEN
    UPDATE SET col1 = expr1, ...]
[WHEN NOT MATCHED BY SOURCE [AND condition] THEN DELETE];
```

| Clause | Purpose |
|--------|---------|
| `USING` | Source table, view, subquery, or CTE |
| `ON` | Join condition linking target and source rows |
| `WHEN MATCHED` | Action for rows that exist in **both** target and source |
| `WHEN NOT MATCHED` | Action for rows in the **source** that have no target match |
| `WHEN NOT MATCHED BY SOURCE` | Action for rows in the **target** that have no source match (Delta 2.4+) |

---

## 🔍 Behavior

1. **At most one match** — Each target row must match at most one source row.
   If multiple source rows match the same target row, Spark raises an error.
   Deduplicate the source first if needed.
2. **Clause ordering** — Multiple `WHEN MATCHED` or `WHEN NOT MATCHED` clauses
   are evaluated in the order they appear; the first matching clause wins.
3. **Star syntax** — `UPDATE SET *` and `INSERT *` map columns by name between
   source and target, simplifying schemas with many columns.
4. **Condition guards** — Adding `AND condition` to any clause allows branching
   logic (e.g., update if changed, delete if flagged).
5. **Atomicity** — The entire merge is a single transaction. Partial application
   is impossible.
6. **Performance** — Spark pushes the `ON` predicate into the scan. Ensure the
   merge key has good data locality (e.g., partitioned or Z-ordered) for best
   performance.

---

## 🧪 Practical Examples

### Basic Upsert

```sql
MERGE INTO customers AS t
USING daily_updates AS s
ON t.customer_id = s.customer_id
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *;
```

### Conditional Update / Insert

```sql
MERGE INTO products AS t
USING new_catalog AS s
ON t.sku = s.sku
WHEN MATCHED AND s.price <> t.price THEN
    UPDATE SET t.price = s.price,
              t.updated_at = current_timestamp()
WHEN NOT MATCHED THEN
    INSERT (sku, name, price, created_at)
    VALUES (s.sku, s.name, s.price, current_timestamp());
```

### Upsert with Delete

```sql
MERGE INTO inventory AS t
USING warehouse_feed AS s
ON t.item_id = s.item_id
WHEN MATCHED AND s.quantity = 0 THEN DELETE
WHEN MATCHED THEN
    UPDATE SET t.quantity = s.quantity
WHEN NOT MATCHED THEN
    INSERT *;
```

### SCD Type 2 (Slowly Changing Dimension)

```sql
-- Step 1: Close existing current records that have changes
MERGE INTO dim_customer AS t
USING (
    SELECT * FROM staging_customer
    WHERE updated_at > (SELECT MAX(effective_from) FROM dim_customer)
) AS s
ON t.customer_id = s.customer_id AND t.is_current = true
WHEN MATCHED AND (t.name <> s.name OR t.email <> s.email) THEN
    UPDATE SET t.is_current   = false,
              t.effective_to  = current_date();

-- Step 2: Insert new current records
INSERT INTO dim_customer
SELECT customer_id, name, email,
       current_date() AS effective_from,
       NULL AS effective_to,
       true AS is_current
FROM staging_customer s
WHERE NOT EXISTS (
    SELECT 1 FROM dim_customer t
    WHERE t.customer_id = s.customer_id
      AND t.is_current = true
      AND t.name = s.name
      AND t.email = s.email
);
```

### Delete Unmatched Target Rows

```sql
MERGE INTO active_users AS t
USING current_roster AS s
ON t.user_id = s.user_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;
-- Removes users no longer in the roster
```

### Merge from a Subquery

```sql
MERGE INTO orders AS t
USING (
    SELECT order_id, MAX(status) AS latest_status
    FROM order_events
    GROUP BY order_id
) AS s
ON t.order_id = s.order_id
WHEN MATCHED THEN
    UPDATE SET t.status = s.latest_status;
```

---

## 🧠 When to Use

| Scenario | Pattern |
|----------|---------|
| CDC / incremental load | `MERGE ... WHEN MATCHED UPDATE WHEN NOT MATCHED INSERT` |
| Idempotent data ingestion | Merge with deduplication on business key |
| SCD Type 1 (overwrite) | `MERGE ... WHEN MATCHED UPDATE SET *` |
| SCD Type 2 (history) | Two-step: merge to close + insert new versions |
| Full sync (mirror source) | Add `WHEN NOT MATCHED BY SOURCE THEN DELETE` |
| Conditional deletes during upsert | `WHEN MATCHED AND flag = 'D' THEN DELETE` |

---

> **Tip:** Deduplicate your source before merging to avoid the
> "multiple source rows matched the same target row" error:
>
> ```sql
> USING (SELECT * FROM (
>     SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) AS rn
>     FROM source) WHERE rn = 1
> ) AS s
> ```
