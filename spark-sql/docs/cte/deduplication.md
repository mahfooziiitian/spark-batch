# :material-content-duplicate: CTE for Deduplication

CTEs combined with window functions (`ROW_NUMBER`, `RANK`) are the standard pattern for
removing duplicate rows before loading, merging, or reporting. The CTE labels duplicates;
the outer query discards them.

---

## :material-information-outline: Behavior

1. `ROW_NUMBER()` assigns a unique sequential integer within each duplicate group — always use this when you want exactly one row per key.
2. `RANK()` assigns the same number to tied rows — use when ties should be treated equally (e.g., multiple rows with the same `updated_at`).
3. The deduplication CTE does **not** modify data; it adds a row number column. The outer `WHERE rn = 1` (or `WHERE rn <= N`) performs the actual filtering.
4. For **MERGE**, deduplicate the source before the `USING` clause — Delta raises `UnsupportedOperationException` if multiple source rows match one target row.
5. Choosing `ORDER BY` inside the window spec determines which duplicate to keep:
   - `ORDER BY updated_at DESC` → keep the most recent.
   - `ORDER BY created_at ASC` → keep the earliest (first occurrence).

---

## :material-flask-outline: Practical Examples

### Keep the most recent row per key

```sql
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY updated_at DESC
        ) AS rn
    FROM staging_orders
)
SELECT order_id, customer_id, amount, status, updated_at
FROM ranked
WHERE rn = 1;
```

### Deduplicate before INSERT

```sql
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY ingested_at DESC
        ) AS rn
    FROM raw_products
)
INSERT INTO dim_product
SELECT product_id, name, category, price, ingested_at
FROM deduped
WHERE rn = 1;
```

### Deduplicate source before MERGE

```sql
WITH deduped_source AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY customer_id
                ORDER BY updated_at DESC
            ) AS rn
        FROM staging_customers
    )
    WHERE rn = 1
)
MERGE INTO dim_customer AS t
USING deduped_source AS s
    ON t.customer_id = s.customer_id
WHEN MATCHED AND t.row_hash <> md5(concat_ws('||', s.name, s.email, s.city)) THEN
    UPDATE SET
        name     = s.name,
        email    = s.email,
        city     = s.city,
        row_hash = md5(concat_ws('||', s.name, s.email, s.city))
WHEN NOT MATCHED THEN
    INSERT (customer_id, name, email, city, row_hash)
    VALUES (s.customer_id, s.name, s.email, s.city,
            md5(concat_ws('||', s.name, s.email, s.city)));
```

### Keep first occurrence (earliest created_at)

```sql
WITH first_occurrence AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY email
            ORDER BY created_at ASC
        ) AS rn
    FROM user_registrations
)
SELECT user_id, email, name, created_at
FROM first_occurrence
WHERE rn = 1;
```

### Flag and inspect duplicates before removal

```sql
WITH flagged AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY updated_at DESC
        ) AS rn,
        COUNT(*) OVER (PARTITION BY order_id) AS dup_count
    FROM staging_orders
)
-- View the duplicates first
SELECT * FROM flagged WHERE dup_count > 1 ORDER BY order_id, rn;
```

### Deduplicate across multiple key columns

```sql
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id, product_id, order_date
            ORDER BY ingested_at DESC
        ) AS rn
    FROM raw_order_lines
)
SELECT customer_id, product_id, order_date, quantity, unit_price
FROM deduped
WHERE rn = 1;
```

### Remove exact duplicates (all columns identical)

```sql
-- When no timestamp is available, any column order works
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, customer_id, amount, status
            ORDER BY (SELECT NULL)   -- arbitrary tie-break
        ) AS rn
    FROM orders
)
SELECT order_id, customer_id, amount, status
FROM deduped
WHERE rn = 1;
```

### Top-N per group (keep 3 most recent orders per customer)

```sql
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY order_date DESC
        ) AS rn
    FROM orders
)
SELECT customer_id, order_id, order_date, amount
FROM ranked
WHERE rn <= 3
ORDER BY customer_id, rn;
```

---

## :material-swap-horizontal: ROW_NUMBER vs RANK vs DENSE_RANK for Deduplication

| Function | Tie behaviour | Use when |
|----------|--------------|----------|
| `ROW_NUMBER` | No ties — always unique | You want exactly one row per key regardless of ties |
| `RANK` | Tied rows share the same rank; next rank skips | Ties should be treated equally; OK to keep all tied rows |
| `DENSE_RANK` | Tied rows share the same rank; no skipping | Same as RANK but ranks are contiguous |

For deduplication, **always prefer `ROW_NUMBER`** — it guarantees exactly one row per
partition group.

---

## :material-lightbulb-outline: When to Use

| Scenario | Pattern |
|----------|---------|
| Staging table has duplicate keys | `ROW_NUMBER` CTE + `WHERE rn = 1` before `INSERT` |
| MERGE fails with duplicate source rows | Dedup CTE in `USING` clause |
| Keep only the latest version of each record | `ORDER BY updated_at DESC` in window spec |
| Keep only the first occurrence | `ORDER BY created_at ASC` in window spec |
| Inspect duplicates before removing | Add `dup_count = COUNT(*) OVER (PARTITION BY key)` |
| Top-N records per group | `WHERE rn <= N` |
