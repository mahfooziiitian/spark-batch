# :material-play-circle: SCD Type 1 — Step-by-Step Demo

This page walks through a complete Type 1 lifecycle: initial load, a batch of incoming
changes, the MERGE execution, and result verification — all runnable in a single Spark SQL
session or Databricks notebook.

---

## :material-numeric-1-circle: Initial State

Create the dimension table and load two seed customers.

```sql
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id  STRING    NOT NULL,
    name         STRING,
    email        STRING,
    city         STRING,
    row_hash     STRING,
    updated_at   TIMESTAMP
)
USING DELTA
PARTITIONED BY (city);

INSERT INTO dim_customer (customer_id, name, email, city, row_hash, updated_at)
SELECT
    customer_id,
    name,
    email,
    city,
    md5(concat_ws('||', name, email, city)) AS row_hash,
    TIMESTAMP '2024-01-01 00:00:00'         AS updated_at
FROM VALUES
    ('cust1', 'Alice', 'alice@example.com', 'NY'),
    ('cust2', 'Bob',   'bob@example.com',   'CA')
AS t(customer_id, name, email, city);
```

**State after seed:**

| customer_id | name  | email               | city | updated_at          |
|-------------|-------|---------------------|------|---------------------|
| cust1       | Alice | alice@example.com   | NY   | 2024-01-01 00:00:00 |
| cust2       | Bob   | bob@example.com     | CA   | 2024-01-01 00:00:00 |

---

## :material-numeric-2-circle: Incoming Batch

Three records arrive from the source system.

```sql
CREATE OR REPLACE TEMP VIEW staging_customer AS
SELECT *
FROM VALUES
    ('cust1', 'Alice',   'alice@example.com',  'NY'),   -- no change
    ('cust2', 'Bobby',   'bob@newdomain.com',  'CA'),   -- name + email changed
    ('cust3', 'Charlie', 'charlie@example.com','WA')    -- new customer
AS t(customer_id, name, email, city);
```

---

## :material-numeric-3-circle: Pre-Merge: Detect Changes

Inspect what the MERGE will do before executing it.

```sql
-- Rows that will trigger UPDATE
SELECT
    s.customer_id,
    tgt.name  AS old_name,  s.name  AS new_name,
    tgt.email AS old_email, s.email AS new_email,
    tgt.city  AS old_city,  s.city  AS new_city
FROM staging_customer AS s
JOIN dim_customer      AS tgt USING (customer_id)
WHERE md5(concat_ws('||', s.name, s.email, s.city))
   <> tgt.row_hash;
```

| customer_id | old_name | new_name | old_email           | new_email           |
|-------------|----------|----------|---------------------|---------------------|
| cust2       | Bob      | Bobby    | bob@example.com     | bob@newdomain.com   |

```sql
-- Rows that will trigger INSERT
SELECT s.*
FROM staging_customer AS s
LEFT JOIN dim_customer AS tgt USING (customer_id)
WHERE tgt.customer_id IS NULL;
```

| customer_id | name    | email                  | city |
|-------------|---------|------------------------|------|
| cust3       | Charlie | charlie@example.com    | WA   |

---

## :material-numeric-4-circle: Execute the MERGE

```sql
MERGE INTO dim_customer AS tgt
USING (
    SELECT
        customer_id,
        name,
        email,
        city,
        md5(concat_ws('||', name, email, city)) AS row_hash
    FROM staging_customer
) AS src
ON src.customer_id = tgt.customer_id

WHEN MATCHED AND src.row_hash <> tgt.row_hash THEN
    UPDATE SET
        name       = src.name,
        email      = src.email,
        city       = src.city,
        row_hash   = src.row_hash,
        updated_at = current_timestamp()

WHEN NOT MATCHED THEN
    INSERT (customer_id, name, email, city, row_hash, updated_at)
    VALUES (src.customer_id, src.name, src.email, src.city,
            src.row_hash, current_timestamp());
```

!!! note "Row count from MERGE"
    Delta's MERGE returns operation metrics: `num_updated_rows = 1`, `num_inserted_rows = 1`,
    `num_deleted_rows = 0`.  Query `DESCRIBE HISTORY dim_customer` to see the commit details.

---

## :material-numeric-5-circle: Post-Merge Verification

### Full table scan

```sql
SELECT * FROM dim_customer ORDER BY customer_id;
```

| customer_id | name    | email                  | city | updated_at          |
|-------------|---------|------------------------|------|---------------------|
| cust1       | Alice   | alice@example.com      | NY   | 2024-01-01 00:00:00 |
| cust2       | Bobby   | bob@newdomain.com      | CA   | 2024-06-15 09:00:00 |
| cust3       | Charlie | charlie@example.com    | WA   | 2024-06-15 09:00:00 |

### Assert `cust1` unchanged

```sql
SELECT updated_at FROM dim_customer WHERE customer_id = 'cust1';
-- updated_at must still be 2024-01-01 (seed value)
```

### Assert `cust2` updated correctly

```sql
SELECT name, email FROM dim_customer WHERE customer_id = 'cust2';
-- Expected: Bobby | bob@newdomain.com
```

### Assert `cust3` inserted

```sql
SELECT COUNT(*) FROM dim_customer WHERE customer_id = 'cust3';
-- Expected: 1
```

### Assert no duplicate natural keys

```sql
SELECT customer_id, COUNT(*) AS cnt
FROM dim_customer
GROUP BY customer_id
HAVING cnt > 1;
-- Expected: 0 rows
```

---

## :material-numeric-6-circle: Idempotency Check

Running the same batch twice must produce **identical results** — no phantom updates.

```sql
-- Re-run the exact same MERGE a second time
MERGE INTO dim_customer AS tgt
USING (
    SELECT
        customer_id, name, email, city,
        md5(concat_ws('||', name, email, city)) AS row_hash
    FROM staging_customer
) AS src
ON src.customer_id = tgt.customer_id

WHEN MATCHED AND src.row_hash <> tgt.row_hash THEN
    UPDATE SET
        name = src.name, email = src.email, city = src.city,
        row_hash = src.row_hash, updated_at = current_timestamp()

WHEN NOT MATCHED THEN
    INSERT (customer_id, name, email, city, row_hash, updated_at)
    VALUES (src.customer_id, src.name, src.email, src.city,
            src.row_hash, current_timestamp());

-- Validate: total row count must still be 3
SELECT COUNT(*) AS total_rows FROM dim_customer;
-- Expected: 3
```

!!! tip "Why does this work?"
    The `WHEN MATCHED AND src.row_hash <> tgt.row_hash` guard prevents updates when the hash
    already matches.  `WHEN NOT MATCHED` only fires for keys absent in the target — which `cust3`
    no longer is after the first run.

---

## :material-numeric-7-circle: Delta History

Inspect the audit log that Delta Lake maintains automatically.

```sql
DESCRIBE HISTORY dim_customer;
```

| version | timestamp           | operation | operationParameters                       |
|---------|---------------------|-----------|-------------------------------------------|
| 1       | 2024-06-15 09:00:xx | MERGE     | `{"numTargetRowsUpdated":"1","numTargetRowsInserted":"1"}` |
| 0       | 2024-01-01 00:00:xx | WRITE     | `{"mode":"Append","numFiles":"2"}`        |

---

## :material-cog-outline: Optimise After Load

```sql
-- Compact small files written by the MERGE
OPTIMIZE dim_customer ZORDER BY (customer_id);

-- Remove old file versions (keep 7 days of time-travel)
VACUUM dim_customer RETAIN 168 HOURS;
```
