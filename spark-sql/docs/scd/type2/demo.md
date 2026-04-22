# :material-play-circle: SCD Type 2 — Step-by-Step Demo

A complete walkthrough: create the dimension, seed initial data, process a change batch,
verify results, and run point-in-time queries — all in a single Spark SQL session.

---

## :material-numeric-1-circle: Create the Dimension Table

```sql
DROP TABLE IF EXISTS dim_customer;

CREATE TABLE dim_customer (
    customer_sk   BIGINT GENERATED ALWAYS AS IDENTITY,
    customer_id   STRING     NOT NULL,
    name          STRING,
    email         STRING,
    city          STRING,
    row_hash      STRING,
    start_date    TIMESTAMP  NOT NULL,
    end_date      TIMESTAMP,
    is_current    BOOLEAN    NOT NULL
)
USING DELTA
PARTITIONED BY (is_current);
```

---

## :material-numeric-2-circle: Seed Initial State

```sql
INSERT INTO dim_customer
    (customer_id, name, email, city, row_hash, start_date, end_date, is_current)
SELECT
    customer_id,
    name,
    email,
    city,
    md5(concat_ws('||', name, email, city)) AS row_hash,
    TIMESTAMP '2024-01-01 00:00:00'          AS start_date,
    NULL                                     AS end_date,
    TRUE                                     AS is_current
FROM VALUES
    ('cust1', 'Alice', 'alice@example.com', 'NY'),
    ('cust2', 'Bob',   'bob@example.com',   'CA')
AS t(customer_id, name, email, city);
```

**State after seed:**

| customer_sk | customer_id | name  | email               | city | is_current | start_date          | end_date |
|-------------|-------------|-------|---------------------|------|------------|---------------------|----------|
| 1           | cust1       | Alice | alice@example.com   | NY   | true       | 2024-01-01 00:00:00 | NULL     |
| 2           | cust2       | Bob   | bob@example.com     | CA   | true       | 2024-01-01 00:00:00 | NULL     |

---

## :material-numeric-3-circle: Incoming Staging Batch

```sql
CREATE OR REPLACE TEMP VIEW staging_customer AS
SELECT *
FROM VALUES
    ('cust1', 'Alice',   'alice@example.com',  'NY'),   -- no change
    ('cust2', 'Bobby',   'bob@newdomain.com',  'TX'),   -- name + email + city changed
    ('cust3', 'Charlie', 'charlie@example.com','WA')    -- new customer
AS t(customer_id, name, email, city);
```

---

## :material-numeric-4-circle: Pre-Merge Inspection

Understand what will happen before executing any writes.

```sql
-- Rows that will be EXPIRED (changed)
SELECT
    d.customer_id,
    d.name   AS old_name,  s.name   AS new_name,
    d.city   AS old_city,  s.city   AS new_city,
    d.email  AS old_email, s.email  AS new_email
FROM staging_customer AS s
JOIN dim_customer      AS d
    ON  d.customer_id = s.customer_id
    AND d.is_current  = TRUE
WHERE md5(concat_ws('||', s.name, s.email, s.city)) <> d.row_hash;
```

| customer_id | old_name | new_name | old_city | new_city | old_email           | new_email           |
|-------------|----------|----------|----------|----------|---------------------|---------------------|
| cust2       | Bob      | Bobby    | CA       | TX       | bob@example.com     | bob@newdomain.com   |

```sql
-- Rows that will be INSERTED (new)
SELECT s.*
FROM staging_customer  AS s
LEFT JOIN dim_customer AS d
    ON d.customer_id = s.customer_id AND d.is_current = TRUE
WHERE d.customer_id IS NULL;
```

| customer_id | name    | email                  | city |
|-------------|---------|------------------------|------|
| cust3       | Charlie | charlie@example.com    | WA   |

---

## :material-numeric-5-circle: Step 1 — Expire Changed Rows

```sql
MERGE INTO dim_customer AS tgt
USING (
    SELECT
        customer_id,
        md5(concat_ws('||', name, email, city)) AS row_hash
    FROM staging_customer
) AS src
ON  tgt.customer_id = src.customer_id
AND tgt.is_current  = TRUE
AND tgt.row_hash   <> src.row_hash

WHEN MATCHED THEN
    UPDATE SET
        end_date   = current_timestamp(),
        is_current = FALSE;
```

**After Step 1** — `cust2`'s original row is now closed:

| customer_sk | customer_id | name | city | is_current | end_date            |
|-------------|-------------|------|------|------------|---------------------|
| 2           | cust2       | Bob  | CA   | **false**  | 2024-06-15 09:00:00 |

---

## :material-numeric-6-circle: Step 2 — Insert New Version Rows

```sql
MERGE INTO dim_customer AS tgt
USING (
    SELECT
        s.customer_id,
        s.name,
        s.email,
        s.city,
        md5(concat_ws('||', s.name, s.email, s.city)) AS row_hash
    FROM staging_customer AS s
    LEFT JOIN dim_customer AS d
        ON  d.customer_id = s.customer_id
        AND d.is_current  = TRUE
    WHERE d.customer_id IS NULL       -- new customer
       OR d.row_hash   <> s.row_hash  -- changed (just expired in Step 1)
) AS src
ON tgt.customer_id = src.customer_id
AND tgt.is_current = TRUE
AND tgt.row_hash   = src.row_hash

WHEN NOT MATCHED THEN
    INSERT (customer_id, name, email, city, row_hash, start_date, end_date, is_current)
    VALUES (src.customer_id, src.name, src.email, src.city, src.row_hash,
            current_timestamp(), NULL, TRUE);
```

!!! note "Why does Step 1 not interfere with Step 2?"
    After Step 1, the expired `cust2` row has `is_current = FALSE` and its `row_hash` no longer
    matches the staging hash.  Step 2's USING query sees `d.is_current = TRUE` returning NULL
    for `cust2` (no active row left), so `cust2` qualifies under `d.customer_id IS NULL` and
    gets a fresh insert — exactly the intended behaviour.

---

## :material-numeric-7-circle: Post-Merge Verification

### Full table scan

```sql
SELECT
    customer_sk, customer_id, name, email, city,
    is_current, start_date, end_date
FROM dim_customer
ORDER BY customer_id, start_date;
```

| customer_sk | customer_id | name    | email                  | city | is_current | start_date          | end_date            |
|-------------|-------------|---------|------------------------|------|------------|---------------------|---------------------|
| 1           | cust1       | Alice   | alice@example.com      | NY   | true       | 2024-01-01 00:00:00 | NULL                |
| 2           | cust2       | Bob     | bob@example.com        | CA   | **false**  | 2024-01-01 00:00:00 | 2024-06-15 09:00:00 |
| 3           | cust2       | Bobby   | bob@newdomain.com      | TX   | **true**   | 2024-06-15 09:00:00 | NULL                |
| 4           | cust3       | Charlie | charlie@example.com    | WA   | true       | 2024-06-15 09:00:00 | NULL                |

### Assertions

```sql
-- 1. Exactly 3 active rows
SELECT COUNT(*) AS active FROM dim_customer WHERE is_current = TRUE;
-- Expected: 3

-- 2. cust1 untouched
SELECT start_date FROM dim_customer WHERE customer_id = 'cust1' AND is_current = TRUE;
-- Expected: 2024-01-01 (original seed timestamp)

-- 3. cust2 has exactly 2 versions
SELECT COUNT(*) AS versions FROM dim_customer WHERE customer_id = 'cust2';
-- Expected: 2

-- 4. Previous cust2 row is closed
SELECT end_date, is_current FROM dim_customer WHERE customer_id = 'cust2' AND name = 'Bob';
-- Expected: end_date IS NOT NULL, is_current = false

-- 5. No duplicate active rows per customer
SELECT customer_id, COUNT(*) AS cnt
FROM dim_customer WHERE is_current = TRUE
GROUP BY customer_id HAVING cnt > 1;
-- Expected: 0 rows
```

---

## :material-numeric-8-circle: Point-in-Time Queries

### What did `cust2` look like on 2024-03-15?

```sql
SELECT customer_id, name, email, city, start_date, end_date
FROM dim_customer
WHERE customer_id = 'cust2'
  AND start_date <= TIMESTAMP '2024-03-15 00:00:00'
  AND (end_date   > TIMESTAMP '2024-03-15 00:00:00' OR end_date IS NULL);
```

| customer_id | name | email           | city | start_date          | end_date            |
|-------------|------|-----------------|------|---------------------|---------------------|
| cust2       | Bob  | bob@example.com | CA   | 2024-01-01 00:00:00 | 2024-06-15 09:00:00 |

### Enrich orders with city at time of purchase

```sql
SELECT
    o.order_id,
    o.order_date,
    o.amount,
    c.name,
    c.city  AS city_at_order_time
FROM orders        AS o
JOIN dim_customer  AS c
    ON  c.customer_id = o.customer_id
    AND c.start_date <= o.order_date
    AND (c.end_date   > o.order_date OR c.end_date IS NULL)
ORDER BY o.order_date;
```

---

## :material-numeric-9-circle: Idempotency Check

Re-running the same batch must produce no new rows and no changed timestamps.

```sql
-- Re-run Step 1 (expire)
MERGE INTO dim_customer AS tgt
USING (
    SELECT customer_id, md5(concat_ws('||', name, email, city)) AS row_hash
    FROM staging_customer
) AS src
ON tgt.customer_id = src.customer_id AND tgt.is_current = TRUE AND tgt.row_hash <> src.row_hash
WHEN MATCHED THEN UPDATE SET end_date = current_timestamp(), is_current = FALSE;

-- Re-run Step 2 (insert)
MERGE INTO dim_customer AS tgt
USING (
    SELECT s.customer_id, s.name, s.email, s.city,
           md5(concat_ws('||', s.name, s.email, s.city)) AS row_hash
    FROM staging_customer AS s
    LEFT JOIN dim_customer AS d
        ON d.customer_id = s.customer_id AND d.is_current = TRUE
    WHERE d.customer_id IS NULL OR d.row_hash <> s.row_hash
) AS src
ON tgt.customer_id = src.customer_id AND tgt.is_current = TRUE AND tgt.row_hash = src.row_hash
WHEN NOT MATCHED THEN
    INSERT (customer_id, name, email, city, row_hash, start_date, end_date, is_current)
    VALUES (src.customer_id, src.name, src.email, src.city, src.row_hash,
            current_timestamp(), NULL, TRUE);

-- Verify: still exactly 3 active rows, still 4 total
SELECT
    COUNT(*)                           AS total_rows,
    SUM(CAST(is_current AS INT))       AS active_rows
FROM dim_customer;
-- Expected: total_rows = 4, active_rows = 3
```

---

## :material-numeric-10-circle: Delta History and Optimisation

```sql
-- Inspect commit log
DESCRIBE HISTORY dim_customer;

-- Compact small files and co-locate versions by customer
OPTIMIZE dim_customer ZORDER BY (customer_id);

-- Remove file versions older than 7 days (keep time-travel window)
VACUUM dim_customer RETAIN 168 HOURS;

-- Health-check view — always query current state without the filter
CREATE OR REPLACE VIEW dim_customer_current AS
SELECT * FROM dim_customer WHERE is_current = TRUE;
```
