# :material-play-circle: SCD Type 5 — Step-by-Step Demo

A complete walkthrough: create both tables, seed them consistently, process a change batch
through all four steps, verify state after each, run a second batch, and explore the
`hist_key` FK join pattern.

---

## :material-numeric-1-circle: Create Both Tables

```sql
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_customer_history;

-- History table first — dim_customer.hist_key is a FK into it
CREATE TABLE dim_customer_history (
    hist_key     BIGINT GENERATED ALWAYS AS IDENTITY,
    customer_id  STRING    NOT NULL,
    name         STRING,
    email        STRING,
    city         STRING,
    row_hash     STRING,
    valid_from   TIMESTAMP NOT NULL,
    valid_to     TIMESTAMP           -- NULL = currently active
)
USING DELTA
PARTITIONED BY (customer_id);

-- Main dimension — hist_key references the active row in the history table
CREATE TABLE dim_customer (
    customer_id  STRING    NOT NULL,
    name         STRING,
    email        STRING,
    city         STRING,
    hist_key     BIGINT,             -- FK → dim_customer_history.hist_key
    updated_at   TIMESTAMP
)
USING DELTA;
```

---

## :material-numeric-2-circle: Seed Both Tables

The history table is seeded first so `hist_key` values exist before the main dimension
references them.

```sql
-- Step A: insert initial history rows (both customers start active)
INSERT INTO dim_customer_history
    (customer_id, name, email, city, row_hash, valid_from, valid_to)
VALUES
    ('cust1', 'Alice', 'alice@example.com', 'NY',
        md5(concat_ws('||', 'Alice', 'alice@example.com', 'NY')),
        TIMESTAMP '2024-01-01 00:00:00', NULL),
    ('cust2', 'Bob',   'bob@example.com',   'CA',
        md5(concat_ws('||', 'Bob',   'bob@example.com',   'CA')),
        TIMESTAMP '2024-01-01 00:00:00', NULL);

-- Step B: populate main dimension, pulling hist_key from the active history rows
INSERT INTO dim_customer (customer_id, name, email, city, hist_key, updated_at)
SELECT
    h.customer_id,
    h.name,
    h.email,
    h.city,
    h.hist_key,
    h.valid_from AS updated_at
FROM dim_customer_history AS h
WHERE h.valid_to IS NULL;
```

**State after seed:**

`dim_customer`

| customer_id | name  | email               | city | hist_key | updated_at          |
|-------------|-------|---------------------|------|----------|---------------------|
| cust1       | Alice | alice@example.com   | NY   | 1        | 2024-01-01 00:00:00 |
| cust2       | Bob   | bob@example.com     | CA   | 2        | 2024-01-01 00:00:00 |

`dim_customer_history`

| hist_key | customer_id | name  | email               | city | valid_from          | valid_to |
|----------|-------------|-------|---------------------|------|---------------------|----------|
| 1        | cust1       | Alice | alice@example.com   | NY   | 2024-01-01 00:00:00 | NULL     |
| 2        | cust2       | Bob   | bob@example.com     | CA   | 2024-01-01 00:00:00 | NULL     |

---

## :material-numeric-3-circle: First Incoming Batch

```sql
CREATE OR REPLACE TEMP VIEW staging_batch1 AS
SELECT *
FROM VALUES
    ('cust1', 'Alice',   'alice@example.com',  'NY'),   -- no change
    ('cust2', 'Bobby',   'bob@newdomain.com',  'TX'),   -- name + email + city changed
    ('cust3', 'Charlie', 'charlie@example.com','WA')    -- new customer
AS t(customer_id, name, email, city);
```

---

## :material-numeric-4-circle: Pre-Batch Inspection

```sql
-- Classify each incoming row before writing anything
WITH hashed AS (
    SELECT *, md5(concat_ws('||', name, email, city)) AS new_hash
    FROM staging_batch1
)
SELECT
    h.customer_id,
    h.name       AS new_name,
    c.name       AS old_name,
    c.city       AS old_city,
    h.city       AS new_city,
    c.hist_key   AS current_hist_key,
    CASE
        WHEN c.customer_id IS NULL          THEN 'NEW'
        WHEN c.row_hash <> h.new_hash       THEN 'CHANGED'
        ELSE                                     'UNCHANGED'
    END AS action
FROM hashed AS h
-- join to dim_customer to get current hash and hist_key
LEFT JOIN (
    SELECT c2.customer_id, c2.hist_key,
           dh.row_hash
    FROM dim_customer AS c2
    JOIN dim_customer_history AS dh ON dh.hist_key = c2.hist_key
) AS c ON c.customer_id = h.customer_id;
```

| customer_id | new_name | old_name | old_city | new_city | current_hist_key | action    |
|-------------|----------|----------|----------|----------|------------------|-----------|
| cust1       | Alice    | Alice    | NY       | NY       | 1                | UNCHANGED |
| cust2       | Bobby    | Bob      | CA       | TX       | 2                | CHANGED   |
| cust3       | Charlie  | NULL     | NULL     | WA       | NULL             | NEW       |

---

## :material-numeric-5-circle: Step 1 — Hash the Staging Data

```sql
CREATE OR REPLACE TEMP VIEW staged_hashed AS
SELECT
    customer_id,
    name,
    email,
    city,
    md5(concat_ws('||', name, email, city)) AS row_hash
FROM staging_batch1;
```

---

## :material-numeric-6-circle: Step 2 — Close Active History Rows for Changed Customers

```sql
MERGE INTO dim_customer_history AS hist
USING (
    SELECT s.customer_id
    FROM staged_hashed   AS s
    JOIN dim_customer    AS c ON c.customer_id = s.customer_id
    JOIN dim_customer_history AS dh ON dh.hist_key = c.hist_key
    WHERE dh.row_hash <> s.row_hash
) AS changed
ON  hist.customer_id = changed.customer_id
AND hist.valid_to    IS NULL

WHEN MATCHED THEN
    UPDATE SET valid_to = current_timestamp();
```

**`dim_customer_history` after Step 2** — `cust2`'s original row is now closed:

| hist_key | customer_id | name | email           | city | valid_from          | valid_to            |
|----------|-------------|------|-----------------|------|---------------------|---------------------|
| 1        | cust1       | Alice | alice@example.com | NY | 2024-01-01 00:00:00 | NULL                |
| 2        | cust2       | Bob  | bob@example.com | CA   | 2024-01-01 00:00:00 | **2024-06-15 09:00** |

---

## :material-numeric-7-circle: Step 3 — Insert New History Rows

```sql
INSERT INTO dim_customer_history
    (customer_id, name, email, city, row_hash, valid_from, valid_to)
SELECT
    s.customer_id,
    s.name,
    s.email,
    s.city,
    s.row_hash,
    current_timestamp() AS valid_from,
    NULL                AS valid_to
FROM staged_hashed AS s
LEFT JOIN dim_customer AS c ON c.customer_id = s.customer_id
WHERE
    c.customer_id IS NULL    -- new customer
    OR NOT EXISTS (           -- changed customer: no active row with the new hash
        SELECT 1 FROM dim_customer_history AS dh
        WHERE dh.customer_id = s.customer_id
          AND dh.row_hash    = s.row_hash
          AND dh.valid_to    IS NULL
    );
```

**`dim_customer_history` after Step 3:**

| hist_key | customer_id | name    | email                  | city | valid_from          | valid_to            |
|----------|-------------|---------|------------------------|------|---------------------|---------------------|
| 1        | cust1       | Alice   | alice@example.com      | NY   | 2024-01-01 00:00:00 | NULL                |
| 2        | cust2       | Bob     | bob@example.com        | CA   | 2024-01-01 00:00:00 | 2024-06-15 09:00:00 |
| 3        | cust2       | Bobby   | bob@newdomain.com      | TX   | 2024-06-15 09:00:00 | NULL                |
| 4        | cust3       | Charlie | charlie@example.com    | WA   | 2024-06-15 09:00:00 | NULL                |

---

## :material-numeric-8-circle: Step 4 — Upsert Main Dimension with New `hist_key`

```sql
MERGE INTO dim_customer AS tgt
USING (
    SELECT
        h.customer_id,
        h.name,
        h.email,
        h.city,
        h.hist_key,
        current_timestamp() AS updated_at
    FROM dim_customer_history AS h
    JOIN staged_hashed        AS s
        ON  s.customer_id = h.customer_id
        AND s.row_hash    = h.row_hash
    WHERE h.valid_to IS NULL    -- only the freshly-opened active row
) AS src
ON tgt.customer_id = src.customer_id

WHEN MATCHED THEN
    UPDATE SET
        name       = src.name,
        email      = src.email,
        city       = src.city,
        hist_key   = src.hist_key,
        updated_at = src.updated_at

WHEN NOT MATCHED THEN
    INSERT (customer_id, name, email, city, hist_key, updated_at)
    VALUES (src.customer_id, src.name, src.email, src.city,
            src.hist_key, src.updated_at);
```

**`dim_customer` after Step 4:**

| customer_id | name    | email                  | city | hist_key | updated_at          |
|-------------|---------|------------------------|------|----------|---------------------|
| cust1       | Alice   | alice@example.com      | NY   | 1        | 2024-01-01 00:00:00 |
| cust2       | Bobby   | bob@newdomain.com      | TX   | **3**    | 2024-06-15 09:00:00 |
| cust3       | Charlie | charlie@example.com    | WA   | **4**    | 2024-06-15 09:00:00 |

`cust2.hist_key` now points to hist_key 3 — the active TX version.

---

## :material-numeric-9-circle: Assertions After Batch 1

```sql
-- 1. Main dim: exactly 3 rows (1 per customer)
SELECT COUNT(*) AS dim_rows FROM dim_customer;
-- Expected: 3

-- 2. History: exactly 4 rows (2 seed + 2 from batch)
SELECT COUNT(*) AS hist_rows FROM dim_customer_history;
-- Expected: 4

-- 3. No active history row still points to the old cust2 values
SELECT COUNT(*) FROM dim_customer_history
WHERE customer_id = 'cust2' AND valid_to IS NULL AND city = 'CA';
-- Expected: 0

-- 4. hist_key FK is consistent: every main dim row has an active history row
SELECT c.customer_id
FROM dim_customer AS c
LEFT JOIN dim_customer_history AS h
    ON h.hist_key = c.hist_key AND h.valid_to IS NULL
WHERE h.hist_key IS NULL;
-- Expected: 0 rows (no dangling FK)

-- 5. cust1 unchanged: hist_key still 1
SELECT hist_key FROM dim_customer WHERE customer_id = 'cust1';
-- Expected: 1
```

---

## :material-numeric-10-circle: Second Batch — `cust2` Changes Again

```sql
CREATE OR REPLACE TEMP VIEW staging_batch2 AS
SELECT * FROM VALUES ('cust2', 'Bobby', 'bobby@newcorp.com', 'FL')
AS t(customer_id, name, email, city);

-- Reuse the same four-step pattern
CREATE OR REPLACE TEMP VIEW staged_hashed AS
SELECT *, md5(concat_ws('||', name, email, city)) AS row_hash FROM staging_batch2;

-- Step 2: close active row for cust2
MERGE INTO dim_customer_history AS hist
USING (
    SELECT s.customer_id FROM staged_hashed AS s
    JOIN dim_customer AS c ON c.customer_id = s.customer_id
    JOIN dim_customer_history AS dh ON dh.hist_key = c.hist_key
    WHERE dh.row_hash <> s.row_hash
) AS changed
ON hist.customer_id = changed.customer_id AND hist.valid_to IS NULL
WHEN MATCHED THEN UPDATE SET valid_to = current_timestamp();

-- Step 3: insert new history row
INSERT INTO dim_customer_history
    (customer_id, name, email, city, row_hash, valid_from, valid_to)
SELECT s.customer_id, s.name, s.email, s.city, s.row_hash,
       current_timestamp(), NULL
FROM staged_hashed AS s
LEFT JOIN dim_customer AS c ON c.customer_id = s.customer_id
WHERE c.customer_id IS NULL
   OR NOT EXISTS (
       SELECT 1 FROM dim_customer_history AS dh
       WHERE dh.customer_id = s.customer_id
         AND dh.row_hash = s.row_hash AND dh.valid_to IS NULL
   );

-- Step 4: update main dim
MERGE INTO dim_customer AS tgt
USING (
    SELECT h.customer_id, h.name, h.email, h.city, h.hist_key,
           current_timestamp() AS updated_at
    FROM dim_customer_history AS h
    JOIN staged_hashed AS s ON s.customer_id = h.customer_id AND s.row_hash = h.row_hash
    WHERE h.valid_to IS NULL
) AS src
ON tgt.customer_id = src.customer_id
WHEN MATCHED THEN
    UPDATE SET name = src.name, email = src.email, city = src.city,
               hist_key = src.hist_key, updated_at = src.updated_at;
```

**`dim_customer_history` — full trail for `cust2`:**

| hist_key | name  | email                | city | valid_from          | valid_to            |
|----------|-------|----------------------|------|---------------------|---------------------|
| 2        | Bob   | bob@example.com      | CA   | 2024-01-01 00:00:00 | 2024-06-15 09:00:00 |
| 3        | Bobby | bob@newdomain.com    | TX   | 2024-06-15 09:00:00 | 2024-08-01 14:00:00 |
| 5        | Bobby | bobby@newcorp.com    | FL   | 2024-08-01 14:00:00 | NULL                |

---

## :material-numeric-11-circle: The `hist_key` FK Join Pattern

Because `dim_customer.hist_key` always points to the active history row, enriching a query
with the latest history record requires a simple equi-join — no date-range predicate needed:

```sql
SELECT
    c.customer_id,
    c.name          AS current_name,
    c.city          AS current_city,
    h.valid_from    AS current_version_since,
    h.row_hash
FROM dim_customer         AS c
JOIN dim_customer_history AS h ON h.hist_key = c.hist_key;
```

For full history, query the history table directly:

```sql
SELECT
    customer_id,
    name,
    email,
    city,
    valid_from,
    COALESCE(valid_to, current_timestamp()) AS valid_to,
    DATEDIFF(COALESCE(valid_to, current_timestamp()), valid_from) AS days_active
FROM dim_customer_history
WHERE customer_id = 'cust2'
ORDER BY valid_from;
```

---

## :material-numeric-12-circle: Optimise and Clean Up

```sql
OPTIMIZE dim_customer         ZORDER BY (customer_id);
OPTIMIZE dim_customer_history ZORDER BY (valid_from);

VACUUM dim_customer         RETAIN 168 HOURS;
VACUUM dim_customer_history RETAIN 168 HOURS;

DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_customer_history;
DROP VIEW  IF EXISTS staging_batch1;
DROP VIEW  IF EXISTS staging_batch2;
DROP VIEW  IF EXISTS staged_hashed;
```
