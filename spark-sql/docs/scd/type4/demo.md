# :material-play-circle: SCD Type 4 — Step-by-Step Demo

A complete walkthrough: create both tables, seed initial state, process a change batch
across two steps, verify each table, run a second batch, and explore point-in-time queries.

---

## :material-numeric-1-circle: Create Both Tables

```sql
DROP TABLE IF EXISTS dim_customer_current;
DROP TABLE IF EXISTS dim_customer_history;

CREATE TABLE dim_customer_current (
    customer_id  STRING    NOT NULL,
    name         STRING,
    email        STRING,
    city         STRING,
    row_hash     STRING,
    updated_at   TIMESTAMP
)
USING DELTA;

CREATE TABLE dim_customer_history (
    customer_id  STRING    NOT NULL,
    name         STRING,
    email        STRING,
    city         STRING,
    row_hash     STRING,
    valid_from   TIMESTAMP NOT NULL,
    valid_to     TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (customer_id);
```

---

## :material-numeric-2-circle: Seed the Current Table

No history rows on initial load — there is nothing to supersede yet.

```sql
INSERT INTO dim_customer_current (customer_id, name, email, city, row_hash, updated_at)
SELECT
    customer_id,
    name,
    email,
    city,
    md5(concat_ws('||', name, email, city)) AS row_hash,
    TIMESTAMP '2024-01-01 00:00:00'          AS updated_at
FROM VALUES
    ('cust1', 'Alice', 'alice@example.com', 'NY'),
    ('cust2', 'Bob',   'bob@example.com',   'CA')
AS t(customer_id, name, email, city);
```

**State after seed:**

`dim_customer_current`

| customer_id | name  | email               | city | updated_at          |
|-------------|-------|---------------------|------|---------------------|
| cust1       | Alice | alice@example.com   | NY   | 2024-01-01 00:00:00 |
| cust2       | Bob   | bob@example.com     | CA   | 2024-01-01 00:00:00 |

`dim_customer_history` — **empty** (0 rows)

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
WITH hashed AS (
    SELECT *, md5(concat_ws('||', name, email, city)) AS new_hash
    FROM staging_batch1
)
SELECT
    h.customer_id,
    h.name        AS new_name,
    c.name        AS old_name,
    c.city        AS old_city,
    h.city        AS new_city,
    CASE
        WHEN c.customer_id IS NULL    THEN 'NEW'
        WHEN c.row_hash <> h.new_hash THEN 'CHANGED'
        ELSE                               'UNCHANGED'
    END AS action
FROM hashed AS h
LEFT JOIN dim_customer_current AS c USING (customer_id);
```

| customer_id | new_name | old_name | old_city | new_city | action    |
|-------------|----------|----------|----------|----------|-----------|
| cust1       | Alice    | Alice    | NY       | NY       | UNCHANGED |
| cust2       | Bobby    | Bob      | CA       | TX       | CHANGED   |
| cust3       | Charlie  | NULL     | NULL     | WA       | NEW       |

---

## :material-numeric-5-circle: Step 1 — Archive Changed Rows

```sql
INSERT INTO dim_customer_history
    (customer_id, name, email, city, row_hash, valid_from, valid_to)
SELECT
    c.customer_id,
    c.name,
    c.email,
    c.city,
    c.row_hash,
    c.updated_at        AS valid_from,
    current_timestamp() AS valid_to
FROM dim_customer_current AS c
WHERE c.row_hash <> (
    SELECT md5(concat_ws('||', s.name, s.email, s.city))
    FROM staging_batch1 AS s
    WHERE s.customer_id = c.customer_id
);
```

**`dim_customer_history` after Step 1:**

| customer_id | name | email           | city | valid_from          | valid_to            |
|-------------|------|-----------------|------|---------------------|---------------------|
| cust2       | Bob  | bob@example.com | CA   | 2024-01-01 00:00:00 | 2024-06-15 09:00:00 |

---

## :material-numeric-6-circle: Step 2 — Upsert the Current Table

```sql
MERGE INTO dim_customer_current AS tgt
USING (
    SELECT
        customer_id,
        name,
        email,
        city,
        md5(concat_ws('||', name, email, city)) AS row_hash,
        current_timestamp()                      AS updated_at
    FROM staging_batch1
) AS src
ON tgt.customer_id = src.customer_id

WHEN MATCHED AND tgt.row_hash <> src.row_hash THEN
    UPDATE SET
        name       = src.name,
        email      = src.email,
        city       = src.city,
        row_hash   = src.row_hash,
        updated_at = src.updated_at

WHEN NOT MATCHED THEN
    INSERT (customer_id, name, email, city, row_hash, updated_at)
    VALUES (src.customer_id, src.name, src.email, src.city,
            src.row_hash, src.updated_at);
```

**`dim_customer_current` after Step 2:**

| customer_id | name    | email                  | city | updated_at          |
|-------------|---------|------------------------|------|---------------------|
| cust1       | Alice   | alice@example.com      | NY   | 2024-01-01 00:00:00 |
| cust2       | Bobby   | bob@newdomain.com      | TX   | 2024-06-15 09:00:00 |
| cust3       | Charlie | charlie@example.com    | WA   | 2024-06-15 09:00:00 |

---

## :material-numeric-7-circle: Assertions After Batch 1

```sql
-- 1. Current table has exactly 3 rows
SELECT COUNT(*) AS current_rows FROM dim_customer_current;
-- Expected: 3

-- 2. History table has exactly 1 row (only cust2 changed)
SELECT COUNT(*) AS history_rows FROM dim_customer_history;
-- Expected: 1

-- 3. cust1 untouched in current table
SELECT updated_at FROM dim_customer_current WHERE customer_id = 'cust1';
-- Expected: 2024-01-01 (seed timestamp)

-- 4. cust2 history row preserves the old values
SELECT name, email, city FROM dim_customer_history WHERE customer_id = 'cust2';
-- Expected: Bob | bob@example.com | CA

-- 5. cust3 has no history (was new — nothing to archive)
SELECT COUNT(*) FROM dim_customer_history WHERE customer_id = 'cust3';
-- Expected: 0
```

---

## :material-numeric-8-circle: Second Batch — `cust2` Changes Again

```sql
CREATE OR REPLACE TEMP VIEW staging_batch2 AS
SELECT * FROM VALUES
    ('cust2', 'Bobby', 'bob@corp.com', 'FL')
AS t(customer_id, name, email, city);

-- Step 1: archive current cust2 row
INSERT INTO dim_customer_history
    (customer_id, name, email, city, row_hash, valid_from, valid_to)
SELECT
    c.customer_id, c.name, c.email, c.city, c.row_hash,
    c.updated_at        AS valid_from,
    current_timestamp() AS valid_to
FROM dim_customer_current AS c
WHERE c.row_hash <> (
    SELECT md5(concat_ws('||', s.name, s.email, s.city))
    FROM staging_batch2 AS s
    WHERE s.customer_id = c.customer_id
);

-- Step 2: overwrite current table
MERGE INTO dim_customer_current AS tgt
USING (
    SELECT *, md5(concat_ws('||', name, email, city)) AS row_hash,
              current_timestamp() AS updated_at
    FROM staging_batch2
) AS src
ON tgt.customer_id = src.customer_id
WHEN MATCHED AND tgt.row_hash <> src.row_hash THEN
    UPDATE SET name = src.name, email = src.email, city = src.city,
               row_hash = src.row_hash, updated_at = src.updated_at;
```

**`dim_customer_history` now shows two rows for `cust2`:**

| customer_id | name  | email                | city | valid_from          | valid_to            |
|-------------|-------|----------------------|------|---------------------|---------------------|
| cust2       | Bob   | bob@example.com      | CA   | 2024-01-01 00:00:00 | 2024-06-15 09:00:00 |
| cust2       | Bobby | bob@newdomain.com    | TX   | 2024-06-15 09:00:00 | 2024-08-01 14:00:00 |

---

## :material-numeric-9-circle: Point-in-Time Query

What did `cust2` look like on 2024-05-01?

```sql
-- Check history first (covers closed versions)
SELECT customer_id, name, email, city, valid_from, valid_to, 'historical' AS source
FROM dim_customer_history
WHERE customer_id = 'cust2'
  AND valid_from  <= TIMESTAMP '2024-05-01 00:00:00'
  AND valid_to    >  TIMESTAMP '2024-05-01 00:00:00'

UNION ALL

-- Fall through to current table only if the timestamp post-dates the last change
SELECT customer_id, name, email, city, updated_at AS valid_from, NULL AS valid_to, 'current' AS source
FROM dim_customer_current
WHERE customer_id = 'cust2'
  AND updated_at  <= TIMESTAMP '2024-05-01 00:00:00';
```

| customer_id | name | email           | city | valid_from          | valid_to            | source     |
|-------------|------|-----------------|------|---------------------|---------------------|------------|
| cust2       | Bob  | bob@example.com | CA   | 2024-01-01 00:00:00 | 2024-06-15 09:00:00 | historical |

---

## :material-numeric-10-circle: Full Timeline View

Union current and history into a single chronological timeline:

```sql
SELECT
    customer_id,
    name,
    email,
    city,
    valid_from,
    valid_to,
    FALSE AS is_current
FROM dim_customer_history

UNION ALL

SELECT
    customer_id,
    name,
    email,
    city,
    updated_at  AS valid_from,
    NULL        AS valid_to,
    TRUE        AS is_current
FROM dim_customer_current

ORDER BY customer_id, valid_from;
```

---

## :material-numeric-11-circle: Optimise and Clean Up

```sql
-- Compact current table
OPTIMIZE dim_customer_current ZORDER BY (customer_id);

-- Compact history table (partition per customer keeps this targeted)
OPTIMIZE dim_customer_history ZORDER BY (valid_from);

-- Remove file snapshots older than 7 days
VACUUM dim_customer_current  RETAIN 168 HOURS;
VACUUM dim_customer_history  RETAIN 168 HOURS;

-- Drop demo tables
DROP TABLE IF EXISTS dim_customer_current;
DROP TABLE IF EXISTS dim_customer_history;
DROP VIEW  IF EXISTS staging_batch1;
DROP VIEW  IF EXISTS staging_batch2;
```
