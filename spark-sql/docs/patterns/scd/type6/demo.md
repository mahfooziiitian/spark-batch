# :material-play-circle: SCD Type 6 — Step-by-Step Demo

A complete walkthrough of the Type 1 + 2 + 3 hybrid: create the table, seed it, process
two change batches, verify every layer (current values, version history, previous-value column,
back-fill), and explore point-in-time queries.

---

## :material-numeric-1-circle: Create the Table

```sql
DROP TABLE IF EXISTS dim_customer;

CREATE TABLE dim_customer (
    customer_sk  BIGINT GENERATED ALWAYS AS IDENTITY,  -- surrogate key (version ID)
    customer_id  STRING      NOT NULL,                  -- natural / business key
    name         STRING,
    email        STRING,
    city         STRING,     -- Type 1: always holds the CURRENT city on every row
    prev_city    STRING,     -- Type 3: city before the most recent change (NULL if never changed)
    row_hash     STRING,     -- change-detection fingerprint
    start_date   TIMESTAMP,  -- Type 2: when this version became active
    end_date     TIMESTAMP,  -- Type 2: when this version was closed (NULL = active)
    is_current   BOOLEAN     -- Type 2: convenience filter flag
)
USING DELTA
PARTITIONED BY (is_current);
```

---

## :material-numeric-2-circle: Seed Initial State

```sql
INSERT INTO dim_customer
    (customer_id, name, email, city, prev_city, row_hash, start_date, end_date, is_current)
SELECT
    customer_id,
    name,
    email,
    city,
    NULL                                     AS prev_city,
    md5(concat_ws('||', name, email, city))  AS row_hash,
    TIMESTAMP '2024-01-01 00:00:00'          AS start_date,
    NULL                                     AS end_date,
    TRUE                                     AS is_current
FROM VALUES
    ('cust1', 'Alice', 'alice@example.com', 'NY'),
    ('cust2', 'Bob',   'bob@example.com',   'CA')
AS t(customer_id, name, email, city);
```

**State after seed** (2 active rows, no history):

| customer_sk | customer_id | name  | email               | city | prev_city | is_current | start_date          | end_date |
|-------------|-------------|-------|---------------------|------|-----------|------------|---------------------|----------|
| 1           | cust1       | Alice | alice@example.com   | NY   | NULL      | true       | 2024-01-01 00:00:00 | NULL     |
| 2           | cust2       | Bob   | bob@example.com     | CA   | NULL      | true       | 2024-01-01 00:00:00 | NULL     |

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
-- Classify each incoming row before any writes
WITH hashed AS (
    SELECT *, md5(concat_ws('||', name, email, city)) AS new_hash
    FROM staging_batch1
)
SELECT
    h.customer_id,
    h.name       AS new_name,
    d.name       AS old_name,
    d.city       AS old_city,
    h.city       AS new_city,
    d.prev_city,
    CASE
        WHEN d.customer_id IS NULL          THEN 'NEW'
        WHEN d.row_hash   <> h.new_hash     THEN 'CHANGED'
        ELSE                                     'UNCHANGED'
    END AS action
FROM hashed AS h
LEFT JOIN dim_customer AS d
    ON  d.customer_id = h.customer_id
    AND d.is_current  = TRUE;
```

| customer_id | new_name | old_name | old_city | new_city | prev_city | action    |
|-------------|----------|----------|----------|----------|-----------|-----------|
| cust1       | Alice    | Alice    | NY       | NY       | NULL      | UNCHANGED |
| cust2       | Bobby    | Bob      | CA       | TX       | NULL      | CHANGED   |
| cust3       | Charlie  | NULL     | NULL     | WA       | NULL      | NEW       |

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

## :material-numeric-6-circle: Step 2 — Identify Changes and New Customers

```sql
CREATE OR REPLACE TEMP VIEW changes AS
SELECT
    s.customer_id,
    s.name,
    s.email,
    s.city,
    s.row_hash,
    d.city      AS old_city,
    d.prev_city AS old_prev_city,
    d.customer_sk IS NOT NULL AS is_existing
FROM staged_hashed AS s
LEFT JOIN dim_customer AS d
    ON  d.customer_id = s.customer_id
    AND d.is_current  = TRUE
WHERE d.customer_id IS NULL     -- new customer
   OR d.row_hash   <> s.row_hash;  -- changed customer
```

---

## :material-numeric-7-circle: Step 3 — Expire Active Rows for Changed Customers

```sql
MERGE INTO dim_customer AS tgt
USING (
    SELECT customer_id FROM changes WHERE is_existing = TRUE
) AS src
ON  tgt.customer_id = src.customer_id
AND tgt.is_current  = TRUE

WHEN MATCHED THEN
    UPDATE SET
        end_date   = current_timestamp(),
        is_current = FALSE;
```

**After Step 3** — `cust2`'s original row is now closed:

| customer_sk | customer_id | name | city | prev_city | is_current | end_date            |
|-------------|-------------|------|------|-----------|------------|---------------------|
| 2           | cust2       | Bob  | CA   | NULL      | **false**  | 2024-06-15 09:00:00 |

---

## :material-numeric-8-circle: Step 4 — Insert New Version Rows

```sql
INSERT INTO dim_customer
    (customer_id, name, email, city, prev_city, row_hash, start_date, end_date, is_current)
SELECT
    c.customer_id,
    c.name,
    c.email,
    c.city,
    CASE
        WHEN c.old_city IS NOT NULL AND c.old_city <> c.city THEN c.old_city
        WHEN c.old_city IS NOT NULL                          THEN c.old_prev_city
        ELSE NULL
    END                  AS prev_city,
    c.row_hash,
    current_timestamp()  AS start_date,
    NULL                 AS end_date,
    TRUE                 AS is_current
FROM changes AS c;
```

**After Step 4** — new active rows inserted:

| customer_sk | customer_id | name    | email                  | city | prev_city | is_current |
|-------------|-------------|---------|------------------------|------|-----------|------------|
| 3           | cust2       | Bobby   | bob@newdomain.com      | TX   | **CA**    | true       |
| 4           | cust3       | Charlie | charlie@example.com    | WA   | NULL      | true       |

`cust2`'s new row carries `prev_city = CA` — the Type 3 memory.

---

## :material-numeric-9-circle: Step 5 — Back-Fill Current City on Historical Rows

This is Type 6's signature step: overwrite `city` on all expired rows so they reflect
the customer's **current** city, not the city they had at the time of that version.

```sql
MERGE INTO dim_customer AS tgt
USING (
    SELECT customer_id, city AS current_city
    FROM dim_customer
    WHERE is_current = TRUE
) AS cur
ON  tgt.customer_id = cur.customer_id
AND tgt.is_current  = FALSE
AND tgt.city       <> cur.current_city

WHEN MATCHED THEN
    UPDATE SET city = cur.current_city;
```

**Full table after Step 5:**

| customer_sk | customer_id | name    | email                  | city | prev_city | is_current | start_date          | end_date            |
|-------------|-------------|---------|------------------------|------|-----------|------------|---------------------|---------------------|
| 1           | cust1       | Alice   | alice@example.com      | NY   | NULL      | true       | 2024-01-01 00:00:00 | NULL                |
| 2           | cust2       | Bobby   | bob@example.com        | **TX** | NULL   | false      | 2024-01-01 00:00:00 | 2024-06-15 09:00:00 |
| 3           | cust2       | Bobby   | bob@newdomain.com      | TX   | CA        | true       | 2024-06-15 09:00:00 | NULL                |
| 4           | cust3       | Charlie | charlie@example.com    | WA   | NULL      | true       | 2024-06-15 09:00:00 | NULL                |

Row 2 (expired `cust2`) now shows `city = TX` — back-filled from the current active row.
The original `CA` value survives only in row 3's `prev_city`.

---

## :material-numeric-10-circle: Assertions After Batch 1

```sql
-- 1. Exactly 3 active rows
SELECT COUNT(*) FROM dim_customer WHERE is_current = TRUE;
-- Expected: 3

-- 2. cust2 has exactly 2 version rows
SELECT COUNT(*) FROM dim_customer WHERE customer_id = 'cust2';
-- Expected: 2

-- 3. Back-fill worked: expired cust2 row shows TX, not CA
SELECT city FROM dim_customer WHERE customer_sk = 2;
-- Expected: TX

-- 4. prev_city correctly set on new active row
SELECT prev_city FROM dim_customer WHERE customer_id = 'cust2' AND is_current = TRUE;
-- Expected: CA

-- 5. cust1 untouched (no version added, updated_at unchanged)
SELECT COUNT(*) FROM dim_customer WHERE customer_id = 'cust1';
-- Expected: 1 (still only the seed row)

-- 6. No duplicate active rows
SELECT customer_id, COUNT(*) AS cnt
FROM dim_customer WHERE is_current = TRUE
GROUP BY customer_id HAVING cnt > 1;
-- Expected: 0 rows
```

---

## :material-numeric-11-circle: Second Batch — `cust2` Changes City Again

`cust2` moves from TX → FL. After this, `prev_city` on the new row will hold TX,
and all older history rows will be back-filled to FL.

```sql
CREATE OR REPLACE TEMP VIEW staging_batch2 AS
SELECT * FROM VALUES ('cust2', 'Bobby', 'bobby@newcorp.com', 'FL')
AS t(customer_id, name, email, city);

-- Re-run all five steps for the new batch
CREATE OR REPLACE TEMP VIEW staged_hashed AS
SELECT *, md5(concat_ws('||', name, email, city)) AS row_hash FROM staging_batch2;

CREATE OR REPLACE TEMP VIEW changes AS
SELECT s.customer_id, s.name, s.email, s.city, s.row_hash,
       d.city AS old_city, d.prev_city AS old_prev_city,
       TRUE AS is_existing
FROM staged_hashed AS s
JOIN dim_customer AS d ON d.customer_id = s.customer_id AND d.is_current = TRUE
WHERE d.row_hash <> s.row_hash;

-- Step 3: expire
MERGE INTO dim_customer AS tgt
USING (SELECT customer_id FROM changes) AS src
ON tgt.customer_id = src.customer_id AND tgt.is_current = TRUE
WHEN MATCHED THEN UPDATE SET end_date = current_timestamp(), is_current = FALSE;

-- Step 4: insert new version
INSERT INTO dim_customer
    (customer_id, name, email, city, prev_city, row_hash, start_date, end_date, is_current)
SELECT c.customer_id, c.name, c.email, c.city,
    CASE WHEN c.old_city <> c.city THEN c.old_city ELSE c.old_prev_city END,
    c.row_hash, current_timestamp(), NULL, TRUE
FROM changes AS c;

-- Step 5: back-fill current city on all historical rows
MERGE INTO dim_customer AS tgt
USING (SELECT customer_id, city AS current_city FROM dim_customer WHERE is_current = TRUE) AS cur
ON tgt.customer_id = cur.customer_id AND tgt.is_current = FALSE AND tgt.city <> cur.current_city
WHEN MATCHED THEN UPDATE SET city = cur.current_city;
```

**Full table after Batch 2:**

| customer_sk | customer_id | name  | city     | prev_city | is_current | start_date          | end_date            |
|-------------|-------------|-------|----------|-----------|------------|---------------------|---------------------|
| 1           | cust1       | Alice | NY       | NULL      | true       | 2024-01-01 00:00:00 | NULL                |
| 2           | cust2       | Bobby | **FL**   | NULL      | false      | 2024-01-01 00:00:00 | 2024-06-15 09:00:00 |
| 3           | cust2       | Bobby | **FL**   | CA        | false      | 2024-06-15 09:00:00 | 2024-08-01 14:00:00 |
| 4           | cust3       | Charlie | WA     | NULL      | true       | 2024-06-15 09:00:00 | NULL                |
| 5           | cust2       | Bobby | FL       | **TX**    | true       | 2024-08-01 14:00:00 | NULL                |

All three `cust2` rows now carry `city = FL` (back-filled). `prev_city` on row 5 shows TX.

---

## :material-numeric-12-circle: Point-in-Time Query

What was `cust2`'s state on 2024-03-15 (before any change)?

```sql
SELECT customer_sk, name, email, city, prev_city, start_date, end_date
FROM dim_customer
WHERE customer_id = 'cust2'
  AND start_date  <= TIMESTAMP '2024-03-15 00:00:00'
  AND (end_date    > TIMESTAMP '2024-03-15 00:00:00' OR end_date IS NULL);
```

| customer_sk | name | email           | city | prev_city | start_date          | end_date            |
|-------------|------|-----------------|------|-----------|---------------------|---------------------|
| 2           | Bobby | bob@example.com | FL  | NULL      | 2024-01-01 00:00:00 | 2024-06-15 09:00:00 |

!!! note "city = FL, not CA"
    The Type 1 back-fill means the historical row now shows FL (current), not CA (original).
    For the original CA value, look at the next active row's `prev_city = CA` (customer_sk 3).

---

## :material-numeric-13-circle: Current-State Convenience View

```sql
CREATE OR REPLACE VIEW dim_customer_current AS
SELECT * FROM dim_customer WHERE is_current = TRUE;

-- Downstream queries use the view — no is_current filter needed
SELECT customer_id, name, city, prev_city
FROM dim_customer_current
ORDER BY customer_id;
```

---

## :material-numeric-14-circle: Optimise and Clean Up

```sql
-- Compact + co-locate by customer
OPTIMIZE dim_customer ZORDER BY (customer_id);

-- Enforce date-range integrity
ALTER TABLE dim_customer
ADD CONSTRAINT valid_dates
    CHECK (end_date IS NULL OR end_date > start_date);

-- Delta file cleanup
VACUUM dim_customer RETAIN 168 HOURS;

-- Inspect commit history
DESCRIBE HISTORY dim_customer;

-- Drop demo objects
DROP TABLE IF EXISTS dim_customer;
DROP VIEW  IF EXISTS dim_customer_current;
DROP VIEW  IF EXISTS staging_batch1;
DROP VIEW  IF EXISTS staging_batch2;
DROP VIEW  IF EXISTS staged_hashed;
DROP VIEW  IF EXISTS changes;
```
