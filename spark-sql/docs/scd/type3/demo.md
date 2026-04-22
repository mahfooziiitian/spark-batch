# :material-play-circle: SCD Type 3 — Step-by-Step Demo

A complete walkthrough: create the dimension, seed initial data, process two change batches,
verify state after each, and explore the built-in limitation when a third change arrives.

---

## :material-numeric-1-circle: Create the Dimension Table

```sql
DROP TABLE IF EXISTS dim_customer;

CREATE TABLE dim_customer (
    customer_id     STRING     NOT NULL,
    name            STRING,
    current_city    STRING,
    previous_city   STRING,
    city_changed_at TIMESTAMP,
    updated_at      TIMESTAMP
)
USING DELTA;
```

---

## :material-numeric-2-circle: Seed Initial State

```sql
INSERT INTO dim_customer
    (customer_id, name, current_city, previous_city, city_changed_at, updated_at)
VALUES
    ('cust1', 'Alice', 'NY', NULL, NULL, TIMESTAMP '2024-01-01 00:00:00'),
    ('cust2', 'Bob',   'CA', NULL, NULL, TIMESTAMP '2024-01-01 00:00:00');
```

**State after seed:**

| customer_id | name  | current_city | previous_city | city_changed_at |
|-------------|-------|-------------|---------------|-----------------|
| cust1       | Alice | NY          | NULL          | NULL            |
| cust2       | Bob   | CA          | NULL          | NULL            |

---

## :material-numeric-3-circle: First Incoming Batch

```sql
CREATE OR REPLACE TEMP VIEW staging_batch1 AS
SELECT *
FROM VALUES
    ('cust1', 'Alice',   'NY'),   -- no change
    ('cust2', 'Bobby',   'TX'),   -- name + city changed (CA → TX)
    ('cust3', 'Charlie', 'WA')    -- new customer
AS t(customer_id, name, city);
```

---

## :material-numeric-4-circle: Pre-Merge Inspection

```sql
-- Rows that will UPDATE (city changed)
SELECT
    d.customer_id,
    d.current_city  AS old_city,
    s.city          AS new_city,
    d.previous_city AS will_become_previous
FROM staging_batch1  AS s
JOIN dim_customer    AS d USING (customer_id)
WHERE s.city <> d.current_city;
```

| customer_id | old_city | new_city | will_become_previous |
|-------------|----------|----------|----------------------|
| cust2       | CA       | TX       | CA                   |

```sql
-- Rows that will INSERT (new customers)
SELECT s.*
FROM staging_batch1  AS s
LEFT JOIN dim_customer AS d USING (customer_id)
WHERE d.customer_id IS NULL;
```

| customer_id | name    | city |
|-------------|---------|------|
| cust3       | Charlie | WA   |

---

## :material-numeric-5-circle: Execute Batch 1 MERGE

```sql
MERGE INTO dim_customer AS tgt
USING staging_batch1 AS src
ON src.customer_id = tgt.customer_id

WHEN MATCHED AND src.city <> tgt.current_city THEN
    UPDATE SET
        name            = src.name,
        previous_city   = tgt.current_city,
        current_city    = src.city,
        city_changed_at = current_timestamp(),
        updated_at      = current_timestamp()

WHEN NOT MATCHED THEN
    INSERT (customer_id, name, current_city, previous_city, city_changed_at, updated_at)
    VALUES (src.customer_id, src.name, src.city, NULL, NULL, current_timestamp());
```

**State after Batch 1:**

| customer_id | name    | current_city | previous_city | city_changed_at     |
|-------------|---------|-------------|---------------|---------------------|
| cust1       | Alice   | NY          | NULL          | NULL                |
| cust2       | Bobby   | TX          | **CA**        | 2024-06-15 09:00:00 |
| cust3       | Charlie | WA          | NULL          | NULL                |

- `cust2` — `previous_city` now holds CA, `current_city` = TX.
- `cust1` — untouched; `updated_at` still `2024-01-01`.

---

## :material-numeric-6-circle: Verify Batch 1

```sql
-- 1. Exactly 3 rows total (no versioning — always 1 row per customer)
SELECT COUNT(*) AS total_rows FROM dim_customer;
-- Expected: 3

-- 2. cust2 carries previous city
SELECT current_city, previous_city FROM dim_customer WHERE customer_id = 'cust2';
-- Expected: TX | CA

-- 3. cust1 untouched
SELECT updated_at FROM dim_customer WHERE customer_id = 'cust1';
-- Expected: 2024-01-01 00:00:00 (seed value)

-- 4. cust3 inserted with NULL previous
SELECT previous_city FROM dim_customer WHERE customer_id = 'cust3';
-- Expected: NULL
```

---

## :material-numeric-7-circle: Second Batch — The Limitation Revealed

`cust2` moves again: TX → FL. The **third** city (CA — the original) will be permanently lost.

```sql
CREATE OR REPLACE TEMP VIEW staging_batch2 AS
SELECT * FROM VALUES ('cust2', 'Bobby', 'FL') AS t(customer_id, name, city);

MERGE INTO dim_customer AS tgt
USING staging_batch2 AS src
ON src.customer_id = tgt.customer_id
WHEN MATCHED AND src.city <> tgt.current_city THEN
    UPDATE SET
        previous_city   = tgt.current_city,   -- TX (CA is now gone forever)
        current_city    = src.city,            -- FL
        city_changed_at = current_timestamp(),
        updated_at      = current_timestamp();
```

**State after Batch 2:**

| customer_id | name  | current_city | previous_city | Note |
|-------------|-------|-------------|---------------|------|
| cust2       | Bobby | FL          | TX            | CA (original) is permanently overwritten |

!!! failure "Only one level of history survives"
    Each MERGE overwrites `previous_city` with the value that was in `current_city`.
    After the second change, CA is gone. If you need CA to be recoverable, switch to Type 2.

---

## :material-numeric-8-circle: Idempotency Check

Re-running Batch 1 again after Batch 2 must not corrupt state.

```sql
-- Re-run Batch 1 MERGE
MERGE INTO dim_customer AS tgt
USING staging_batch1 AS src
ON src.customer_id = tgt.customer_id

WHEN MATCHED AND src.city <> tgt.current_city THEN
    UPDATE SET
        name            = src.name,
        previous_city   = tgt.current_city,
        current_city    = src.city,
        city_changed_at = current_timestamp(),
        updated_at      = current_timestamp()

WHEN NOT MATCHED THEN
    INSERT (customer_id, name, current_city, previous_city, city_changed_at, updated_at)
    VALUES (src.customer_id, src.name, src.city, NULL, NULL, current_timestamp());

-- Verify: cust2 should now show TX again (Batch 1 is authoritative for this check)
SELECT current_city, previous_city FROM dim_customer WHERE customer_id = 'cust2';
```

!!! note
    Because `WHEN MATCHED` only fires when `src.city <> tgt.current_city`, a batch
    that delivers the same city as the current value produces zero writes — idempotent
    for the unchanged case.  Re-running with a *different* city legitimately updates again.

---

## :material-numeric-9-circle: Analytical Queries on Final State

```sql
-- Customers who have changed city
SELECT customer_id, name, previous_city, current_city, city_changed_at
FROM dim_customer
WHERE previous_city IS NOT NULL
ORDER BY city_changed_at DESC;

-- Migration count by destination
SELECT current_city, COUNT(*) AS arrivals
FROM dim_customer
WHERE previous_city IS NOT NULL
GROUP BY current_city
ORDER BY arrivals DESC;

-- Days since last move
SELECT
    customer_id,
    name,
    current_city,
    DATEDIFF(current_date(), CAST(city_changed_at AS DATE)) AS days_since_move
FROM dim_customer
WHERE city_changed_at IS NOT NULL;
```

---

## :material-numeric-10-circle: Optimise and Clean Up

```sql
-- Compact files (Type 3 does fewer writes than Type 2, but still benefits from OPTIMIZE)
OPTIMIZE dim_customer ZORDER BY (customer_id);

-- Inspect Delta history
DESCRIBE HISTORY dim_customer;

-- Drop demo tables
DROP TABLE IF EXISTS dim_customer;
DROP VIEW  IF EXISTS staging_batch1;
DROP VIEW  IF EXISTS staging_batch2;
```
