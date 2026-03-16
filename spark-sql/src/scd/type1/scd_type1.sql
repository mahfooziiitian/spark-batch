-- SCD Type 1: Overwrite history
-- In a Type 1 SCD the dimension row is updated in place whenever source data changes.
-- No history is preserved — the table always reflects the latest known values.

---------------------------------------------------------------------------------------------------
-- Setup: dimension table
---------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS dim_customer;

CREATE TABLE dim_customer (
    customer_id INT,
    name STRING,
    email STRING,
    city STRING,
    updated_at TIMESTAMP
) USING DELTA;

-- Seed the dimension with initial values
INSERT INTO dim_customer VALUES
(1, 'Alice', 'alice@example.com', 'New York', CURRENT_TIMESTAMP()),
(2, 'Bob', 'bob@example.com', 'Chicago', CURRENT_TIMESTAMP()),
(3, 'Carol', 'carol@example.com', 'Austin', CURRENT_TIMESTAMP());

---------------------------------------------------------------------------------------------------
-- Setup: staging (incoming) data
---------------------------------------------------------------------------------------------------

-- Represents the latest state coming from the source system.
-- Includes updated values for existing customers and a brand-new customer.
CREATE OR REPLACE TEMP VIEW staging_customer AS
SELECT
    customer_id,
    name,
    email,
    city
FROM
    VALUES
    (1, 'Alice', 'alice@newemail.com', 'Boston'),   -- email and city changed
    (2, 'Bob', 'bob@example.com', 'Chicago'),  -- no change
    (4, 'Dave', 'dave@example.com', 'Seattle')   -- new customer
        AS t (customer_id, name, email, city);

---------------------------------------------------------------------------------------------------
-- 1. Basic Type 1 upsert: UPDATE matched rows, INSERT new rows
--    Every matched row is overwritten regardless of whether any value changed.
---------------------------------------------------------------------------------------------------

MERGE INTO dim_customer AS t
USING staging_customer AS s
    ON t.customer_id = s.customer_id
WHEN MATCHED THEN
    UPDATE SET
        t.name = s.name,
        t.email = s.email,
        t.city = s.city,
        t.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (customer_id, name, email, city, updated_at)
    VALUES (s.customer_id, s.name, s.email, s.city, CURRENT_TIMESTAMP());

-- Result: Alice's email and city are overwritten; Bob's row is re-written (same values);
--         Dave is inserted; Carol (not in staging) is untouched.

---------------------------------------------------------------------------------------------------
-- 2. Conditional update: only touch rows where at least one attribute has changed.
--    This avoids unnecessary write amplification and Delta commit overhead.
---------------------------------------------------------------------------------------------------

MERGE INTO dim_customer AS t
USING staging_customer AS s
    ON t.customer_id = s.customer_id
WHEN MATCHED AND (
    t.name <> s.name
    OR t.email <> s.email
    OR t.city <> s.city
) THEN
    UPDATE SET
        t.name = s.name,
        t.email = s.email,
        t.city = s.city,
        t.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (customer_id, name, email, city, updated_at)
    VALUES (s.customer_id, s.name, s.email, s.city, CURRENT_TIMESTAMP());

-- Result: Bob's row (unchanged) is skipped; Alice and Dave are written as before.

---------------------------------------------------------------------------------------------------
-- Verify final state
---------------------------------------------------------------------------------------------------

SELECT
    customer_id,
    name,
    email,
    city,
    updated_at
FROM dim_customer
ORDER BY customer_id;

-- Type 1 note: customer 1's previous email/city are gone forever — no history is kept.
