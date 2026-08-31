-- SCD Type 2: Preserve full history
-- In a Type 2 SCD every change to a dimension attribute creates a new row with a new
-- effective date range.  The old row is "closed" (effective_to set, is_current = false)
-- and the new row is "opened" (effective_from = today, is_current = true).
-- A surrogate key uniquely identifies each version of a dimension member.

---------------------------------------------------------------------------------------------------
-- Setup: dimension table
---------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS dim_customer;

CREATE TABLE dim_customer (
    surrogate_key  BIGINT,
    customer_id    INT,
    name           STRING,
    email          STRING,
    city           STRING,
    effective_from DATE,
    effective_to   DATE,       -- NULL means the row is still current (open-ended)
    is_current     BOOLEAN
) USING DELTA;

-- Seed with initial customer versions (all current, no end date)
INSERT INTO dim_customer (customer_id, name, email, city, effective_from, effective_to, is_current)
VALUES
    (1, 'Alice', 'alice@example.com', 'New York', DATE '2023-01-01', NULL, TRUE),
    (2, 'Bob',   'bob@example.com',   'Chicago',  DATE '2023-01-01', NULL, TRUE),
    (3, 'Carol', 'carol@example.com', 'Austin',   DATE '2023-01-01', NULL, TRUE);

---------------------------------------------------------------------------------------------------
-- Setup: staging (incoming) data
---------------------------------------------------------------------------------------------------

CREATE OR REPLACE TEMP VIEW staging_customer AS
SELECT customer_id, name, email, city
FROM
    VALUES
    (1, 'Alice', 'alice@newemail.com', 'Boston'),   -- email + city changed
    (2, 'Bob',   'bob@example.com',    'Chicago'),  -- no change
    (4, 'Dave',  'dave@example.com',   'Seattle')   -- brand-new customer
        AS t (customer_id, name, email, city);

---------------------------------------------------------------------------------------------------
-- 1. Two-step SCD Type 2 approach
--
-- Step 1: Close existing current records that have changed.
--         Set is_current = false and effective_to = today.
-- Step 2: Insert the new current version for those same customers plus any new customers.
---------------------------------------------------------------------------------------------------

-- Step 1: close changed rows
MERGE INTO dim_customer AS t
USING staging_customer AS s
    ON t.customer_id = s.customer_id
   AND t.is_current  = TRUE
WHEN MATCHED AND (
    t.name  <> s.name  OR
    t.email <> s.email OR
    t.city  <> s.city
) THEN
    UPDATE SET
        t.is_current   = FALSE,
        t.effective_to = CURRENT_DATE();

-- Step 2: open new current rows for changed + new customers
INSERT INTO dim_customer (customer_id, name, email, city, effective_from, effective_to, is_current)
SELECT s.customer_id, s.name, s.email, s.city, CURRENT_DATE(), NULL, TRUE
FROM staging_customer AS s
LEFT JOIN dim_customer AS t
    ON s.customer_id = t.customer_id
   AND t.is_current  = TRUE
WHERE
    t.customer_id IS NULL                       -- brand-new customer
    OR t.name  <> s.name                        -- any attribute changed
    OR t.email <> s.email
    OR t.city  <> s.city;

-- Result: Alice has two rows (old closed, new open); Bob unchanged; Dave inserted;
--         Carol (not in staging) remains current and is untouched.

---------------------------------------------------------------------------------------------------
-- 2. Single-MERGE approach (Databricks Delta extension)
--    NOT MATCHED BY SOURCE closes rows that disappeared from the source entirely.
--    This variant is useful for a full-refresh feed where absence = deletion.
--    NOTE: inserting a new version still requires a second INSERT statement because
--          MERGE cannot insert two rows for the same key in one pass.
---------------------------------------------------------------------------------------------------

-- Close rows that are no longer present in the source at all
MERGE INTO dim_customer AS t
USING staging_customer AS s
    ON t.customer_id = s.customer_id
   AND t.is_current  = TRUE
WHEN MATCHED AND (
    t.name  <> s.name  OR
    t.email <> s.email OR
    t.city  <> s.city
) THEN
    UPDATE SET
        t.is_current   = FALSE,
        t.effective_to = CURRENT_DATE()
WHEN NOT MATCHED BY SOURCE AND t.is_current = TRUE THEN
    UPDATE SET
        t.is_current   = FALSE,
        t.effective_to = CURRENT_DATE();

-- Then open new current rows (same INSERT as Step 2 above)
INSERT INTO dim_customer (customer_id, name, email, city, effective_from, effective_to, is_current)
SELECT s.customer_id, s.name, s.email, s.city, CURRENT_DATE(), NULL, TRUE
FROM staging_customer AS s
LEFT JOIN dim_customer AS t
    ON s.customer_id = t.customer_id
   AND t.is_current  = TRUE
WHERE
    t.customer_id IS NULL
    OR t.name  <> s.name
    OR t.email <> s.email
    OR t.city  <> s.city;

---------------------------------------------------------------------------------------------------
-- 3. Current snapshot query
--    Returns the single active row per customer.
---------------------------------------------------------------------------------------------------

SELECT customer_id, name, email, city, effective_from
FROM dim_customer
WHERE is_current = TRUE
ORDER BY customer_id;

-- Result: one row per customer, showing the most recent attributes

---------------------------------------------------------------------------------------------------
-- 4. Point-in-time snapshot query
--    Returns the dimension values that were active on a specific date.
---------------------------------------------------------------------------------------------------

SELECT customer_id, name, email, city, effective_from, effective_to
FROM dim_customer
WHERE
    effective_from <= DATE '2024-06-01'
    AND (effective_to > DATE '2024-06-01' OR effective_to IS NULL)
ORDER BY customer_id;

-- Result: the row that was current for each customer on 2024-06-01
-- Type 2 note: all historical versions are preserved; no data is ever physically deleted.
