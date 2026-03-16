-- SCD Type 3: Track one previous value per attribute
-- In a Type 3 SCD the dimension table holds both the current value and the immediately
-- previous value for each tracked attribute.  Only one prior version is stored — any
-- changes before the most recent are permanently lost.
-- Use Type 3 when "where was this customer before?" is useful but full history is not needed.

---------------------------------------------------------------------------------------------------
-- Setup: dimension table
---------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS dim_customer;

CREATE TABLE dim_customer (
    customer_id INT,
    name STRING,
    current_city STRING,
    previous_city STRING,    -- NULL if the customer has never moved
    current_since DATE       -- date when current_city became active
) USING DELTA;

-- Seed with initial customer values (no prior city yet)
INSERT INTO dim_customer VALUES
(1, 'Alice', 'New York', NULL, DATE '2023-01-01'),
(2, 'Bob', 'Chicago', NULL, DATE '2023-01-01'),
(3, 'Carol', 'Austin', NULL, DATE '2023-01-01');

---------------------------------------------------------------------------------------------------
-- Setup: staging data (latest incoming values from the source system)
---------------------------------------------------------------------------------------------------

CREATE OR REPLACE TEMP VIEW staging_customer AS
SELECT
    customer_id,
    name,
    city
FROM
    VALUES
    (1, 'Alice', 'Boston'),    -- Alice relocated: New York -> Boston
    (2, 'Bob', 'Chicago'),   -- Bob unchanged
    (4, 'Dave', 'Seattle')    -- Dave is a new customer
        AS t (customer_id, name, city);

---------------------------------------------------------------------------------------------------
-- 1. MERGE to apply a Type 3 update
--    For rows where city has changed:
--      - shift current_city into previous_city
--      - set current_city to the new value
--      - update current_since to today
--    For unchanged rows: no-op (no WHEN MATCHED without condition = skip if no change)
--    For new customers: insert with previous_city = NULL
---------------------------------------------------------------------------------------------------

MERGE INTO dim_customer AS t
USING staging_customer AS s
    ON t.customer_id = s.customer_id
WHEN MATCHED AND t.current_city <> s.city THEN
    UPDATE SET
        t.name = s.name,
        t.previous_city = t.current_city,   -- shift current -> previous
        t.current_city = s.city,           -- apply new value
        t.current_since = CURRENT_DATE()
WHEN NOT MATCHED THEN
    INSERT (customer_id, name, current_city, previous_city, current_since)
    VALUES (s.customer_id, s.name, s.city, NULL, CURRENT_DATE());

-- Result:
--   Alice:  previous_city = 'New York', current_city = 'Boston'   (Result: city shifted)
--   Bob:    unchanged (current_city = 'Chicago' already matches)
--   Dave:   new row inserted with previous_city = NULL             (Result: new customer)
--   Carol:  not in staging, therefore untouched                    (Result: no change)

---------------------------------------------------------------------------------------------------
-- 2. Query to compare current and previous city for all customers
---------------------------------------------------------------------------------------------------

SELECT
    customer_id,
    name,
    current_city,
    previous_city,
    current_since,
    CASE
        WHEN previous_city IS NULL THEN 'No prior location on record'
        ELSE previous_city || ' -> ' || current_city
    END AS city_change_summary
FROM dim_customer
ORDER BY customer_id;

-- Result: human-readable migration path per customer

---------------------------------------------------------------------------------------------------
-- 3. Filter customers who have relocated at least once
---------------------------------------------------------------------------------------------------

SELECT
    customer_id,
    name,
    previous_city,
    current_city,
    current_since
FROM dim_customer
WHERE previous_city IS NOT NULL
ORDER BY current_since DESC;

-- Result: only Alice (who moved) appears; Bob and Dave are excluded

-- Type 3 note: if Alice moves again (Boston -> Denver), 'New York' is lost permanently.
--              Only the most recent transition (Boston -> Denver) would be stored.
