-- DELETE examples for Delta Lake
-- DELETE is only supported on Delta (and other transactional) tables.
-- After deleting rows, run VACUUM to physically reclaim the underlying Parquet files;
-- until VACUUM runs the deleted data remains on disk (retained for time-travel).

---------------------------------------------------------------------------------------------------
-- Setup: target Delta tables
---------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS orders;

CREATE TABLE orders (
    order_id INT,
    customer STRING,
    amount DOUBLE,
    region STRING,
    status STRING,
    order_date DATE
) USING DELTA
PARTITIONED BY (region);

INSERT INTO orders VALUES
(1, 'Alice', 250.00, 'EAST', 'shipped', DATE '2024-01-10'),
(2, 'Bob', 900.00, 'WEST', 'cancelled', DATE '2024-01-11'),
(3, 'Carol', 75.00, 'EAST', 'delivered', DATE '2024-01-12'),
(4, 'Dave', 1200.00, 'WEST', 'shipped', DATE '2024-01-13'),
(5, 'Eve', 430.00, 'EAST', 'cancelled', DATE '2024-01-14'),
(6, 'Frank', 100.00, 'NORTH', 'delivered', DATE '2024-01-15'),
(7, 'Grace', 520.00, 'NORTH', 'shipped', DATE '2024-01-16'),
(8, 'Hank', 60.00, 'SOUTH', 'cancelled', DATE '2024-01-17'),
-- orphaned / stale row
(99, 'Ghost', 0.00, 'EAST', 'cancelled', DATE '2023-12-01');

-- Reference table of valid customer IDs
CREATE OR REPLACE TEMP VIEW valid_customers AS
SELECT customer_id
FROM
    VALUES
    ('Alice'),
    ('Bob'),
    ('Carol'),
    ('Dave'),
    ('Eve'),
    ('Frank'),
    ('Grace'),
    ('Hank')
        AS t (customer_id);

---------------------------------------------------------------------------------------------------
-- 1. Simple DELETE with a condition
---------------------------------------------------------------------------------------------------

-- Remove all cancelled orders
DELETE FROM orders
WHERE status = 'cancelled';

-- Result: rows 2, 5, 8 (and 99) are logically deleted; 5 rows remain

---------------------------------------------------------------------------------------------------
-- 2. DELETE with IN subquery (remove orphaned / invalid records)
---------------------------------------------------------------------------------------------------

-- Re-insert some data for demonstration
INSERT INTO orders VALUES (
    99, 'Ghost', 0.00, 'EAST', 'cancelled', DATE '2023-12-01'
);

-- Delete rows whose customer does not appear in the valid_customers reference table
DELETE FROM orders
WHERE customer NOT IN (SELECT customer_id FROM valid_customers);

-- Result: the 'Ghost' row (order_id 99) is removed

---------------------------------------------------------------------------------------------------
-- 3. DELETE with NOT EXISTS (correlated subquery)
---------------------------------------------------------------------------------------------------

-- Delete orders that have no matching customer in the valid list (same intent, different syntax)
DELETE FROM orders
WHERE NOT EXISTS (
    SELECT 1
    FROM valid_customers AS vc
    WHERE vc.customer_id = orders.customer
);

-- Result: equivalent to the IN subquery above; no rows deleted if Ghost was already removed

---------------------------------------------------------------------------------------------------
-- 4. DELETE all rows (truncate semantics)
--    DELETE FROM <table> without a WHERE clause removes every row.
--    Prefer TRUNCATE TABLE for this operation — it is faster because it rewrites the metadata
--    without scanning each file, and it does not create a new Delta commit per deleted row.
---------------------------------------------------------------------------------------------------

-- Slow path: scans the whole table (creates a new Delta version, preserves time-travel)
-- DELETE FROM orders;

-- Fast path: recommended for full table wipes on Delta
-- TRUNCATE TABLE orders;

---------------------------------------------------------------------------------------------------
-- 5. DELETE by partition column for partition pruning
--    When the WHERE clause filters on the partition column, Spark skips unrelated partitions,
--    making the DELETE much cheaper than a full-table scan.
---------------------------------------------------------------------------------------------------

-- Drop all rows in the NORTH region partition without touching EAST, WEST, or SOUTH
DELETE FROM orders
WHERE region = 'NORTH';

-- Result: Frank (NORTH) and Grace (NORTH) are removed; other regions intact

---------------------------------------------------------------------------------------------------
-- 6. VACUUM to reclaim storage after deletes
--    By default Delta retains 7 days of history.
--    Set retentionDuration to 0 ONLY in dev/test — never in production.
---------------------------------------------------------------------------------------------------

-- Standard vacuum (keeps 7-day history)
-- VACUUM orders;

-- Aggressive vacuum for dev/test (disables retention check first)
-- SET spark.databricks.delta.retentionDurationCheck.enabled = false;
-- VACUUM orders RETAIN 0 HOURS;

---------------------------------------------------------------------------------------------------
-- Verify remaining rows
---------------------------------------------------------------------------------------------------

SELECT
    order_id,
    customer,
    amount,
    region,
    status
FROM orders
ORDER BY order_id;
