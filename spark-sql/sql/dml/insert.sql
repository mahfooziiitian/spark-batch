-- INSERT examples for Spark SQL / Delta Lake
-- Covers: INSERT INTO ... SELECT, INSERT OVERWRITE, VALUES, partition overwrite,
-- dynamic partition overwrite, and multi-table INSERT FROM (Hive-style).

---------------------------------------------------------------------------------------------------
-- Setup: target tables
---------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS orders;

CREATE TABLE orders (
    order_id INT,
    customer STRING,
    amount DOUBLE,
    region STRING,
    order_date DATE
) USING DELTA;

DROP TABLE IF EXISTS orders_archive;

CREATE TABLE orders_archive (
    order_id INT,
    customer STRING,
    amount DOUBLE,
    region STRING,
    order_date DATE
) USING DELTA
PARTITIONED BY (region);

DROP TABLE IF EXISTS high_value_orders;

CREATE TABLE high_value_orders (
    order_id INT,
    customer STRING,
    amount DOUBLE
) USING DELTA;

DROP TABLE IF EXISTS low_value_orders;

CREATE TABLE low_value_orders (
    order_id INT,
    customer STRING,
    amount DOUBLE
) USING DELTA;

-- Source view: simulates a raw staging dataset
CREATE OR REPLACE TEMP VIEW raw_orders AS
SELECT
    order_id,
    customer,
    amount,
    region,
    order_date
FROM
    VALUES
    (1, 'Alice', 250.00, 'EAST', DATE '2024-01-10'),
    (2, 'Bob', 900.00, 'WEST', DATE '2024-01-11'),
    (3, 'Carol', 75.00, 'EAST', DATE '2024-01-12'),
    (4, 'Dave', 1200.00, 'WEST', DATE '2024-01-13'),
    (5, 'Eve', 430.00, 'EAST', DATE '2024-01-14')
        AS t (order_id, customer, amount, region, order_date);

---------------------------------------------------------------------------------------------------
-- 1. INSERT INTO ... SELECT (append rows from a query)
---------------------------------------------------------------------------------------------------

-- Append all rows from the staging view into the target table
INSERT INTO orders
SELECT
    order_id,
    customer,
    amount,
    region,
    order_date
FROM raw_orders;

-- Result: orders now contains 5 rows

---------------------------------------------------------------------------------------------------
-- 2. INSERT OVERWRITE (replace ALL data in the table)
---------------------------------------------------------------------------------------------------

-- Replaces the entire contents of orders with only high-value rows (>= 500)
INSERT OVERWRITE orders
SELECT
    order_id,
    customer,
    amount,
    region,
    order_date
FROM raw_orders
WHERE amount >= 500.00;

-- Result: orders contains 2 rows (Bob 900, Dave 1200); previous rows are gone

---------------------------------------------------------------------------------------------------
-- 3. INSERT INTO ... VALUES (literal row insertion)
---------------------------------------------------------------------------------------------------

-- Insert specific rows inline; useful for tests and seed data
INSERT INTO orders VALUES
(10, 'Frank', 150.00, 'NORTH', DATE '2024-02-01'),
(11, 'Grace', 800.00, 'SOUTH', DATE '2024-02-02'),
(12, 'Hank', 330.00, 'NORTH', DATE '2024-02-03');

-- Result: 3 new rows appended to orders

---------------------------------------------------------------------------------------------------
-- 4. INSERT OVERWRITE PARTITION (replace a specific static partition)
---------------------------------------------------------------------------------------------------

-- Replace only the EAST partition; other partitions are untouched
INSERT OVERWRITE orders_archive PARTITION (region = 'EAST')
SELECT
    order_id,
    customer,
    amount,
    order_date
FROM raw_orders
WHERE region = 'EAST';

-- Result: only the EAST partition in orders_archive is refreshed

---------------------------------------------------------------------------------------------------
-- 5. Dynamic partition overwrite
--    Each distinct value of the partition column in the SELECT replaces its own partition.
--    Partitions not present in the source are left intact.
---------------------------------------------------------------------------------------------------

SET spark.sql.sources.partitionoverwritemode = dynamic;

-- Overwrites only the partitions that appear in raw_orders (EAST and WEST here)
INSERT OVERWRITE orders_archive PARTITION (region)
SELECT
    order_id,
    customer,
    amount,
    order_date,
    region
FROM raw_orders;

-- Result: EAST and WEST partitions are replaced; any other existing partition is preserved

---------------------------------------------------------------------------------------------------
-- 6. Multi-table INSERT FROM (Hive-style fan-out from a single table scan)
--    FROM <source> INSERT INTO t1 SELECT ... INSERT INTO t2 SELECT ...
--    The source table is read only once; multiple inserts are applied in parallel.
---------------------------------------------------------------------------------------------------

FROM raw_orders
INSERT INTO high_value_orders
SELECT
    order_id,
    customer,
    amount
WHERE amount >= 500.00
INSERT INTO low_value_orders
SELECT
    order_id,
    customer,
    amount
WHERE amount < 500.00;

-- Result: high_value_orders has rows for Bob and Dave; low_value_orders has Alice, Carol, Eve
