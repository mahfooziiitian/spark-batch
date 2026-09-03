-- ============================================================
-- Topic: ANY_VALUE — arbitrary value from a group
-- Dialect: Databricks / Spark SQL 3.5
-- Description: Returns an arbitrary (non-deterministic) value
--              from a group.  Supports ignoreNulls flag.
-- ============================================================

-- ---------------------------------------------------------
-- 1. Basic usage — single value from all rows
-- ---------------------------------------------------------
SELECT ANY_VALUE(col) AS sample_val
FROM VALUES (10), (20), (30) AS tab (col);
-- Result: one of 10, 20, or 30 (non-deterministic)

-- ---------------------------------------------------------
-- 2. Grouped — pick one value per group
-- ---------------------------------------------------------
CREATE OR REPLACE TEMP VIEW orders AS
SELECT -- noqa: LT09
    * FROM VALUES
('East', 'Alice', 100),
('East', 'Bob', 200),
('West', 'Carol', 150),
('West', 'Dave', 300),
('East', 'Eve', 50)
    AS orders (region, rep, amount);

SELECT
    region,
    ANY_VALUE(rep) AS sample_rep,
    SUM(amount) AS total_amount
FROM orders
GROUP BY region;
-- sample_rep is one of the reps in that region (non-deterministic)

-- ---------------------------------------------------------
-- 3. NULL handling — default (ignoreNulls = false)
-- ---------------------------------------------------------
SELECT ANY_VALUE(col) AS sample_val
FROM VALUES (NULL), (5), (20) AS tab (col);
-- Result: could be NULL, 5, or 20

-- ---------------------------------------------------------
-- 4. ignoreNulls = true — skip NULL values
-- ---------------------------------------------------------
SELECT ANY_VALUE(col, TRUE) AS sample_val
FROM VALUES (NULL), (5), (20) AS tab (col);
-- Result: 5 or 20 (never NULL when non-NULL values exist)

-- ---------------------------------------------------------
-- 5. All NULLs — returns NULL regardless of flag
-- ---------------------------------------------------------
SELECT ANY_VALUE(CAST(NULL AS INT), TRUE) AS sample_val
FROM VALUES (CAST(NULL AS INT)), (CAST(NULL AS INT)) AS tab (col);
-- Result: NULL

-- ---------------------------------------------------------
-- 6. Practical — non-aggregated column alongside GROUP BY
--    Safe when any representative value is acceptable
--    or the column is functionally dependent on the key.
-- ---------------------------------------------------------
CREATE OR REPLACE TEMP VIEW employees AS
SELECT
    * FROM VALUES
(1, 'Engineering', 'Alice', 90000),
(2, 'Engineering', 'Bob', 100000),
(3, 'Sales', 'Carol', 70000),
(4, 'Sales', 'Dave', 80000)
    AS employees (id, dept, name, salary);

SELECT
    dept,
    ANY_VALUE(name) AS sample_employee,
    AVG(salary) AS avg_salary
FROM employees
GROUP BY dept;
-- sample_employee: any name from each dept (non-deterministic)
-- Use MIN_BY/MAX_BY or window functions when you need a specific row.
