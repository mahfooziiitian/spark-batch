-- ============================================================
-- Topic: Aggregation — pivoting in Spark SQL
-- Dialect: Databricks / Spark SQL 3.5
-- Description: Demonstrates PIVOT output for student weights by gender.
-- ============================================================

-- 1. Creating table
DROP TABLE IF EXISTS students;

CREATE TABLE students (
    id INT,
    name STRING,
    gender STRING,
    weight INT
);

-- 2. Insert into table
INSERT INTO students (id, name, gender, weight)
VALUES
    (1, 'Alice', 'F', 55),
    (2, 'Bob', 'M', 75),
    (3, 'Charlie', 'M', 85),
    (4, 'Diana', 'F', 65),
    (5, 'Eva', 'F', 70),
    (6, 'Frank', 'M', 90);

-- 3. Pivot data
SELECT * --noqa: AM04
FROM students PIVOT (
    min(weight) AS min,
    max(weight) AS max,
    avg(weight) AS avg FOR gender IN ('M' AS male, 'F' AS female)
);

-- 4. Clean up
DROP TABLE students;
