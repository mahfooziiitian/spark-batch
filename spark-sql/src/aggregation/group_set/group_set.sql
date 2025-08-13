-- Step 1: Create the Table
DROP TABLE IF EXISTS employee;

CREATE TABLE employee (
    emp_id INT,
    name STRING,
    dept STRING,
    role STRING,
    salary INT
)
USING DELTA;

-- 🧪 Step 2: Insert Sample Data
INSERT INTO employee VALUES
(1, 'Alice', 'IT', 'Engineer', 1000),
(2, 'Bob', 'IT', 'Manager', 1500),
(3, 'Charlie', 'HR', 'Exec', 800),
(4, 'David', 'HR', 'Manager', 1200),
(5, 'Eva', 'IT', 'Engineer', 1200),
(6, 'Frank', 'Sales', 'Exec', 1100),
(7, 'Grace', 'Sales', 'Manager', 1300);

-- GROUPING SETS Query on this Table
-- Step 3: Query with GROUPING SETS

SELECT
    dept,
    role,
    SUM(salary) AS total_salary
FROM employee
GROUP BY
    GROUPING SETS (
            (dept, role),
            (dept),
            ()
    )
ORDER BY dept, role;

-- 4. Clean up
DROP TABLE IF EXISTS employee;
