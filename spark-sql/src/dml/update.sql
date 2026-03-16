-- UPDATE examples for Delta Lake
-- UPDATE is only supported on Delta (and other transactional) tables.
-- Attempting UPDATE on a plain Parquet / CSV table raises an AnalysisException.

---------------------------------------------------------------------------------------------------
-- Setup: target Delta table
---------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS employees;

CREATE TABLE employees (
    emp_id INT,
    name STRING,
    department STRING,
    salary DOUBLE,
    level STRING,
    address STRUCT<city: STRING, country: STRING>,
    updated_at TIMESTAMP
) USING DELTA;

INSERT INTO employees VALUES
(
    1,
    'Alice',
    'Engineering',
    85000.0,
    'L3',
    STRUCT('New York', 'USA'),
    CURRENT_TIMESTAMP()
),
(
    2,
    'Bob',
    'Engineering',
    72000.0,
    'L2',
    STRUCT('Austin', 'USA'),
    CURRENT_TIMESTAMP()
),
(
    3,
    'Carol',
    'Marketing',
    65000.0,
    'L2',
    STRUCT('Chicago', 'USA'),
    CURRENT_TIMESTAMP()
),
(
    4,
    'Dave',
    'Marketing',
    58000.0,
    'L1',
    STRUCT('Los Angeles', 'USA'),
    CURRENT_TIMESTAMP()
),
(
    5,
    'Eve',
    'Finance',
    95000.0,
    'L4',
    STRUCT('London', 'GBR'),
    CURRENT_TIMESTAMP()
),
(
    6,
    'Frank',
    'Finance',
    88000.0,
    'L3',
    STRUCT('London', 'GBR'),
    CURRENT_TIMESTAMP()
);

-- Reference table for department budget caps
CREATE OR REPLACE TEMP VIEW dept_caps AS
SELECT
    department,
    max_salary
FROM
    VALUES
    ('Engineering', 100000.0),
    ('Marketing', 80000.0),
    ('Finance', 110000.0)
        AS t (department, max_salary);

---------------------------------------------------------------------------------------------------
-- 1. Simple UPDATE with WHERE
---------------------------------------------------------------------------------------------------

-- Give a 10 % raise to all Engineering employees
UPDATE employees
SET
    salary = salary * 1.10,
    updated_at = CURRENT_TIMESTAMP()
WHERE department = 'Engineering';

-- Result: Alice -> 93500, Bob -> 79200

---------------------------------------------------------------------------------------------------
-- 2. UPDATE multiple columns at once
---------------------------------------------------------------------------------------------------

-- Promote Dave: update his level, salary, and timestamp in one statement
UPDATE employees
SET
    level = 'L2',
    salary = 62000.0,
    updated_at = CURRENT_TIMESTAMP()
WHERE emp_id = 4;

-- Result: Dave's row now shows L2 / 62000

---------------------------------------------------------------------------------------------------
-- 3. UPDATE with CASE expression in SET
---------------------------------------------------------------------------------------------------

-- Apply a tiered raise: L1 +5 %, L2 +8 %, L3+ +12 %
UPDATE employees
SET salary = salary * CASE
    WHEN level = 'L1' THEN 1.05
    WHEN level = 'L2' THEN 1.08
    ELSE 1.12
END,
updated_at = CURRENT_TIMESTAMP();

-- Result: each employee's salary is bumped according to their level band

---------------------------------------------------------------------------------------------------
-- 4. UPDATE with subquery in WHERE (correlated subquery)
---------------------------------------------------------------------------------------------------

-- Cap salaries that exceed the department budget limit
UPDATE employees
SET
    salary
    = (
        SELECT dept_caps.max_salary FROM dept_caps
        WHERE dept_caps.department = employees.department
    ),
    updated_at = CURRENT_TIMESTAMP()
WHERE salary > (
    SELECT dept_caps.max_salary
    FROM dept_caps
    WHERE dept_caps.department = employees.department
);

-- Result: any employee earning above their dept cap is set exactly to the cap

---------------------------------------------------------------------------------------------------
-- 5. UPDATE a nested struct field
--    Delta Lake allows dot-notation to target individual fields inside a STRUCT column.
---------------------------------------------------------------------------------------------------

-- Eve has relocated from London to Dublin; update only the city inside her address struct
UPDATE employees
SET address.city = 'Dublin'
WHERE emp_id = 5;

-- Result: Eve's address is now STRUCT('Dublin', 'GBR'); the country field is unchanged

---------------------------------------------------------------------------------------------------
-- Verify final state
---------------------------------------------------------------------------------------------------

SELECT
    emp_id,
    name,
    department,
    salary,
    level,
    address,
    updated_at
FROM employees
ORDER BY emp_id;
