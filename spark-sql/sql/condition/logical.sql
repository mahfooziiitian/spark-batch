-- Logical operator examples in Spark SQL (Databricks).
-- Covers AND, OR, NOT, NULL short-circuit rules, De Morgan's laws,
-- complex multi-condition filters, and IS DISTINCT FROM (Spark 3.x).

-- ----------------------------------------------------------------------------
-- Setup: employees table
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TEMP VIEW employees AS
SELECT
    emp_id,
    emp_name,
    department,
    salary,
    active,
    manager_id
FROM
    VALUES
    (1, 'Alice', 'Engineering', 95000, TRUE, 10),
    (2, 'Bob', 'Engineering', 72000, TRUE, 10),
    (3, 'Carol', 'HR', 60000, FALSE, 20),
    (4, 'Dave', 'Sales', 55000, TRUE, 20),
    (5, 'Eve', 'Engineering', 110000, TRUE, NULL),
    (6, 'Frank', 'HR', 68000, TRUE, 20),
    (7, 'Grace', 'Sales', 48000, FALSE, NULL)
        AS t (emp_id, emp_name, department, salary, active, manager_id);

-- ----------------------------------------------------------------------------
-- 1. AND: both conditions must be TRUE
-- ----------------------------------------------------------------------------
SELECT
    emp_name,
    department,
    salary
FROM employees
WHERE
    department = 'Engineering'
    AND salary > 80000;
-- Result: Alice (95000), Eve (110000)

-- ----------------------------------------------------------------------------
-- 2. OR: at least one condition must be TRUE
-- ----------------------------------------------------------------------------
SELECT
    emp_name,
    department,
    salary
FROM employees
WHERE
    department = 'HR'
    OR salary > 90000;
-- Result: Carol, Frank (HR), plus Alice and Eve (salary > 90000)

-- ----------------------------------------------------------------------------
-- 3. NOT: negates a condition
-- ----------------------------------------------------------------------------
SELECT
    emp_name,
    department,
    active
FROM employees
WHERE NOT active;
-- Result: Carol (FALSE), Grace (FALSE)

SELECT
    emp_name,
    department
FROM employees
WHERE department NOT IN ('HR', 'Sales');
-- Result: Engineering employees only

-- ----------------------------------------------------------------------------
-- 4. Short-circuit behaviour with NULL
-- ----------------------------------------------------------------------------
-- NULL AND false = FALSE   (false wins — no need to evaluate NULL side)
-- NULL AND true  = NULL    (unknown, because NULL could be true or false)
-- NULL OR  true  = TRUE    (true wins — no need to evaluate NULL side)
-- NULL OR  false = NULL    (unknown)
SELECT
    NULL AND FALSE AS null_and_false,   -- Result: false
    NULL AND TRUE AS null_and_true,     -- Result: null
    NULL OR TRUE AS null_or_true,       -- Result: true
    NULL OR FALSE AS null_or_false;     -- Result: null

-- Practical effect: rows with NULL salary are excluded by salary > 0.
SELECT
    emp_name,
    salary
FROM employees
WHERE
    active = TRUE
    AND salary > 60000;
-- Result: active employees with salary > 60000 (NULL salary rows dropped).

-- ----------------------------------------------------------------------------
-- 5. De Morgan's laws equivalence
-- ----------------------------------------------------------------------------
-- NOT (A OR B)  ≡  NOT A AND NOT B
-- NOT (A AND B) ≡  NOT A OR  NOT B
-- Both queries below return the same rows.

-- Using NOT + OR:
SELECT
    emp_name,
    department
FROM employees
WHERE NOT (department = 'HR' OR department = 'Sales');
-- Result: Engineering employees only

-- Equivalent using AND + NOT:
SELECT
    emp_name,
    department
FROM employees
WHERE
    department != 'HR'
    AND department != 'Sales';
-- Result: Engineering employees only (same set)

-- ----------------------------------------------------------------------------
-- 6. Complex multi-condition filter
-- ----------------------------------------------------------------------------
-- Active Engineering employees earning > 80k, or any inactive employee.
SELECT
    emp_id,
    emp_name,
    department,
    salary,
    active
FROM employees
WHERE (
    department = 'Engineering'
    AND salary > 80000
    AND active = TRUE
)
OR active = FALSE;
-- Result: Alice, Eve (active Engineering > 80k), Carol, Grace (inactive).

-- Multiple tier evaluation with explicit parentheses to avoid precedence bugs:
SELECT
    emp_name,
    department,
    salary,
    active
FROM employees
WHERE
    (department = 'Engineering' OR department = 'HR')
    AND (salary >= 68000 OR active = FALSE);
-- Result: Alice (95k), Bob (72k), Carol (inactive), Frank (68k).

-- ----------------------------------------------------------------------------
-- 7. IS DISTINCT FROM / IS NOT DISTINCT FROM (Spark 3.x)
-- ----------------------------------------------------------------------------
-- IS DISTINCT FROM is NULL-safe: treats NULL as an ordinary value.
-- NULL IS DISTINCT FROM NULL → FALSE  (both NULL → same)
-- 1    IS DISTINCT FROM NULL → TRUE   (different values)
-- Equivalent: a IS NOT DISTINCT FROM b  ←→  a <=> b

-- Using <=> (null-safe equal) to demonstrate the same semantics:
SELECT
    emp_id,
    emp_name,
    manager_id,
    -- manager_id IS DISTINCT FROM NULL  = NOT (manager_id <=> NULL)
    -- TRUE when manager_id is not NULL
    NOT(manager_id <=> NULL) AS has_manager,
    manager_id <=> NULL AS no_manager           -- TRUE when manager_id IS NULL
FROM employees;
-- Result: Eve and Grace → no_manager TRUE; others → has_manager TRUE.

-- IS DISTINCT FROM is especially useful in MERGE to detect any change,
-- including NULL ↔ value transitions:
-- ON t.manager_id IS DISTINCT FROM s.manager_id
