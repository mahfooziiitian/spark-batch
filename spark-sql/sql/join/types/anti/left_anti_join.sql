-- Left anti join examples: rows on the left with NO matching key on the right.
-- Equivalence to NOT IN / NOT EXISTS is shown for each case.
-- Tables: employee(id, name, age, department),
--         department(department_id, department_name)

-- -----------------------------------------------------------------------
-- Test data
-- -----------------------------------------------------------------------

CREATE OR REPLACE TEMP VIEW employee AS
SELECT
    id,
    name,
    age,
    department
FROM
    VALUES
    (1, 'John Doe', 30, 'IT'),
    (2, 'Jane Smith', 25, 'HR'),
    (3, 'Michael Johnson', 35, 'Finance'),
    (4, 'Mahfooz Doe', 30, 'HR')
        AS employee (id, name, age, department);

CREATE OR REPLACE TEMP VIEW department AS
SELECT
    department_id,
    department_name
FROM
    VALUES
    (1, 'IT'),
    (2, 'HR'),
    (3, 'Finance'),
    (4, 'Admin')  -- Admin has no employees assigned
        AS department (department_id, department_name);

-- -----------------------------------------------------------------------
-- 1. LEFT ANTI JOIN — employees whose department has no matching row
--    in the department table (join is on department name, not id)
-- -----------------------------------------------------------------------

-- Every employee.department value (IT, HR, Finance) exists as a
-- department_name, so no rows are returned.
SELECT
    e.id,
    e.name,
    e.age,
    e.department
FROM employee AS e
LEFT ANTI JOIN department AS d  -- noqa: ST11, AL05
    ON e.department = d.department_name;
-- Result: 0 rows — every employee department has a matching department_name

-- -----------------------------------------------------------------------
-- 2. LEFT ANTI JOIN — departments with no matching employee.department
-- -----------------------------------------------------------------------

-- 'Admin' is the only department_name absent from employee.department.
SELECT
    d.department_id,
    d.department_name
FROM department AS d
LEFT ANTI JOIN employee AS e  -- noqa: ST11, AL05
    ON d.department_name = e.department;
-- Result: 1 row — Admin (department_id 4)

-- -----------------------------------------------------------------------
-- 3. LEFT ANTI JOIN equivalent using NOT EXISTS subquery
-- -----------------------------------------------------------------------

SELECT
    d.department_id,
    d.department_name
FROM department AS d
WHERE NOT EXISTS (
    SELECT 1
    FROM employee AS e
    WHERE e.department = d.department_name
);
-- Result: same as example 2 — Admin (department_id 4)
