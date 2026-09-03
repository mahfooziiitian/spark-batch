-- ============================================================
-- Topic: Application — finding duplicate rows
-- Dialect: Databricks / Spark SQL 3.5
-- Description: Shows several duplicate-detection patterns for rows and emails.
-- ============================================================

-- Finding duplicate rows
SELECT
    name,
    age,
    COUNT(*) AS cnt
FROM students
GROUP BY
    name,
    age
HAVING COUNT(*) > 1;

-- To get full duplicate records (with all columns)
-- using a CTE (Common Table Expression)
WITH duplicates AS (
    SELECT
        name,
        age
    FROM students
    GROUP BY
        name,
        age
    HAVING COUNT(*) > 1
)

SELECT s.* --noqa: AM04
FROM students AS s
INNER JOIN duplicates AS d
    ON
        s.name = d.name
        AND s.age = d.age;

-- To get all rows with duplicate emails
SELECT * --noqa: AM04
FROM users AS outer_users
WHERE outer_users.email IN ( -- noqa: RF03
    SELECT inner_users.email
    FROM users AS inner_users
    GROUP BY inner_users.email
    HAVING COUNT(*) > 1
);

-- If you want duplicates by a specific column (e.g., email)
SELECT
    email,
    COUNT(*) AS cnt
FROM users
GROUP BY email
HAVING COUNT(*) > 1;

-- Candidate Primary Key
SELECT COUNT(DISTINCT column_name) = COUNT(*) AS is_candidate_pk
FROM your_table;
