--  Finding Duplicate Rows

SELECT
    name,
    age,
    COUNT(*) AS cnt
FROM students
GROUP BY name, age
HAVING COUNT(*) > 1;

-- To get full duplicate records (with all columns)

-- using a CTE (Common Table Expression)
WITH duplicates AS (
    SELECT
        name,
        age
    FROM students
    GROUP BY name, age
    HAVING COUNT(*) > 1
)

SELECT s.*
FROM students AS s
INNER JOIN duplicates AS d
    ON s.name = d.name AND s.age = d.age;

-- To get all rows with duplicate emails

SELECT *
FROM users
WHERE email IN (
    SELECT email
    FROM users
    GROUP BY email
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
