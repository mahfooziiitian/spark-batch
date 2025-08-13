-- Deduplication
-- ✅ 1. Remove Exact Duplicates (All Columns Match)
WITH students AS (
    SELECT
        id,
        name,
        age
    FROM
        VALUES
        (1, 'Alice', 20),
        (2, 'Bob', 22),
        (1, 'Alice', 20),
        (4, 'Dave', 23),
        (2, 'Bob', 22),
        (6, 'Eve', 24),
        (1, 'Alice', 20)
        AS students (id, name, age)
)

SELECT DISTINCT *
FROM students;
-- ✅ 2. Remove Duplicates Based on Key Columns (Keep First)
WITH students AS (
    SELECT
        id,
        name,
        age
    FROM
        VALUES
        (1, 'Alice', 20),
        (2, 'Bob', 22),
        (1, 'Alice', 19),
        (4, 'Dave', 23),
        (2, 'Bob', 23),
        (6, 'Eve', 24),
        (1, 'Alice', 22)
        AS students (id, name, age)
)

SELECT *
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY age DESC
        ) AS rn
    FROM students
)
WHERE rn = 1;

-- ✅ 3. Remove Duplicates Based on Key Columns (Keep Any Row)
WITH students AS (
    SELECT
        id,
        name,
        age
    FROM
        VALUES
        (1, 'Alice', 20),
        (2, 'Bob', 22),
        (1, 'Alice', 19),
        (4, 'Dave', 23),
        (2, 'Bob', 23),
        (6, 'Eve', 24),
        (1, 'Alice', 22)
        AS students (id, name, age)
)

SELECT
    id,
    FIRST(name) AS name
FROM students
GROUP BY id;
-- ✅ 4. Remove Duplicates and Keep the Latest Row (by timestamp)
WITH students AS (
    SELECT
        id,
        name,
        age,
        create_datetime
    FROM
        VALUES
        (1, 'Alice', 20, '2023-08-01 10:00:00'),
        (2, 'Bob', 22, '2023-08-01 11:00:00'),
        (1, 'Alice', 19, '2023-08-02 10:00:00'),
        (4, 'Dave', 23, '2023-08-02 12:00:00'),
        (2, 'Bob', 23, '2023-08-03 10:00:00'),
        (6, 'Eve', 24, '2023-08-03 11:00:00'),
        (1, 'Alice', 22, '2023-08-04 10:00:00')
        AS students (id, name, age, create_datetime)
)

SELECT *
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY create_datetime DESC
        ) AS rn
    FROM students
)
WHERE rn = 1;

-- ✅ 5. Remove Duplicates with Conditional Logic
SELECT *
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY CASE
                WHEN condition1 THEN 1
                WHEN condition2 THEN 2
                ELSE 3
            END
        ) AS rn
    FROM my_table
)
WHERE rn = 1;

-- ✅ 7. Remove Duplicates with Window Functions (e.g., RANK, DENSE_RANK)
SELECT
    id,
    FIRST(name) AS name,
    RANK() OVER (
        PARTITION BY id
        ORDER BY some_column
    ) AS rank
FROM my_table
GROUP BY id, name, some_column;

-- ✅ 8. Remove Duplicates with Self-Join
WITH b AS (
    SELECT
        id,
        MIN(some_column) AS min_col
    FROM my_table
    GROUP BY id
)

SELECT a.*
FROM my_table AS a
INNER JOIN b ON a.id = b.id AND a.some_column = b.min_col;

-- ✅ 9. Remove Duplicates with CTE (Common Table Expression)
WITH cte AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY some_column
        ) AS rn
    FROM my_table
)

SELECT *
FROM cte
WHERE rn = 1;

-- ✅ 10. Remove Duplicates with Temporary Table
CREATE TEMPORARY TABLE temp_table AS
SELECT
    *,
    ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY some_column
    ) AS rn
FROM my_table;
SELECT *
FROM temp_table
WHERE rn = 1;

-- ✅ 11. Remove Duplicates with Subquery
SELECT *
FROM my_table AS t1
WHERE NOT EXISTS (
    SELECT 1
    FROM my_table AS t2
    WHERE
        t1.id = t2.id
        AND t1.some_column < t2.some_column
);

-- ✅ 12. Remove Duplicates with JSON Functions (if applicable)
SELECT
    id,
    JSON_AGG(DISTINCT name) AS names
FROM my_table
GROUP BY id;

-- ✅ 13. Remove Duplicates with XML Functions (if applicable)
SELECT
    id,
    XMLAGG(DISTINCT name) AS names
FROM my_table
GROUP BY id;

-- ✅ 14. Remove Duplicates with Array Functions (if applicable)
SELECT
    id,
    ARRAY_AGG(DISTINCT name) AS names
FROM my_table
GROUP BY id;
