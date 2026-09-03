-- 1. Unpivot Fixed Columns into Rows

-- 🔄 Unpivot Subjects using STACK

CREATE OR REPLACE TEMP VIEW scores AS
SELECT--noqa
    * --noqa
FROM
    VALUES
    (1, 85, 90, 78),
    (2, 88, 76, 92)
        AS scores (student_id, math, science, history);
SELECT
    student_id,
    subject,
    score
FROM scores
    LATERAL VIEW STACK(
        3,
        'math', math,
        'science', science,
        'history', history
    ) AS subject, score;

-- 2. Generate Static Rows

SELECT *
FROM (SELECT 1) AS d
    LATERAL VIEW STACK(
        3,
        'A', 100,
        'B', 200,
        'C', 300
    ) AS label, value;

-- 3. Use with Constants and Variables

CREATE OR REPLACE TEMP VIEW features AS
SELECT
    101 AS id,
    TRUE AS feature_a,
    FALSE AS feature_b,
    TRUE AS feature_c;
SELECT
    id,
    feature_name,
    is_enabled
FROM features
    LATERAL VIEW STACK(
        3,
        'feature_a', feature_a,
        'feature_b', feature_b,
        'feature_c', feature_c
    ) AS feature_name, is_enabled;


-- 4. Create Pivot-Like Summary with Labels

CREATE OR REPLACE TEMP VIEW summary AS
SELECT
    'Product A' AS name,
    1200 AS sales,
    300 AS profit;
SELECT
    name,
    metric,
    value
FROM summary
    LATERAL VIEW STACK(
        2,
        'sales', sales,
        'profit', profit
    ) AS metric, value;

-- 5. Use Without a Table (Manual Data Creation)

SELECT *
FROM (SELECT 1) AS d
    LATERAL VIEW STACK(
        2,
        'apple', 50,
        'banana', 30
    ) AS fruit, quantity;
