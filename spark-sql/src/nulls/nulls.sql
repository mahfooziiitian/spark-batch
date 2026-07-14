-- NULL semantics examples in Spark SQL (Databricks).
-- Demonstrates NULL comparison rules, logical operators, aggregation behaviour,
-- NULL-handling functions, GROUP BY / ORDER BY, IN traps, and CASE WHEN.

-- ----------------------------------------------------------------------------
-- Setup: person table with some NULL ages
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TEMP VIEW person AS
SELECT
    id,
    name,
    age
FROM
    VALUES
    (1, 'Alice', 30),
    (2, 'Bob', NULL),
    (3, 'Carol', 25),
    (4, 'Dave', NULL),
    (5, 'Eve', 40)
        AS t (id, name, age);

-- ----------------------------------------------------------------------------
-- 1. Comparison: = NULL vs IS NULL (NULL ≠ NULL)
-- ----------------------------------------------------------------------------
-- Using = NULL always evaluates to NULL (unknown), never TRUE.
SELECT
    id,
    name,
    -- Result: always NULL (NULL = NULL → unknown)
    age IS NULL AS eq_null_result,
    age IS NULL AS is_null_result    -- Result: TRUE when age is NULL
FROM person;
-- Result: Bob and Dave show NULL for eq_null_result, TRUE for is_null_result.

-- ----------------------------------------------------------------------------
-- 2. NULL-safe equality <=>
-- ----------------------------------------------------------------------------
-- <=> returns TRUE when both sides are NULL (unlike =).
SELECT
    id,
    name,
    age,
    age <=> NULL AS null_safe_eq,    -- TRUE only when age IS NULL
    age <=> 30 AS null_safe_30       -- TRUE only for Alice
FROM person;
-- Result: Bob and Dave → null_safe_eq TRUE; Alice → null_safe_30 TRUE.

-- ----------------------------------------------------------------------------
-- 3. Logical operators with NULL: AND, OR, NOT truth tables
-- ----------------------------------------------------------------------------
-- NULL AND false = FALSE   (false dominates AND)
-- NULL AND true  = NULL    (unknown)
-- NULL OR  true  = TRUE    (true dominates OR)
-- NULL OR  false = NULL    (unknown)
-- NOT NULL       = NULL
SELECT
    NULL AND FALSE AS null_and_false,   -- Result: false
    NULL AND TRUE AS null_and_true,     -- Result: null
    NULL OR TRUE AS null_or_true,       -- Result: true
    NULL OR FALSE AS null_or_false,     -- Result: null
    NOT NULL AS not_null;               -- Result: null

-- ----------------------------------------------------------------------------
-- 4. NULL in aggregations: COUNT(*) vs COUNT(col) vs SUM
-- ----------------------------------------------------------------------------
SELECT
    COUNT(*) AS count_all,          -- 5 — counts every row
    COUNT(age) AS count_age,        -- 3 — ignores NULL ages
    COUNT(DISTINCT age) AS count_distinct_age,  -- 3: 25, 30, 40
    SUM(age) AS sum_age,            -- 95 (NULLs skipped)
    AVG(age) AS avg_age,            -- 31.67 (95/3, not 95/5)
    MIN(age) AS min_age,            -- 25
    MAX(age) AS max_age             -- 40
FROM person;
-- NULLs are excluded from all aggregate functions except COUNT(*).

-- ----------------------------------------------------------------------------
-- 5. COALESCE: return first non-NULL value
-- ----------------------------------------------------------------------------
SELECT
    id,
    name,
    age,
    COALESCE(age, 0) AS age_or_zero,               -- replace NULL with 0
    COALESCE(age, 99, -1) AS age_first_non_null    -- first non-NULL wins
FROM person;
-- Result: Bob and Dave get 0 for age_or_zero.

-- ----------------------------------------------------------------------------
-- 6. NULLIF: return NULL when two values are equal
-- ----------------------------------------------------------------------------
-- Useful for avoiding division by zero.
SELECT
    10 / NULLIF(0, 0) AS safe_divide,   -- Result: NULL instead of error
    10 / NULLIF(2, 0) AS normal_divide, -- Result: 5.0
    NULLIF('abc', 'abc') AS same_str,   -- Result: NULL
    NULLIF('abc', 'xyz') AS diff_str;   -- Result: 'abc'

-- ----------------------------------------------------------------------------
-- 7. NVL and NVL2: Oracle-compatible NULL substitution
-- ----------------------------------------------------------------------------
SELECT
    id,
    name,
    age,
    -- NVL: same as COALESCE(x, default)
    COALESCE(age, -1) AS nvl_age,
    -- NVL2(x, not_null_val, null_val)
    NVL2(age, 'known', 'unknown') AS nvl2_age
FROM person;
-- Result: Bob/Dave get -1 for nvl_age and 'unknown' for nvl2_age.

-- ----------------------------------------------------------------------------
-- 8. IFNULL: return replacement when value is NULL
-- ----------------------------------------------------------------------------
SELECT
    id,
    name,
    COALESCE(age, 0) AS age_ifnull   -- equivalent to COALESCE(age, 0)
FROM person;
-- Result: Bob and Dave get 0.

-- ----------------------------------------------------------------------------
-- 9. NULL in GROUP BY (NULLs are grouped together)
-- ----------------------------------------------------------------------------
-- Spark treats NULL as a single group key, so all NULL ages form one group.
SELECT
    age,
    COUNT(*) AS person_count
FROM person
GROUP BY age
ORDER BY age NULLS LAST;
-- Result: 25→1, 30→1, 40→1, NULL→2 (Bob + Dave in one NULL group).

-- ----------------------------------------------------------------------------
-- 10. NULL in ORDER BY: NULLS FIRST vs NULLS LAST
-- ----------------------------------------------------------------------------
-- Default Spark behaviour: NULLs sort LAST for ASC, FIRST for DESC.
SELECT
    id,
    name,
    age
FROM person
ORDER BY age ASC NULLS FIRST;   -- override: NULLs appear at the top
-- Result: Bob, Dave (NULL), then Carol (25), Alice (30), Eve (40).

SELECT
    id,
    name,
    age
FROM person
ORDER BY age DESC NULLS LAST;   -- NULLs appear at the bottom
-- Result: Eve (40), Alice (30), Carol (25), then Bob, Dave (NULL).

-- ----------------------------------------------------------------------------
-- 11. NULL in IN list: the NOT IN NULL trap
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TEMP VIEW age_candidates AS
SELECT age
FROM
    VALUES (25), (30), (NULL)
        AS t (age);

-- IN: NULLs in the list cause unmatched rows to return NULL, not FALSE.
SELECT
    name,
    age
FROM person
WHERE age IN (25, NULL);   -- matches age=25 only; NULL ages never match
-- Result: Carol only.

-- NOT IN with a NULL in the list returns NO rows (NULL poisons comparisons).
SELECT
    name,
    age
FROM person
-- Result: empty — all rows filtered by NULL uncertainty
WHERE age NOT IN (25, NULL);
-- ⚠ Always ensure NOT IN subqueries exclude NULLs with IS NOT NULL.

-- Safe NOT IN (explicit NULL exclusion):
SELECT
    name,
    age
FROM person
WHERE
    age NOT IN (25, 30)
    AND age IS NOT NULL;
-- Result: Eve (40).

-- ----------------------------------------------------------------------------
-- 12. NULL in CASE WHEN
-- ----------------------------------------------------------------------------
-- CASE WHEN evaluates conditions in order; NULL does not match any WHEN clause.
SELECT
    id,
    name,
    age,
    CASE
        WHEN age IS NULL THEN 'unknown'         -- explicit NULL check first
        WHEN age < 30 THEN 'young'
        WHEN age < 50 THEN 'middle'
        ELSE 'senior'
    END AS age_group
FROM person;
-- Result: Bob → unknown, Dave → unknown, Carol → young, Alice/Eve → middle.
