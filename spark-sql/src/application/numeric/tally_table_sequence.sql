-- Uses SEQUENCE and EXPLODE to generate sequential number lists (tally tables)
-- for date scaffolding, gap filling, and iterative calculations.

-- =============================================================================
-- Section 1: Generate a Sequence 1-12
-- =============================================================================

-- SEQUENCE(start, stop) produces an array; EXPLODE expands it to rows.

SELECT EXPLODE(SEQUENCE(1, 12)) AS n;

-- =============================================================================
-- Section 2: Generate Daily Dates for a Month
-- =============================================================================

-- SEQUENCE with a date step produces a date array.

SELECT EXPLODE(SEQUENCE(TO_DATE('2024-01-01'), TO_DATE('2024-01-31'), INTERVAL 1 DAY)) AS day_date;

-- =============================================================================
-- Section 3: Month Scaffold Using MAKE_DATE
-- =============================================================================

-- Use the tally table as a month scaffold and build first-of-month dates.

WITH months AS (
    SELECT EXPLODE(SEQUENCE(1, 12)) AS n
)

SELECT
    n AS month_num,
    MAKE_DATE(2024, n, 1) AS month_start
FROM months
ORDER BY n;

-- =============================================================================
-- Section 4: Last Day of Each Month
-- =============================================================================

-- Combine MAKE_DATE with LAST_DAY to get the final date of each month.

WITH months AS (
    SELECT EXPLODE(SEQUENCE(1, 12)) AS n
)

SELECT
    n AS month_num,
    MAKE_DATE(2024, n, 1) AS month_start,
    LAST_DAY(MAKE_DATE(2024, n, 1)) AS month_end
FROM months
ORDER BY n;

-- =============================================================================
-- Section 5: Generate a Fibonacci-Like Sequence Using ROW_NUMBER
-- =============================================================================

-- ROW_NUMBER over a generated sequence gives ordered row numbers.

WITH nums AS (
    SELECT EXPLODE(SEQUENCE(1, 10)) AS n        
),

numbered AS (
    SELECT
        n,
        ROW_NUMBER() OVER (ORDER BY n) AS rn
    FROM nums
)

SELECT
    rn,
    n,
    rn * rn AS rn_squared
FROM numbered
ORDER BY rn;

-- =============================================================================
-- Section 6: Calendar Week Scaffold for 2024 Q1
-- =============================================================================

-- Generate one row per week in Q1 2024 using a date sequence with 7-day intervals.

SELECT
    EXPLODE(
        SEQUENCE(TO_DATE('2024-01-01'), TO_DATE('2024-03-31'), INTERVAL 7 DAY)
    ) AS week_start;
