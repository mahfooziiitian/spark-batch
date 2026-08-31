-- Demonstrates the modulo operator (%) for remainder calculations and financial rounding.

-- =============================================================================
-- Section 1: Modulo Basics
-- =============================================================================

-- The % operator returns the remainder after integer division.

SELECT
    17 % 5 AS remainder_17_div_5,
    20 % 4 AS remainder_20_div_4,
    100 % 7 AS remainder_100_div_7;

-- =============================================================================
-- Section 2: Practical Use — Leap Year Check
-- =============================================================================

-- A year is a leap year if divisible by 4 (simplified rule).

SELECT
    saledate,
    YEAR(saledate) AS sale_year,
    CASE
        WHEN YEAR(saledate) % 4 = 0 THEN 'Leap Year'
        ELSE 'Non-Leap Year'
    END AS leap_year_flag
FROM allsales
ORDER BY saledate;

-- =============================================================================
-- Section 3: Financial Rounding to Nearest £500
-- =============================================================================

-- Round saleprice to the nearest £500 boundary.

SELECT
    saleprice,
    ROUND(saleprice / 500, 0) * 500 AS rounded_to_500
FROM allsales
ORDER BY saleprice;

-- =============================================================================
-- Section 4: Banker's Rounding vs Standard Rounding
-- =============================================================================

-- Spark ROUND() uses half-even (banker's rounding): ties round to nearest even number.
-- 0.5 rounds to 0 (even), 1.5 rounds to 2 (even), 2.5 rounds to 2 (even).

SELECT
    0.5 AS value,
    ROUND(0.5, 0) AS bankers_round
UNION ALL
SELECT
    1.5 AS value,
    ROUND(1.5, 0) AS bankers_round
UNION ALL
SELECT
    2.5 AS value,
    ROUND(2.5, 0) AS bankers_round
UNION ALL
SELECT
    3.5 AS value,
    ROUND(3.5, 0) AS bankers_round;

-- =============================================================================
-- Section 5: Sales Commission Tiers Using CASE
-- =============================================================================

-- Commission rate depends on sale amount tier.

SELECT
    saleprice,
    CASE
        WHEN saleprice >= 100000 THEN saleprice * 0.05
        WHEN saleprice >= 60000 THEN saleprice * 0.03
        ELSE saleprice * 0.02
    END AS commission
FROM allsales
ORDER BY saleprice DESC;

-- =============================================================================
-- Section 6: Window Function Equivalent for Running Total
-- =============================================================================

-- Window function equivalent (efficient):

SELECT
    saledate,
    saleprice,
    SUM(saleprice) OVER (ORDER BY saledate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
FROM allsales
ORDER BY saledate;

-- =============================================================================
-- Section 7: Sample Data
-- =============================================================================

WITH sample_data AS (
    SELECT 65500.00 AS saleprice
    UNION ALL
    SELECT 90250.00 AS saleprice
    UNION ALL
    SELECT 120750.00 AS saleprice
    UNION ALL
    SELECT 45100.00 AS saleprice
    UNION ALL
    SELECT 200999.00 AS saleprice
)

SELECT
    saleprice,
    saleprice % 500 AS remainder_mod_500,
    ROUND(saleprice / 500, 0) * 500 AS rounded_to_500,
    CASE
        WHEN saleprice >= 100000 THEN saleprice * 0.05
        WHEN saleprice >= 60000 THEN saleprice * 0.03
        ELSE saleprice * 0.02
    END AS commission
FROM sample_data
ORDER BY saleprice;
