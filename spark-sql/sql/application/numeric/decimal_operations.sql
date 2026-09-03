-- Demonstrates functions that control decimal precision and remove fractional parts.

-- =============================================================================
-- Section 1: ROUND
-- =============================================================================

-- ROUND(value, decimal_places): rounds to specified decimal places.
-- Positive places round fractional part; negative places round left of decimal.

SELECT
    saleprice,
    ROUND(saleprice, 2) AS rounded_2dp,
    ROUND(saleprice, 0) AS rounded_whole,
    ROUND(saleprice, -3) AS rounded_nearest_thousand
FROM allsales
ORDER BY saleprice;

-- =============================================================================
-- Section 2: FLOOR and CEIL
-- =============================================================================

-- FLOOR drops fractional part downward; CEIL rounds upward to next integer.

SELECT
    saleprice,
    FLOOR(saleprice) AS floor_val,
    CEIL(saleprice) AS ceil_val
FROM allsales
ORDER BY saleprice;

-- =============================================================================
-- Section 3: TRUNCATE
-- =============================================================================

-- TRUNCATE chops the value at the specified decimal place without rounding.

SELECT
    saleprice,
    TRUNCATE(saleprice, 2) AS truncated_2dp
FROM allsales
ORDER BY saleprice;

-- =============================================================================
-- Section 4: Casting to Integer
-- =============================================================================

-- CAST to INT removes all decimal digits (equivalent to FLOOR for positive numbers).

SELECT
    saleprice,
    CAST(saleprice AS INT) AS int_val
FROM allsales
ORDER BY saleprice;

-- =============================================================================
-- Section 5: Comparison of All Four Methods
-- =============================================================================

-- Shows how ROUND, FLOOR, CEIL, and TRUNCATE behave differently on same value.

SELECT
    cost,
    CAST(cost AS INT) AS cast_int,
    ROUND(cost, 0) AS rounded,
    FLOOR(cost) AS floored,
    CEIL(cost) AS ceiled,
    TRUNCATE(cost, 0) AS truncated
FROM allsales
ORDER BY cost;

-- =============================================================================
-- Section 6: Sample Data — All Methods Side-by-Side
-- =============================================================================

WITH sample_data AS (
    SELECT
        12345.678 AS saleprice,
        8000.123 AS cost
    UNION ALL
    SELECT
        67890.999 AS saleprice,
        45000.500 AS cost
    UNION ALL
    SELECT
        100000.001 AS saleprice,
        72000.900 AS cost
    UNION ALL
    SELECT
        55555.555 AS saleprice,
        30000.100 AS cost
    UNION ALL
    SELECT
        9999.499 AS saleprice,
        5000.050 AS cost
)

SELECT
    saleprice,
    cost,
    CAST(saleprice AS INT) AS sale_int,
    CAST(cost AS INT) AS cost_int,
    ROUND(saleprice, 2) AS sale_round2,
    FLOOR(saleprice) AS sale_floor,
    CEIL(saleprice) AS sale_ceil,
    TRUNCATE(saleprice, 2) AS sale_trunc2,
    ROUND(cost, 2) AS cost_round2,
    FLOOR(cost) AS cost_floor,
    CEIL(cost) AS cost_ceil,
    TRUNCATE(cost, 2) AS cost_trunc2
FROM sample_data
ORDER BY saleprice;
