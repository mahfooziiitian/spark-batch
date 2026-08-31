-- Reference for Spark SQL numeric data types, their ranges, and safe conversion patterns.

-- =============================================================================
-- Section 1: Data Type Overview
-- =============================================================================

-- Each Spark SQL numeric type has a specific range and precision.

SELECT
    CAST(42 AS TINYINT) AS tinyint_val,
    CAST(42 AS SMALLINT) AS smallint_val,
    CAST(42 AS INT) AS int_val,
    CAST(42 AS BIGINT) AS bigint_val,
    CAST(42.5 AS FLOAT) AS float_val,
    CAST(42.5 AS DOUBLE) AS double_val,
    CAST(42.50 AS DECIMAL(10, 2)) AS decimal_val;

-- =============================================================================
-- Section 2: Converting Between Types
-- =============================================================================

-- CAST converts values between numeric types safely when within range.

SELECT
    saleprice,
    cost,
    CAST(saleprice AS BIGINT) AS sale_bigint,
    CAST(cost AS DECIMAL(12, 2)) AS cost_decimal
FROM allsales
ORDER BY saleprice;

-- =============================================================================
-- Section 3: TRY_CAST — Safe Conversion
-- =============================================================================

-- TRY_CAST returns NULL on failure instead of raising an error.

SELECT
    TRY_CAST('12345.67' AS DECIMAL(10, 2)) AS valid_cast,
    TRY_CAST('not_a_number' AS DECIMAL(10, 2)) AS failed_cast,
    TRY_CAST('99999' AS TINYINT) AS overflow_cast;

-- =============================================================================
-- Section 4: Type Promotion in Arithmetic
-- =============================================================================

-- INT / INT truncates in Spark. Cast to DOUBLE first for decimal result.

SELECT
    7 / 2 AS int_division,
    CAST(7 AS DOUBLE) / 2 AS double_division,
    7 / CAST(2 AS DOUBLE) AS double_division_alt,
    7.0 / 2 AS literal_double_division;

-- =============================================================================
-- Section 5: Sample Data — Type Conversion in Context
-- =============================================================================

WITH sample_data AS (
    SELECT
        65000.75 AS saleprice,
        42000.50 AS cost
    UNION ALL
    SELECT
        120000.00 AS saleprice,
        85000.00 AS cost
    UNION ALL
    SELECT
        45000.99 AS saleprice,
        28000.75 AS cost
    UNION ALL
    SELECT
        200000.50 AS saleprice,
        140000.25 AS cost
    UNION ALL
    SELECT
        30000.01 AS saleprice,
        18000.00 AS cost
)

SELECT
    saleprice,
    CAST(saleprice AS INT) AS as_int,
    CAST(saleprice AS BIGINT) AS as_bigint,
    CAST(saleprice AS FLOAT) AS as_float,
    CAST(saleprice AS DOUBLE) AS as_double,
    CAST(saleprice AS DECIMAL(12, 2)) AS as_decimal,
    TRY_CAST(saleprice AS DECIMAL(10, 2)) AS try_decimal
FROM sample_data
ORDER BY saleprice;
