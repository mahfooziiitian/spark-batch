-- Demonstrates safe division patterns to avoid divide-by-zero runtime errors.

-- =============================================================================
-- Section 1: The Problem — Division by Zero
-- =============================================================================

-- 100 / 0 raises a runtime error in Spark SQL.
-- The queries below show how to handle zero denominators safely.

-- SELECT 100 / 0;  -- would raise: java.lang.ArithmeticException: / by zero

-- =============================================================================
-- Section 2: NULLIF Approach
-- =============================================================================

-- NULLIF(cost, 0) returns NULL when cost = 0, making division safe.

SELECT
    saleprice,
    cost,
    saleprice / NULLIF(cost, 0) AS safe_ratio
FROM allsales
ORDER BY saleprice;

-- =============================================================================
-- Section 3: CASE WHEN Approach
-- =============================================================================

-- Explicit CASE guards against zero before dividing.

SELECT
    saleprice,
    cost,
    CASE
        WHEN cost = 0 THEN NULL
        ELSE saleprice / cost
    END AS case_ratio
FROM allsales
ORDER BY saleprice;

-- =============================================================================
-- Section 4: TRY_DIVIDE
-- =============================================================================

-- TRY_DIVIDE(numerator, denominator) is a Databricks built-in that returns NULL on zero.

SELECT
    saleprice,
    cost,
    TRY_DIVIDE(saleprice, cost) AS try_divide_ratio
FROM allsales
ORDER BY saleprice;

-- =============================================================================
-- Section 5: COALESCE to Substitute a Default
-- =============================================================================

-- COALESCE replaces NULL (from NULLIF) with a fallback value such as 0.

SELECT
    saleprice,
    cost,
    COALESCE(saleprice / NULLIF(cost, 0), 0) AS ratio_or_zero
FROM allsales
ORDER BY saleprice;

-- =============================================================================
-- Section 6: Sample Data with a Zero-Cost Row
-- =============================================================================

WITH sample_data AS (
    SELECT
        65000.00 AS saleprice,
        42000.00 AS cost
    UNION ALL
    SELECT
        90000.00 AS saleprice,
        0.00 AS cost
    UNION ALL
    SELECT
        120000.00 AS saleprice,
        85000.00 AS cost
    UNION ALL
    SELECT
        45000.00 AS saleprice,
        0.00 AS cost
    UNION ALL
    SELECT
        200000.00 AS saleprice,
        140000.00 AS cost
)

SELECT
    saleprice,
    cost,
    saleprice / NULLIF(cost, 0) AS nullif_ratio,
    CASE
        WHEN cost = 0 THEN NULL
        ELSE saleprice / cost
    END AS case_ratio,
    TRY_DIVIDE(saleprice, cost) AS try_divide_ratio,
    COALESCE(saleprice / NULLIF(cost, 0), 0) AS coalesce_ratio
FROM sample_data
ORDER BY saleprice;
