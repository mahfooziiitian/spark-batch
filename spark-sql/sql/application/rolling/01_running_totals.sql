-- Demonstrates running (cumulative) calculations using Spark SQL window functions.
-- A running calculation accumulates from the first row up to the current row.
-- Schema: allsales(makename, saleprice, saledate)

-- =============================================================================
-- Section 1: Running total using SUM OVER with explicit frame
-- =============================================================================
SELECT
    saledate,
    makename,
    saleprice,
    SUM(saleprice) OVER (
        ORDER BY saledate
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total
FROM allsales
ORDER BY saledate;

-- =============================================================================
-- Section 2: Running average using AVG OVER same frame
-- =============================================================================
SELECT
    saledate,
    makename,
    saleprice,
    ROUND(AVG(saleprice) OVER (
        ORDER BY saledate
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 2) AS running_average
FROM allsales
ORDER BY saledate;

-- =============================================================================
-- Section 3: Running count using COUNT(*) OVER same frame
-- =============================================================================
SELECT
    saledate,
    makename,
    saleprice,
    COUNT(*) OVER (
        ORDER BY saledate
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_count
FROM allsales
ORDER BY saledate;

-- =============================================================================
-- Section 4: All three using a named WINDOW clause
-- =============================================================================
SELECT
    saledate,
    makename,
    saleprice,
    SUM(saleprice) OVER running_w AS running_total,
    ROUND(AVG(saleprice) OVER running_w, 2) AS running_average,
    COUNT(*) OVER running_w AS running_count
FROM allsales
WINDOW
    running_w AS (
        ORDER BY saledate
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
ORDER BY saledate;

-- =============================================================================
-- Section 5: Sample data (6 rows) with expected output
-- =============================================================================
-- saledate    makename    saleprice  running_total  running_average  running_count
-- 2017-01-01  Ferrari     65000.00   65000.00       65000.00         1
-- 2017-01-02  Bentley     90000.00   155000.00      77500.00         2
-- 2017-01-03  Rolls Royce 115000.00  270000.00      90000.00         3
-- 2017-01-04  Ferrari     72000.00   342000.00      85500.00         4
-- 2017-01-05  Aston Martin 55000.00 397000.00      79400.00         5
-- 2017-01-06  Bentley     80000.00   477000.00      79500.00         6
