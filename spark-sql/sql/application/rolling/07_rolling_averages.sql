-- Demonstrates N-record rolling averages using ROWS BETWEEN N PRECEDING AND CURRENT ROW.
-- Early rows have fewer than N predecessors so the average covers fewer records.
-- Schema: allsales(makename, saleprice, saledate)

-- =============================================================================
-- Section 1: 3-record rolling average (ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
-- =============================================================================
SELECT
    saledate,
    makename,
    saleprice,
    ROUND(AVG(saleprice) OVER (
        ORDER BY saledate
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2) AS rolling_avg_3
FROM allsales
ORDER BY saledate;

-- =============================================================================
-- Section 2: 5-record rolling average (ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
-- =============================================================================
SELECT
    saledate,
    makename,
    saleprice,
    ROUND(AVG(saleprice) OVER (
        ORDER BY saledate
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ), 2) AS rolling_avg_5
FROM allsales
ORDER BY saledate;

-- =============================================================================
-- Section 3: Both 3 and 5 record rolling averages in one query for comparison
-- =============================================================================
SELECT
    saledate,
    makename,
    saleprice,
    ROUND(AVG(saleprice) OVER (
        ORDER BY saledate
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2) AS rolling_avg_3,
    ROUND(AVG(saleprice) OVER (
        ORDER BY saledate
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ), 2) AS rolling_avg_5
FROM allsales
ORDER BY saledate;

-- =============================================================================
-- Section 4: Per-make rolling average (PARTITION BY + rolling frame)
-- =============================================================================
SELECT
    makename,
    saledate,
    saleprice,
    ROUND(AVG(saleprice) OVER (
        PARTITION BY makename
        ORDER BY saledate
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2) AS make_rolling_avg_3
FROM allsales
ORDER BY makename, saledate;

-- =============================================================================
-- Section 5: Sample data with 8 rows showing early-row behavior
-- =============================================================================
-- saledate    makename    saleprice  rolling_avg_3  rolling_avg_5
-- 2017-01-01  Ferrari     65000.00   65000.00       65000.00      (1 row only)
-- 2017-01-02  Bentley     90000.00   77500.00       77500.00      (2 rows)
-- 2017-01-03  Rolls Royce 115000.00  90000.00       90000.00      (3 rows: full)
-- 2017-01-04  Ferrari     72000.00   92333.33       85500.00      (3 rows, 4 rows)
-- 2017-01-05  Aston Martin 55000.00  80666.67       79400.00      (3 rows, 5: full)
-- 2017-01-06  Bentley     80000.00   69000.00       82400.00      (3 rows, 5 rows)
-- 2017-01-07  Ferrari     78000.00   71000.00       78000.00      (3 rows, 5 rows)
-- 2017-01-08  Porsche     95000.00   84333.33       76000.00      (3 rows, 5 rows)
