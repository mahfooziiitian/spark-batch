-- Demonstrates LAG and LEAD window functions for period-over-period comparison.
-- Shows default values, PARTITION BY, and string comparisons.
-- Schema: allsales(makename, saleprice, saledate)

-- =============================================================================
-- Section 1: LAG(saleprice, 1) with change calculation
-- =============================================================================
SELECT
    saledate,
    makename,
    saleprice,
    LAG(saleprice, 1) OVER (ORDER BY saledate) AS prev_saleprice,
    saleprice - LAG(saleprice, 1) OVER (ORDER BY saledate) AS price_change
FROM allsales
ORDER BY saledate;

-- =============================================================================
-- Section 2: LAG with default value (0.00 for first row)
-- =============================================================================
SELECT
    saledate,
    makename,
    saleprice,
    LAG(saleprice, 1, 0.00) OVER (ORDER BY saledate) AS prev_saleprice,
    saleprice - LAG(saleprice, 1, 0.00) OVER (ORDER BY saledate) AS price_change
FROM allsales
ORDER BY saledate;

-- =============================================================================
-- Section 3: LEAD(saleprice, 1) to look ahead one row
-- =============================================================================
SELECT
    saledate,
    makename,
    saleprice,
    LEAD(saleprice, 1) OVER (ORDER BY saledate) AS next_saleprice,
    LEAD(saleprice, 1) OVER (ORDER BY saledate) - saleprice AS next_change
FROM allsales
ORDER BY saledate;

-- =============================================================================
-- Section 4: PARTITION BY for per-make LAG (compare each make's own previous sale)
-- =============================================================================
SELECT
    makename,
    saledate,
    saleprice,
    LAG(saleprice, 1) OVER (PARTITION BY makename ORDER BY saledate) AS prev_make_price,
    saleprice - LAG(saleprice, 1) OVER (
        PARTITION BY makename ORDER BY saledate
    ) AS make_price_change
FROM allsales
ORDER BY makename, saledate;

-- =============================================================================
-- Section 5: LAG with string data — alphabetical comparison of makename
-- =============================================================================
SELECT
    saledate,
    makename,
    LAG(makename, 1) OVER (ORDER BY saledate) AS prev_makename,
    CASE
        WHEN makename > LAG(makename, 1) OVER (ORDER BY saledate)
            THEN 'Later alphabetically'
        WHEN makename < LAG(makename, 1) OVER (ORDER BY saledate)
            THEN 'Earlier alphabetically'
        ELSE 'Same make'
    END AS alpha_comparison
FROM allsales
ORDER BY saledate;

-- =============================================================================
-- Section 6: Sample data with expected output (NULLs for first/last rows)
-- =============================================================================
-- saledate    makename    saleprice  prev_saleprice  price_change
-- 2017-01-01  Ferrari     65000.00   NULL            NULL
-- 2017-01-02  Bentley     90000.00   65000.00        25000.00
-- 2017-01-03  Rolls Royce 115000.00  90000.00        25000.00
-- 2017-01-04  Ferrari     72000.00   115000.00       -43000.00
-- 2017-01-05  Aston Martin 55000.00  72000.00        -17000.00
-- 2017-01-06  Bentley     80000.00   55000.00        25000.00
