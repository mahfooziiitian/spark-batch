-- Demonstrates PERCENTILE, PERCENTILE_APPROX, median calculation,
-- and percentage of each record within the dataset and within groups.
-- Schema: allsales(makename, saleprice, saledate)

-- =============================================================================
-- Section 1: PERCENTILE(col, 0.5) — exact median sale price
-- =============================================================================
SELECT PERCENTILE(saleprice, 0.5) AS median_price
FROM allsales;

-- =============================================================================
-- Section 2: Quartile boundaries — 25th and 75th percentile
-- =============================================================================
SELECT
    PERCENTILE(saleprice, 0.25) AS q1_price,
    PERCENTILE(saleprice, 0.5) AS median_price,
    PERCENTILE(saleprice, 0.75) AS q3_price
FROM allsales;

-- =============================================================================
-- Section 3: PERCENTILE_APPROX — Spark native approximate percentile
-- =============================================================================
SELECT
    PERCENTILE_APPROX(saleprice, 0.5) AS approx_median,
    PERCENTILE_APPROX(saleprice, 0.25) AS approx_q1,
    PERCENTILE_APPROX(saleprice, 0.75) AS approx_q3
FROM allsales;

-- =============================================================================
-- Section 4: Percentage each record represents of total (empty OVER = grand total)
-- =============================================================================
SELECT
    makename,
    saledate,
    saleprice,
    ROUND(saleprice * 100.0 / SUM(saleprice) OVER (), 4) AS pct_of_total
FROM allsales
ORDER BY saleprice DESC;

-- =============================================================================
-- Section 5: Percentage within group (per-make contribution)
-- =============================================================================
SELECT
    makename,
    saledate,
    saleprice,
    ROUND(saleprice * 100.0 / SUM(saleprice) OVER (), 4) AS pct_of_total,
    ROUND(saleprice * 100.0 / SUM(saleprice) OVER (PARTITION BY makename), 4) AS pct_of_make
FROM allsales
ORDER BY makename ASC, saleprice DESC;

-- =============================================================================
-- Section 6: Sample data with expected values
-- =============================================================================
-- makename    saleprice  pct_of_total  pct_of_make
-- Ferrari     65000.00   9.5238        47.7941
-- Ferrari     72000.00   10.5495       52.2059
-- Bentley     80000.00   11.7216       47.0588
-- Bentley     90000.00   13.1868       52.9412
-- Rolls Royce 115000.00  16.8566       100.0000
-- Aston Martin 55000.00  8.0645        100.0000
