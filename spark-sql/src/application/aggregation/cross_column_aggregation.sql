-- Demonstrates GREATEST/LEAST across columns, NULL-safe row-level sums,
-- and aggregating across both columns and rows.
-- Schema: allsales(makename, saleprice, totalsaleprice, cost, saledate)

-- =============================================================================
-- Section 1: GREATEST and LEAST across multiple numeric columns
-- =============================================================================
SELECT
    makename,
    saleprice,
    totalsaleprice,
    cost,
    GREATEST(saleprice, totalsaleprice, cost) AS highest_value,
    LEAST(saleprice, totalsaleprice, cost) AS lowest_value
FROM allsales
ORDER BY makename;

-- =============================================================================
-- Section 2: NULL-safe row-level sum using COALESCE
-- =============================================================================
SELECT
    makename,
    saleprice,
    totalsaleprice,
    cost,
    saleprice + COALESCE(totalsaleprice, 0) + COALESCE(cost, 0) AS row_total
FROM allsales
ORDER BY makename;

-- =============================================================================
-- Section 3: Aggregate across columns AND rows — SUM of two revenue columns
-- =============================================================================
SELECT
    makename,
    SUM(saleprice) AS total_saleprice,
    SUM(totalsaleprice) AS total_totalsaleprice,
    SUM(saleprice + COALESCE(totalsaleprice, 0)) AS combined_revenue
FROM allsales
GROUP BY makename
ORDER BY combined_revenue DESC;

-- =============================================================================
-- Section 4: Sample data with NULLs to show COALESCE importance
-- =============================================================================
-- makename     saleprice  totalsaleprice  cost       row_total
-- Ferrari      65000.00   NULL            50000.00   115000.00  (totalsaleprice NULL -> 0)
-- Bentley      90000.00   85000.00        70000.00   245000.00
-- Rolls Royce  115000.00  110000.00       NULL       225000.00  (cost NULL -> 0)
-- (Without COALESCE, any NULL in sum would make row_total NULL)
