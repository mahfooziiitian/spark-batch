-- Demonstrates COUNT(*) vs COUNT(col), COUNT DISTINCT, and approximate
-- distinct counts. NULLs in columns affect COUNT(col) but not COUNT(*).
-- Schema: allsales(makename, color, customername, saleprice, saledate)

-- =============================================================================
-- Section 1: COUNT(*) vs COUNT(color) — NULL behavior
-- =============================================================================
SELECT
    COUNT(*) AS total_rows,
    COUNT(color) AS non_null_colors
FROM allsales;

-- =============================================================================
-- Section 2: COUNT(DISTINCT color) — unique colour count
-- =============================================================================
SELECT
    COUNT(DISTINCT color) AS unique_colors,
    COUNT(DISTINCT makename) AS unique_makes
FROM allsales;

-- =============================================================================
-- Section 3: COUNT(DISTINCT customername) per make
-- =============================================================================
SELECT
    makename,
    COUNT(DISTINCT customername) AS unique_customers,
    COUNT(*) AS total_sales
FROM allsales
GROUP BY makename
ORDER BY unique_customers DESC;

-- =============================================================================
-- Section 4: Multiple distinct counts in one query
-- =============================================================================
SELECT
    COUNT(*) AS total_rows,
    COUNT(color) AS non_null_colors,
    COUNT(DISTINCT color) AS unique_colors,
    COUNT(DISTINCT makename) AS unique_makes,
    COUNT(DISTINCT customername) AS unique_customers
FROM allsales;

-- =============================================================================
-- Section 5: APPROX_COUNT_DISTINCT for large datasets
-- =============================================================================
SELECT
    APPROX_COUNT_DISTINCT(customername) AS approx_unique_customers,
    APPROX_COUNT_DISTINCT(color) AS approx_unique_colors
FROM allsales;

-- =============================================================================
-- Section 6: Sample data with deliberate NULLs showing COUNT behavior
-- =============================================================================
-- Sample: 8 rows, 2 have NULL color
-- total_rows  non_null_colors  unique_colors
-- 8           6                4
-- (COUNT(*) = 8, COUNT(color) = 6 because 2 NULLs are excluded)
