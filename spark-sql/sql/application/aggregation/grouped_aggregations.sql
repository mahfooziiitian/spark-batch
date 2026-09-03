-- Demonstrates GROUP BY at single and multiple levels, ROLLUP for
-- hierarchical subtotals, and CUBE for all combination subtotals.
-- Schema: allsales(makename, color, saleprice, saledate)

-- =============================================================================
-- Section 1: GROUP BY makename — per-make totals
-- =============================================================================
SELECT
    makename,
    SUM(saleprice) AS total_sales,
    COUNT(*) AS sale_count
FROM allsales
GROUP BY makename
ORDER BY total_sales DESC;

-- =============================================================================
-- Section 2: GROUP BY makename, color — two-level grouping
-- =============================================================================
SELECT
    makename,
    color,
    SUM(saleprice) AS total_sales,
    COUNT(*) AS sale_count
FROM allsales
GROUP BY makename, color
ORDER BY makename ASC, total_sales DESC;

-- =============================================================================
-- Section 3: ROUND(AVG(saleprice), 2) per group
-- =============================================================================
SELECT
    makename,
    color,
    ROUND(AVG(saleprice), 2) AS avg_sale_price,
    COUNT(*) AS sale_count
FROM allsales
GROUP BY makename, color
ORDER BY makename ASC, avg_sale_price DESC;

-- =============================================================================
-- Section 4: ROLLUP(makename, color) — detail + make subtotal + grand total
-- =============================================================================
SELECT
    makename,
    color,
    SUM(saleprice) AS total_sales,
    COUNT(*) AS sale_count
FROM allsales
GROUP BY ROLLUP (makename, color)
ORDER BY makename NULLS LAST, color NULLS LAST;

-- =============================================================================
-- Section 5: CUBE(makename, color) — all combinations including color-only subtotals
-- =============================================================================
SELECT
    makename,
    color,
    SUM(saleprice) AS total_sales,
    COUNT(*) AS sale_count
FROM allsales
GROUP BY CUBE (makename, color)
ORDER BY makename NULLS LAST, color NULLS LAST;

-- =============================================================================
-- Section 6: Sample data
-- =============================================================================
-- makename     color   total_sales  sale_count
-- Bentley      Black   92000.00     1
-- Bentley      Red     80000.00     1
-- Ferrari      Blue    65000.00     1
-- Ferrari      Red     72000.00     1
-- Rolls Royce  Silver  115000.00    1
-- (ROLLUP adds rows with NULL color for make subtotals, and NULL/NULL grand total)
