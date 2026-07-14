-- Demonstrates HAVING clause for filtering aggregated groups.
-- Covers SUM threshold, COUNT threshold, WHERE+HAVING, subquery in HAVING,
-- and sorting by aggregate expressions.
-- Schema: allsales(makename, color, saleprice, saledate)

-- =============================================================================
-- Section 1: HAVING SUM(saleprice) > 200000
-- =============================================================================
SELECT
    makename,
    SUM(saleprice) AS total_sales
FROM allsales
GROUP BY makename
HAVING SUM(saleprice) > 200000
ORDER BY total_sales DESC;

-- =============================================================================
-- Section 2: HAVING COUNT(*) >= 3 — makes with at least 3 sales
-- =============================================================================
SELECT
    makename,
    COUNT(*) AS sale_count,
    SUM(saleprice) AS total_sales
FROM allsales
GROUP BY makename
HAVING COUNT(*) >= 3
ORDER BY sale_count DESC;

-- =============================================================================
-- Section 3: WHERE + HAVING together (WHERE before grouping, HAVING after)
-- =============================================================================
SELECT
    makename,
    color,
    COUNT(*) AS sale_count,
    SUM(saleprice) AS total_sales
FROM allsales
WHERE saledate >= '2017-01-01'
GROUP BY makename, color
HAVING SUM(saleprice) > 50000
ORDER BY total_sales DESC;

-- =============================================================================
-- Section 4: HAVING with average vs overall average (CTE for clarity)
-- =============================================================================
WITH overall_avg AS (
    SELECT AVG(saleprice) AS avg_price
    FROM allsales
)

SELECT
    a.makename,
    ROUND(AVG(a.saleprice), 2) AS make_avg_price
FROM allsales AS a
CROSS JOIN overall_avg AS oa
GROUP BY a.makename, oa.avg_price
HAVING AVG(a.saleprice) > oa.avg_price
ORDER BY make_avg_price DESC;

-- =============================================================================
-- Section 5: ORDER BY aggregated expression: ORDER BY SUM(saleprice) DESC
-- =============================================================================
SELECT
    makename,
    SUM(saleprice) AS total_sales,
    COUNT(*) AS sale_count
FROM allsales
GROUP BY makename
ORDER BY SUM(saleprice) DESC;

-- =============================================================================
-- Section 6: ORDER BY COUNT(*) with HAVING filtering
-- =============================================================================
SELECT
    color,
    COUNT(*) AS sale_count,
    SUM(saleprice) AS total_sales
FROM allsales
GROUP BY color
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC;

-- =============================================================================
-- Section 7: Sample data
-- =============================================================================
-- Section 1 result (HAVING SUM > 200000):
-- makename     total_sales
-- Ferrari      425000.00
-- Bentley      340000.00
--
-- Section 2 result (HAVING COUNT >= 3):
-- makename     sale_count  total_sales
-- Ferrari      5           425000.00
-- Bentley      3           340000.00
