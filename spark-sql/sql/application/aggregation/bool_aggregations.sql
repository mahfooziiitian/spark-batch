-- Demonstrates boolean-style aggregations in Spark SQL.
-- Spark SQL has no BOOL_AND/BOOL_OR; use MIN/MAX or COUNT patterns instead.
-- Schema: allsales(makename, color, saleprice, saledate)

-- =============================================================================
-- Section 1: Makes where ALL sales exceed 50000 — MIN(saleprice) > 50000
-- =============================================================================
SELECT
    makename,
    MIN(saleprice) AS min_sale,
    COUNT(*) AS sale_count
FROM allsales
GROUP BY makename
HAVING MIN(saleprice) > 50000
ORDER BY min_sale DESC;

-- =============================================================================
-- Section 2: Makes where ANY sale exceeds 150000 — MAX(saleprice) > 150000
-- =============================================================================
SELECT
    makename,
    MAX(saleprice) AS max_sale,
    COUNT(*) AS sale_count
FROM allsales
GROUP BY makename
HAVING MAX(saleprice) > 150000
ORDER BY max_sale DESC;

-- =============================================================================
-- Section 3: Makes where ALL cars are the same colour — COUNT(DISTINCT color) = 1
-- =============================================================================
SELECT
    makename,
    COUNT(DISTINCT color) AS unique_color_count,
    MIN(color) AS the_color,
    COUNT(*) AS sale_count
FROM allsales
GROUP BY makename
HAVING COUNT(DISTINCT color) = 1
ORDER BY makename;

-- =============================================================================
-- Section 4: Sample data with expected output
-- =============================================================================
-- Section 1 result (all sales > 50000):
-- makename     min_sale   sale_count
-- Rolls Royce  115000.00  3
-- Bentley      80000.00   2
--
-- Section 2 result (any sale > 150000):
-- makename     max_sale   sale_count
-- Rolls Royce  170000.00  3
--
-- Section 3 result (all same colour):
-- makename     unique_color_count  the_color  sale_count
-- Rolls Royce  1                   Silver     3
-- (All Rolls Royce entries are Silver in sample data)
