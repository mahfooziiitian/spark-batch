-- Demonstrates MIN and MAX as threshold bookends, per-group ranges,
-- above/below threshold flags, and dynamic thresholds from CTEs.
-- Schema: allsales(makename, saleprice, cost, saledate)

-- =============================================================================
-- Section 1: MIN and MAX as threshold bookends across entire table
-- =============================================================================
SELECT
    MIN(saleprice) AS lowest_price,
    MAX(saleprice) AS highest_price,
    MAX(saleprice) - MIN(saleprice) AS overall_range
FROM allsales;

-- =============================================================================
-- Section 2: MIN and MAX per make
-- =============================================================================
SELECT
    makename,
    MIN(saleprice) AS lowest_price,
    MAX(saleprice) AS highest_price
FROM allsales
GROUP BY makename
ORDER BY highest_price DESC;

-- =============================================================================
-- Section 3: Price range per make: MAX - MIN
-- =============================================================================
SELECT
    makename,
    MIN(saleprice) AS lowest_price,
    MAX(saleprice) AS highest_price,
    MAX(saleprice) - MIN(saleprice) AS price_range
FROM allsales
GROUP BY makename
ORDER BY price_range DESC;

-- =============================================================================
-- Section 4: Flag rows above/below threshold using CASE in SELECT
-- =============================================================================
SELECT
    makename,
    saleprice,
    CASE
        WHEN saleprice > 100000 THEN 'Above 100k'
        WHEN saleprice >= 75000 THEN '75k-100k'
        ELSE 'Below 75k'
    END AS price_band
FROM allsales
ORDER BY saleprice DESC;

-- =============================================================================
-- Section 5: Dynamic threshold from CTE — sales above average
-- =============================================================================
WITH avg_price AS (
    SELECT AVG(saleprice) AS avg_sale
    FROM allsales
)

SELECT
    a.makename,
    a.saleprice,
    ROUND(ap.avg_sale, 2) AS avg_sale_price,
    CASE
        WHEN a.saleprice > ap.avg_sale THEN 'Above Average'
        ELSE 'At or Below Average'
    END AS vs_average
FROM allsales AS a
CROSS JOIN avg_price AS ap
ORDER BY a.saleprice DESC;

-- =============================================================================
-- Section 6: Sample data
-- =============================================================================
-- lowest_price  highest_price  overall_range
-- 55000.00      170000.00      115000.00
--
-- makename     lowest_price  highest_price  price_range
-- Ferrari      55000.00      125000.00      70000.00
-- Bentley      80000.00      120000.00      40000.00
-- Rolls Royce  115000.00     170000.00      55000.00
