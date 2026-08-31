-- GROUPING SETS: Custom aggregation combinations
-- Define exactly which grouping combinations you need
-- More efficient than CUBE (avoids unwanted combinations)
-- More flexible than ROLLUP (not restricted to a fixed hierarchy)

-- ──────────────────────────────────────────────────────────────────────────────
-- 1. Basic GROUPING SETS: specific combinations only
-- ──────────────────────────────────────────────────────────────────────────────
SELECT
    country,
    makename,
    COUNT(*) AS sale_count,
    ROUND(SUM(saleprice), 2) AS total_revenue
FROM allsales
GROUP BY GROUPING SETS (
    (country, makename),  -- detail: per-country-per-make
    (country),            -- country totals only
    ()                    -- grand total
)
ORDER BY country NULLS LAST, makename NULLS LAST;

/*
Produces 3 levels only — skips the "makename only" subtotal that CUBE would add.
Useful when "Ferrari globally" is not a meaningful metric for this report.
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- 2. Dashboard tiles: region totals + make totals (no detail, no grand total)
-- ──────────────────────────────────────────────────────────────────────────────
SELECT
    country,
    makename,
    color,
    ROUND(SUM(saleprice), 2) AS total_revenue,
    COUNT(*) AS sale_count
FROM allsales
GROUP BY GROUPING SETS (
    (country),       -- tile 1: revenue by country
    (makename),      -- tile 2: revenue by make
    (color)          -- tile 3: revenue by color
)
ORDER BY country NULLS LAST, makename NULLS LAST, color NULLS LAST;

-- ──────────────────────────────────────────────────────────────────────────────
-- 3. GROUPING() labelling: identify which aggregation level each row represents
-- ──────────────────────────────────────────────────────────────────────────────
SELECT
    COALESCE(country, 'All Countries') AS country,
    COALESCE(makename, 'All Makes') AS makename,
    ROUND(SUM(saleprice), 2) AS total_revenue,
    COUNT(*) AS sale_count,
    CASE
        WHEN GROUPING(country) = 0 AND GROUPING(makename) = 0 THEN 'Country + Make'
        WHEN GROUPING(country) = 0 AND GROUPING(makename) = 1 THEN 'Country Total'
        WHEN GROUPING(country) = 1 AND GROUPING(makename) = 0 THEN 'Make Total'
        ELSE 'Grand Total'
    END AS aggregation_level
FROM allsales
GROUP BY GROUPING SETS (
    (country, makename),
    (country),
    (makename),
    ()
)
ORDER BY country NULLS LAST, makename NULLS LAST;

-- ──────────────────────────────────────────────────────────────────────────────
-- 4. Comparison: GROUPING SETS vs ROLLUP vs CUBE equivalence
-- These three queries produce identical results:
-- ──────────────────────────────────────────────────────────────────────────────

-- 4a. ROLLUP equivalent
SELECT
country,
makename,
ROUND(SUM(saleprice), 2) AS total_revenue
FROM allsales
GROUP BY ROLLUP (country, makename);

-- 4b. GROUPING SETS equivalent to the ROLLUP above
SELECT
country,
makename,
ROUND(SUM(saleprice), 2) AS total_revenue
FROM allsales
GROUP BY GROUPING SETS (
    (country, makename),
    (country),
    ()
);

-- 4c. CUBE equivalent
SELECT
country,
makename,
ROUND(SUM(saleprice), 2) AS total_revenue
FROM allsales
GROUP BY CUBE (country, makename);

-- 4d. GROUPING SETS equivalent to the CUBE above
SELECT
country,
makename,
ROUND(SUM(saleprice), 2) AS total_revenue
FROM allsales
GROUP BY GROUPING SETS (
    (country, makename),
    (country),
    (makename),
    ()
);

-- ──────────────────────────────────────────────────────────────────────────────
-- 5. Performance-optimized dashboard: controlled combinations
-- Only the combinations the dashboard tiles actually need
-- ──────────────────────────────────────────────────────────────────────────────
SELECT
    COALESCE(CAST(YEAR(saledate) AS STRING), 'All') AS sale_year,
    COALESCE(country, 'All') AS country,
    COALESCE(makename, 'All') AS makename,
    COUNT(*) AS sale_count,
    ROUND(SUM(saleprice), 2) AS total_revenue,
    ROUND(AVG(saleprice), 2) AS avg_price
FROM allsales
GROUP BY GROUPING SETS (
    (YEAR(saledate), country, makename),  -- most granular: year + country + make
    (YEAR(saledate), country),            -- yearly country summaries
    (YEAR(saledate)),                     -- annual totals
    ()                                    -- overall grand total
)
ORDER BY
    YEAR(saledate) NULLS LAST,
    country NULLS LAST,
    makename NULLS LAST;

-- ──────────────────────────────────────────────────────────────────────────────
-- 6. Sample data CTE — self-contained demo
-- ──────────────────────────────────────────────────────────────────────────────
WITH sample_sales AS (
    SELECT
'UK' AS country,
'Ferrari' AS makename,
'Red' AS color,
55000.00 AS saleprice
    UNION ALL
    SELECT
'UK' AS country,
'Ferrari' AS makename,
'Blue' AS color,
62000.00 AS saleprice
    UNION ALL
    SELECT
'UK' AS country,
'Bentley' AS makename,
'Black' AS color,
105000.00 AS saleprice
    UNION ALL
    SELECT
'France' AS country,
'Ferrari' AS makename,
'Red' AS color,
49000.00 AS saleprice
    UNION ALL
    SELECT
'France' AS country,
'Bentley' AS makename,
'Silver' AS color,
98000.00 AS saleprice
)

SELECT
    COALESCE(country, 'All Countries') AS country,
    COALESCE(makename, 'All Makes') AS makename,
    ROUND(SUM(saleprice), 2) AS total_revenue,
    COUNT(*) AS sale_count,
    CASE
        WHEN GROUPING(country) = 0 AND GROUPING(makename) = 0 THEN 'Country + Make'
        WHEN GROUPING(country) = 0 THEN 'Country Total'
        WHEN GROUPING(makename) = 0 THEN 'Make Total'
        ELSE 'Grand Total'
    END AS level
FROM sample_sales
GROUP BY GROUPING SETS (
    (country, makename),
    (country),
    ()
)
ORDER BY country NULLS LAST, makename NULLS LAST;

/*
Expected output:
 country       | makename  | total_revenue | sale_count | level
---------------+-----------+---------------+------------+---------------
 France        | Bentley   | 98000.00      | 1          | Country + Make
 France        | Ferrari   | 49000.00      | 1          | Country + Make
 France        | All Makes | 147000.00     | 2          | Country Total
 UK            | Bentley   | 105000.00     | 1          | Country + Make
 UK            | Ferrari   | 117000.00     | 2          | Country + Make
 UK            | All Makes | 222000.00     | 3          | Country Total
 All Countries | All Makes | 369000.00     | 5          | Grand Total

Note: "Make Total" row absent — we chose NOT to include (makename) set,
unlike CUBE which would add it automatically.
*/
