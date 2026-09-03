-- CUBE: All-combinations aggregation for cross-dimensional analysis
-- Generates every possible subset of dimension combinations
-- For n dimensions → 2ⁿ grouping combinations (use with care on wide schemas)
-- NULL in a column = that dimension was excluded from this aggregation level

-- ──────────────────────────────────────────────────────────────────────────────
-- 1. Basic CUBE: Region × Make — all four combinations
-- ──────────────────────────────────────────────────────────────────────────────
SELECT
    country,
    makename,
    COUNT(*) AS sale_count,
    ROUND(SUM(saleprice), 2) AS total_revenue
FROM allsales
GROUP BY CUBE (country, makename)
ORDER BY country NULLS LAST, makename NULLS LAST;

/*
Combinations produced (2² = 4):
  country + makename  → per-country-per-make subtotal
  country only        → country total  (makename IS NULL)
  makename only       → make total     (country  IS NULL)
  (nothing)           → grand total    (both NULL)
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- 2. Three-dimension CUBE: Country × Make × Color (2³ = 8 combinations)
-- ──────────────────────────────────────────────────────────────────────────────
SELECT
    country,
    makename,
    color,
    COUNT(*) AS sale_count,
    ROUND(SUM(saleprice), 2) AS total_revenue
FROM allsales
GROUP BY CUBE (country, makename, color)
ORDER BY country NULLS LAST, makename NULLS LAST, color NULLS LAST;

-- ──────────────────────────────────────────────────────────────────────────────
-- 3. GROUPING() to label each row's aggregation level clearly
-- ──────────────────────────────────────────────────────────────────────────────
SELECT
    CASE WHEN GROUPING(country) = 1 THEN 'All Countries' ELSE country END AS country,
    CASE WHEN GROUPING(makename) = 1 THEN 'All Makes' ELSE makename END AS makename,
    COUNT(*) AS sale_count,
    ROUND(SUM(saleprice), 2) AS total_revenue,
    CASE
        WHEN GROUPING(country) = 0 AND GROUPING(makename) = 0 THEN 'Detail'
        WHEN GROUPING(country) = 0 AND GROUPING(makename) = 1 THEN 'Country Total'
        WHEN GROUPING(country) = 1 AND GROUPING(makename) = 0 THEN 'Make Total'
        ELSE 'Grand Total'
    END AS aggregation_level
FROM allsales
GROUP BY CUBE (country, makename)
ORDER BY country NULLS LAST, makename NULLS LAST;

-- ──────────────────────────────────────────────────────────────────────────────
-- 4. Pivot-style dashboard: Year × Quarter across all combinations
-- ──────────────────────────────────────────────────────────────────────────────
SELECT
    COALESCE(CAST(YEAR(saledate) AS STRING), 'All Years') AS sale_year,
    COALESCE(CONCAT('Q', CAST(QUARTER(saledate) AS STRING)), 'All Quarters') AS sale_quarter,
    COUNT(*) AS total_sales,
    ROUND(SUM(saleprice), 2) AS total_revenue,
    ROUND(AVG(saleprice), 2) AS avg_sale_price
FROM allsales
GROUP BY CUBE (YEAR(saledate), QUARTER(saledate))
ORDER BY YEAR(saledate) NULLS LAST, QUARTER(saledate) NULLS LAST;

-- ──────────────────────────────────────────────────────────────────────────────
-- 5. Sample data CTE — self-contained demo
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
    COUNT(*) AS sale_count
FROM sample_sales
GROUP BY CUBE (country, makename)
ORDER BY country NULLS LAST, makename NULLS LAST;

/*
Expected output (2² = 4 combinations):
 country       | makename   | total_revenue | sale_count
---------------+------------+---------------+------------
 France        | Bentley    | 98000.00      | 1          ← detail
 France        | Ferrari    | 49000.00      | 1          ← detail
 France        | All Makes  | 147000.00     | 2          ← France total
 UK            | Bentley    | 105000.00     | 1          ← detail
 UK            | Ferrari    | 117000.00     | 2          ← detail
 UK            | All Makes  | 222000.00     | 3          ← UK total
 All Countries | Bentley    | 203000.00     | 2          ← Bentley total
 All Countries | Ferrari    | 166000.00     | 3          ← Ferrari total
 All Countries | All Makes  | 369000.00     | 5          ← grand total
*/
