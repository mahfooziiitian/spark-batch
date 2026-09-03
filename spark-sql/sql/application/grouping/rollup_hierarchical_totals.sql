-- ROLLUP: Hierarchical totals for drill-down analysis
-- Generates subtotals at each level of a dimension hierarchy
-- e.g. Year → Quarter → Month, Country → State → City
-- NULL in a grouping column = that level was rolled up (subtotal/grand total)

-- ──────────────────────────────────────────────────────────────────────────────
-- 1. Basic ROLLUP: Year → Quarter totals
-- ──────────────────────────────────────────────────────────────────────────────
SELECT
    YEAR(saledate) AS sale_year,
    QUARTER(saledate) AS sale_quarter,
    COUNT(*) AS total_sales,
    ROUND(SUM(saleprice), 2) AS total_revenue
FROM allsales
GROUP BY ROLLUP (YEAR(saledate), QUARTER(saledate))
ORDER BY sale_year NULLS LAST, sale_quarter NULLS LAST;

/*
Expected output (illustrative):
 sale_year | sale_quarter | total_sales | total_revenue
-----------+--------------+-------------+---------------
 2015      | 1            | 12          | 180000.00      ← Q1 2015
 2015      | 2            | 15          | 225000.00      ← Q2 2015
 2015      | NULL         | 27          | 405000.00      ← 2015 subtotal
 2016      | 1            | 10          | 160000.00      ← Q1 2016
 2016      | NULL         | 10          | 160000.00      ← 2016 subtotal
 NULL      | NULL         | 37          | 565000.00      ← grand total
*/

-- ──────────────────────────────────────────────────────────────────────────────
-- 2. Three-level hierarchy: Year → Quarter → Month
-- ──────────────────────────────────────────────────────────────────────────────
SELECT
    YEAR(saledate) AS sale_year,
    QUARTER(saledate) AS sale_quarter,
    MONTH(saledate) AS sale_month,
    COUNT(*) AS total_sales,
    ROUND(SUM(saleprice), 2) AS total_revenue
FROM allsales
GROUP BY ROLLUP (YEAR(saledate), QUARTER(saledate), MONTH(saledate))
ORDER BY sale_year NULLS LAST, sale_quarter NULLS LAST, sale_month NULLS LAST;

-- ──────────────────────────────────────────────────────────────────────────────
-- 3. Geographic hierarchy: Country → Make
-- ──────────────────────────────────────────────────────────────────────────────
SELECT
    country,
    makename,
    ROUND(SUM(saleprice), 2) AS total_revenue,
    COUNT(*) AS sale_count
FROM allsales
GROUP BY ROLLUP (country, makename)
ORDER BY country NULLS LAST, makename NULLS LAST;

-- ──────────────────────────────────────────────────────────────────────────────
-- 4. GROUPING() function: label subtotal rows clearly
-- GROUPING(col) returns 1 when col is NULL due to rollup, 0 otherwise
-- ──────────────────────────────────────────────────────────────────────────────
SELECT
    CASE
        WHEN GROUPING(YEAR(saledate)) = 1 THEN 'Grand Total'
        ELSE CAST(YEAR(saledate) AS STRING)
    END AS sale_year,
    CASE
        WHEN GROUPING(QUARTER(saledate)) = 1 THEN 'Year Total'
        ELSE CONCAT('Q', CAST(QUARTER(saledate) AS STRING))
    END AS sale_quarter,
    COUNT(*) AS total_sales,
    ROUND(SUM(saleprice), 2) AS total_revenue,
    GROUPING(YEAR(saledate)) AS is_year_rollup,
    GROUPING(QUARTER(saledate)) AS is_quarter_rollup
FROM allsales
GROUP BY ROLLUP (YEAR(saledate), QUARTER(saledate))
ORDER BY YEAR(saledate) NULLS LAST, QUARTER(saledate) NULLS LAST;

-- ──────────────────────────────────────────────────────────────────────────────
-- 5. Sample data CTE — self-contained demo
-- ──────────────────────────────────────────────────────────────────────────────
WITH sample_sales AS (
    SELECT
        'UK' AS country,
        'Ferrari' AS makename,
        2015 AS yr,
        1 AS qtr,
        55000.00 AS saleprice
    UNION ALL
    SELECT
        'UK' AS country,
        'Ferrari' AS makename,
        2015 AS yr,
        2 AS qtr,
        62000.00 AS saleprice
    UNION ALL
    SELECT
        'UK' AS country,
        'Bentley' AS makename,
        2015 AS yr,
        1 AS qtr,
        105000.00 AS saleprice
    UNION ALL
    SELECT
        'France' AS country,
        'Ferrari' AS makename,
        2015 AS yr,
        1 AS qtr,
        49000.00 AS saleprice
    UNION ALL
    SELECT
        'France' AS country,
        'Bentley' AS makename,
        2015 AS yr,
        2 AS qtr,
        98000.00 AS saleprice
    UNION ALL
    SELECT
        'UK' AS country,
        'Ferrari' AS makename,
        2016 AS yr,
        1 AS qtr,
        71000.00 AS saleprice
)

SELECT
    COALESCE(CAST(yr AS STRING), 'Grand Total') AS sale_year,
    COALESCE(CAST(qtr AS STRING), 'Subtotal') AS sale_quarter,
    ROUND(SUM(saleprice), 2) AS total_revenue,
    COUNT(*) AS sale_count
FROM sample_sales
GROUP BY ROLLUP (yr, qtr)
ORDER BY yr NULLS LAST, qtr NULLS LAST;

/*
Expected output:
 sale_year   | sale_quarter | total_revenue | sale_count
-------------+--------------+---------------+------------
 2015        | 1            | 209000.00     | 3
 2015        | 2            | 160000.00     | 2
 2015        | Subtotal     | 369000.00     | 5          ← 2015 year subtotal
 2016        | 1            | 71000.00      | 1
 2016        | Subtotal     | 71000.00      | 1          ← 2016 year subtotal
 Grand Total | Subtotal     | 440000.00     | 6          ← overall grand total
*/
