-- Aggregates 2017 sales into hourly bands (e.g. "9-10", "14-15") so the
-- CEO can identify which hours of the day generate the most revenue.
--
-- HOUR(timestamp) extracts the hour component (0–23).
-- The band label CONCAT(hour, '-', hour + 1) produces a readable range string.
--
-- Two approaches are shown:
--   1. Derived table  — matches the original query structure (noqa ST05)
--   2. CTE refactor   — Spark-idiomatic, avoids the nested subquery entirely

-- Sample data: car sales at various hours in 2017
-- (one 2016 and one 2018 row to demonstrate the YEAR filter)
WITH salesbycountry AS (
    SELECT
        CAST('2017-03-15 09:15:00' AS TIMESTAMP) AS saledate,
        'UK' AS country,
        28500.00 AS saleprice
    UNION ALL
    SELECT
        CAST('2017-06-22 09:45:00' AS TIMESTAMP) AS saledate,
        'DE' AS country,
        42000.00 AS saleprice
    UNION ALL
    SELECT
        CAST('2017-01-08 10:30:00' AS TIMESTAMP) AS saledate,
        'FR' AS country,
        19800.00 AS saleprice
    UNION ALL
    SELECT
        CAST('2017-09-14 11:20:00' AS TIMESTAMP) AS saledate,
        'UK' AS country,
        51000.00 AS saleprice
    UNION ALL
    SELECT
        CAST('2017-04-30 12:05:00' AS TIMESTAMP) AS saledate,
        'DE' AS country,
        33200.00 AS saleprice
    UNION ALL
    SELECT
        CAST('2017-11-03 14:30:00' AS TIMESTAMP) AS saledate,
        'UK' AS country,
        47500.00 AS saleprice
    UNION ALL
    SELECT
        CAST('2017-07-19 14:55:00' AS TIMESTAMP) AS saledate,
        'FR' AS country,
        22300.00 AS saleprice
    UNION ALL
    SELECT
        CAST('2017-02-27 16:10:00' AS TIMESTAMP) AS saledate,
        'UK' AS country,
        16500.00 AS saleprice
    UNION ALL
    -- 2016 row — excluded by YEAR(SaleDate) = 2017
    SELECT
        CAST('2016-08-10 11:00:00' AS TIMESTAMP) AS saledate,
        'DE' AS country,
        58000.00 AS saleprice
    UNION ALL
    -- 2018 row — excluded by YEAR(SaleDate) = 2017
    SELECT
        CAST('2018-01-05 10:00:00' AS TIMESTAMP) AS saledate,
        'UK' AS country,
        62000.00 AS saleprice
)

-- ── Approach 1: Derived table (original pattern) ──────────────────────────────

SELECT
    CONCAT(a.hourofday, '-', a.hourofday + 1) AS hourband,
    SUM(a.saleprice) AS salesbyhourband
FROM (  -- noqa: ST05
    SELECT
        saleprice,
        HOUR(saledate) AS hourofday
    FROM salesbycountry
    WHERE YEAR(saledate) = 2017
) AS a
GROUP BY a.hourofday
ORDER BY a.hourofday;

/* Expected output:
   hourband | salesbyhourband
   ---------+----------------
   9-10     |        70500.00   (28500 + 42000)
   10-11    |        19800.00
   11-12    |        51000.00
   12-13    |        33200.00
   14-15    |        69800.00   (47500 + 22300)
   16-17    |        16500.00
*/

-- ── Approach 2: CTE refactor (Spark-idiomatic, no nested subquery) ────────────

WITH hourly_sales AS (
    SELECT
        saleprice,
        HOUR(saledate) AS hourofday
    FROM salesbycountry
    WHERE YEAR(saledate) = 2017
)

SELECT
    CONCAT(hourofday, '-', hourofday + 1) AS hourband,
    SUM(saleprice) AS salesbyhourband
FROM hourly_sales
GROUP BY hourofday
ORDER BY hourofday;
