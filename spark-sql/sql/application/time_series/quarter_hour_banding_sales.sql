-- Aggregates 2017 sales into quarter-hourly tranches (1–4) to reveal
-- which part of the hour generates the most revenue.
--
-- Quarter derivation — FLOOR((MINUTE(ts) / 15) + 1):
--   minutes  0–14  → quarter 1  (:00 – :14)
--   minutes 15–29  → quarter 2  (:15 – :29)
--   minutes 30–44  → quarter 3  (:30 – :44)
--   minutes 45–59  → quarter 4  (:45 – :59)
--
-- Two approaches shown:
--   1. Derived table — matches the original query structure (noqa ST05)
--   2. CTE refactor  — Spark-idiomatic, adds a human-readable range label

-- Sample data: sales spread across all four quarters of the hour in 2017
WITH salesbycountry AS (
    -- Quarter 1  (:00–:14)
    SELECT
        CAST('2017-03-15 09:05:00' AS TIMESTAMP) AS saledate,
        'UK' AS country,
        28500.00 AS saleprice
    UNION ALL
    SELECT
        CAST('2017-06-22 09:10:00' AS TIMESTAMP) AS saledate,
        'DE' AS country,
        42000.00 AS saleprice
    UNION ALL
    SELECT
        CAST('2017-02-27 16:00:00' AS TIMESTAMP) AS saledate,
        'FR' AS country,
        16500.00 AS saleprice
    UNION ALL
    -- Quarter 2  (:15–:29)
    SELECT
        CAST('2017-01-08 10:20:00' AS TIMESTAMP) AS saledate,
        'UK' AS country,
        19800.00 AS saleprice
    UNION ALL
    SELECT
        CAST('2017-09-14 11:20:00' AS TIMESTAMP) AS saledate,
        'DE' AS country,
        51000.00 AS saleprice
    UNION ALL
    -- Quarter 3  (:30–:44)
    SELECT
        CAST('2017-04-30 12:35:00' AS TIMESTAMP) AS saledate,
        'UK' AS country,
        33200.00 AS saleprice
    UNION ALL
    -- Quarter 4  (:45–:59)
    SELECT
        CAST('2017-11-03 14:45:00' AS TIMESTAMP) AS saledate,
        'FR' AS country,
        47500.00 AS saleprice
    UNION ALL
    SELECT
        CAST('2017-07-19 15:55:00' AS TIMESTAMP) AS saledate,
        'UK' AS country,
        22300.00 AS saleprice
    UNION ALL
    -- 2016 row — excluded by YEAR(saledate) = 2017
    SELECT
        CAST('2016-08-10 11:00:00' AS TIMESTAMP) AS saledate,
        'DE' AS country,
        58000.00 AS saleprice
    UNION ALL
    -- 2018 row — excluded by YEAR(saledate) = 2017
    SELECT
        CAST('2018-01-05 10:00:00' AS TIMESTAMP) AS saledate,
        'UK' AS country,
        62000.00 AS saleprice
)

-- ── Approach 1: Derived table (original pattern) ──────────────────────────────

SELECT
    a.quarterofhour,
    SUM(a.saleprice) AS salesbyquarterhourbband
FROM (  -- noqa: ST05
    SELECT
        saleprice,
        FLOOR((MINUTE(saledate) / 15) + 1) AS quarterofhour
    FROM salesbycountry
    WHERE YEAR(saledate) = 2017
) AS a
GROUP BY a.quarterofhour
ORDER BY a.quarterofhour;

/* Expected output:
   quarterofhour | salesbyquarterhourband
   --------------+-----------------------
               1 |               87000.00   (28500 + 42000 + 16500)
               2 |               70800.00   (19800 + 51000)
               3 |               33200.00
               4 |               69800.00   (47500 + 22300)
*/

-- ── Approach 2: CTE refactor with readable range label ────────────────────────

WITH quarter_sales AS (
    SELECT
        saleprice,
        FLOOR((MINUTE(saledate) / 15) + 1) AS quarterofhour
    FROM salesbycountry
    WHERE YEAR(saledate) = 2017
)

SELECT
    quarterofhour,
    CASE quarterofhour
        WHEN 1 THEN ':00–:14'
        WHEN 2 THEN ':15–:29'
        WHEN 3 THEN ':30–:44'
        WHEN 4 THEN ':45–:59'
    END AS quarterlabel,
    SUM(saleprice) AS salesbyquarterhourband
FROM quarter_sales
GROUP BY quarterofhour
ORDER BY quarterofhour;

/* Expected output:
   quarterofhour | quarterlabel | salesbyquarterhourband
   --------------+--------------+-----------------------
               1 | :00–:14      |               87000.00
               2 | :15–:29      |               70800.00
               3 | :30–:44      |               33200.00
               4 | :45–:59      |               69800.00
*/
