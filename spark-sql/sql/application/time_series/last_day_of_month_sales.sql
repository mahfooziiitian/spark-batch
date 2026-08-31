-- Aggregates total sales that occurred on the last day of each month in 2016.
--
-- Pattern:
--   1. Tally CTE  → integers 1..12 (one per month)
--   2. LastDay CTE → LAST_DAY(MAKE_DATE(2016, nm, 1)) for each month
--   3. JOIN onto sales table on the exact last-day date
--
-- MAKE_DATE(year, month, day) avoids the string-padding issue in
-- CONCAT('2016-', nm, '-01') when nm < 10 (gives '2016-1-01' which
-- some dialects reject).  LAST_DAY() handles the leap-year February
-- automatically (2016 is a leap year → Feb last day = 2016-02-29).

WITH tally_cte AS (
    SELECT EXPLODE(SEQUENCE(1, 12)) AS nm
),

last_day_of_month_cte AS (
    SELECT LAST_DAY(MAKE_DATE(2016, nm, 1)) AS lastdaydate
    FROM tally_cte
),

-- Sample sales data: some on last days of month, some mid-month (excluded)
salesbycountry AS (
    -- 2016-01-31  last day of January
    SELECT
        CAST('2016-01-31' AS DATE) AS saledate,
        'UK' AS country,
        15000.00 AS saleprice
    UNION ALL
    SELECT
        CAST('2016-01-31' AS DATE) AS saledate,
        'DE' AS country,
        22000.00 AS saleprice
    UNION ALL
    -- 2016-02-29  last day of February (leap year)
    SELECT
        CAST('2016-02-29' AS DATE) AS saledate,
        'FR' AS country,
        18500.00 AS saleprice
    UNION ALL
    -- 2016-06-30  last day of June
    SELECT
        CAST('2016-06-30' AS DATE) AS saledate,
        'UK' AS country,
        31000.00 AS saleprice
    UNION ALL
    SELECT
        CAST('2016-06-30' AS DATE) AS saledate,
        'DE' AS country,
        9500.00 AS saleprice
    UNION ALL
    -- 2016-09-30  last day of September
    SELECT
        CAST('2016-09-30' AS DATE) AS saledate,
        'FR' AS country,
        27000.00 AS saleprice
    UNION ALL
    -- 2016-12-31  last day of December
    SELECT
        CAST('2016-12-31' AS DATE) AS saledate,
        'UK' AS country,
        42000.00 AS saleprice
    UNION ALL
    SELECT
        CAST('2016-12-31' AS DATE) AS saledate,
        'DE' AS country,
        27500.00 AS saleprice
    UNION ALL
    -- Mid-month rows — excluded because date ≠ last day of month
    SELECT
        CAST('2016-01-15' AS DATE) AS saledate,
        'UK' AS country,
        19000.00 AS saleprice
    UNION ALL
    SELECT
        CAST('2016-06-14' AS DATE) AS saledate,
        'FR' AS country,
        14000.00 AS saleprice
    UNION ALL
    SELECT
        CAST('2016-12-25' AS DATE) AS saledate,
        'DE' AS country,
        8500.00 AS saleprice
)

SELECT
    cte.lastdaydate,
    SUM(sls.saleprice) AS totaldailysales
FROM salesbycountry AS sls
INNER JOIN last_day_of_month_cte AS cte
    ON DATE(sls.saledate) = cte.lastdaydate
GROUP BY cte.lastdaydate
ORDER BY cte.lastdaydate;

/* Expected output (months with at least one sale on the last day):
   lastdaydate | totaldailysales
   ------------+----------------
   2016-01-31  |        37000.00   (15000 + 22000)
   2016-02-29  |        18500.00
   2016-06-30  |        40500.00   (31000 + 9500)
   2016-09-30  |        27000.00
   2016-12-31  |        69500.00   (42000 + 27500)

   Months with no last-day sales (Mar–May, Jul–Aug, Oct–Nov) produce
   no rows because the INNER JOIN finds no match.
*/
