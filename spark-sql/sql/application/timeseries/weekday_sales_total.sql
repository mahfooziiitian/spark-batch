-- Finds the total sale price for every weekday (Mon–Fri) in a given year
-- where at least one sale occurred, ordered chronologically by day of year.
--
-- Key functions:
--   DAYOFYEAR(date)           → ordinal day within the year (1–366)
--   DATE_FORMAT(date, 'EEEE') → full weekday name  (e.g. 'Monday')
--   WEEKDAY(date)             → 0=Mon … 4=Fri, 5=Sat, 6=Sun
--   NOT IN (5, 6)             → exclude Saturday and Sunday

-- Sample data: car sales across weekdays and weekends in 2018
WITH allsales AS (
    -- Monday  2018-01-01  (DAYOFYEAR = 1)
    SELECT
        CAST('2018-01-01' AS DATE) AS saledate,
        12500.00 AS saleprice
    UNION ALL
    SELECT
        CAST('2018-01-01' AS DATE) AS saledate,
        18000.00 AS saleprice
    UNION ALL
    -- Tuesday  2018-01-02  (DAYOFYEAR = 2)
    SELECT
        CAST('2018-01-02' AS DATE) AS saledate,
        9800.00 AS saleprice
    UNION ALL
    -- Wednesday  2018-01-03  (DAYOFYEAR = 3)
    SELECT
        CAST('2018-01-03' AS DATE) AS saledate,
        22000.00 AS saleprice
    UNION ALL
    SELECT
        CAST('2018-01-03' AS DATE) AS saledate,
        15500.00 AS saleprice
    UNION ALL
    -- Thursday  2018-01-04  (DAYOFYEAR = 4)
    SELECT
        CAST('2018-01-04' AS DATE) AS saledate,
        31000.00 AS saleprice
    UNION ALL
    -- Friday  2018-01-05  (DAYOFYEAR = 5)
    SELECT
        CAST('2018-01-05' AS DATE) AS saledate,
        14200.00 AS saleprice
    UNION ALL
    -- Saturday 2018-01-06 (WEEKDAY = 5) — excluded by NOT IN (5, 6)
    SELECT
        CAST('2018-01-06' AS DATE) AS saledate,
        27000.00 AS saleprice
    UNION ALL
    -- Sunday 2018-01-07 (WEEKDAY = 6) — excluded by NOT IN (5, 6)
    SELECT
        CAST('2018-01-07' AS DATE) AS saledate,
        19500.00 AS saleprice
    UNION ALL
    -- Monday  2018-01-08  (DAYOFYEAR = 8)
    SELECT
        CAST('2018-01-08' AS DATE) AS saledate,
        8900.00 AS saleprice
    UNION ALL
    -- Friday  2018-06-15  (DAYOFYEAR = 166)
    SELECT
        CAST('2018-06-15' AS DATE) AS saledate,
        41000.00 AS saleprice
    UNION ALL
    SELECT
        CAST('2018-06-15' AS DATE) AS saledate,
        23500.00 AS saleprice
    UNION ALL
    -- Prior year row — excluded by YEAR(saledate) = 2018
    SELECT
        CAST('2017-03-10' AS DATE) AS saledate,
        16000.00 AS saleprice
)

SELECT
    DAYOFYEAR(saledate) AS daynumber,
    SUM(saleprice) AS saleprice,
    DATE_FORMAT(saledate, 'EEEE') AS dayname
FROM allsales
WHERE
    YEAR(saledate) = 2018
    AND WEEKDAY(saledate) NOT IN (5, 6)
GROUP BY
    DAYOFYEAR(saledate),
    DATE_FORMAT(saledate, 'EEEE')
ORDER BY daynumber;

/* Expected output (weekdays in 2018 with sales, sorted by day of year):
   daynumber | saleprice | dayname
   ----------+-----------+-----------
           1 |  30500.00 | Monday
           2 |   9800.00 | Tuesday
           3 |  37500.00 | Wednesday
           4 |  31000.00 | Thursday
           5 |  14200.00 | Friday
           8 |   8900.00 | Monday
         166 |  64500.00 | Friday

   Rows for 2018-01-06 (Saturday) and 2018-01-07 (Sunday) are excluded.
   Row for 2017-03-10 is excluded by the YEAR filter.
*/
