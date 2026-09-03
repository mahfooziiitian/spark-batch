-- Lists cars sold in 2017 with the exact time of sale, ordered by time of day.
--
-- DATE_FORMAT pattern notes:
--   'hh:mm'  → 12-hour clock (01–12).  Ordering by this string puts PM sales
--              (02:xx, 03:xx …) BEFORE AM sales (09:xx, 10:xx …) alphabetically.
--   'HH:mm'  → 24-hour clock (00–23).  Use this for correct chronological order.
--
-- The original query uses 'hh:mm' — output and ordering reflect 12-hour format.
-- See the alternative at the bottom for a chronologically correct version.

-- Sample data: car sales across different times of day in 2017 (and one
-- excluded row each for 2016 and 2018 to demonstrate the YEAR filter)
WITH allsales AS (
    SELECT
        'Toyota' AS makename,
        'Camry' AS modelname,
        28500.00 AS saleprice,
        CAST('2017-03-15 09:05:00' AS TIMESTAMP) AS saledate
    UNION ALL
    SELECT
        'Ford' AS makename,
        'Mustang' AS modelname,
        42000.00 AS saleprice,
        CAST('2017-06-22 09:45:00' AS TIMESTAMP) AS saledate
    UNION ALL
    SELECT
        'Honda' AS makename,
        'Civic' AS modelname,
        19800.00 AS saleprice,
        CAST('2017-01-08 10:30:00' AS TIMESTAMP) AS saledate
    UNION ALL
    SELECT
        'BMW' AS makename,
        '3 Series' AS modelname,
        51000.00 AS saleprice,
        CAST('2017-09-14 11:55:00' AS TIMESTAMP) AS saledate
    UNION ALL
    SELECT
        'Toyota' AS makename,
        'RAV4' AS modelname,
        33200.00 AS saleprice,
        CAST('2017-04-30 12:10:00' AS TIMESTAMP) AS saledate
    UNION ALL
    SELECT
        'Audi' AS makename,
        'A4' AS modelname,
        47500.00 AS saleprice,
        CAST('2017-11-03 14:20:00' AS TIMESTAMP) AS saledate
    UNION ALL
    SELECT
        'Ford' AS makename,
        'Focus' AS modelname,
        22300.00 AS saleprice,
        CAST('2017-07-19 15:45:00' AS TIMESTAMP) AS saledate
    UNION ALL
    SELECT
        'Honda' AS makename,
        'Jazz' AS modelname,
        16500.00 AS saleprice,
        CAST('2017-02-27 17:00:00' AS TIMESTAMP) AS saledate
    UNION ALL
    -- 2016 row — excluded by YEAR(SaleDate) = 2017
    SELECT
        'Audi' AS makename,
        'Q5' AS modelname,
        58000.00 AS saleprice,
        CAST('2016-08-10 11:00:00' AS TIMESTAMP) AS saledate
    UNION ALL
    -- 2018 row — excluded by YEAR(SaleDate) = 2017
    SELECT
        'BMW' AS makename,
        '5 Series' AS modelname,
        62000.00 AS saleprice,
        CAST('2018-01-05 10:00:00' AS TIMESTAMP) AS saledate
)

SELECT
    makename,
    modelname,
    saleprice,
    saledate,
    DATE_FORMAT(saledate, 'hh:mm') AS timeofdaysold
FROM allsales
WHERE YEAR(saledate) = 2017
ORDER BY timeofdaysold;

/* Expected output (ordered by 12-hour clock string — note PM before AM):
   makename | modelname | saleprice | saledate            | timeofdaysold
   ---------+-----------+-----------+---------------------+--------------
   Audi     | A4        |  47500.00 | 2017-11-03 14:20:00 | 02:20   ← 2 PM sorts before 9 AM
   Ford     | Focus     |  22300.00 | 2017-07-19 15:45:00 | 03:45   ← 3 PM
   Honda    | Jazz      |  16500.00 | 2017-02-27 17:00:00 | 05:00   ← 5 PM
   Toyota   | Camry     |  28500.00 | 2017-03-15 09:05:00 | 09:05
   Ford     | Mustang   |  42000.00 | 2017-06-22 09:45:00 | 09:45
   Honda    | Civic     |  19800.00 | 2017-01-08 10:30:00 | 10:30
   BMW      | 3 Series  |  51000.00 | 2017-09-14 11:55:00 | 11:55
   Toyota   | RAV4      |  33200.00 | 2017-04-30 12:10:00 | 12:10   ← noon
*/

-- ── Chronologically correct version — use 'HH:mm' (24-hour) ─────────────────

SELECT
    makename,
    modelname,
    saleprice,
    saledate,
    DATE_FORMAT(saledate, 'HH:mm') AS timeofdaysold
FROM allsales
WHERE YEAR(saledate) = 2017
ORDER BY timeofdaysold;

/* Expected output (24-hour ordering — chronologically correct):
   makename | modelname | saleprice | saledate            | timeofdaysold
   ---------+-----------+-----------+---------------------+--------------
   Toyota   | Camry     |  28500.00 | 2017-03-15 09:05:00 | 09:05
   Ford     | Mustang   |  42000.00 | 2017-06-22 09:45:00 | 09:45
   Honda    | Civic     |  19800.00 | 2017-01-08 10:30:00 | 10:30
   BMW      | 3 Series  |  51000.00 | 2017-09-14 11:55:00 | 11:55
   Toyota   | RAV4      |  33200.00 | 2017-04-30 12:10:00 | 12:10
   Audi     | A4        |  47500.00 | 2017-11-03 14:20:00 | 14:20
   Ford     | Focus     |  22300.00 | 2017-07-19 15:45:00 | 15:45
   Honda    | Jazz      |  16500.00 | 2017-02-27 17:00:00 | 17:00
*/
