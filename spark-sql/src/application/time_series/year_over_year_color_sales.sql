-- Compares average sale price by colour for a given month against the same
-- month in the prior year, using a derived table (subquery in the FROM clause).
--
-- A single reference date drives both windows — change '2018-06-01' to shift
-- the analysis to any target month and its automatic prior-year equivalent.
--
-- Pattern:
--   Outer query  → target year / month   (e.g. June 2018)
--   Derived table → prior  year / month   (e.g. June 2017)
--   LEFT JOIN ensures colours with no prior-year data still appear.

-- Sample data: car sales spanning two Junes plus off-month rows
WITH allsales AS (
    -- June 2018 (target year) — included in outer WHERE
    SELECT
        'Silver' AS color,
        15000.00 AS totalsaleprice,
        CAST('2018-06-05' AS DATE) AS saledate
    UNION ALL
    SELECT
        'Silver' AS color,
        17500.00 AS totalsaleprice,
        CAST('2018-06-12' AS DATE) AS saledate
    UNION ALL
    SELECT
        'Black' AS color,
        22000.00 AS totalsaleprice,
        CAST('2018-06-08' AS DATE) AS saledate
    UNION ALL
    SELECT
        'Black' AS color,
        19500.00 AS totalsaleprice,
        CAST('2018-06-21' AS DATE) AS saledate
    UNION ALL
    SELECT
        'Red' AS color,
        13000.00 AS totalsaleprice,
        CAST('2018-06-15' AS DATE) AS saledate
    UNION ALL
    -- June 2017 (prior year) — included in derived-table WHERE
    SELECT
        'Silver' AS color,
        14000.00 AS totalsaleprice,
        CAST('2017-06-10' AS DATE) AS saledate
    UNION ALL
    SELECT
        'Silver' AS color,
        16000.00 AS totalsaleprice,
        CAST('2017-06-22' AS DATE) AS saledate
    UNION ALL
    SELECT
        'Black' AS color,
        20000.00 AS totalsaleprice,
        CAST('2017-06-07' AS DATE) AS saledate
    UNION ALL
    SELECT
        'Red' AS color,
        11500.00 AS totalsaleprice,
        CAST('2017-06-18' AS DATE) AS saledate
    UNION ALL
    SELECT
        'Red' AS color,
        12500.00 AS totalsaleprice,
        CAST('2017-06-29' AS DATE) AS saledate
    UNION ALL
    -- Off-month / off-year rows — excluded by both filters
    SELECT
        'Silver' AS color,
        16000.00 AS totalsaleprice,
        CAST('2018-03-15' AS DATE) AS saledate
    UNION ALL
    SELECT
        'Black' AS color,
        21000.00 AS totalsaleprice,
        CAST('2017-09-20' AS DATE) AS saledate
)

SELECT
    sa.color,
    AVG(sa.totalsaleprice) AS averagemonthsales,
    MIN(sq.averagepreviousmonthsales) AS averagepreviousmonthsales
FROM allsales AS sa
LEFT OUTER JOIN (  -- noqa: ST05
    SELECT
        color,
        AVG(totalsaleprice) AS averagepreviousmonthsales
    FROM allsales
    WHERE
        YEAR(saledate) = YEAR(TO_DATE('2018-06-01')) - 1
        AND MONTH(saledate) = MONTH(TO_DATE('2018-06-01'))
    GROUP BY color
) AS sq
    ON sa.color = sq.color
WHERE
    YEAR(sa.saledate) = YEAR(TO_DATE('2018-06-01'))
    AND MONTH(sa.saledate) = MONTH(TO_DATE('2018-06-01'))
GROUP BY sa.color
ORDER BY sa.color;

/* Expected output (reference date 2018-06-01 → Jun 2018 vs Jun 2017):
   color  | averagemonthsales | averagepreviousmonthsales
   -------+-------------------+--------------------------
   Black  |          20750.00 |                  20000.00
   Red    |          13000.00 |                  12000.00
   Silver |          16250.00 |                  15000.00
*/
