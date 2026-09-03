-- Creates crosstab (pivot) reports using Spark SQL PIVOT and UNPIVOT.
-- Rotates distinct row values (e.g. years) into column headers, producing
-- the compact side-by-side layout that finance and management reports demand.
--
-- Schema: stock(stockcode, color)
--         salesdetails(stockid, salesid, saleprice)
--         sales(salesid, saledate)

-- ─────────────────────────────────────────────────────────────────────────────
-- Section 1: Basic PIVOT — total sales revenue per colour per year
-- ─────────────────────────────────────────────────────────────────────────────

-- The inner query feeds three columns (color, saleprice, yearofsale) to PIVOT.
-- PIVOT groups by color and aggregates saleprice for each declared year value.
SELECT
    color,
    y2015,
    y2016,
    y2017,
    y2018
FROM ( -- noqa: ST05
    SELECT
        st.color,
        sd.saleprice,
        YEAR(sa.saledate) AS yearofsale
    FROM stock AS st
    INNER JOIN salesdetails AS sd
        ON st.stockcode = sd.stockid
    INNER JOIN sales AS sa
        ON sd.salesid = sa.salesid
) AS sq
    PIVOT (
        SUM(saleprice) FOR yearofsale IN (2015 y2015, 2016 y2016, 2017 y2017, 2018 y2018)
    );

/* Expected output (NULL where no sales exist for that colour / year):
   color  | y2015   | y2016   | y2017   | y2018
   -------+---------+---------+---------+---------
   Black  | 285000  | 310000  | 298000  | 321000
   Blue   |  95000  | 118000  | 142000  | NULL
   Green  | NULL    |  78000  |  66000  |  88000
   Red    | 215000  | 243000  | 198000  | 267000
   Silver | 185000  | 201000  | 223000  | 212000
*/

-- ─────────────────────────────────────────────────────────────────────────────
-- Section 2: Replace NULL with zero — cleaner finance reports
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    color,
    COALESCE(y2015, 0) AS y2015,
    COALESCE(y2016, 0) AS y2016,
    COALESCE(y2017, 0) AS y2017,
    COALESCE(y2018, 0) AS y2018
FROM ( -- noqa: ST05
    SELECT
        st.color,
        sd.saleprice,
        YEAR(sa.saledate) AS yearofsale
    FROM stock AS st
    INNER JOIN salesdetails AS sd
        ON st.stockcode = sd.stockid
    INNER JOIN sales AS sa
        ON sd.salesid = sa.salesid
) AS sq
    PIVOT (
        SUM(saleprice) FOR yearofsale IN (2015 y2015, 2016 y2016, 2017 y2017, 2018 y2018)
    );

-- ─────────────────────────────────────────────────────────────────────────────
-- Section 3: PIVOT with two aggregate functions — revenue and unit count
-- ─────────────────────────────────────────────────────────────────────────────

-- When multiple aggregates are listed, Spark names output columns
-- as <year>_<alias>, e.g. 2017_total_revenue, 2017_units_sold.
-- Backtick-quoting is required because the names start with a digit.
SELECT
    color,
    2015_total_revenue,
    2015_units_sold,
    2016_total_revenue,
    2016_units_sold,
    2017_total_revenue,
    2017_units_sold,
    2018_total_revenue,
    2018_units_sold
FROM ( -- noqa: ST05
    SELECT
        st.color,
        sd.saleprice,
        YEAR(sa.saledate) AS yearofsale
    FROM stock AS st
    INNER JOIN salesdetails AS sd
        ON st.stockcode = sd.stockid
    INNER JOIN sales AS sa
        ON sd.salesid = sa.salesid
) AS sq
    PIVOT (
        SUM(saleprice) AS total_revenue,
        COUNT(*) AS units_sold
        FOR yearofsale IN (2015, 2016, 2017, 2018)
    );

-- ─────────────────────────────────────────────────────────────────────────────
-- Section 4: Sample data — verify PIVOT output with inline CTE rows
-- ─────────────────────────────────────────────────────────────────────────────

WITH raw_sales AS (
    SELECT
        'Red' AS color,
        12000.00 AS saleprice,
        2016 AS yearofsale
    UNION ALL
    SELECT
        'Red' AS color,
        18500.00 AS saleprice,
        2016 AS yearofsale
    UNION ALL
    SELECT
        'Red' AS color,
        21000.00 AS saleprice,
        2017 AS yearofsale
    UNION ALL
    SELECT
        'Blue' AS color,
        9500.00 AS saleprice,
        2016 AS yearofsale
    UNION ALL
    SELECT
        'Blue' AS color,
        14000.00 AS saleprice,
        2017 AS yearofsale
    UNION ALL
    SELECT
        'Blue' AS color,
        11500.00 AS saleprice,
        2017 AS yearofsale
    UNION ALL
    SELECT
        'Black' AS color,
        25000.00 AS saleprice,
        2017 AS yearofsale
    UNION ALL
    SELECT
        'Black' AS color,
        31000.00 AS saleprice,
        2018 AS yearofsale
    UNION ALL
    SELECT
        'Silver' AS color,
        19000.00 AS saleprice,
        2016 AS yearofsale
    UNION ALL
    SELECT
        'Silver' AS color,
        22000.00 AS saleprice,
        2018 AS yearofsale
)

SELECT
    color,
    COALESCE(`2016`, 0) AS y2016,
    COALESCE(`2017`, 0) AS y2017,
    COALESCE(`2018`, 0) AS y2018
FROM raw_sales
    PIVOT (
        ROUND(SUM(saleprice), 2) FOR yearofsale IN (2016, 2017, 2018)
    )
ORDER BY color;

/* Expected output:
   color  | y2016    | y2017    | y2018
   -------+----------+----------+----------
   Black  |     0.00 | 25000.00 | 31000.00
   Blue   |  9500.00 | 25500.00 |     0.00
   Red    | 30500.00 | 21000.00 |     0.00
   Silver | 19000.00 |     0.00 | 22000.00
*/

-- ─────────────────────────────────────────────────────────────────────────────
-- Section 5: UNPIVOT — rotate columns back into rows (inverse of PIVOT)
-- ─────────────────────────────────────────────────────────────────────────────

-- Useful when a pre-pivoted report must feed a normalised downstream system.
-- UNPIVOT drops NULL rows by default (Blue y2018, Red y2018 do not appear).

WITH pivoted AS (
    SELECT
        'Red' AS color,
        30500.00 AS y2016,
        21000.00 AS y2017,
        NULL AS y2018
    UNION ALL
    SELECT
        'Blue' AS color,
        9500.00 AS y2016,
        25500.00 AS y2017,
        NULL AS y2018
    UNION ALL
    SELECT
        'Black' AS color,
        NULL AS y2016,
        25000.00 AS y2017,
        31000.00 AS y2018
    UNION ALL
    SELECT
        'Silver' AS color,
        19000.00 AS y2016,
        NULL AS y2017,
        22000.00 AS y2018
)

SELECT
    color,
    year_label,
    total_revenue
FROM pivoted
    UNPIVOT (
        total_revenue FOR year_label IN (y2016, y2017, y2018)
    )
ORDER BY color, year_label;

/* Expected output (NULL rows are silently excluded):
   color  | year_label | total_revenue
   -------+------------+--------------
   Black  | y2017      |      25000.00
   Black  | y2018      |      31000.00
   Blue   | y2016      |       9500.00
   Blue   | y2017      |      25500.00
   Red    | y2016      |      30500.00
   Red    | y2017      |      21000.00
   Silver | y2016      |      19000.00
   Silver | y2018      |      22000.00
*/
