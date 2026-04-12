-- Aggregates cumulative year-to-date sales per make, from 1 Jan of the
-- current year up to today.

-- Sample data: vehicle sales across multiple years and makes
WITH all_sales AS (
    SELECT
        'Toyota' AS makename,
        CAST('2026-01-05' AS DATE) AS saledate,
        32000.00 AS saleprice
    UNION ALL
    SELECT
        'Toyota' AS makename,
        CAST('2026-02-14' AS DATE) AS saledate,
        28500.00 AS saleprice
    UNION ALL
    SELECT
        'Toyota' AS makename,
        CAST('2026-03-22' AS DATE) AS saledate,
        41000.00 AS saleprice
    UNION ALL
    SELECT
        'Honda' AS makename,
        CAST('2026-01-18' AS DATE) AS saledate,
        24000.00 AS saleprice
    UNION ALL
    SELECT
        'Honda' AS makename,
        CAST('2026-03-30' AS DATE) AS saledate,
        26500.00 AS saleprice
    UNION ALL
    SELECT
        'Ford' AS makename,
        CAST('2026-02-08' AS DATE) AS saledate,
        35000.00 AS saleprice
    UNION ALL
    SELECT
        'Ford' AS makename,
        CAST('2026-04-01' AS DATE) AS saledate,
        38000.00 AS saleprice
    UNION ALL
    SELECT
        'Ford' AS makename,
        CAST('2026-04-10' AS DATE) AS saledate,
        29500.00 AS saleprice
    -- Prior-year rows — excluded by the YTD filter
    UNION ALL
    SELECT
        'Toyota' AS makename,
        CAST('2025-11-20' AS DATE) AS saledate,
        31000.00 AS saleprice
    UNION ALL
    SELECT
        'Honda' AS makename,
        CAST('2025-12-15' AS DATE) AS saledate,
        22000.00 AS saleprice
)

SELECT
    makename,
    SUM(saleprice) AS cumulative_sales_ytd
FROM all_sales
WHERE
    saledate BETWEEN
    DATE_TRUNC('YEAR', CURDATE())
    AND CURDATE()
GROUP BY makename
ORDER BY makename ASC;

/* Expected output (run on 2026-04-12):
   makename | cumulative_sales_ytd
   ---------+-------------------
   Ford     |           102500.00
   Honda    |            50500.00
   Toyota   |           101500.00
*/
