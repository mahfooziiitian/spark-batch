-- Demonstrates finding gaps in date data using SEQUENCE and EXPLODE
-- to generate a complete date range, then LEFT JOIN to find missing dates.
-- Schema: allsales(saledate, saleprice, makename)

-- =============================================================================
-- Section 1: Daily sales with GROUP BY saledate (baseline)
-- =============================================================================
SELECT
    saledate,
    COUNT(*) AS sale_count,
    SUM(saleprice) AS daily_total
FROM allsales
GROUP BY saledate
ORDER BY saledate;

-- =============================================================================
-- Section 2: Complete date range using EXPLODE(SEQUENCE(...)) + LEFT JOIN
-- =============================================================================
WITH date_range AS (
    SELECT
        EXPLODE(SEQUENCE(
            DATE '2017-01-01',
            DATE '2017-01-10',
            INTERVAL 1 DAY
        )) AS full_date
),

daily_sales AS (
    SELECT
        saledate,
        COUNT(*) AS sale_count,
        SUM(saleprice) AS daily_total
    FROM allsales
    GROUP BY saledate
)

SELECT
    dr.full_date,
    COALESCE(ds.sale_count, 0) AS sale_count,
    COALESCE(ds.daily_total, 0.00) AS daily_total
FROM date_range AS dr
LEFT JOIN daily_sales AS ds
    ON dr.full_date = ds.saledate
ORDER BY dr.full_date;

-- =============================================================================
-- Section 3: Only the missing dates (no sales recorded)
-- =============================================================================
WITH date_range AS (
    SELECT
        EXPLODE(SEQUENCE(
            DATE '2017-01-01',
            DATE '2017-01-10',
            INTERVAL 1 DAY
        )) AS full_date
),

daily_sales AS (
    SELECT saledate
    FROM allsales
    GROUP BY saledate
)

SELECT dr.full_date AS missing_date
FROM date_range AS dr
LEFT JOIN daily_sales AS ds
    ON dr.full_date = ds.saledate
WHERE ds.saledate IS NULL
ORDER BY dr.full_date;

-- =============================================================================
-- Section 4: Sample data demo — 5 sale dates in 10-day window, find 5 missing
-- =============================================================================
-- Sale dates in dataset: 2017-01-01, 2017-01-03, 2017-01-05, 2017-01-07, 2017-01-09
-- Full range: 2017-01-01 to 2017-01-10 (10 days)
-- Missing dates: 2017-01-02, 2017-01-04, 2017-01-06, 2017-01-08, 2017-01-10
