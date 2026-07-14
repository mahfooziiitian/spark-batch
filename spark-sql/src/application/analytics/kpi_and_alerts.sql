-- Demonstrates KPI calculations and alert conditions using CASE expressions and threshold comparisons.

-- =============================================================================
-- Section 1: Price Alert Thresholds
-- =============================================================================

SELECT
    makename,
    saledate,
    saleprice,
    CASE
        WHEN saleprice > 120000 THEN 'ALERT: Above premium threshold'
        WHEN saleprice > 80000 THEN 'WARNING: Mid-range exceeded'
        ELSE 'OK'
    END AS price_alert
FROM allsales
ORDER BY saleprice DESC;

-- =============================================================================
-- Section 2: Statistical Outlier Alert
-- =============================================================================

WITH make_stats AS (
    SELECT
        makename,
        AVG(saleprice) AS avg_price,
        STDDEV(saleprice) AS stddev_price
    FROM allsales
    GROUP BY makename
)

SELECT
    a.makename,
    a.saleprice,
    ROUND(ms.avg_price, 2) AS make_avg,
    CASE
        WHEN a.saleprice > ms.avg_price + 2 * ms.stddev_price THEN 'HIGH OUTLIER'
        WHEN a.saleprice < ms.avg_price - 2 * ms.stddev_price THEN 'LOW OUTLIER'
        ELSE 'Normal'
    END AS anomaly_flag
FROM allsales AS a
INNER JOIN make_stats AS ms
    ON a.makename = ms.makename
ORDER BY a.makename, a.saleprice;

-- =============================================================================
-- Section 3: KPI Dashboard — Multiple KPIs in One Query
-- =============================================================================

SELECT
    COUNT(*) AS total_sales,
    ROUND(SUM(saleprice), 2) AS total_revenue,
    ROUND(AVG(saleprice), 2) AS avg_sale_price,
    MAX(saleprice) AS highest_sale,
    MIN(saleprice) AS lowest_sale,
    COUNT(DISTINCT makename) AS makes_sold,
    COUNT(DISTINCT customername) AS unique_customers,
    ROUND(SUM(saleprice - cost), 2) AS total_profit,
    ROUND(SUM(saleprice - cost) * 100.0 / SUM(saleprice), 2) AS profit_margin_pct
FROM allsales;

-- =============================================================================
-- Section 4: KPI Trend — Monthly KPIs
-- =============================================================================

SELECT
    DATE_TRUNC('MONTH', saledate) AS month,
    COUNT(*) AS units_sold,
    ROUND(SUM(saleprice), 2) AS revenue,
    ROUND(AVG(saleprice), 2) AS avg_price
FROM allsales
GROUP BY DATE_TRUNC('MONTH', saledate)
ORDER BY month;

-- =============================================================================
-- Section 5: Sample Data
-- =============================================================================

WITH sample_data AS (
    SELECT
        'Ferrari' AS makename,
        65000.00 AS saleprice,
        42000.00 AS cost,
        TO_DATE('2017-03-15') AS saledate
    UNION ALL
    SELECT
        'Ferrari' AS makename,
        130000.00 AS saleprice,
        90000.00 AS cost,
        TO_DATE('2017-06-20') AS saledate
    UNION ALL
    SELECT
        'Bentley' AS makename,
        90000.00 AS saleprice,
        65000.00 AS cost,
        TO_DATE('2017-09-10') AS saledate
    UNION ALL
    SELECT
        'Rolls Royce' AS makename,
        140000.00 AS saleprice,
        100000.00 AS cost,
        TO_DATE('2017-01-05') AS saledate
    UNION ALL
    SELECT
        'Lamborghini' AS makename,
        75000.00 AS saleprice,
        50000.00 AS cost,
        TO_DATE('2017-11-22') AS saledate
)

SELECT
    COUNT(*) AS total_sales,
    ROUND(SUM(saleprice), 2) AS total_revenue,
    ROUND(AVG(saleprice), 2) AS avg_sale_price,
    MAX(saleprice) AS highest_sale,
    MIN(saleprice) AS lowest_sale,
    ROUND(SUM(saleprice - cost), 2) AS total_profit,
    ROUND(SUM(saleprice - cost) * 100.0 / SUM(saleprice), 2) AS profit_margin_pct
FROM sample_data;
