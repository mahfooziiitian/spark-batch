-- ============================================================
-- Topic: GROUPING SETS — explicit multi-combination aggregation
-- Dialect: Databricks / Spark SQL 3.5
-- Description: GROUPING SETS computes aggregates for exactly
--              the listed column combinations in a single scan.
--              Covers GROUPING(), GROUPING_ID(), non-hierarchical
--              sets, UNION ALL replacement, partial CUBE, CTE
--              patterns, and a real-world e-commerce use case.
-- ============================================================

-- ─── Setup: sales fact table (8 rows, no NULLs) ──────────────
DROP TABLE IF EXISTS sales;

CREATE TABLE sales (
    order_id BIGINT,
    region STRING,
    product STRING,
    amount DOUBLE,
    order_date DATE
);

INSERT INTO sales VALUES
(1, 'East', 'Widget', 120.00, DATE '2024-01-15'),
(2, 'West', 'Gadget', 340.00, DATE '2024-01-15'),
(3, 'East', 'Widget', 80.00, DATE '2024-02-10'),
(4, 'North', 'Gadget', 210.00, DATE '2024-02-10'),
(5, 'West', 'Widget', 150.00, DATE '2024-03-05'),
(6, 'East', 'Gadget', 450.00, DATE '2024-03-05'),
(7, 'North', 'Widget', 90.00, DATE '2024-03-20'),
(8, 'West', 'Gadget', 270.00, DATE '2024-03-20');

-- ─── Setup: web_sessions — multi-channel e-commerce data ─────
DROP TABLE IF EXISTS web_sessions;

CREATE TABLE web_sessions (
    session_date DATE,
    channel STRING,
    device STRING,
    country STRING,
    sessions BIGINT,
    conversions BIGINT,
    revenue DOUBLE
);

INSERT INTO web_sessions VALUES
(DATE '2024-01-10', 'Organic', 'Desktop', 'US', 4200, 210, 18900.0),
(DATE '2024-01-10', 'Organic', 'Mobile', 'US', 5100, 180, 14400.0),
(DATE '2024-01-10', 'Paid', 'Desktop', 'US', 2800, 196, 19600.0),
(DATE '2024-01-10', 'Paid', 'Mobile', 'UK', 3300, 165, 15675.0),
(DATE '2024-01-11', 'Email', 'Desktop', 'US', 1500, 135, 13500.0),
(DATE '2024-01-11', 'Email', 'Mobile', 'UK', 2200, 110, 9900.0),
(DATE '2024-01-11', 'Social', 'Mobile', 'US', 6800, 204, 18360.0),
(DATE '2024-01-11', 'Social', 'Desktop', 'UK', 1900, 76, 7600.0);

-- ============================================================
-- 1. Custom subtotals — (region, product), (region), ()
--    Only 3 sets enumerated; (product) alone is omitted.
-- ============================================================
SELECT
    region,
    product,
    SUM(amount) AS total_sales
FROM sales
GROUP BY
    GROUPING SETS (
        (region, product),
        (region),
        ()
    )
ORDER BY region ASC NULLS LAST, product ASC NULLS LAST;
-- region | product | total_sales
-- East    | Gadget  | 450.0        ← (region, product)
-- East    | Widget  | 200.0        ← (region, product)
-- East    | NULL    | 650.0        ← (region) subtotal
-- North   | Gadget  | 210.0
-- North   | Widget  | 90.0
-- North   | NULL    | 300.0        ← (region) subtotal
-- West    | Gadget  | 610.0
-- West    | Widget  | 150.0
-- West    | NULL    | 760.0        ← (region) subtotal
-- NULL    | NULL    | 1710.0       ← grand total ()

-- ============================================================
-- 2. GROUPING_ID() — bitmask classifier
--    For GROUPING SETS ((region,product),(region),(product),()):
--      grp_id=0 → detail        (region, product)
--      grp_id=1 → region sub    (region)
--      grp_id=2 → product sub   (product)
--      grp_id=3 → grand total   ()
--    You choose which IDs appear by controlling the set list.
-- ============================================================
SELECT
    region,
    product,
    SUM(amount) AS total_sales,
    GROUPING_ID(region, product) AS grp_id,
    CASE GROUPING_ID(region, product)
        WHEN 0 THEN 'Detail'
        WHEN 1 THEN 'Region Subtotal'
        WHEN 2 THEN 'Product Subtotal'
        WHEN 3 THEN 'Grand Total'
    END AS row_type
FROM sales
GROUP BY
    GROUPING SETS (
        (region, product),
        (region),
        (product),
        ()
    )
ORDER BY grp_id ASC, region ASC NULLS LAST, product ASC NULLS LAST;

-- ============================================================
-- 3. GROUPING() flags — identify each synthetic NULL
--    GROUPING(col) = 1 → synthetic NULL (grouping placeholder)
--    GROUPING(col) = 0 → real grouping value
-- ============================================================
SELECT
    region,
    product,
    SUM(amount) AS total_sales,
    GROUPING(region) AS g_region,
    GROUPING(product) AS g_product
FROM sales
GROUP BY
    GROUPING SETS (
        (region),
        (product)
    )
ORDER BY g_region ASC, g_product ASC, region ASC NULLS LAST, product ASC NULLS LAST;
-- region | product | total_sales | g_region | g_product
-- East    | NULL    | 650.0       | 0        | 1
-- North   | NULL    | 300.0       | 0        | 1
-- West    | NULL    | 760.0       | 0        | 1
-- NULL    | Gadget  | 1270.0      | 1        | 0
-- NULL    | Widget  | 440.0       | 1        | 0

-- ============================================================
-- 4. Readable labels with GROUPING()
-- ============================================================
SELECT
    CASE WHEN GROUPING(region) = 1 THEN 'All Regions' ELSE region END AS region_label,
    CASE WHEN GROUPING(product) = 1 THEN 'All Products' ELSE product END AS product_label,
    SUM(amount) AS total_sales
FROM sales
GROUP BY
    GROUPING SETS (
        (region, product),
        (region),
        (product),
        ()
    )
ORDER BY region_label ASC, product_label ASC;

-- ============================================================
-- 5. Single-scan equivalence to UNION ALL
--    Both forms produce identical output; GROUPING SETS reads
--    the table once — UNION ALL scans it twice.
-- ============================================================

-- Single scan (preferred)
SELECT
    region,
    NULL AS product,
    SUM(amount) AS total_sales
FROM sales
GROUP BY GROUPING SETS ((region), ());

-- Equivalent two-scan UNION ALL
SELECT
    region,
    NULL AS product,
    SUM(amount) AS total_sales
FROM sales
GROUP BY region
UNION ALL
SELECT
    NULL AS region,
    NULL AS product,
    SUM(amount) AS total_sales
FROM sales;

-- ============================================================
-- 6. Three-column GROUPING SETS — select specific combinations
--    4 explicitly named sets instead of CUBE's 8.
-- ============================================================
SELECT
    region,
    product,
    SUM(amount) AS total_sales,
    YEAR(order_date) AS yr
FROM sales
GROUP BY
    GROUPING SETS (
        (YEAR(order_date), region, product),   -- full detail
        (YEAR(order_date), region),            -- year + region subtotal
        (region),                              -- region across all years
        ()                                     -- grand total
    )
ORDER BY yr ASC NULLS LAST, region ASC NULLS LAST, product ASC NULLS LAST;

-- ============================================================
-- 7. Multiple aggregates in one scan
-- ============================================================
SELECT
    CASE WHEN GROUPING(region) = 1 THEN 'All Regions' ELSE region END AS region_label,
    CASE WHEN GROUPING(product) = 1 THEN 'All Products' ELSE product END AS product_label,
    ROUND(SUM(amount), 2) AS total_revenue,
    COUNT(*) AS order_count,
    ROUND(AVG(amount), 2) AS avg_order_value,
    MAX(amount) AS max_order_value
FROM sales
GROUP BY
    GROUPING SETS (
        (region, product),
        (region),
        ()
    )
ORDER BY GROUPING_ID(region, product) ASC, region_label ASC, product_label ASC;

-- ============================================================
-- 8. Replacing three UNION ALL GROUP BY queries
--    Before: three scans. After: one scan, same 10 output rows.
-- ============================================================

-- Before: three separate table scans
SELECT
    region,
    product,
    SUM(amount) AS revenue
FROM sales
GROUP BY region, product
UNION ALL
SELECT
    region,
    NULL AS product,
    SUM(amount) AS revenue
FROM sales
GROUP BY region
UNION ALL
SELECT
    NULL AS region,
    NULL AS product,
    SUM(amount) AS revenue
FROM sales;

-- After: single scan with GROUPING SETS
SELECT
    region,
    product,
    ROUND(SUM(amount), 2) AS revenue,
    CASE GROUPING_ID(region, product)
        WHEN 0 THEN 'Region+Product'
        WHEN 1 THEN 'Region'
        WHEN 3 THEN 'Grand Total'
    END AS report_level
FROM sales
GROUP BY
    GROUPING SETS (
        (region, product),
        (region),
        ()
    )
ORDER BY GROUPING_ID(region, product) ASC, region ASC NULLS LAST, product ASC NULLS LAST;

-- ============================================================
-- 9. Partial CUBE — 5 of CUBE's 8 sets
--    CUBE(year, region, product) would generate 8 grouping sets.
--    Enumerate only the 5 combinations this report needs;
--    (region), (product,year), and (region,product) are skipped.
-- ============================================================
SELECT
    CASE WHEN GROUPING(YEAR(order_date)) = 1 THEN 'All Years' ELSE CAST(YEAR(order_date) AS STRING) END AS yr,
    CASE WHEN GROUPING(region) = 1 THEN 'All Regions' ELSE region END AS rgn,
    CASE WHEN GROUPING(product) = 1 THEN 'All Products' ELSE product END AS prd,
    ROUND(SUM(amount), 2) AS total_revenue,
    COUNT(*) AS order_count
FROM sales
GROUP BY
    GROUPING SETS (
        (YEAR(order_date), region, product),   -- full detail
        (YEAR(order_date), region),            -- region within year
        (YEAR(order_date)),                    -- year total
        (product),                             -- product across all time
        ()                                     -- grand total
    )
ORDER BY
    GROUPING_ID(YEAR(order_date), region, product) ASC,
    yr ASC NULLS LAST,
    rgn ASC NULLS LAST,
    prd ASC NULLS LAST;

-- ============================================================
-- 10. CTE + GROUPING SETS — pre-filter, group, label
-- ============================================================
WITH base AS (
    SELECT
        region,
        product,
        amount,
        YEAR(order_date) AS sale_year
    FROM sales
    WHERE order_date >= DATE '2024-01-01'
),

grouped AS (
    SELECT
        region,
        product,
        sale_year,
        SUM(amount) AS total_revenue,
        COUNT(*) AS order_count,
        GROUPING_ID(region, product, sale_year) AS grp_id
    FROM base
    GROUP BY GROUPING SETS (
        (region, product, sale_year),
        (region, sale_year),
        (product),
        ()
    )
)

SELECT
    order_count,
    grp_id,
    ROUND(total_revenue, 2) AS total_revenue,
    COALESCE(region, 'All Regions') AS region_label,
    COALESCE(product, 'All Products') AS product_label,
    COALESCE(CAST(sale_year AS STRING), 'All Years') AS year_label
FROM grouped
ORDER BY grp_id ASC, region_label ASC, product_label ASC, year_label ASC;

-- ============================================================
-- 11. Real-world use case: multi-channel e-commerce dashboard
--     6 custom sets — (device) and (device,country) omitted
--     because they have no business value for this report.
--     6 of CUBE's 8 combinations for channel×device×country.
-- ============================================================
SELECT
    SUM(sessions) AS total_sessions,
    SUM(conversions) AS total_conversions,
    ROUND(SUM(revenue), 2) AS total_revenue,
    GROUPING_ID(channel, device, country) AS grp_id,
    ROUND(SUM(conversions) * 100.0 / NULLIF(SUM(sessions), 0), 2) AS conversion_rate_pct,
    CASE WHEN GROUPING(channel) = 1 THEN 'All Channels' ELSE channel END AS channel_label,
    CASE WHEN GROUPING(device) = 1 THEN 'All Devices' ELSE device END AS device_label,
    CASE WHEN GROUPING(country) = 1 THEN 'All Countries' ELSE country END AS country_label
FROM web_sessions
GROUP BY
    GROUPING SETS (
        (channel, device, country),   -- full detail
        (channel, device),            -- channel x device cross
        (channel, country),           -- channel x country cross
        (channel),                    -- channel summary
        (country),                    -- country summary
        ()                            -- grand total
    )
ORDER BY grp_id ASC, channel_label ASC, device_label ASC, country_label ASC;
