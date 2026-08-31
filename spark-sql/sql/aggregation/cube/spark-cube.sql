-- ============================================================
-- Topic: CUBE — cross-dimensional aggregation
-- Dialect: Databricks / Spark SQL 3.5
-- Description: CUBE generates aggregates for every combination
--              of n grouping columns (2ⁿ sets). Covers basic
--              usage, GROUPING(), GROUPING_ID(), NULL handling,
--              CTE patterns, HAVING threshold, window ranking,
--              real-world domains, and Delta pre-aggregation.
-- ============================================================

-- ─── Setup: sales fact table (8 rows, no NULLs) ──────────────
DROP TABLE IF EXISTS sales;

CREATE TABLE sales (
    order_id   BIGINT,
    region     STRING,
    product    STRING,
    amount     DOUBLE,
    order_date DATE
);

INSERT INTO sales VALUES
    (1, 'East',  'Widget',  120.00, DATE '2024-01-15'),
    (2, 'West',  'Gadget',  340.00, DATE '2024-01-15'),
    (3, 'East',  'Widget',   80.00, DATE '2024-02-10'),
    (4, 'North', 'Gadget',  210.00, DATE '2024-02-10'),
    (5, 'West',  'Widget',  150.00, DATE '2024-03-05'),
    (6, 'East',  'Gadget',  450.00, DATE '2024-03-05'),
    (7, 'North', 'Widget',   90.00, DATE '2024-03-20'),
    (8, 'West',  'Gadget',  270.00, DATE '2024-03-20');

-- ─── Setup: sales_dt — contains one real NULL region row ─────
-- Used in NULL-handling examples to demonstrate the difference
-- between COALESCE (unsafe) and GROUPING() (safe).
DROP TABLE IF EXISTS sales_dt;

CREATE TABLE sales_dt (
    sale_date  DATE,
    region     STRING,
    product    STRING,
    amount     DOUBLE
);

INSERT INTO sales_dt VALUES
    (DATE '2024-07-01', 'East', 'ProductA', 1000.50),
    (DATE '2024-07-01', 'West', 'ProductB', 1500.75),
    (DATE '2024-07-02', 'East', 'ProductA', 1200.25),
    (DATE '2024-07-02', 'West', 'ProductB', 1800.30),
    (DATE '2024-07-03', 'East', 'ProductA',  900.75),
    (DATE '2024-07-03', 'West', 'ProductB', 1600.20),
    (DATE '2024-07-03', NULL,   'ProductB', 1600.20);  -- real NULL region

-- ============================================================
-- 1. Basic CUBE on two columns
--    CUBE(region, product) → 4 grouping sets:
--    (region, product), (region), (product), ()
-- ============================================================
SELECT
    region,
    product,
    SUM(amount) AS total_sales
FROM sales
GROUP BY CUBE (region, product)
ORDER BY region NULLS LAST, product NULLS LAST;
-- region | product | total_sales
-- East    | Gadget  | 450.0        ← (region, product) detail
-- East    | Widget  | 200.0
-- East    | NULL    | 650.0        ← (region) subtotal
-- North   | Gadget  | 210.0
-- North   | Widget  | 90.0
-- North   | NULL    | 300.0        ← (region) subtotal
-- West    | Gadget  | 610.0
-- West    | Widget  | 150.0
-- West    | NULL    | 760.0        ← (region) subtotal
-- NULL    | Gadget  | 1270.0       ← (product) subtotal
-- NULL    | Widget  | 440.0        ← (product) subtotal
-- NULL    | NULL    | 1710.0       ← grand total ()

-- ============================================================
-- 2. Distinguish subtotals with GROUPING()
--    GROUPING(col) = 1 → synthetic NULL (subtotal marker)
--    GROUPING(col) = 0 → real grouping value
-- ============================================================
SELECT
    region,
    product,
    SUM(amount)       AS total_sales,
    GROUPING(region)  AS is_region_subtotal,
    GROUPING(product) AS is_product_subtotal
FROM sales
GROUP BY CUBE (region, product)
ORDER BY GROUPING(region), GROUPING(product), region NULLS LAST, product NULLS LAST;
-- is_region_subtotal=0, is_product_subtotal=0 → detail row
-- is_region_subtotal=0, is_product_subtotal=1 → region subtotal
-- is_region_subtotal=1, is_product_subtotal=0 → product subtotal
-- is_region_subtotal=1, is_product_subtotal=1 → grand total

-- ============================================================
-- 3. Readable labels with GROUPING()
-- ============================================================
SELECT
    CASE WHEN GROUPING(region)  = 1 THEN 'All Regions'  ELSE region  END AS region_label,
    CASE WHEN GROUPING(product) = 1 THEN 'All Products' ELSE product END AS product_label,
    SUM(amount) AS total_sales
FROM sales
GROUP BY CUBE (region, product)
ORDER BY region_label, product_label;

-- ============================================================
-- 4. NULL handling — COALESCE (unsafe) vs GROUPING() (safe)
--    sales_dt contains a row where region IS NULL (real data).
--    COALESCE cannot tell the difference between that real NULL
--    and a synthetic NULL produced by CUBE.
-- ============================================================

-- Unsafe: COALESCE conflates real NULLs with subtotal markers
SELECT
    COALESCE(CAST(sale_date AS STRING), 'All') AS sale_date,
    COALESCE(region,                    'All') AS region,
    COALESCE(product,                   'All') AS product,
    SUM(amount) AS total_sales
FROM sales_dt
GROUP BY CUBE (sale_date, region, product)
ORDER BY sale_date, region, product;

-- Safe: GROUPING() identifies only synthetic subtotal NULLs
SELECT
    CASE WHEN GROUPING(sale_date) = 1 THEN 'All' ELSE CAST(sale_date AS STRING) END AS sale_date,
    CASE WHEN GROUPING(region)    = 1 THEN 'All' ELSE region                    END AS region,
    CASE WHEN GROUPING(product)   = 1 THEN 'All' ELSE product                   END AS product,
    SUM(amount)         AS total_sales,
    GROUPING(sale_date) AS is_date_subtotal,
    GROUPING(region)    AS is_region_subtotal,
    GROUPING(product)   AS is_product_subtotal
FROM sales_dt
GROUP BY CUBE (sale_date, region, product)
ORDER BY sale_date, region, product;

-- ============================================================
-- 5. Filter to grand total only
-- ============================================================
SELECT SUM(amount) AS grand_total
FROM sales
GROUP BY CUBE (region, product)
HAVING GROUPING(region) = 1 AND GROUPING(product) = 1;

-- ============================================================
-- 6. Three-column CUBE — 2³ = 8 grouping combinations
--    (year,region,product), (year,region), (year,product),
--    (region,product), (year), (region), (product), ()
-- ============================================================
SELECT
    CASE WHEN GROUPING(YEAR(order_date)) = 1 THEN 'All Years'    ELSE CAST(YEAR(order_date) AS STRING) END AS yr,
    CASE WHEN GROUPING(region)           = 1 THEN 'All Regions'  ELSE region                              END AS rgn,
    CASE WHEN GROUPING(product)          = 1 THEN 'All Products' ELSE product                             END AS prd,
    SUM(amount)  AS total_sales,
    COUNT(*)     AS order_count
FROM sales
GROUP BY CUBE (YEAR(order_date), region, product)
ORDER BY yr, rgn, prd;

-- ============================================================
-- 7. GROUPING_ID() — compact bitmask row classifier
--    Bit positions (leftmost = most-significant):
--      grp_id=0 → detail        (region, product)
--      grp_id=1 → region sub    (region)
--      grp_id=2 → product sub   (product)
--      grp_id=3 → grand total   ()
-- ============================================================
SELECT
    region,
    product,
    SUM(amount)                  AS total_sales,
    GROUPING_ID(region, product) AS grp_id,
    CASE GROUPING_ID(region, product)
        WHEN 0 THEN 'Detail'
        WHEN 1 THEN 'Region Subtotal'
        WHEN 2 THEN 'Product Subtotal'
        WHEN 3 THEN 'Grand Total'
    END AS row_type
FROM sales
GROUP BY CUBE (region, product)
ORDER BY grp_id, region NULLS LAST, product NULLS LAST;

-- ============================================================
-- 8. Multiple aggregates in one scan
-- ============================================================
SELECT
    CASE WHEN GROUPING(region)  = 1 THEN 'All Regions'  ELSE region  END AS region_label,
    CASE WHEN GROUPING(product) = 1 THEN 'All Products' ELSE product END AS product_label,
    ROUND(SUM(amount), 2)  AS total_revenue,
    COUNT(*)               AS order_count,
    ROUND(AVG(amount), 2)  AS avg_order_value,
    MAX(amount)            AS max_order_value
FROM sales
GROUP BY CUBE (region, product)
ORDER BY GROUPING_ID(region, product), region_label, product_label;

-- ============================================================
-- 9. CTE + CUBE — compute once, apply labels downstream
--    Filter to specific grp_id values without re-scanning.
-- ============================================================
WITH cube_base AS (
    SELECT
        region,
        product,
        YEAR(order_date)                               AS sale_year,
        SUM(amount)                                    AS total_revenue,
        COUNT(*)                                       AS order_count,
        GROUPING_ID(region, product, YEAR(order_date)) AS grp_id
    FROM sales
    GROUP BY CUBE (region, product, YEAR(order_date))
),

labelled AS (
    SELECT
        total_revenue,
        order_count,
        grp_id,
        COALESCE(region,                    'All Regions')  AS region_label,
        COALESCE(product,                   'All Products') AS product_label,
        COALESCE(CAST(sale_year AS STRING), 'All Years')    AS year_label,
        ROUND(total_revenue / NULLIF(order_count, 0), 2)   AS avg_order_value
    FROM cube_base
)

SELECT
    grp_id,
    region_label,
    product_label,
    year_label,
    total_revenue,
    order_count,
    avg_order_value
FROM labelled
WHERE grp_id IN (0, 3)   -- detail rows and grand total; adjust as needed
ORDER BY year_label, region_label, product_label;

-- ============================================================
-- 10. CUBE vs UNION ALL — same output, one scan vs four
-- ============================================================

-- Before: four separate GROUP BY queries — four table scans
SELECT
    region,
    product,
    SUM(amount) AS revenue
FROM sales
GROUP BY region, product
UNION ALL
SELECT
    region,
    NULL    AS product,
    SUM(amount) AS revenue
FROM sales
GROUP BY region
UNION ALL
SELECT
    NULL    AS region,
    product,
    SUM(amount) AS revenue
FROM sales
GROUP BY product
UNION ALL
SELECT
    NULL AS region,
    NULL AS product,
    SUM(amount) AS revenue
FROM sales;

-- After: identical 12 output rows, single table scan
SELECT
    region,
    product,
    ROUND(SUM(amount), 2) AS revenue
FROM sales
GROUP BY CUBE (region, product)
ORDER BY GROUPING_ID(region, product), region NULLS LAST, product NULLS LAST;

-- ============================================================
-- 11. HAVING threshold — keep all subtotals, prune low-value
--     detail rows
-- ============================================================
SELECT
    CASE WHEN GROUPING(region)  = 1 THEN 'All Regions'  ELSE region  END AS region_label,
    CASE WHEN GROUPING(product) = 1 THEN 'All Products' ELSE product END AS product_label,
    ROUND(SUM(amount), 2)        AS total_revenue,
    GROUPING_ID(region, product) AS grp_id
FROM sales
GROUP BY CUBE (region, product)
HAVING
    GROUPING_ID(region, product) > 0  -- always keep subtotals and grand total
    OR SUM(amount) > 200              -- detail: only high-revenue combinations
ORDER BY grp_id, region_label, product_label;
-- East|Widget (200.0) is excluded; all subtotal rows are retained.

-- ============================================================
-- 12. Filter to subtotals only — exclude all detail rows
-- ============================================================
SELECT
    COALESCE(region,  'All Regions')  AS region_label,
    COALESCE(product, 'All Products') AS product_label,
    SUM(amount) AS total_revenue
FROM sales
GROUP BY CUBE (region, product)
HAVING GROUPING_ID(region, product) > 0
ORDER BY GROUPING_ID(region, product) DESC, region_label ASC, product_label ASC;

-- ============================================================
-- 13. CUBE + window function — rank within each grouping level
--     grp_id partitions rows so RANK() operates independently
--     at detail, region-subtotal, product-subtotal, and
--     grand-total levels.
-- ============================================================
WITH cubed AS (
    SELECT
        region,
        product,
        ROUND(SUM(amount), 2)        AS total_revenue,
        GROUPING_ID(region, product) AS grp_id
    FROM sales
    GROUP BY CUBE (region, product)
)

SELECT
    total_revenue,
    grp_id,
    COALESCE(region,  'All Regions')  AS region_label,
    COALESCE(product, 'All Products') AS product_label,
    RANK() OVER (
        PARTITION BY grp_id
        ORDER BY total_revenue DESC
    ) AS revenue_rank_within_level
FROM cubed
ORDER BY grp_id, revenue_rank_within_level;
-- grp_id=1 rank 1 → West (760.0)        — top region
-- grp_id=2 rank 1 → Gadget (1270.0)     — top product
-- grp_id=0 rank 1 → East|Gadget (450.0) — top detail combination

-- ============================================================
-- 14. Real-world use case: quarterly marketing spend
--     campaign × channel × quarter → 2³ = 8 grouping sets
-- ============================================================
DROP TABLE IF EXISTS marketing_spend;

CREATE TABLE marketing_spend (
    campaign    STRING,
    channel     STRING,
    quarter     STRING,
    spend       DOUBLE,
    conversions BIGINT
);

INSERT INTO marketing_spend VALUES
    ('Summer Sale',  'Email',   'Q1', 12000.0, 340),
    ('Summer Sale',  'Social',  'Q1',  8500.0, 210),
    ('Black Friday', 'Email',   'Q2', 15000.0, 520),
    ('Black Friday', 'Social',  'Q2', 22000.0, 890),
    ('Summer Sale',  'Email',   'Q2',  9500.0, 280),
    ('Black Friday', 'Display', 'Q2',  6000.0, 120),
    ('New Year',     'Social',  'Q3', 18000.0, 640),
    ('New Year',     'Email',   'Q3', 11000.0, 390);

SELECT
    CASE WHEN GROUPING(campaign) = 1 THEN 'All Campaigns' ELSE campaign END AS campaign_label,
    CASE WHEN GROUPING(channel)  = 1 THEN 'All Channels'  ELSE channel  END AS channel_label,
    CASE WHEN GROUPING(quarter)  = 1 THEN 'All Quarters'  ELSE quarter  END AS quarter_label,
    ROUND(SUM(spend), 2)                                AS total_spend,
    SUM(conversions)                                    AS total_conversions,
    ROUND(SUM(spend) / NULLIF(SUM(conversions), 0), 2) AS cost_per_conversion,
    GROUPING_ID(campaign, channel, quarter)             AS grp_id
FROM marketing_spend
GROUP BY CUBE (campaign, channel, quarter)
ORDER BY grp_id, campaign_label, channel_label, quarter_label;

-- ============================================================
-- 15. Real-world use case: inventory analysis
--     warehouse × category × supplier → 2³ = 8 grouping sets
-- ============================================================
DROP TABLE IF EXISTS inventory;

CREATE TABLE inventory (
    warehouse   STRING,
    category    STRING,
    supplier    STRING,
    qty_on_hand BIGINT,
    unit_cost   DOUBLE
);

INSERT INTO inventory VALUES
    ('London',    'Electronics', 'TechCorp',    1200, 85.0),
    ('London',    'Electronics', 'MegaSupply',   800, 92.0),
    ('London',    'Clothing',    'FashionCo',   3500,  8.5),
    ('Frankfurt', 'Electronics', 'TechCorp',     950, 85.0),
    ('Frankfurt', 'Clothing',    'FashionCo',   2200,  8.5),
    ('Frankfurt', 'Home',        'HomePro',     1800, 22.0),
    ('Singapore', 'Electronics', 'MegaSupply',  1500, 90.0),
    ('Singapore', 'Home',        'HomePro',     2100, 20.0),
    ('Singapore', 'Clothing',    'FashionCo',   4000,  7.5);

SELECT
    CASE WHEN GROUPING(warehouse) = 1 THEN 'All Warehouses' ELSE warehouse END AS warehouse_label,
    CASE WHEN GROUPING(category)  = 1 THEN 'All Categories' ELSE category  END AS category_label,
    CASE WHEN GROUPING(supplier)  = 1 THEN 'All Suppliers'  ELSE supplier  END AS supplier_label,
    SUM(qty_on_hand)                           AS total_units,
    ROUND(SUM(qty_on_hand * unit_cost), 2)     AS total_inventory_value,
    ROUND(AVG(unit_cost), 2)                   AS avg_unit_cost,
    GROUPING_ID(warehouse, category, supplier) AS grp_id
FROM inventory
GROUP BY CUBE (warehouse, category, supplier)
ORDER BY grp_id, warehouse_label, category_label, supplier_label;

-- ============================================================
-- 16. Data warehouse: pre-compute CUBE into a Delta table
--     Run once per ETL cycle; dashboards query the summary
--     table instead of the raw fact table.
-- ============================================================
CREATE OR REPLACE TABLE sales_cube_summary
USING DELTA
COMMENT 'Pre-aggregated CUBE over region × product — refreshed nightly'
AS
SELECT
    COALESCE(region,  'All Regions')  AS region_label,
    COALESCE(product, 'All Products') AS product_label,
    ROUND(SUM(amount), 2)             AS total_revenue,
    COUNT(*)                          AS order_count,
    ROUND(AVG(amount), 2)             AS avg_order_value,
    GROUPING_ID(region, product)      AS grp_id
FROM sales
GROUP BY CUBE (region, product);

-- Cluster on grp_id — dashboard queries that filter by level hit minimal files
OPTIMIZE sales_cube_summary ZORDER BY (grp_id);

-- ============================================================
-- 17. Data warehouse: incremental CUBE refresh via MERGE
--     Recompute today's partial cube and upsert into summary.
--     SUM and COUNT are additive; AVG is derived from both.
-- ============================================================
WITH new_data AS (
    SELECT
        COALESCE(region,  'All Regions')  AS region_label,
        COALESCE(product, 'All Products') AS product_label,
        ROUND(SUM(amount), 2)             AS total_revenue,
        COUNT(*)                          AS order_count,
        GROUPING_ID(region, product)      AS grp_id
    FROM sales
    WHERE order_date = CURRENT_DATE()
    GROUP BY CUBE (region, product)
)

MERGE INTO sales_cube_summary AS tgt
USING new_data AS src
    ON tgt.region_label  = src.region_label
   AND tgt.product_label = src.product_label
   AND tgt.grp_id        = src.grp_id
WHEN MATCHED THEN
    UPDATE SET
        total_revenue   = tgt.total_revenue + src.total_revenue,
        order_count     = tgt.order_count   + src.order_count,
        avg_order_value = ROUND(
            (tgt.total_revenue + src.total_revenue)
            / NULLIF(tgt.order_count + src.order_count, 0), 2)
WHEN NOT MATCHED THEN
    INSERT (region_label, product_label, total_revenue, order_count, avg_order_value, grp_id)
    VALUES (
        src.region_label,
        src.product_label,
        src.total_revenue,
        src.order_count,
        ROUND(src.total_revenue / NULLIF(src.order_count, 0), 2),
        src.grp_id
    );
