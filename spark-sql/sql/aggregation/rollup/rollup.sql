-- ============================================================
-- Topic: ROLLUP — left-to-right hierarchical aggregation
-- Dialect: Databricks / Spark SQL 3.5
-- Description: ROLLUP generates n+1 grouping sets by removing
--              columns from the right, producing one subtotal
--              row per hierarchy prefix plus a grand total.
--              Covers GROUPING(), GROUPING_ID(), NULL handling,
--              time hierarchies, CTE patterns, HAVING filters,
--              and real-world category/product use cases.
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

-- ─── Setup: sales_nullable — includes one real NULL region ───
-- Used in NULL-handling example to contrast COALESCE (unsafe)
-- with GROUPING() (safe).
DROP TABLE IF EXISTS sales_nullable;

CREATE TABLE sales_nullable (
    order_id   BIGINT,
    region     STRING,
    product    STRING,
    amount     DOUBLE,
    order_date DATE
);

INSERT INTO sales_nullable VALUES
    (1, 'East',  'Widget',  120.00, DATE '2024-01-15'),
    (2, 'West',  'Gadget',  340.00, DATE '2024-01-15'),
    (3, 'East',  'Widget',   80.00, DATE '2024-02-10'),
    (4, 'North', 'Gadget',  210.00, DATE '2024-02-10'),
    (5, NULL,    'Widget',  150.00, DATE '2024-03-05');  -- real NULL region

-- ─── Setup: monthly_revenue — year/quarter/month hierarchy ───
DROP TABLE IF EXISTS monthly_revenue;

CREATE TABLE monthly_revenue (
    sale_year  INT,
    quarter    STRING,
    month_name STRING,
    revenue    DOUBLE
);

INSERT INTO monthly_revenue VALUES
    (2024, 'Q1', 'Jan', 12000.0),
    (2024, 'Q1', 'Feb',  9500.0),
    (2024, 'Q1', 'Mar', 11000.0),
    (2024, 'Q2', 'Apr', 13500.0),
    (2024, 'Q2', 'May', 16000.0),
    (2024, 'Q2', 'Jun', 14500.0),
    (2024, 'Q3', 'Jul', 10000.0),
    (2024, 'Q3', 'Aug',  8500.0),
    (2024, 'Q3', 'Sep', 17000.0),
    (2024, 'Q4', 'Oct', 19000.0),
    (2024, 'Q4', 'Nov', 22000.0),
    (2024, 'Q4', 'Dec', 25000.0);

-- ─── Setup: product_sales — category/subcategory/product ─────
DROP TABLE IF EXISTS product_sales;

CREATE TABLE product_sales (
    category    STRING,
    subcategory STRING,
    product     STRING,
    revenue     DOUBLE,
    units_sold  BIGINT
);

INSERT INTO product_sales VALUES
    ('Electronics', 'Phones',    'iPhone 15',     95000.0,  19),
    ('Electronics', 'Phones',    'Galaxy S24',    78000.0,  18),
    ('Electronics', 'Laptops',   'MacBook Pro',  125000.0,  10),
    ('Electronics', 'Laptops',   'ThinkPad X1',   89000.0,  11),
    ('Clothing',    'Tops',      'Polo Shirt',     4500.0, 150),
    ('Clothing',    'Tops',      'T-Shirt Pack',   3200.0, 200),
    ('Clothing',    'Bottoms',   'Slim Jeans',     6800.0,  85),
    ('Clothing',    'Bottoms',   'Chinos',         5100.0,  68),
    ('Home',        'Kitchen',   'Air Fryer',     12000.0,  60),
    ('Home',        'Kitchen',   'Coffee Maker',   8500.0,  85),
    ('Home',        'Furniture', 'Office Chair',  22000.0,  44),
    ('Home',        'Furniture', 'Standing Desk', 35000.0,  28);

-- ============================================================
-- 1. Basic single-column ROLLUP
--    ROLLUP(region) → 2 sets: (region), ()
-- ============================================================
SELECT
    region,
    SUM(amount) AS total_sales
FROM sales
GROUP BY ROLLUP (region)
ORDER BY region NULLS LAST;
-- region | total_sales
-- East    | 650.0        ← detail  (region)
-- North   | 300.0        ← detail  (region)
-- West    | 760.0        ← detail  (region)
-- NULL    | 1710.0       ← grand total  ()

-- ============================================================
-- 2. Two-column ROLLUP — region → product hierarchy
--    ROLLUP(region, product) → 3 sets:
--    (region, product), (region), ()
-- ============================================================
SELECT
    region,
    product,
    SUM(amount) AS total_sales
FROM sales
GROUP BY ROLLUP (region, product)
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
-- NULL    | NULL    | 1710.0       ← grand total  ()

-- ============================================================
-- 3. Three-level hierarchy — year → region → product
--    ROLLUP(year, region, product) → 4 sets:
--    (year,region,product), (year,region), (year), ()
-- ============================================================
SELECT
    region,
    product,
    YEAR(order_date) AS sale_year,
    SUM(amount)      AS total_sales,
    COUNT(*)         AS order_count
FROM sales
GROUP BY ROLLUP (YEAR(order_date), region, product)
ORDER BY sale_year NULLS LAST, region NULLS LAST, product NULLS LAST;
-- 4 grouping levels generated in a single scan:
--   (year, region, product) → detail rows
--   (year, region)          → region subtotal within each year
--   (year)                  → year subtotal
--   ()                      → grand total

-- ============================================================
-- 4. GROUPING() flags — identify subtotal rows
--    GROUPING(col) = 1 → synthetic NULL (subtotal marker)
--    GROUPING(col) = 0 → real grouping value
-- ============================================================
SELECT
    region,
    product,
    SUM(amount)       AS total_sales,
    GROUPING(region)  AS region_is_subtotal,
    GROUPING(product) AS product_is_subtotal
FROM sales
GROUP BY ROLLUP (region, product)
ORDER BY region NULLS LAST, product NULLS LAST;

-- ============================================================
-- 5. Readable labels with GROUPING()
-- ============================================================
SELECT
    CASE WHEN GROUPING(region)  = 1 THEN 'All Regions'  ELSE region  END AS region_label,
    CASE WHEN GROUPING(product) = 1 THEN 'All Products' ELSE product END AS product_label,
    SUM(amount) AS total_sales
FROM sales
GROUP BY ROLLUP (region, product)
ORDER BY region_label ASC, product_label ASC;
-- region_label | product_label | total_sales
-- All Regions  | All Products  | 1710.0       ← grand total
-- East         | All Products  | 650.0        ← region subtotal
-- East         | Gadget        | 450.0
-- East         | Widget        | 200.0
-- North        | All Products  | 300.0
-- West         | All Products  | 760.0

-- ============================================================
-- 6. GROUPING_ID() — compact bitmask row classifier
--    For ROLLUP(region, product), only these IDs are valid:
--      grp_id=0 → detail        (region, product)
--      grp_id=1 → region sub    (region)           — product dropped
--      grp_id=3 → grand total   ()                 — both dropped
--    grp_id=2 is absent: that would be (product) only,
--    produced by CUBE but NOT ROLLUP.
-- ============================================================
SELECT
    region,
    product,
    SUM(amount)                  AS total_sales,
    GROUPING_ID(region, product) AS grp_id,
    CASE GROUPING_ID(region, product)
        WHEN 0 THEN 'Detail'
        WHEN 1 THEN 'Region Subtotal'
        WHEN 3 THEN 'Grand Total'
    END AS row_type
FROM sales
GROUP BY ROLLUP (region, product)
ORDER BY grp_id ASC, region ASC NULLS LAST, product ASC NULLS LAST;

-- ============================================================
-- 7. NULL handling — COALESCE (unsafe) vs GROUPING() (safe)
--    sales_nullable has a row where region IS NULL (real data).
--    COALESCE cannot distinguish that from a subtotal NULL.
-- ============================================================

-- Unsafe: COALESCE conflates real NULL region with subtotal markers
SELECT
    COALESCE(region,  'All Regions')  AS region,
    COALESCE(product, 'All Products') AS product,
    SUM(amount) AS total_sales
FROM sales_nullable
GROUP BY ROLLUP (region, product)
ORDER BY region ASC, product ASC;

-- Safe: GROUPING() identifies only synthetic subtotal NULLs
SELECT
    CASE WHEN GROUPING(region)  = 1 THEN 'All Regions'  ELSE region  END AS region,
    CASE WHEN GROUPING(product) = 1 THEN 'All Products' ELSE product END AS product,
    SUM(amount)       AS total_sales,
    GROUPING(region)  AS is_region_subtotal,
    GROUPING(product) AS is_product_subtotal
FROM sales_nullable
GROUP BY ROLLUP (region, product)
ORDER BY region NULLS LAST, product NULLS LAST;

-- ============================================================
-- 8. Multiple aggregates in one pass
-- ============================================================
SELECT
    CASE WHEN GROUPING(region)  = 1 THEN 'All Regions'  ELSE region  END AS region_label,
    CASE WHEN GROUPING(product) = 1 THEN 'All Products' ELSE product END AS product_label,
    ROUND(SUM(amount), 2)  AS total_revenue,
    COUNT(*)               AS order_count,
    ROUND(AVG(amount), 2)  AS avg_order_value,
    MAX(amount)            AS max_order_value
FROM sales
GROUP BY ROLLUP (region, product)
ORDER BY GROUPING_ID(region, product) ASC, region_label ASC, product_label ASC;

-- ============================================================
-- 9. Year → Quarter → Month time hierarchy
--    Classic BI pattern: ROLLUP(year, quarter, month)
--    Valid GROUPING_ID values: 0, 1, 3, 7
--      0 → monthly detail    (year, quarter, month)
--      1 → quarterly total   (year, quarter)
--      3 → annual total      (year)
--      7 → grand total       ()
-- ============================================================
SELECT
    CASE WHEN GROUPING(sale_year)  = 1 THEN 'All Years'    ELSE CAST(sale_year AS STRING) END AS yr,
    CASE WHEN GROUPING(quarter)    = 1 THEN 'All Quarters' ELSE quarter                   END AS qtr,
    CASE WHEN GROUPING(month_name) = 1 THEN 'All Months'   ELSE month_name                END AS mth,
    ROUND(SUM(revenue), 2) AS total_revenue,
    COUNT(*)               AS month_count
FROM monthly_revenue
GROUP BY ROLLUP (sale_year, quarter, month_name)
ORDER BY GROUPING_ID(sale_year, quarter, month_name) ASC, yr ASC, qtr ASC, mth ASC;

-- ============================================================
-- 10. CTE + ROLLUP — pre-filter, compute, label downstream
-- ============================================================
WITH raw AS (
    SELECT
        region,
        amount,
        YEAR(order_date)    AS sale_year,
        QUARTER(order_date) AS sale_quarter
    FROM sales
    WHERE order_date >= DATE '2024-01-01'
),

rollup_base AS (
    SELECT
        sale_year,
        sale_quarter,
        region,
        SUM(amount)                                  AS total_revenue,
        COUNT(*)                                     AS order_count,
        GROUPING_ID(sale_year, sale_quarter, region) AS grp_id
    FROM raw
    GROUP BY ROLLUP (sale_year, sale_quarter, region)
)

SELECT
    grp_id,
    order_count,
    ROUND(total_revenue, 2)                        AS total_revenue,
    COALESCE(CAST(sale_year    AS STRING), 'All Years')    AS year_label,
    COALESCE(CAST(sale_quarter AS STRING), 'All Quarters') AS quarter_label,
    COALESCE(region,                       'All Regions')  AS region_label
FROM rollup_base
ORDER BY grp_id ASC, year_label ASC, quarter_label ASC, region_label ASC;

-- ============================================================
-- 11. Filter to a specific hierarchy level
--     Retrieve only quarterly subtotals: HAVING grp_id = 1
--     (excludes monthly detail 0, annual total 3, grand total 7)
-- ============================================================
SELECT
    CASE WHEN GROUPING(sale_year) = 1 THEN 'All Years' ELSE CAST(sale_year AS STRING) END AS yr,
    CASE WHEN GROUPING(quarter)   = 1 THEN 'All Qtrs'  ELSE quarter                   END AS qtr,
    ROUND(SUM(revenue), 2) AS quarterly_revenue
FROM monthly_revenue
GROUP BY ROLLUP (sale_year, quarter, month_name)
HAVING GROUPING_ID(sale_year, quarter, month_name) = 1
ORDER BY yr ASC, qtr ASC;

-- ============================================================
-- 12. Real-world use case: product category hierarchy
--     category → subcategory → product
--     ROLLUP → 4 sets, grp_id values: 0, 1, 3, 7
-- ============================================================
SELECT
    CASE WHEN GROUPING(category)    = 1 THEN 'All Categories'    ELSE category    END AS category_label,
    CASE WHEN GROUPING(subcategory) = 1 THEN 'All Subcategories' ELSE subcategory END AS subcategory_label,
    CASE WHEN GROUPING(product)     = 1 THEN 'All Products'      ELSE product     END AS product_label,
    ROUND(SUM(revenue), 2)                              AS total_revenue,
    SUM(units_sold)                                     AS total_units,
    ROUND(SUM(revenue) / NULLIF(SUM(units_sold), 0), 2) AS avg_unit_price,
    GROUPING_ID(category, subcategory, product)         AS grp_id
FROM product_sales
GROUP BY ROLLUP (category, subcategory, product)
ORDER BY grp_id ASC, category_label ASC, subcategory_label ASC, product_label ASC;
-- grp_id = 0 → product detail
-- grp_id = 1 → subcategory subtotal
-- grp_id = 3 → category subtotal
-- grp_id = 7 → grand total

-- ============================================================
-- 13. Column order is critical — ROLLUP(a,b) ≠ ROLLUP(b,a)
--     ROLLUP(region, product): subtotals by region, then grand
--     ROLLUP(product, region): subtotals by product, then grand
-- ============================================================

-- Subtotals per region (product drops first)
SELECT
    region,
    product,
    SUM(amount)                  AS total_sales,
    GROUPING_ID(region, product) AS grp_id
FROM sales
GROUP BY ROLLUP (region, product)
ORDER BY grp_id ASC, region ASC NULLS LAST, product ASC NULLS LAST;
-- grp_id=1 rows show per-region totals with product=NULL

-- Subtotals per product (region drops first)
SELECT
    region,
    product,
    SUM(amount)                  AS total_sales,
    GROUPING_ID(region, product) AS grp_id
FROM sales
GROUP BY ROLLUP (product, region)
ORDER BY grp_id ASC, product ASC NULLS LAST, region ASC NULLS LAST;
-- grp_id=1 rows now show per-product totals with region=NULL
-- These are completely different results from the query above.
