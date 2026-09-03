-- ============================================================
-- Topic: GROUP BY — the foundation of SQL aggregation
-- Dialect: Databricks / Spark SQL 3.5
-- Description: GROUP BY collapses rows sharing the same key
--              values into a single row per group, over which
--              aggregate functions (COUNT, SUM, AVG, MIN, MAX)
--              are computed. Covers single/multi-column keys,
--              grouping by expressions, COUNT variants, DISTINCT,
--              HAVING vs WHERE, the FILTER clause, NULL grouping
--              semantics, grouping by ordinal position, and
--              real-world CTE reporting patterns.
-- ============================================================

-- --- Setup: orders fact table (12 rows, one real NULL) -------
DROP TABLE IF EXISTS orders;

CREATE TABLE orders (
    order_id BIGINT,
    region STRING,
    channel STRING,
    product STRING,
    quantity INT,
    amount DOUBLE,
    order_date DATE
);

INSERT INTO orders VALUES
(1, 'East', 'Online', 'Widget', 3, 120.00, DATE '2024-01-15'),
(2, 'West', 'Online', 'Gadget', 5, 340.00, DATE '2024-01-18'),
(3, 'East', 'Retail', 'Widget', 2, 80.00, DATE '2024-02-10'),
(4, 'North', 'Online', 'Gadget', 4, 210.00, DATE '2024-02-14'),
(5, 'West', 'Retail', 'Widget', 6, 150.00, DATE '2024-03-05'),
(6, 'East', 'Online', 'Gadget', 8, 450.00, DATE '2024-03-09'),
(7, 'North', 'Retail', 'Widget', 1, 90.00, DATE '2024-03-20'),
(8, 'West', 'Online', 'Gadget', 5, 270.00, DATE '2024-03-22'),
(9, 'East', 'Online', 'Gadget', 7, 380.00, DATE '2024-04-01'),
(10, 'North', 'Retail', 'Widget', 3, 130.00, DATE '2024-04-11'),
(11, 'West', 'Retail', 'Gadget', 2, 160.00, DATE '2024-04-18'),
(12, NULL, 'Online', 'Widget', 4, 200.00, DATE '2024-04-25');  -- real NULL region

-- ============================================================
-- 1. Basic single-column GROUP BY
--    One output row per distinct region; SUM aggregates the
--    amount column across all rows in each group.
-- ============================================================
SELECT
    region,
    SUM(amount) AS total_sales
FROM orders
GROUP BY region
ORDER BY region NULLS LAST;
-- region | total_sales
-- East    | 1030.0
-- North   | 430.0
-- West    | 920.0
-- NULL    | 200.0        <- NULL is its own group (see example 9)

-- ============================================================
-- 2. The core aggregate functions in one pass
--    COUNT / SUM / AVG / MIN / MAX all share the same groups.
-- ============================================================
SELECT
    region,
    COUNT(*) AS order_count,
    SUM(amount) AS total_sales,
    ROUND(AVG(amount), 2) AS avg_order_value,
    MIN(amount) AS smallest_order,
    MAX(amount) AS largest_order
FROM orders
GROUP BY region
ORDER BY total_sales DESC;

-- ============================================================
-- 3. Multi-column GROUP BY
--    The group key is the tuple (region, channel); one row per
--    observed combination -- Cartesian pairs with no data are
--    simply absent.
-- ============================================================
SELECT
    region,
    channel,
    COUNT(*) AS order_count,
    SUM(amount) AS total_sales
FROM orders
GROUP BY region, channel
ORDER BY region ASC NULLS LAST, channel ASC;

-- ============================================================
-- 4. COUNT variants -- they answer different questions
--    COUNT(*)          -> number of rows in the group
--    COUNT(col)        -> non-NULL values of col in the group
--    COUNT(DISTINCT c) -> distinct non-NULL values of c
-- ============================================================
SELECT
    channel,
    COUNT(*) AS total_rows,
    COUNT(region) AS non_null_regions,
    COUNT(DISTINCT region) AS distinct_regions,
    COUNT(DISTINCT product) AS distinct_products
FROM orders
GROUP BY channel
ORDER BY channel ASC;
-- channel | total_rows | non_null_regions | distinct_regions | distinct_products
-- Online   | 7          | 6                | 3                | 2
-- Retail   | 5          | 5                | 3                | 2

-- ============================================================
-- 5. GROUP BY an expression
--    Any deterministic expression may be a grouping key. Here
--    the month extracted from order_date buckets the rows.
-- ============================================================
SELECT
    MONTH(order_date) AS order_month,
    COUNT(*) AS order_count,
    SUM(amount) AS monthly_sales
FROM orders
GROUP BY MONTH(order_date)
ORDER BY order_month ASC;

-- ============================================================
-- 6. GROUP BY a CASE bucket
--    Classify each order into a size band, then aggregate the
--    bands. The same CASE expression must appear in GROUP BY.
-- ============================================================
SELECT
    CASE
        WHEN amount < 150 THEN 'Small'
        WHEN amount < 300 THEN 'Medium'
        ELSE 'Large'
    END AS order_band,
    COUNT(*) AS order_count,
    ROUND(AVG(amount), 2) AS avg_amount
FROM orders
GROUP BY
    CASE
        WHEN amount < 150 THEN 'Small'
        WHEN amount < 300 THEN 'Medium'
        ELSE 'Large'
    END
ORDER BY avg_amount ASC;

-- ============================================================
-- 7. WHERE vs HAVING -- filter timing matters
--    WHERE  removes rows BEFORE grouping (per-row predicate).
--    HAVING removes groups AFTER aggregation (per-group).
--    Here: keep only Online orders (WHERE), then keep regions
--    whose online total exceeds 300 (HAVING).
-- ============================================================
SELECT
    region,
    SUM(amount) AS online_sales
FROM orders
WHERE channel = 'Online'
GROUP BY region
HAVING SUM(amount) > 300
ORDER BY online_sales DESC;

-- ============================================================
-- 8. HAVING on an aggregate not in the SELECT list
--    HAVING can reference any aggregate over the group, even
--    one the query does not project.
-- ============================================================
SELECT
    region,
    SUM(amount) AS total_sales
FROM orders
GROUP BY region
HAVING COUNT(*) >= 3
ORDER BY total_sales DESC;
-- Only regions with 3+ orders survive; the COUNT itself is hidden.

-- ============================================================
-- 9. NULL grouping semantics
--    Unlike joins/equality (where NULL never matches), GROUP BY
--    gathers ALL NULL keys into a single group. Use a label to
--    surface it explicitly in reports.
-- ============================================================
SELECT
    COALESCE(region, '(unknown)') AS region_label,
    COUNT(*) AS order_count,
    SUM(amount) AS total_sales
FROM orders
GROUP BY region
ORDER BY total_sales DESC;
-- The single NULL-region order forms its own '(unknown)' group.

-- ============================================================
-- 10. Conditional aggregation with the FILTER clause
--     FILTER (WHERE ...) restricts which rows feed a single
--     aggregate, enabling multiple sliced measures side by side
--     without self-joins or repeated scans.
-- ============================================================
SELECT
    region,
    COUNT(*) AS total_orders,
    COUNT(*) FILTER (WHERE channel = 'Online') AS online_orders,
    COUNT(*) FILTER (WHERE channel = 'Retail') AS retail_orders,
    ROUND(SUM(amount) FILTER (WHERE product = 'Gadget'), 2) AS gadget_sales,
    ROUND(SUM(amount) FILTER (WHERE product = 'Widget'), 2) AS widget_sales
FROM orders
GROUP BY region
ORDER BY total_orders DESC;

-- ============================================================
-- 11. GROUP BY ordinal position
--     Integers in GROUP BY reference SELECT-list positions.
--     Convenient for grouped expressions, but positional keys
--     are brittle under column reordering -- prefer explicit
--     expressions in production code.
-- ============================================================
SELECT
    region,
    channel,
    SUM(amount) AS total_sales
FROM orders
GROUP BY 1, 2  -- noqa: AM06
ORDER BY 1 NULLS LAST, 2 ASC;  -- noqa: AM06

-- ============================================================
-- 12. Derived per-group metrics
--     Aggregates combine freely into ratios and rates. Guard
--     division with NULLIF to avoid divide-by-zero.
-- ============================================================
SELECT
    region,
    SUM(quantity) AS total_units,
    ROUND(SUM(amount), 2) AS total_revenue,
    ROUND(SUM(amount) / NULLIF(SUM(quantity), 0), 2) AS revenue_per_unit,
    ROUND(SUM(amount) / NULLIF(COUNT(*), 0), 2) AS revenue_per_order
FROM orders
GROUP BY region
ORDER BY revenue_per_unit DESC NULLS LAST;

-- ============================================================
-- 13. Real-world reporting: CTE -> aggregate -> rank
--     Pre-shape rows in a CTE, aggregate to a channel/product
--     grain, then keep the top revenue combinations. This layered
--     pattern keeps each stage readable and testable.
-- ============================================================
WITH enriched AS (
    SELECT
        channel,
        product,
        amount,
        QUARTER(order_date) AS order_quarter
    FROM orders
    WHERE order_date >= DATE '2024-01-01'
),

by_channel_product AS (
    SELECT
        channel,
        product,
        COUNT(*) AS order_count,
        ROUND(SUM(amount), 2) AS total_revenue,
        ROUND(AVG(amount), 2) AS avg_revenue
    FROM enriched
    GROUP BY channel, product
)

SELECT
    channel,
    product,
    order_count,
    total_revenue,
    avg_revenue
FROM by_channel_product
WHERE total_revenue > 200
ORDER BY total_revenue DESC, channel ASC, product ASC;
