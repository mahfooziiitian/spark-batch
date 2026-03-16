-- HAVING clause examples in Spark SQL (Databricks).
-- Demonstrates filtering on aggregate results, HAVING vs WHERE,
-- multiple conditions, ROLLUP, window functions, and FILTER clause.

-- ----------------------------------------------------------------------------
-- Setup: sales table
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TEMP VIEW sales AS
SELECT
    sale_id,
    rep_name,
    region,
    product,
    amount,
    sale_date
FROM
    VALUES
    (1, 'Alice', 'North', 'A', 500.0, DATE '2024-01-10'),
    (2, 'Bob', 'North', 'B', 150.0, DATE '2024-01-11'),
    (3, 'Alice', 'North', 'A', 300.0, DATE '2024-01-12'),
    (4, 'Carol', 'South', 'A', 800.0, DATE '2024-01-13'),
    (5, 'Bob', 'South', 'B', 420.0, DATE '2024-01-14'),
    (6, 'Carol', 'South', 'C', 90.0, DATE '2024-01-15'),
    (7, 'Alice', 'North', 'B', 640.0, DATE '2024-01-16'),
    (8, 'Dave', 'East', 'A', 200.0, DATE '2024-01-17'),
    (9, 'Dave', 'East', 'C', 310.0, DATE '2024-01-18')
        AS t (sale_id, rep_name, region, product, amount, sale_date);

-- ----------------------------------------------------------------------------
-- 1. Basic HAVING: filter groups on aggregate condition
-- ----------------------------------------------------------------------------
-- Regions where total sales exceed 1000.
SELECT
    region,
    SUM(amount) AS total_sales
FROM sales
GROUP BY region
HAVING SUM(amount) > 1000.0;
-- Result: North (1590.0), South (1310.0); East (510.0) excluded.

-- ----------------------------------------------------------------------------
-- 2. HAVING vs WHERE: filter before vs after aggregation
-- ----------------------------------------------------------------------------
-- WHERE filters individual rows BEFORE aggregation.
-- HAVING filters aggregated groups AFTER aggregation.

-- WHERE only: exclude product C rows, then aggregate.
SELECT
    region,
    SUM(amount) AS total_sales
FROM sales
WHERE product != 'C'
GROUP BY region;
-- Result: North 1590.0, South 1220.0, East 200.0 (C rows removed pre-agg)

-- HAVING only: aggregate all rows, then exclude small groups.
SELECT
    region,
    SUM(amount) AS total_sales
FROM sales
GROUP BY region
HAVING SUM(amount) >= 500.0;
-- Result: North 1590.0, South 1310.0, East 510.0

-- WHERE + HAVING: filter rows first, then filter groups.
SELECT
    region,
    SUM(amount) AS total_sales
FROM sales
WHERE product != 'C'
GROUP BY region
HAVING SUM(amount) > 500.0;
-- Result: North 1590.0, South 1220.0; East 200.0 excluded by HAVING.

-- ----------------------------------------------------------------------------
-- 3. Multiple HAVING conditions
-- ----------------------------------------------------------------------------
-- Reps with more than 2 sales AND average sale above 300.
SELECT
    rep_name,
    COUNT(*) AS sale_count,
    AVG(amount) AS avg_sale
FROM sales
GROUP BY rep_name
HAVING
    COUNT(*) > 2
    AND AVG(amount) > 300.0;
-- Result: Alice (3 sales, avg 480.0)

-- ----------------------------------------------------------------------------
-- 4. HAVING with ROLLUP
-- ----------------------------------------------------------------------------
-- ROLLUP adds subtotal and grand total rows;
-- HAVING filters which totals to show.
SELECT
    region,
    product,
    SUM(amount) AS total_sales
FROM sales
GROUP BY ROLLUP (region, product)
HAVING SUM(amount) > 500.0;
-- Result: subtotals and grand total rows with sum > 500
-- (NULL in group columns marks rollup rows).

-- ----------------------------------------------------------------------------
-- 5. HAVING with window function (using a subquery / CTE)
-- ----------------------------------------------------------------------------
-- Window functions cannot appear directly in HAVING. Wrap in a subquery.
-- Find reps whose total sales exceed the average rep total.
SELECT
    rep_summary.rep_name,
    rep_summary.total_sales
FROM (
    SELECT
        rep_name,
        SUM(amount) AS total_sales,
        AVG(SUM(amount)) OVER () AS avg_rep_total
    FROM sales
    GROUP BY rep_name
) AS rep_summary
WHERE rep_summary.total_sales > rep_summary.avg_rep_total;
-- Result: Alice (1440.0) and Carol (890.0) exceed the per-rep mean.

-- ----------------------------------------------------------------------------
-- 6. HAVING with FILTER clause
-- ----------------------------------------------------------------------------
-- FILTER inside an aggregate applies a per-row condition before the aggregate.
-- HAVING then filters which groups to keep based on the aggregate result.
SELECT
    region,
    SUM(amount) FILTER (WHERE product = 'A') AS product_a_sales,
    SUM(amount) FILTER (WHERE product = 'B') AS product_b_sales,
    SUM(amount) AS total_sales
FROM sales
GROUP BY region
HAVING SUM(amount) FILTER (WHERE product = 'A') > 400.0;
-- Result: North and South (Product A = 800 each); East excluded (A = 200).
