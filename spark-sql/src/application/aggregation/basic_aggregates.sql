-- Demonstrates core aggregate functions: SUM, AVG, MIN, MAX, COUNT.
-- Covers simple totals, calculated aggregations, and conditional aggregation.
-- Schema: allsales(makename, color, saleprice, cost, saledate)

-- =============================================================================
-- Section 1: Simple table totals — all five aggregate functions in one query
-- =============================================================================
SELECT
    SUM(saleprice) AS total_sales,
    AVG(saleprice) AS avg_sale,
    MIN(saleprice) AS min_sale,
    MAX(saleprice) AS max_sale,
    COUNT(*) AS total_count
FROM allsales;

-- =============================================================================
-- Section 2: Calculated aggregations — profit and rounded average
-- =============================================================================
SELECT
    SUM(saleprice - cost) AS total_profit,
    ROUND(AVG(saleprice), 2) AS avg_sale_price,
    ROUND(AVG(saleprice - cost), 2) AS avg_profit
FROM allsales;

-- =============================================================================
-- Section 3: Conditional aggregation — totals per colour in one pass
-- =============================================================================
SELECT
    SUM(CASE WHEN color = 'Red' THEN saleprice ELSE 0 END) AS red_total,
    SUM(CASE WHEN color = 'Blue' THEN saleprice ELSE 0 END) AS blue_total,
    SUM(CASE WHEN color = 'Black' THEN saleprice ELSE 0 END) AS black_total,
    SUM(CASE WHEN color NOT IN ('Red', 'Blue', 'Black') THEN saleprice ELSE 0 END) AS other_total
FROM allsales;

-- =============================================================================
-- Section 4: Sample data with expected output
-- =============================================================================
-- total_sales   avg_sale    min_sale   max_sale    total_count
-- 682000.00     97428.57    55000.00   170000.00   7
--
-- total_profit  avg_sale_price  avg_profit
-- 102000.00     97428.57        14571.43
--
-- red_total  blue_total  black_total  other_total
-- 137000.00  145000.00   92000.00     308000.00
