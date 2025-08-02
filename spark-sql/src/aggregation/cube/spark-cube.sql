--- 1. Create table
DROP TABLE IF EXISTS sales_dt;
CREATE TABLE sales_dt (
    date DATE,
    region STRING,
    product STRING,
    amount DOUBLE
);
-- 2. Load data
INSERT INTO sales_dt
VALUES (DATE '2024-07-01', 'East', 'ProductA', 1000.50),
    (DATE '2024-07-01', 'West', 'ProductB', 1500.75),
    (DATE '2024-07-02', 'East', 'ProductA', 1200.25),
    (DATE '2024-07-02', 'West', 'ProductB', 1800.30),
    (DATE '2024-07-03', 'East', 'ProductA', 900.75),
    (DATE '2024-07-03', 'West', 'ProductB', 1600.20),
    (DATE '2024-07-03', NULL, 'ProductB', 1600.20);
-- 3. Query using cube
SELECT date,
    region,
    product,
    SUM(amount) AS total_sales,
    GROUPING_ID() AS grouping_id
FROM sales_dt
GROUP BY CUBE(date, region, product);
-- Null data handling
-- COALESCE(date, 'All') blindly replaces both aggregated NULLs and real NULLs.

SELECT COALESCE(date, 'All') AS date,
    COALESCE(region, 'All') AS region,
    COALESCE(product, 'All') AS product,
    SUM(amount) AS total_sales,
    GROUPING(date) AS is_date_grouping,
    GROUPING(region) AS is_region_grouping,
    GROUPING(product) AS is_product_grouping
FROM sales_dt
GROUP BY CUBE(date, region, product)
ORDER BY date,
    region,
    product;

--  To differentiate real NULLs vs aggregation NULLs.

SELECT 
  CASE WHEN GROUPING(date) = 1 THEN 'All' ELSE CAST(date AS STRING) END AS date,
  CASE WHEN GROUPING(region) = 1 THEN 'All' ELSE region END AS region,
  CASE WHEN GROUPING(product) = 1 THEN 'All' ELSE product END AS product,
  SUM(amount) AS total_sales,
  GROUPING(date) AS is_date_grouping,
  GROUPING(region) AS is_region_grouping,
  GROUPING(product) AS is_product_grouping
FROM sales_dt
GROUP BY CUBE(date, region, product)
ORDER BY  date, 
          region,
         product;