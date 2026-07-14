-- Create temporary view for sales data
CREATE OR REPLACE TEMP VIEW sales AS (
    SELECT
        name,
        region,
        product,
        amount
    FROM
        VALUES
        ('Alice', 'North', 'A', 100),
        ('Bob', 'North', 'B', 150),
        ('Alice', 'South', 'A', 200),
        ('Bob', 'South', 'B', 300),
        ('Charlie', 'North', 'C', 400),
        ('Alice', 'North', 'B', 250),
        ('Charlie', 'South', 'A', 350)
            AS sales (name, region, product, amount)
);

-- Aggregate sales data by region and product with conditional filtering
-- to calculate total sales for each product in each region
-- This query uses the FILTER clause to sum amounts conditionally based on product type
SELECT
    region,
    SUM(amount) FILTER (WHERE product = 'A') AS sales_product_a,
    SUM(amount) FILTER (WHERE product = 'B') AS sales_product_b,
    SUM(amount) FILTER (WHERE product = 'C') AS sales_product_c
FROM sales
GROUP BY region;

-- Filtered Average with Fallback

SELECT
    region,
    COALESCE(AVG(amount) FILTER (WHERE product = 'B'), 0) AS avg_b
FROM sales
GROUP BY region;

-- Equivalent Using CASE WHEN

SELECT
    region,
    SUM(CASE WHEN product = 'A' THEN amount ELSE 0 END) AS sales_product_a
FROM sales
GROUP BY region;
