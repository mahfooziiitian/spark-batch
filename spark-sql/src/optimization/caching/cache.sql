-- ✅ Step 1: Create Sample Table
-- Create a temp view with large synthetic data
WITH base_data AS (
    SELECT
        date,
        region,
        product,
        amount
    FROM
        VALUES
        ('2025-01-01', 'North', 'Pen', 10),
        ('2025-01-01', 'North', 'Notebook', 20),
        ('2025-01-02', 'South', 'Pen', 5),
        ('2025-01-03', 'East', 'Pencil', 8),
        ('2025-01-04', 'South', 'Notebook', 15),
        ('2025-01-05', 'West', 'Pen', 12)
            AS sales (date, region, product, amount)
)

SELECT *
FROM base_data
    LATERAL VIEW explode(sequence(1, 50000)) x;


-- Step 2: Without Caching

-- Aggregation 1
SELECT
    region,
    product,
    sum(amount) AS total_amount
FROM sales_data
GROUP BY region, product
ORDER BY total_amount DESC;

-- Aggregation 2
SELECT
    region,
    product,
    sum(amount) AS total_amount
FROM sales_data
GROUP BY region, product
HAVING sum(amount) > 100000
ORDER BY region;

-- Step 3: With Caching

-- Cache the transformed aggregated data
CREATE OR REPLACE TEMP VIEW sales_agg AS
SELECT
    region,
    product,
    sum(amount) AS total_amount
FROM sales_data
GROUP BY region, product;

-- Cache it in memory
CACHE TABLE sales_agg;

-- Materialize the cache
SELECT count(*) FROM sales_agg;

-- Now Run Optimized Queries

-- Query 1 (uses cached view)
SELECT *
FROM sales_agg
ORDER BY total_amount DESC;

-- Query 2 (uses cached view)
SELECT *
FROM sales_agg
WHERE total_amount > 100000
ORDER BY region;

-- 🔄 To Uncache

UNCACHE TABLE sales_agg;
