
--- 1. Create table

    CREATE TABLE sales_dt (
        date DATE,
        region STRING,
        product STRING,
        amount DOUBLE
    );

-- 2. Load data

    INSERT INTO sales_dt VALUES
    (DATE '2024-07-01', 'East', 'ProductA', 1000.50),
    (DATE '2024-07-01', 'West', 'ProductB', 1500.75),
    (DATE '2024-07-02', 'East', 'ProductA', 1200.25),
    (DATE '2024-07-02', 'West', 'ProductB', 1800.30),
    (DATE '2024-07-03', 'East', 'ProductA', 900.75),
    (DATE '2024-07-03', 'West', 'ProductB', 1600.20);

-- 3. Query using cube

    SELECT 
        date,
        region,
        product,
        SUM(amount) AS total_sales
    FROM 
        sales_dt
    GROUP BY 
        CUBE(date, region, product);

-- Null data handling

    SELECT
        COALESCE(date, 'All') AS date,
        COALESCE(region, 'All') AS region,
        COALESCE(product, 'All') AS product,
        SUM(amount) AS total_sales,
        GROUPING(date) AS is_date_grouping,
        GROUPING(region) AS is_region_grouping,
        GROUPING(product) AS is_product_grouping
    FROM
        sales_dt
    GROUP BY
        CUBE(date, region, product)
    ORDER BY
        date, region, product;