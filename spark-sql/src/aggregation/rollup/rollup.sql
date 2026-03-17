CREATE TABLE sales (
    date DATE,
    region STRING,
    amount DOUBLE
);
--- Load data
INSERT INTO sales
VALUES (DATE '2024-07-01', 'East', 1000.50),
(DATE '2024-07-01', 'West', 1500.75),
(DATE '2024-07-02', 'East', 1200.25),
(DATE '2024-07-02', 'West', 1800.30),
(DATE '2024-07-03', 'East', 900.75),
(DATE '2024-07-03', 'West', 1600.20);
--- Query the data
SELECT
    region,
    SUM(amount) AS total_sales
FROM sales
GROUP BY ROLLUP (region);
--- Replacing NULL with Descriptive Labels
SELECT
    COALESCE(date, 'All Dates') AS date,
    COALESCE(region, 'All Regions') AS region,
    SUM(amount) AS total_sales,
    GROUPING(date) AS is_date_rollup,
    GROUPING(region) AS is_region_rollup
FROM sales
GROUP BY ROLLUP (date, region)
ORDER BY
    date,
    region;
