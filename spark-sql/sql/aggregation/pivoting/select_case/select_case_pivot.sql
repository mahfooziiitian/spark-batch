CREATE TABLE IF NOT EXISTS sales (
    sale_id INT,
    year INT,
    month STRING,
    region STRING,
    product STRING,
    quantity INT,
    revenue INT
);
INSERT INTO sales VALUES
(1, 2023, 'Jan', 'East', 'Laptop', 5, 5000),
(2, 2023, 'Jan', 'East', 'Monitor', 10, 2000),
(3, 2023, 'Jan', 'West', 'Laptop', 3, 3000),
(4, 2023, 'Feb', 'West', 'Monitor', 7, 1400),
(5, 2023, 'Feb', 'South', 'Laptop', 4, 4000),
(6, 2023, 'Feb', 'South', 'Monitor', 6, 1200),
(7, 2024, 'Jan', 'East', 'Laptop', 6, 6000),
(8, 2024, 'Jan', 'West', 'Laptop', 5, 5000),
(9, 2024, 'Feb', 'South', 'Monitor', 8, 1600),
(10, 2024, 'Feb', 'East', 'Monitor', 7, 1400);

SELECT
    year,
    SUM(CASE WHEN region = 'East' THEN revenue ELSE 0 END) AS east_revenue,
    SUM(CASE WHEN region = 'West' THEN revenue ELSE 0 END) AS west_revenue,
    SUM(CASE WHEN region = 'South' THEN revenue ELSE 0 END) AS south_revenue
FROM sales
GROUP BY year
ORDER BY year;
