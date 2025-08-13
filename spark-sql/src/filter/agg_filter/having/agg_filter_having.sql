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
SELECT
    region,
    SUM(amount) AS total_sales
FROM sales
GROUP BY region
HAVING SUM(amount) > 900;

SELECT
    region,
    SUM(amount) FILTER (WHERE product = 'A') AS a_sales,
    SUM(amount) FILTER (WHERE product = 'B') AS b_sales
FROM sales
GROUP BY region
HAVING SUM(amount) > 300;
