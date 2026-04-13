-- Create a table for sales data
DROP TABLE IF EXISTS sales_dt;
CREATE TABLE IF NOT EXISTS sales_dt (
    dt DATE,
    region STRING,
    product STRING,
    sales DOUBLE,
    price DOUBLE,
    quantity INT,
    amount DOUBLE
);

INSERT INTO sales_dt VALUES
('2023-08-01', 'US', 'A', 100.0, 10.0, 10, 100.0),
('2023-08-01', 'US', 'B', 200.0, 20.0, 10, 200.0),
('2023-08-02', 'IN', 'A', 300.0, 30.0, 10, 300.0),
('2023-08-02', 'IN', 'B', 400.0, 40.0, 10, 400.0),
('2023-08-03', 'US', 'C', 150.0, 15.0, 10, 150.0),
('2023-08-03', 'IN', 'C', 250.0, 25.0, 10, 250.0);


-- Basic Aggregation
SELECT
    region,
    avg(sales) AS avg_sales,
    stddev_samp(sales) AS std_sales,
    var_samp(sales) AS var_sales,
    skewness(sales) AS skewness_sales,
    kurtosis(sales) AS kurtosis_sales
FROM sales_dt
GROUP BY region;

-- Correlation and Covariance
SELECT
    corr(price, quantity) AS corr_price_quantity,
    covar_samp(price, quantity) AS covar_sample,
    covar_pop(price, quantity) AS covar_population
FROM sales_dt;

-- Percentiles and Quantiles
SELECT
    percentile_cont(0.5) AS median_sales,
    percentile_disc(0.9) AS p90_sales,
    percentile(sales, 0.5) AS q1_sales,
    percentile(sales, 0.75) AS q3_sales
FROM sales_dt;
