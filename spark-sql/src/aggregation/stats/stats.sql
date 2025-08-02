-- Create a table for sales data
DROP TABLE IF EXISTS sales_dt;
CREATE TABLE IF NOT EXISTS sales_dt (
    date DATE,
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
  AVG(sales) AS avg_sales,
  STDDEV_SAMP(sales) AS std_sales,
  VAR_SAMP(sales) AS var_sales,
  SKEWNESS(sales) AS skewness_sales,
  KURTOSIS(sales) AS kurtosis_sales
FROM sales_dt
GROUP BY region;

-- Correlation and Covariance
SELECT
  CORR(price, quantity) AS corr_price_quantity,
  COVAR_SAMP(price, quantity) AS covar_sample,
  COVAR_POP(price, quantity) AS covar_population
FROM sales_dt;

-- Percentiles and Quantiles
SELECT
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sales) AS median_sales,
  PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY sales) AS p90_sales,
  --QUANTILE(0.25) WITHIN GROUP (ORDER BY sales) AS q1_sales,
  PERCENTILE(sales, 0.5) --- WITHIN GROUP (ORDER BY sales) 
  AS q1_sales,
  --QUANTILE(0.75) WITHIN GROUP (ORDER BY sales) AS q3_sales
  PERCENTILE(sales, 0.75) 
  --WITHIN GROUP (ORDER BY sales) 
  AS q3_sales
FROM sales_dt;

