-- Standardizing text values (data cleaning)

WITH dataset AS (
    SELECT
        1 AS id,
        array(' Apple ', ' BANANA ', 'orange ') AS fruits
)

SELECT
    id,
    transform(fruits, f -> trim(lower(f))) AS cleaned_fruits
FROM dataset;

-- Convert Celsius readings to Fahrenheit

WITH sensors AS (
    SELECT
        'd1' AS device,
        array(25.0, 30.0, 35.0) AS temps
)

SELECT
    device,
    transform(temps, t -> (t * 9 / 5) + 32) AS temps_f
FROM sensors;

-- Add a prefix to email domains

WITH users AS (
    SELECT array('gmail.com', 'yahoo.com', 'outlook.com') AS domains
)

SELECT transform(domains, d -> concat('user@', d)) AS emails
FROM users;

-- Scale feature values (ML pipelines)
WITH dataset AS (
    SELECT
        1 AS id,
        array(10, 20, 30) AS features
)

SELECT
    id,
    transform(features, f -> f / 100.0) AS scaled_features
FROM dataset;

-- Mask sensitive data
WITH customers AS (
    SELECT array('123-45-6789', '987-65-4321') AS ssns
)

SELECT transform(ssns, s -> concat('XXX-XX-', right(s, 4))) AS masked
FROM customers;

-- Round monetary values
WITH sales AS (
    SELECT
        1 AS id,
        array(10.235, 99.995, 123.456) AS prices
)

SELECT
    id,
    transform(prices, p -> round(p, 2)) AS rounded_prices
FROM sales;

-- Tagging logs with severity
WITH logs AS (
    SELECT array(200, 404, 500) AS status_codes
)

SELECT
    transform(
        status_codes,
        sc -> CASE
            WHEN sc >= 500 THEN 'ERROR'
            WHEN sc >= 400 THEN 'WARN'
            ELSE 'INFO'
        END
    ) AS log_levels
FROM logs;

-- Enriching product IDs with descriptions
WITH products AS (
    SELECT array(101, 102, 103) AS ids
)

SELECT transform(ids, i -> concat('product-', i)) AS product_labels
FROM products;
