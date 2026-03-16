-- Common Table Expression (CTE) examples in Spark SQL (Databricks).
-- Covers: single CTE, chained pipeline, self-join, MERGE, recursive sequence,
-- and CTE replacing a subquery for incremental pipeline readability.

-- ----------------------------------------------------------------------------
-- Setup: orders and customers inline data
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TEMP VIEW raw_orders AS
SELECT
    order_id,
    customer_id,
    country,
    amount,
    order_date
FROM
    VALUES
    (1, 'C1', 'US', 120.0, DATE '2024-01-05'),
    (2, 'C2', 'UK', 45.0, DATE '2024-01-06'),
    (3, 'C1', 'US', 300.0, DATE '2024-01-07'),
    (4, 'C3', 'US', 80.0, DATE '2024-01-08'),
    (5, 'C2', 'UK', 210.0, DATE '2024-01-09'),
    (6, 'C4', 'DE', 95.0, DATE '2024-01-10'),
    (7, 'C3', 'US', 175.0, DATE '2024-01-11')
        AS t (order_id, customer_id, country, amount, order_date);

CREATE OR REPLACE TEMP VIEW raw_customers AS
SELECT
    customer_id,
    customer_name,
    tier
FROM
    VALUES
    ('C1', 'Alice', 'gold'),
    ('C2', 'Bob', 'silver'),
    ('C3', 'Carol', 'gold'),
    ('C4', 'Dave', 'bronze')
        AS t (customer_id, customer_name, tier);

-- ----------------------------------------------------------------------------
-- 1. Single CTE: filter then query
-- ----------------------------------------------------------------------------
-- Isolate US orders, then summarise per customer.
WITH us_orders AS (
    SELECT *
    FROM raw_orders
    WHERE country = 'US'
)

SELECT
    customer_id,
    COUNT(*) AS order_count,
    SUM(amount) AS total_amount
FROM us_orders
GROUP BY customer_id;
-- Result: C1 → 2 orders / 420.0, C3 → 2 orders / 255.0

-- ----------------------------------------------------------------------------
-- 2. Multiple chained CTEs: raw → filtered → aggregated
-- ----------------------------------------------------------------------------
-- Pipeline: start from raw, keep high-value orders, then summarise by country.
WITH filtered AS (
    SELECT *
    FROM raw_orders
    WHERE amount >= 100.0
),

aggregated AS (
    SELECT
        country,
        COUNT(*) AS order_count,
        SUM(amount) AS total_amount,
        AVG(amount) AS avg_amount
    FROM filtered
    GROUP BY country
)

SELECT *
FROM aggregated
ORDER BY total_amount DESC;
-- Result: US 630.0, UK 255.0

-- ----------------------------------------------------------------------------
-- 3. CTE referenced twice in the same query (self-join pattern)
-- ----------------------------------------------------------------------------
-- Compare each order to the previous order from the same customer.
WITH ordered_orders AS (
    SELECT
        order_id,
        customer_id,
        amount,
        LAG(amount) OVER (
            PARTITION BY customer_id ORDER BY order_date
        ) AS prev_amount
    FROM raw_orders
)

SELECT
    o1.order_id,
    o1.customer_id,
    o1.amount AS current_amount,
    o2.prev_amount,
    o1.amount - o2.prev_amount AS amount_delta
FROM ordered_orders AS o1
INNER JOIN ordered_orders AS o2
    ON o1.order_id = o2.order_id
WHERE o2.prev_amount IS NOT NULL;
-- Result: orders where a prior purchase exists, with the spend delta.

-- ----------------------------------------------------------------------------
-- 4. CTE in a MERGE statement (upsert use case)
-- ----------------------------------------------------------------------------
-- Identify changed US orders to merge into a target table.
CREATE OR REPLACE TEMP VIEW orders_target AS
SELECT
    order_id,
    customer_id,
    country,
    amount
FROM
    VALUES
    (1, 'C1', 'US', 100.0),
    (4, 'C3', 'US', 80.0)
        AS t (order_id, customer_id, country, amount);

WITH us_source AS (
    SELECT
        order_id,
        customer_id,
        country,
        amount
    FROM raw_orders
    WHERE country = 'US'
)

MERGE INTO orders_target AS tgt
USING us_source AS src
    ON tgt.order_id = src.order_id
WHEN MATCHED AND tgt.amount != src.amount
THEN UPDATE SET tgt.amount = src.amount
WHEN NOT MATCHED
THEN
    INSERT (order_id, customer_id, country, amount)
    VALUES (src.order_id, src.customer_id, src.country, src.amount);
-- Result: row 1 updated (amount 100 → 120), rows 3 and 7 inserted.

-- ----------------------------------------------------------------------------
-- 5. Recursive CTE: generate integer sequence 1..10 (Spark 3.5+ WITH RECURSIVE)
-- ----------------------------------------------------------------------------
WITH RECURSIVE int_sequence (n) AS (
    SELECT 1               -- anchor: seed value
    UNION ALL
    SELECT n + 1           -- recursive step
    FROM int_sequence
    WHERE n < 10           -- termination condition
)

SELECT n FROM int_sequence;
-- Result: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

-- ----------------------------------------------------------------------------
-- 6. CTE replacing a subquery for incremental pipeline readability
-- ----------------------------------------------------------------------------
-- Without CTE this would require a deeply nested subquery.
-- Step 1: customer totals. Step 2: rank them. Step 3: keep top 2 per country.
WITH customer_totals AS (
    SELECT
        customer_id,
        country,
        SUM(amount) AS total_amount
    FROM raw_orders
    GROUP BY customer_id, country
),

ranked AS (
    SELECT
        customer_id,
        country,
        total_amount,
        DENSE_RANK() OVER (
            PARTITION BY country ORDER BY total_amount DESC
        ) AS country_rank
    FROM customer_totals
)

SELECT
    customer_id,
    country,
    total_amount,
    country_rank
FROM ranked
WHERE country_rank <= 2
ORDER BY country ASC, country_rank ASC;
-- Result: top 2 spenders per country.
-- More readable than the equivalent deeply nested subquery version.
