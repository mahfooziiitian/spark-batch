-- Sliding window (moving / rolling) examples in Spark SQL (Databricks dialect).
-- Sliding windows overlap: each row has its own window frame anchored to that row.
-- Implemented with SQL window functions and ROWS / RANGE frame specs.

CREATE OR REPLACE TEMP VIEW daily_sales AS
SELECT *
FROM
    VALUES
    (DATE '2024-01-01', 'US', 120.0),
    (DATE '2024-01-02', 'US', 200.0),
    (DATE '2024-01-03', 'US', 80.0),
    (DATE '2024-01-04', 'US', 300.0),
    (DATE '2024-01-05', 'US', 150.0),
    (DATE '2024-01-06', 'US', 90.0),
    (DATE '2024-01-07', 'US', 210.0),
    (DATE '2024-01-08', 'US', 175.0),
    (DATE '2024-01-09', 'US', 250.0),
    (DATE '2024-01-10', 'US', 130.0),
    (DATE '2024-01-01', 'CA', 60.0),
    (DATE '2024-01-02', 'CA', 90.0),
    (DATE '2024-01-03', 'CA', 110.0),
    (DATE '2024-01-04', 'CA', 140.0),
    (DATE '2024-01-05', 'CA', 70.0),
    (DATE '2024-01-06', 'CA', 130.0),
    (DATE '2024-01-07', 'CA', 160.0)
        AS daily_sales (sale_date, region, amount);

---
-- 1. 3-period (row-based) moving average
-- ROWS BETWEEN 2 PRECEDING AND CURRENT ROW looks back exactly 2 prior rows.
---

SELECT
    sale_date,
    region,
    amount,
    AVG(amount) OVER (
        PARTITION BY region
        ORDER BY sale_date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS ma_3                -- Result: average of current + 2 previous rows
FROM daily_sales
ORDER BY region, sale_date;

---
-- 2. 7-day rolling sum using RANGE frame on a DATE column
-- RANGE BETWEEN INTERVAL 6 DAYS PRECEDING AND CURRENT ROW includes all rows
-- whose sale_date is within the preceding 6 days (i.e. a 7-day window).
---

SELECT
    sale_date,
    region,
    amount,
    SUM(amount) OVER (
        PARTITION BY region
        ORDER BY sale_date
        RANGE BETWEEN INTERVAL 6 DAYS PRECEDING AND CURRENT ROW
    ) AS rolling_7d_sum
FROM daily_sales
ORDER BY region, sale_date;

---
-- 3. Rolling MIN and MAX over 7 days
---

SELECT
    sale_date,
    region,
    amount,
    MIN(amount) OVER (
        PARTITION BY region
        ORDER BY sale_date
        RANGE BETWEEN INTERVAL 6 DAYS PRECEDING AND CURRENT ROW
    ) AS rolling_7d_min,
    MAX(amount) OVER (
        PARTITION BY region
        ORDER BY sale_date
        RANGE BETWEEN INTERVAL 6 DAYS PRECEDING AND CURRENT ROW
    ) AS rolling_7d_max
FROM daily_sales
ORDER BY region, sale_date;

---
-- 4. Rolling average with variable window sizes via ROWS frame
---

SELECT
    sale_date,
    region,
    amount,
    AVG(amount) OVER (
        PARTITION BY region
        ORDER BY sale_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS ma_7,
    AVG(amount) OVER (
        PARTITION BY region
        ORDER BY sale_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS ma_30
FROM daily_sales
ORDER BY region, sale_date;

---
-- 5. Exponential Weighted Moving Average (EWMA) approximation
-- True EWMA requires recursive computation; this 2-row approximation is common
-- in SQL: EMA_t = alpha * x_t + (1-alpha) * EMA_{t-1}.
-- We use a window of 2 rows with weights [alpha, 1-alpha] via a weighted sum.
-- For a proper EWMA over all history, use a recursive CTE.
---

-- Approximate 2-period EWMA (alpha = 0.5)
SELECT
    sale_date,
    region,
    amount,
    ROUND(
        0.5 * amount
        + 0.5 * LAG(amount, 1, amount) OVER (
            PARTITION BY region ORDER BY sale_date
        ),
        2
    ) AS ewma_alpha_0_5
FROM daily_sales
ORDER BY region, sale_date;

-- Recursive EWMA using a CTE (alpha = 0.3, exact)
WITH ordered AS (
    SELECT
        sale_date,
        region,
        amount,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY sale_date) AS rn
    FROM daily_sales
),

ewma_cte (sale_date, region, amount, ewma, rn) AS (
    -- Anchor: first row per region seeds the EWMA
    SELECT
        sale_date,
        region,
        amount,
        amount AS ewma,
        rn
    FROM ordered
    WHERE rn = 1

    UNION ALL

    -- Recursive: EMA_t = 0.3 * x_t + 0.7 * EMA_{t-1}
    SELECT
        o.sale_date,
        o.region,
        o.amount,
        ROUND(0.3 * o.amount + 0.7 * e.ewma, 4),
        o.rn
    FROM ordered AS o
    INNER JOIN ewma_cte AS e
        ON o.region = e.region AND o.rn = e.rn + 1
)

SELECT
    sale_date,
    region,
    amount,
    ewma
FROM ewma_cte
ORDER BY region, sale_date;

---
-- 6. Rolling count of distinct values approximation
-- Exact DISTINCT in a window frame is not supported in Spark SQL.
-- Approximation: APPROX_COUNT_DISTINCT within a bounded ROWS frame.
---

SELECT
    sale_date,
    region,
    amount,
    COUNT(*) OVER (
        PARTITION BY region
        ORDER BY sale_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_count,
    -- Deduplicate-based distinct count via subquery (exact, but heavier)
    COUNT(DISTINCT amount) OVER (
        PARTITION BY region
        ORDER BY sale_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_distinct_count
FROM daily_sales
ORDER BY region, sale_date;
