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
-- 7. Running cumulative sum (unbounded window)
---

SELECT
    sale_date,
    region,
    amount,
    SUM(amount) OVER (
        PARTITION BY region
        ORDER BY sale_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_sum
FROM daily_sales
ORDER BY region, sale_date;

---
-- 8. Day-over-day change and percentage change
---

SELECT
    sale_date,
    region,
    amount,
    LAG(amount, 1) OVER (PARTITION BY region ORDER BY sale_date) AS prev_day_amount,
    amount - LAG(amount, 1) OVER (PARTITION BY region ORDER BY sale_date) AS dod_change,
    ROUND(
        (amount - LAG(amount, 1) OVER (PARTITION BY region ORDER BY sale_date))
        / NULLIF(LAG(amount, 1) OVER (PARTITION BY region ORDER BY sale_date), 0) * 100,
        2
    ) AS dod_pct_change
FROM daily_sales
ORDER BY region, sale_date;
-- Result: NULLs on first row of each region (no prior day)

---
-- 9. Bollinger Bands — MA ± 2 × rolling standard deviation
-- Standard finance signal for volatility bands.
---

SELECT
    sale_date,
    region,
    amount,
    ROUND(AVG(amount) OVER w, 2) AS ma_20,
    ROUND(STDDEV(amount) OVER w, 2) AS std_20,
    ROUND(AVG(amount) OVER w + 2 * STDDEV(amount) OVER w, 2) AS upper_band,
    ROUND(AVG(amount) OVER w - 2 * STDDEV(amount) OVER w, 2) AS lower_band
FROM daily_sales
WINDOW
    w AS (
        PARTITION BY region
        ORDER BY sale_date
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    )
ORDER BY region, sale_date;

---
-- 10. Rolling median approximation using PERCENTILE_APPROX
-- Exact median requires sorting within a frame; PERCENTILE_APPROX is efficient.
---

SELECT
    sale_date,
    region,
    amount,
    ROUND(
        AVG(amount) OVER (
            PARTITION BY region
            ORDER BY sale_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ),
        2
    ) AS ma_7,
    PERCENTILE_APPROX(amount, 0.5) OVER (
        PARTITION BY region
        ORDER BY sale_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS median_7d
FROM daily_sales
ORDER BY region, sale_date;

---
-- 11. Z-score normalisation over a rolling 7-day window
-- z = (x - mean) / stddev — flags values far from the rolling mean.
---

SELECT
    sale_date,
    region,
    amount,
    ROUND(
        (amount - AVG(amount) OVER w) / NULLIF(STDDEV(amount) OVER w, 0),
        4
    ) AS z_score_7d
FROM daily_sales
WINDOW
    w AS (
        PARTITION BY region
        ORDER BY sale_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )
ORDER BY region, sale_date;
-- z_score > 2 or < -2 typically flags an outlier

---
-- 12. Rate of change (momentum) — slope approximation over N periods
-- (current - N-periods-ago) / N  gives the average change per period.
---

SELECT
    sale_date,
    region,
    amount,
    ROUND(
        (amount - LAG(amount, 3) OVER (PARTITION BY region ORDER BY sale_date)) / 3.0,
        2
    ) AS momentum_3d,
    ROUND(
        (amount - LAG(amount, 7) OVER (PARTITION BY region ORDER BY sale_date)) / 7.0,
        2
    ) AS momentum_7d
FROM daily_sales
ORDER BY region, sale_date;

---
-- 13. ROWS vs RANGE comparison on the same dataset
-- ROWS counts physical rows; RANGE includes all rows with the same ORDER BY value.
-- On a DATE column with no duplicates they are identical — add a duplicate to see
-- the difference.
---

CREATE OR REPLACE TEMP VIEW sales_with_dup AS
SELECT *
FROM
    VALUES
    (DATE '2024-01-05', 'US', 100.0),
    (DATE '2024-01-05', 'US', 200.0),   -- duplicate date
    (DATE '2024-01-06', 'US', 150.0)
        AS t (sale_date, region, amount);

SELECT
    sale_date,
    amount,
    SUM(amount) OVER (
        ORDER BY sale_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS rows_sum,
    SUM(amount) OVER (
        ORDER BY sale_date
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS range_sum    -- includes BOTH rows for 2024-01-05 even on the first one
FROM sales_with_dup
ORDER BY sale_date, amount;
