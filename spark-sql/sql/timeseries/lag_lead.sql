-- LAG and LEAD pattern examples in Spark SQL (Databricks dialect).
--
-- LAG(col, n) accesses the value n rows BEFORE the current row in the window order.
-- LEAD(col, n) accesses the value n rows AFTER the current row.
-- Both accept a third argument as the default when the referenced row does not exist.
--
-- Common use-cases:
--   Period-over-period comparison  (day-over-day, week-over-week, month-over-month)
--   Consecutive event detection    (back-to-back anomalies, streaks)
--   Time-to-next / time-since-last (funnel latency, churn gap)
--   State transition detection     (status changes, entry/exit signals)

CREATE OR REPLACE TEMP VIEW daily_metrics AS
SELECT *
FROM
    VALUES
    (DATE '2024-01-01', 'US', 120.0, 30),
    (DATE '2024-01-02', 'US', 200.0, 45),
    (DATE '2024-01-03', 'US', 80.0, 18),
    (DATE '2024-01-04', 'US', 300.0, 62),
    (DATE '2024-01-05', 'US', 150.0, 35),
    (DATE '2024-01-06', 'US', 90.0, 22),
    (DATE '2024-01-07', 'US', 210.0, 50),
    (DATE '2024-01-01', 'CA', 60.0, 14),
    (DATE '2024-01-02', 'CA', 90.0, 20),
    (DATE '2024-01-03', 'CA', 110.0, 25),
    (DATE '2024-01-04', 'CA', 140.0, 32),
    (DATE '2024-01-05', 'CA', 70.0, 16),
    (DATE '2024-01-06', 'CA', 130.0, 29),
    (DATE '2024-01-07', 'CA', 160.0, 38)
        AS daily_metrics (sale_date, region, revenue, orders);

---
-- 1. Day-over-day revenue comparison
---

SELECT
    sale_date,
    region,
    revenue,
    LAG(revenue, 1) OVER (PARTITION BY region ORDER BY sale_date) AS prev_day_revenue,
    revenue - LAG(revenue, 1) OVER (PARTITION BY region ORDER BY sale_date) AS dod_delta,
    ROUND(
        (revenue - LAG(revenue, 1) OVER (PARTITION BY region ORDER BY sale_date))
        / NULLIF(LAG(revenue, 1) OVER (PARTITION BY region ORDER BY sale_date), 0) * 100,
        2
    ) AS dod_pct
FROM daily_metrics
ORDER BY region, sale_date;

---
-- 2. Week-over-week comparison (LAG by 7 rows, assuming one row per day)
---

SELECT
    sale_date,
    region,
    revenue,
    LAG(revenue, 7) OVER (PARTITION BY region ORDER BY sale_date) AS wow_revenue,
    ROUND(
        (revenue - LAG(revenue, 7) OVER (PARTITION BY region ORDER BY sale_date))
        / NULLIF(LAG(revenue, 7) OVER (PARTITION BY region ORDER BY sale_date), 0) * 100,
        2
    ) AS wow_pct
FROM daily_metrics
ORDER BY region, sale_date;
-- Result: NULLs for the first 7 rows of each region (no prior week data)

---
-- 3. Look ahead with LEAD — next day's revenue
---

SELECT
    sale_date,
    region,
    revenue,
    LEAD(revenue, 1) OVER (PARTITION BY region ORDER BY sale_date) AS next_day_revenue,
    LEAD(revenue, 1) OVER (PARTITION BY region ORDER BY sale_date) - revenue AS expected_delta
FROM daily_metrics
ORDER BY region, sale_date;
-- Result: last row of each region has NULL next_day_revenue

---
-- 4. LEAD with a default value to avoid NULLs on the last row
---

SELECT
    sale_date,
    region,
    revenue,
    LEAD(revenue, 1, revenue) OVER (PARTITION BY region ORDER BY sale_date) AS next_day_or_self
FROM daily_metrics
ORDER BY region, sale_date;
-- The third argument (revenue) is returned instead of NULL when there is no next row

---
-- 5. Consecutive decline detection — flag days where revenue dropped vs previous day
---

WITH with_lag AS (
    SELECT
        sale_date,
        region,
        revenue,
        LAG(revenue, 1) OVER (PARTITION BY region ORDER BY sale_date) AS prev_revenue
    FROM daily_metrics
)

SELECT
    sale_date,
    region,
    revenue,
    prev_revenue,
    revenue < prev_revenue AS is_decline
FROM with_lag
WHERE prev_revenue IS NOT NULL
ORDER BY region, sale_date;

---
-- 6. Streak counter — count consecutive days of revenue growth
-- Uses a "group and count" technique: when a decline resets the streak,
-- a new group starts. The group ID is (row_number - running_count_of_growth_days).
---

WITH flagged AS (
    SELECT
        sale_date,
        region,
        revenue,
        CASE
            WHEN revenue > LAG(revenue, 1) OVER (PARTITION BY region ORDER BY sale_date)
                THEN 1
            ELSE 0
        END AS is_growth
    FROM daily_metrics
),

numbered AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY sale_date) AS rn,
        SUM(is_growth) OVER (
            PARTITION BY region
            ORDER BY sale_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS growth_count
    FROM flagged
)

SELECT
    sale_date,
    region,
    revenue,
    is_growth,
    -- streak resets to 0 whenever is_growth = 0
    SUM(is_growth) OVER (
        PARTITION BY region, (rn - growth_count)
        ORDER BY sale_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS growth_streak
FROM numbered
ORDER BY region, sale_date;

---
-- 7. Time since last purchase (using event-level data)
---

CREATE OR REPLACE TEMP VIEW purchases AS
SELECT *
FROM
    VALUES
    ('alice', TIMESTAMP '2024-01-05 10:00:00', 'purchase', 29.99),
    ('alice', TIMESTAMP '2024-01-10 14:30:00', 'purchase', 15.00),
    ('alice', TIMESTAMP '2024-01-18 09:00:00', 'purchase', 75.50),
    ('bob', TIMESTAMP '2024-01-03 08:00:00', 'purchase', 10.00),
    ('bob', TIMESTAMP '2024-01-12 16:00:00', 'purchase', 55.00)
        AS purchases (user_id, event_time, event_type, amount);

SELECT
    user_id,
    event_time,
    amount,
    LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time) AS prev_purchase_time,
    ROUND(
        (
            UNIX_TIMESTAMP(event_time)
            - UNIX_TIMESTAMP(LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time))
        )
        / 86400.0,
        1
    ) AS days_since_last_purchase
FROM purchases
ORDER BY user_id, event_time;

---
-- 8. State transition detection — detect when status changes
---

CREATE OR REPLACE TEMP VIEW device_status AS
SELECT *
FROM
    VALUES
    ('dev-1', TIMESTAMP '2024-01-01 00:00:00', 'online'),
    ('dev-1', TIMESTAMP '2024-01-01 01:00:00', 'online'),
    ('dev-1', TIMESTAMP '2024-01-01 02:00:00', 'offline'),
    ('dev-1', TIMESTAMP '2024-01-01 03:00:00', 'offline'),
    ('dev-1', TIMESTAMP '2024-01-01 04:00:00', 'online'),
    ('dev-2', TIMESTAMP '2024-01-01 00:00:00', 'offline'),
    ('dev-2', TIMESTAMP '2024-01-01 01:00:00', 'online')
        AS device_status (device_id, event_time, status);

SELECT
    device_id,
    event_time,
    status,
    LAG(status) OVER (PARTITION BY device_id ORDER BY event_time) AS prev_status,
    COALESCE(status <> LAG(status) OVER (PARTITION BY device_id ORDER BY event_time), FALSE) AS is_transition
FROM device_status
ORDER BY device_id, event_time;
-- Result: rows where is_transition = TRUE mark the exact moment status changed

---
-- 9. Period-over-period revenue index (current / prior × 100)
-- Useful for normalised comparison across regions with different scales.
---

SELECT
    sale_date,
    region,
    revenue,
    ROUND(
        revenue
        / NULLIF(LAG(revenue, 1) OVER (PARTITION BY region ORDER BY sale_date), 0) * 100,
        1
    ) AS revenue_index   -- 100 = flat, > 100 = growth, < 100 = decline
FROM daily_metrics
ORDER BY region, sale_date;

---
-- 10. Lead-lag spread — difference between next and previous value (central difference)
-- Useful as a symmetric momentum signal.
---

SELECT
    sale_date,
    region,
    revenue,
    LEAD(revenue, 1) OVER (PARTITION BY region ORDER BY sale_date) AS next_rev,
    LAG(revenue, 1) OVER (PARTITION BY region ORDER BY sale_date) AS prev_rev,
    ROUND(
        (
            LEAD(revenue, 1) OVER (PARTITION BY region ORDER BY sale_date)
            - LAG(revenue, 1) OVER (PARTITION BY region ORDER BY sale_date)
        ) / 2.0,
        2
    ) AS central_diff
FROM daily_metrics
ORDER BY region, sale_date;
-- NULLs on first and last rows of each partition where lead/lag is unavailable
