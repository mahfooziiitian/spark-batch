-- Tumbling (fixed, non-overlapping) window examples in Spark SQL (Databricks dialect).
-- Tumbling windows divide time into equal, adjacent, non-overlapping buckets.
-- Every event belongs to exactly one window.
--
-- Note: Spark Structured Streaming has a built-in WINDOW() function for tumbling
-- windows on streaming DataFrames. The examples below are the SQL-batch equivalents
-- using DATE_TRUNC, CAST, and UNIX_TIMESTAMP floor-bucketing — these work on both
-- batch and micro-batch streaming via spark.sql().

CREATE OR REPLACE TEMP VIEW clickstream AS
SELECT *
FROM
    VALUES
    (1, TIMESTAMP '2024-06-01 00:10:00', 'US', 29.99),
    (2, TIMESTAMP '2024-06-01 00:45:00', 'US', 49.00),
    (3, TIMESTAMP '2024-06-01 01:05:00', 'CA', 15.50),
    (4, TIMESTAMP '2024-06-01 01:50:00', 'US', 99.99),
    (5, TIMESTAMP '2024-06-01 02:20:00', 'CA', 34.00),
    (6, TIMESTAMP '2024-06-02 09:00:00', 'US', 12.00),
    (7, TIMESTAMP '2024-06-02 09:30:00', 'CA', 77.50),
    (8, TIMESTAMP '2024-06-02 10:15:00', 'US', 55.00),
    (9, TIMESTAMP '2024-06-03 14:00:00', 'US', 200.00),
    (10, TIMESTAMP '2024-06-03 14:52:00', 'CA', 88.00)
        AS clickstream (event_id, event_time, region, revenue);

---
-- 1. Revenue per 1-hour tumbling window using DATE_TRUNC
---

SELECT
    region,
    DATE_TRUNC('HOUR', event_time) AS window_start,
    SUM(revenue) AS total_revenue,
    COUNT(*) AS event_count
FROM clickstream
GROUP BY
    DATE_TRUNC('HOUR', event_time),
    region
ORDER BY
    window_start,
    region;
-- Result: each row represents one region's revenue in a distinct 1-hour bucket

---
-- 2. Revenue per day (daily tumbling window)
---

SELECT
    region,
    CAST(event_time AS DATE) AS window_day,
    SUM(revenue) AS daily_revenue,
    COUNT(*) AS daily_events
FROM clickstream
GROUP BY
    CAST(event_time AS DATE),
    region
ORDER BY
    window_day,
    region;
-- Result: one row per (day, region) pair

-- Alternatively using DATE_TRUNC('DAY', ...)
SELECT
    DATE_TRUNC('DAY', event_time) AS window_day,
    SUM(revenue) AS total_revenue
FROM clickstream
GROUP BY DATE_TRUNC('DAY', event_time)
ORDER BY window_day;

---
-- 3. Monthly tumbling windows
---

SELECT
    region,
    DATE_TRUNC('MONTH', event_time) AS window_month,
    SUM(revenue) AS monthly_revenue
FROM clickstream
GROUP BY
    DATE_TRUNC('MONTH', event_time),
    region
ORDER BY
    window_month,
    region;

---
-- 4. 15-minute tumbling buckets using UNIX_TIMESTAMP floor arithmetic
-- FLOOR(epoch / bucket_seconds) * bucket_seconds gives the bucket start epoch.
---

SELECT
    region,
    FROM_UNIXTIME(
        FLOOR(UNIX_TIMESTAMP(event_time) / 900) * 900
    ) AS window_15min,
    COUNT(*) AS event_count,
    SUM(revenue) AS bucket_revenue
FROM clickstream
GROUP BY
    FLOOR(UNIX_TIMESTAMP(event_time) / 900) * 900,
    region
ORDER BY
    window_15min,
    region;
-- Result: one row per (15-minute bucket, region)

---
-- 5. Arbitrary N-minute buckets — parameterised pattern
-- Replace 300 with any multiple of 60 to get 5-minute, 10-minute, etc.
---

SELECT
    FROM_UNIXTIME(
        FLOOR(UNIX_TIMESTAMP(event_time) / 300) * 300
    ) AS window_5min,
    COUNT(*) AS events,
    SUM(revenue) AS revenue
FROM clickstream
GROUP BY FLOOR(UNIX_TIMESTAMP(event_time) / 300) * 300
ORDER BY window_5min;

---
-- 6. Window with explicit start / end boundaries
---

SELECT
    DATE_TRUNC('HOUR', event_time) AS window_start,
    DATE_TRUNC('HOUR', event_time) + INTERVAL 1 HOUR AS window_end,
    SUM(revenue) AS total_revenue
FROM clickstream
GROUP BY DATE_TRUNC('HOUR', event_time)
ORDER BY window_start;

---
-- 7. Structured Streaming equivalent (comment — not executable in batch)
--
-- In Spark Structured Streaming the same 1-hour tumbling window is expressed as:
--
--   SELECT
--       window.start,
--       window.end,
--       region,
--       SUM(revenue) AS total_revenue
--   FROM clickstream_stream
--   GROUP BY window(event_time, '1 hour'), region
--
-- The WINDOW() function is only available on streaming DataFrames and is NOT
-- supported in batch spark.sql() calls. Use DATE_TRUNC / FLOOR as shown above.
---
