-- Hopping window examples in Spark SQL (Databricks dialect).
--
-- A hopping window (also called a sliding window in streaming systems) is defined
-- by two parameters:
--   window_size — the duration of each window (e.g., 1 hour)
--   hop_size    — how often a new window starts (e.g., every 15 minutes)
--
-- Because hop_size < window_size, windows OVERLAP: each event belongs to
-- (window_size / hop_size) windows simultaneously.
-- This differs from:
--   Tumbling windows — non-overlapping (hop_size == window_size)
--   Session windows  — gap-based, variable length
--
-- SQL approach: generate all applicable window start times per event using
-- SEQUENCE + EXPLODE, then aggregate over the resulting (event, window) pairs.

CREATE OR REPLACE TEMP VIEW clickstream AS
SELECT *
FROM
    VALUES
    (1, TIMESTAMP '2024-06-01 00:05:00', 'US', 29.99),
    (2, TIMESTAMP '2024-06-01 00:18:00', 'US', 49.00),
    (3, TIMESTAMP '2024-06-01 00:35:00', 'CA', 15.50),
    (4, TIMESTAMP '2024-06-01 00:50:00', 'US', 99.99),
    (5, TIMESTAMP '2024-06-01 01:10:00', 'CA', 34.00),
    (6, TIMESTAMP '2024-06-01 01:25:00', 'US', 12.00),
    (7, TIMESTAMP '2024-06-01 01:40:00', 'CA', 77.50),
    (8, TIMESTAMP '2024-06-01 02:05:00', 'US', 55.00),
    (9, TIMESTAMP '2024-06-01 02:50:00', 'US', 200.00),
    (10, TIMESTAMP '2024-06-01 03:10:00', 'CA', 88.00)
        AS clickstream (event_id, event_time, region, revenue);

---
-- Helper: convert timestamps to epoch seconds for arithmetic
---

CREATE OR REPLACE TEMP VIEW clickstream_epoch AS
SELECT
    event_id,
    event_time,
    region,
    revenue,
    UNIX_TIMESTAMP(event_time) AS epoch
FROM clickstream;

---
-- 1. Generate hopping windows per event
--
-- window_size = 3600 s (1 hour)
-- hop_size    =  900 s (15 minutes)
-- Each event belongs to ceil(3600 / 900) = 4 windows.
--
-- Window start candidates: floor to the nearest hop, then step back
-- (window_size / hop_size - 1) more hops.
-- We use SEQUENCE(earliest_start, latest_start, hop_size) to enumerate them.
---

CREATE OR REPLACE TEMP VIEW hopping_assignments AS
SELECT
    event_id,
    event_time,
    region,
    revenue,
    -- EXPLODE the sequence of window start epochs for this event
    window_start_epoch,
    FROM_UNIXTIME(window_start_epoch) AS window_start,
    FROM_UNIXTIME(window_start_epoch + 3600) AS window_end
FROM clickstream_epoch
    LATERAL VIEW EXPLODE(
    -- Generate all hop-aligned starts that this event falls inside.
    -- latest hop-aligned start ≤ epoch:  FLOOR(epoch / 900) * 900
    -- earliest such start that still covers epoch: latest_start - (4-1)*900
        SEQUENCE(
            -- earliest window that contains this event
            (FLOOR(epoch / 900) - 3) * 900,
            -- latest window that contains this event
            FLOOR(epoch / 900) * 900,
            900                              -- step = hop_size
        )
    ) AS window_start_epoch
-- Keep only windows where the event actually falls within [start, start + window_size)
WHERE
    epoch >= window_start_epoch
    AND epoch < window_start_epoch + 3600;

---
-- 6. Top-N events per hopping window using RANK
---

WITH windows AS (
    SELECT
        event_id,
        event_time,
        region,
        revenue,
        FROM_UNIXTIME(window_start_epoch) AS window_start,
        FROM_UNIXTIME(window_start_epoch + 3600) AS window_end
    FROM clickstream_epoch
        LATERAL VIEW EXPLODE(
            SEQUENCE(
                (FLOOR(epoch / 900) - 3) * 900,
                FLOOR(epoch / 900) * 900,
                900
            )
        ) AS window_start_epoch
    WHERE
        epoch >= window_start_epoch
        AND epoch < window_start_epoch + 3600
),

ranked AS (
    SELECT
        window_start,
        window_end,
        event_id,
        region,
        revenue,
        RANK() OVER (PARTITION BY window_start ORDER BY revenue DESC) AS revenue_rank
    FROM windows
)

SELECT *
FROM ranked
WHERE revenue_rank <= 3
ORDER BY
    window_start,
    revenue_rank;
-- Result: top 3 highest-revenue events in each 1-hour hopping window

---
-- 7. Anomaly detection — windows where total revenue exceeds mean + 1 stddev
---

WITH windows AS (
    SELECT
        event_id,
        region,
        revenue,
        window_start_epoch,
        FROM_UNIXTIME(window_start_epoch) AS window_start,
        FROM_UNIXTIME(window_start_epoch + 3600) AS window_end
    FROM clickstream_epoch
        LATERAL VIEW EXPLODE(
            SEQUENCE(
                (FLOOR(epoch / 900) - 3) * 900,
                FLOOR(epoch / 900) * 900,
                900
            )
        ) AS window_start_epoch
    WHERE
        epoch >= window_start_epoch
        AND epoch < window_start_epoch + 3600
),

aggregated AS (
    SELECT
        window_start,
        window_end,
        SUM(revenue) AS total_revenue,
        COUNT(*) AS event_count
    FROM windows
    GROUP BY window_start, window_end
),

stats AS (
    SELECT
        AVG(total_revenue) AS mean_revenue,
        STDDEV(total_revenue) AS stddev_revenue
    FROM aggregated
)

SELECT
    a.window_start,
    a.window_end,
    a.total_revenue,
    a.event_count,
    ROUND(s.mean_revenue, 2) AS mean_revenue,
    ROUND(s.mean_revenue + s.stddev_revenue, 2) AS anomaly_threshold,
    a.total_revenue > (s.mean_revenue + s.stddev_revenue) AS is_anomaly
FROM aggregated AS a
CROSS JOIN stats AS s
ORDER BY a.window_start;

---
-- 8. Peak window — single window with the highest total revenue per region
---

WITH windows AS (
    SELECT
        event_id,
        region,
        revenue,
        FROM_UNIXTIME(window_start_epoch) AS window_start,
        FROM_UNIXTIME(window_start_epoch + 3600) AS window_end
    FROM clickstream_epoch
        LATERAL VIEW EXPLODE(
            SEQUENCE(
                (FLOOR(epoch / 900) - 3) * 900,
                FLOOR(epoch / 900) * 900,
                900
            )
        ) AS window_start_epoch
    WHERE
        epoch >= window_start_epoch
        AND epoch < window_start_epoch + 3600
),

aggregated AS (
    SELECT
        window_start,
        window_end,
        region,
        SUM(revenue) AS total_revenue
    FROM windows
    GROUP BY window_start, window_end, region
)

SELECT *
FROM (
    SELECT
        *,
        RANK() OVER (PARTITION BY region ORDER BY total_revenue DESC) AS rnk
    FROM aggregated
)
WHERE rnk = 1
ORDER BY region;
-- Result: the single busiest hopping window per region

---
-- 2. Aggregate over hopping windows
---

SELECT
    window_start,
    window_end,
    region,
    COUNT(*) AS event_count,
    SUM(revenue) AS total_revenue,
    AVG(revenue) AS avg_revenue
FROM hopping_assignments
GROUP BY
    window_start,
    window_end,
    region
ORDER BY
    window_start,
    region;
-- Result: each (window_start, region) row aggregates all events in that 1-hour
--         window, with windows repeating every 15 minutes

---
-- 3. Cross-region aggregate per hopping window (no region partition)
---

SELECT
    window_start,
    window_end,
    COUNT(*) AS total_events,
    SUM(revenue) AS total_revenue
FROM hopping_assignments
GROUP BY window_start, window_end
ORDER BY window_start;

---
-- 4. Parameterised hopping window — 30-minute window, 10-minute hop
-- window_size = 1800 s  |  hop_size = 600 s  |  windows per event = 3
---

SELECT
    event_id,
    event_time,
    region,
    revenue,
    FROM_UNIXTIME(window_start_epoch) AS window_start,
    FROM_UNIXTIME(window_start_epoch + 1800) AS window_end
FROM clickstream_epoch
    LATERAL VIEW EXPLODE(
        SEQUENCE(
            (FLOOR(epoch / 600) - 2) * 600,
            FLOOR(epoch / 600) * 600,
            600
        )
    ) AS window_start_epoch
WHERE
    epoch >= window_start_epoch
    AND epoch < window_start_epoch + 1800
ORDER BY window_start, event_id;

---
-- 5. Full pipeline: build windows → aggregate → filter hot windows
---

WITH windows AS (
    SELECT
        event_id,
        region,
        revenue,
        window_start_epoch,
        FROM_UNIXTIME(window_start_epoch) AS window_start,
        FROM_UNIXTIME(window_start_epoch + 3600) AS window_end
    FROM clickstream_epoch
        LATERAL VIEW EXPLODE(
            SEQUENCE(
                (FLOOR(epoch / 900) - 3) * 900,
                FLOOR(epoch / 900) * 900,
                900
            )
        ) AS window_start_epoch
    WHERE
        epoch >= window_start_epoch
        AND epoch < window_start_epoch + 3600
),

aggregated AS (
    SELECT
        window_start,
        window_end,
        region,
        COUNT(*) AS event_count,
        SUM(revenue) AS total_revenue
    FROM windows
    GROUP BY window_start, window_end, region
)

-- Return only windows where revenue exceeded a threshold
SELECT *
FROM aggregated
WHERE total_revenue > 100
ORDER BY window_start, region;
