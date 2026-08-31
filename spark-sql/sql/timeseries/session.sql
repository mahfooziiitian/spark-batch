-- Session window examples in Spark SQL (Databricks dialect).
--
-- A session window groups events that are close in time into a single session.
-- A new session starts whenever the gap between consecutive events exceeds a
-- configurable timeout (e.g., 30 minutes of inactivity ends the session).
--
-- Unlike tumbling or hopping windows, session windows are gap-based and
-- variable in length: one session can span 2 minutes while another spans 3 hours.
--
-- SQL approach:
--   1. Use LAG to detect whether the gap to the previous event exceeds the timeout.
--   2. Mark each such event as a session boundary (is_new_session = 1).
--   3. Cumulative SUM of the boundary flag assigns a monotonically increasing
--      session ID within each user/partition.

CREATE OR REPLACE TEMP VIEW user_events AS
SELECT *
FROM
    VALUES
    (1, 'alice', TIMESTAMP '2024-06-01 09:00:00', 'page_view',  0.0),
    (2, 'alice', TIMESTAMP '2024-06-01 09:05:00', 'click',      5.99),
    (3, 'alice', TIMESTAMP '2024-06-01 09:12:00', 'purchase',  49.99),
    (4, 'alice', TIMESTAMP '2024-06-01 10:50:00', 'page_view',  0.0),   -- gap > 30 min → new session
    (5, 'alice', TIMESTAMP '2024-06-01 10:55:00', 'click',      2.99),
    (6, 'bob',   TIMESTAMP '2024-06-01 08:00:00', 'page_view',  0.0),
    (7, 'bob',   TIMESTAMP '2024-06-01 08:20:00', 'click',     12.00),
    (8, 'bob',   TIMESTAMP '2024-06-01 09:30:00', 'purchase',  75.00),   -- gap > 30 min → new session
    (9, 'bob',   TIMESTAMP '2024-06-01 09:45:00', 'click',      3.50),
    (10,'bob',   TIMESTAMP '2024-06-01 12:00:00', 'page_view',  0.0)    -- gap > 30 min → new session
        AS user_events (event_id, user_id, event_time, event_type, revenue);

---
-- 1. Detect session boundaries (gap > 30 minutes = 1800 seconds)
---

CREATE OR REPLACE TEMP VIEW events_with_gaps AS
SELECT
    event_id,
    user_id,
    event_time,
    event_type,
    revenue,
    LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time) AS prev_event_time,
    UNIX_TIMESTAMP(event_time)
    - UNIX_TIMESTAMP(
        LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time)
    ) AS gap_seconds
FROM user_events;

SELECT * FROM events_with_gaps ORDER BY user_id, event_time;

---
-- 2. Assign session IDs using cumulative sum of boundary flags
---

CREATE OR REPLACE TEMP VIEW sessions_raw AS
SELECT
    event_id,
    user_id,
    event_time,
    event_type,
    revenue,
    gap_seconds,
    -- is_new_session = 1 when gap exceeds 30 minutes or there is no prior event
    CASE
        WHEN gap_seconds > 1800 OR gap_seconds IS NULL THEN 1
        ELSE 0
    END AS is_new_session
FROM events_with_gaps;

CREATE OR REPLACE TEMP VIEW sessions AS
SELECT
    event_id,
    user_id,
    event_time,
    event_type,
    revenue,
    gap_seconds,
    -- Cumulative sum of boundary flags gives a unique, monotone session index
    SUM(is_new_session) OVER (
        PARTITION BY user_id
        ORDER BY event_time
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS session_id
FROM sessions_raw;

SELECT * FROM sessions ORDER BY user_id, event_time;

---
-- 3. Session-level aggregations
---

SELECT
    user_id,
    session_id,
    MIN(event_time) AS session_start,
    MAX(event_time) AS session_end,
    -- Session duration in minutes
    ROUND(
        (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60.0,
        1
    ) AS duration_minutes,
    COUNT(*) AS event_count,
    SUM(revenue) AS total_revenue,
    COLLECT_LIST(event_type) AS event_sequence
FROM sessions
GROUP BY user_id, session_id
ORDER BY user_id, session_id;

---
-- 4. Sessions that contain a purchase
---

SELECT
    user_id,
    session_id,
    MIN(event_time) AS session_start,
    SUM(revenue) AS session_revenue,
    COUNT(*) AS event_count
FROM sessions
WHERE session_id IN (
    SELECT DISTINCT session_id
    FROM sessions
    WHERE event_type = 'purchase'
)
GROUP BY user_id, session_id
ORDER BY user_id, session_id;

---
-- 5. Average session duration and revenue per user
---

WITH session_summary AS (
    SELECT
        user_id,
        session_id,
        ROUND(
            (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60.0,
            1
        ) AS duration_minutes,
        SUM(revenue) AS session_revenue
    FROM sessions
    GROUP BY user_id, session_id
)

SELECT
    user_id,
    COUNT(*) AS total_sessions,
    ROUND(AVG(duration_minutes), 1) AS avg_session_duration_min,
    ROUND(AVG(session_revenue), 2) AS avg_session_revenue,
    SUM(session_revenue) AS total_revenue
FROM session_summary
GROUP BY user_id
ORDER BY total_revenue DESC;

---
-- 6. Session funnel — first event type per session
---

WITH ranked AS (
    SELECT
        user_id,
        session_id,
        event_type,
        ROW_NUMBER() OVER (PARTITION BY user_id, session_id ORDER BY event_time) AS rn
    FROM sessions
)

SELECT
    user_id,
    session_id,
    event_type AS entry_event
FROM ranked
WHERE rn = 1
ORDER BY user_id, session_id;

---
-- 7. Variable timeout: 15-minute session (600-second gap threshold)
---

WITH boundaries AS (
    SELECT
        event_id,
        user_id,
        event_time,
        event_type,
        revenue,
        CASE
            WHEN UNIX_TIMESTAMP(event_time)
                 - UNIX_TIMESTAMP(
                     LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time)
                 ) > 900
                 OR LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time) IS NULL
            THEN 1
            ELSE 0
        END AS is_new_session
    FROM user_events
)

SELECT
    user_id,
    SUM(is_new_session) OVER (
        PARTITION BY user_id
        ORDER BY event_time
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS session_15min,
    event_id,
    event_time,
    event_type,
    revenue
FROM boundaries
ORDER BY user_id, event_time;
-- Result: same dataset produces more sessions with a 15-min timeout than 30-min

---
-- 8. Structured Streaming equivalent (comment — not executable in batch)
--
-- Spark Structured Streaming supports session windows natively (Spark 3.2+):
--
--   SELECT
--       window.start,
--       window.end,
--       user_id,
--       SUM(revenue) AS session_revenue
--   FROM user_event_stream
--   GROUP BY session_window(event_time, '30 minutes'), user_id
--
-- The SESSION_WINDOW() function is only available on streaming DataFrames.
-- Use the LAG + cumulative SUM approach above for batch processing.
---
