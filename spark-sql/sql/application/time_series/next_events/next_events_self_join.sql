-- ============================================================
-- Topic: Next-event matching — self-join approach
-- Dialect: Databricks / Spark SQL 3.5
-- Description: For each 'Y' event, find the first subsequent
--              'X' event for the same user and return the time
--              difference. Uses a LEFT self-join with a MIN
--              aggregate to locate the nearest future match.
--
--              event_id provides a stable tiebreaker when two
--              events share the same timestamp.
-- ============================================================

-- Sample data: mixed event stream with noise events (A, B, C)
WITH events AS (
    -- User 1: A → Y → B → C → X → X → Y → X
    SELECT
        1 AS event_id,
        1 AS user_id,
        TIMESTAMP '2024-03-01 10:00:00' AS event_time,
        'A' AS event_type
    UNION ALL
    SELECT
        2 AS event_id,
        1 AS user_id,
        TIMESTAMP '2024-03-01 10:05:00' AS event_time,
        'Y' AS event_type
    UNION ALL
    SELECT
        3 AS event_id,
        1 AS user_id,
        TIMESTAMP '2024-03-01 10:06:00' AS event_time,
        'B' AS event_type
    UNION ALL
    SELECT
        4 AS event_id,
        1 AS user_id,
        TIMESTAMP '2024-03-01 10:08:00' AS event_time,
        'C' AS event_type
    UNION ALL
    SELECT
        5 AS event_id,
        1 AS user_id,
        TIMESTAMP '2024-03-01 10:10:00' AS event_time,
        'X' AS event_type
    UNION ALL
    SELECT
        6 AS event_id,
        1 AS user_id,
        TIMESTAMP '2024-03-01 10:12:00' AS event_time,
        'X' AS event_type
    UNION ALL
    SELECT
        7 AS event_id,
        1 AS user_id,
        TIMESTAMP '2024-03-01 10:20:00' AS event_time,
        'Y' AS event_type
    UNION ALL
    SELECT
        8 AS event_id,
        1 AS user_id,
        TIMESTAMP '2024-03-01 10:45:00' AS event_time,
        'X' AS event_type
    UNION ALL
    -- User 2: two Ys before a single X, then unmatched Y
    SELECT
        9 AS event_id,
        2 AS user_id,
        TIMESTAMP '2024-03-01 11:00:00' AS event_time,
        'Y' AS event_type
    UNION ALL
    SELECT
        10 AS event_id,
        2 AS user_id,
        TIMESTAMP '2024-03-01 11:03:00' AS event_time,
        'Y' AS event_type
    UNION ALL
    SELECT
        11 AS event_id,
        2 AS user_id,
        TIMESTAMP '2024-03-01 11:10:00' AS event_time,
        'X' AS event_type
    UNION ALL
    SELECT
        12 AS event_id,
        2 AS user_id,
        TIMESTAMP '2024-03-01 12:00:00' AS event_time,
        'Y' AS event_type
),

-- Step 1: isolate the trigger events (type = 'Y')
y_events AS (
    SELECT
        user_id,
        event_time AS y_time
    FROM events
    WHERE event_type = 'Y'
),

-- Step 2: for each Y, find the earliest X after it for the same user
next_x AS (
    SELECT
        y.user_id,
        y.y_time,
        MIN(x.event_time) AS x_time
    FROM y_events AS y
    LEFT JOIN events AS x
        ON  y.user_id   = x.user_id
        AND x.event_type = 'X'
        AND y.y_time     < x.event_time
    GROUP BY
        y.user_id,
        y.y_time
)

-- Step 3: compute the elapsed time
SELECT
    user_id,
    y_time,
    x_time,
    TIMESTAMPDIFF(SECOND, y_time, x_time) AS diff_seconds
FROM next_x
ORDER BY
    user_id,
    y_time;

-- Expected output:
-- +---------+---------------------+---------------------+--------------+
-- | user_id | y_time              | x_time              | diff_seconds |
-- +---------+---------------------+---------------------+--------------+
-- |       1 | 2024-03-01 10:05:00 | 2024-03-01 10:10:00 |          300 |
-- |       1 | 2024-03-01 10:20:00 | 2024-03-01 10:45:00 |         1500 |
-- |       2 | 2024-03-01 11:00:00 | 2024-03-01 11:10:00 |          600 |
-- |       2 | 2024-03-01 11:03:00 | 2024-03-01 11:10:00 |          420 |
-- |       2 | 2024-03-01 12:00:00 | NULL                |         NULL |
-- +---------+---------------------+---------------------+--------------+
--
-- The LEFT JOIN retains unmatched Y rows with NULLs.
-- Change to INNER JOIN to drop them.
