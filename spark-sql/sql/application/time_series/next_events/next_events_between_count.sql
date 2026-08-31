-- ============================================================
-- Topic: Next-event matching — with events-between count
-- Dialect: Databricks / Spark SQL 3.5
-- Description: For each 'Y' event, find the first subsequent
--              'X' event and also count how many events of any
--              type occurred strictly between the two.
--              Combines a self-join pairing step with a second
--              join to count intermediate rows.
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

-- Step 1: pair each Y with its next X (same as self-join approach)
paired AS (
    SELECT
        y.user_id,
        y.event_time AS y_time,
        MIN(x.event_time) AS x_time
    FROM events AS y
    LEFT JOIN events AS x
        ON  y.user_id   = x.user_id
        AND x.event_type = 'X'
        AND y.event_time < x.event_time
    WHERE y.event_type = 'Y'
    GROUP BY
        y.user_id,
        y.event_time
)

-- Step 2: count events strictly between y_time and x_time
SELECT
    p.user_id,
    p.y_time,
    p.x_time,
    TIMESTAMPDIFF(SECOND, p.y_time, p.x_time) AS diff_seconds,
    COUNT(e.event_id)                          AS events_between
FROM paired AS p
LEFT JOIN events AS e
    ON  p.user_id    = e.user_id
    AND p.y_time     < e.event_time
    AND p.x_time     > e.event_time
GROUP BY
    p.user_id,
    p.y_time,
    p.x_time
ORDER BY
    p.user_id,
    p.y_time;

-- Expected output:
-- +---------+---------------------+---------------------+--------------+----------------+
-- | user_id | y_time              | x_time              | diff_seconds | events_between |
-- +---------+---------------------+---------------------+--------------+----------------+
-- |       1 | 2024-03-01 10:05:00 | 2024-03-01 10:10:00 |          300 |              2 |
-- |       1 | 2024-03-01 10:20:00 | 2024-03-01 10:45:00 |         1500 |              0 |
-- |       2 | 2024-03-01 11:00:00 | 2024-03-01 11:10:00 |          600 |              1 |
-- |       2 | 2024-03-01 11:03:00 | 2024-03-01 11:10:00 |          420 |              0 |
-- |       2 | 2024-03-01 12:00:00 | NULL                |         NULL |              0 |
-- +---------+---------------------+---------------------+--------------+----------------+
--
-- Y at 10:05 → X at 10:10: 2 events between (B at 10:06, C at 10:08).
-- Y at 11:00 → X at 11:10: 1 event between (Y at 11:03).
-- Unmatched Y rows show events_between = 0 because the LEFT JOIN
-- on events finds no rows when x_time IS NULL.
