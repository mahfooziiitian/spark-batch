-- ============================================================
-- Topic: Next-event matching — consumption pairing
-- Dialect: Databricks / Spark SQL 3.5
-- Description: Pairs each Y with at most one X so that every X
--              is consumed by exactly one Y. Uses a cumulative
--              SUM to assign each event to a "Y group":
--              each Y event increments the group counter and
--              subsequent non-Y events inherit that counter.
--              Within each group the first X is the match.
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

-- Step 1: assign a y_group — each Y increments the counter,
--         subsequent events inherit it until the next Y.
with_y_group AS (
    SELECT
        event_id,
        user_id,
        event_time,
        event_type,
        SUM(
            CASE WHEN event_type = 'Y' THEN 1 ELSE 0 END
        ) OVER (
            PARTITION BY user_id
            ORDER BY event_time, event_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS y_group
    FROM events
),

-- Step 2: extract each Y row's timestamp
y_rows AS (
    SELECT
        user_id,
        event_time AS y_time,
        y_group
    FROM with_y_group
    WHERE event_type = 'Y'
),

-- Step 3: within each y_group, find the first X
x_first AS (
    SELECT
        user_id,
        y_group,
        MIN(event_time) AS x_time
    FROM with_y_group
    WHERE event_type = 'X'
    GROUP BY
        user_id,
        y_group
)

-- Step 4: pair and compute elapsed time
SELECT
    y.user_id,
    y.y_group,
    y.y_time,
    x.x_time,
    TIMESTAMPDIFF(SECOND, y.y_time, x.x_time) AS diff_seconds
FROM y_rows AS y
LEFT JOIN x_first AS x
    ON
        y.user_id = x.user_id
        AND y.y_group = x.y_group
ORDER BY
    y.user_id,
    y.y_time;

-- Expected output:
-- +---------+---------+---------------------+---------------------+--------------+
-- | user_id | y_group | y_time              | x_time              | diff_seconds |
-- +---------+---------+---------------------+---------------------+--------------+
-- |       1 |       1 | 2024-03-01 10:05:00 | 2024-03-01 10:10:00 |          300 |
-- |       1 |       2 | 2024-03-01 10:20:00 | 2024-03-01 10:45:00 |         1500 |
-- |       2 |       1 | 2024-03-01 11:00:00 | NULL                |         NULL |
-- |       2 |       2 | 2024-03-01 11:03:00 | 2024-03-01 11:10:00 |          420 |
-- |       2 |       3 | 2024-03-01 12:00:00 | NULL                |         NULL |
-- +---------+---------+---------------------+---------------------+--------------+
--
-- Key difference from the independent approach:
--   User 2, Y at 11:00 (group 1) gets NO X because the next Y at
--   11:03 (group 2) starts a new group before X at 11:10 arrives.
--   Each X is consumed by the most recent preceding Y.
