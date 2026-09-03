-- ============================================================
-- Topic: Next-event matching — window-function approach
-- Dialect: Databricks / Spark SQL 3.5
-- Description: For each 'Y' event, find the first subsequent
--              'X' event for the same user and return the time
--              difference. Uses a forward-fill technique with
--              MIN(CASE …) OVER (ROWS BETWEEN CURRENT ROW AND
--              UNBOUNDED FOLLOWING). Single scan — no self-join.
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

-- Step 1: for every row, find the MIN event_time of all 'X' rows
--         at or after the current row within the same user.
--         Non-X rows contribute NULL to the MIN, which is ignored.
--         CURRENT ROW is safe because a Y row's CASE returns NULL.
with_next_x AS (
    SELECT
        user_id,
        event_time,
        event_type,
        MIN(
            CASE WHEN event_type = 'X' THEN event_time END
        ) OVER (
            PARTITION BY user_id
            ORDER BY event_time, event_id
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        ) AS next_x_time
    FROM events
)

-- Step 2: keep only Y rows, compute elapsed time
SELECT
    user_id,
    event_time AS y_time,
    next_x_time AS x_time,
    TIMESTAMPDIFF(SECOND, event_time, next_x_time) AS diff_seconds
FROM with_next_x
WHERE event_type = 'Y'
ORDER BY
    user_id,
    event_time;

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
-- Y at 10:05 → next X at 10:10 = 5 min (noise events B, C skipped).
-- Unmatched Y rows (no subsequent X) naturally return NULL.
-- Advantage: single-pass — avoids the quadratic explosion of a
-- self-join when many Y and X events share the same user.
