-- Time-series gap filling in Spark SQL (Databricks dialect).
--
-- Real-world time series are often sparse: some dates or hours have no events.
-- Gap filling (also called "date spine" or "time spine") involves:
--   1. Generating a complete, contiguous sequence of time buckets.
--   2. LEFT JOINing actual data onto that spine.
--   3. Filling NULLs with a strategy: zero-fill, forward-fill, interpolation.
--
-- These patterns are essential before computing rolling averages or charting,
-- where missing points would silently distort the result.

CREATE OR REPLACE TEMP VIEW sparse_sales AS
SELECT *
FROM
    VALUES
    (DATE '2024-01-01', 'US', 120.0),
    -- 2024-01-02 missing for US
    (DATE '2024-01-03', 'US',  80.0),
    (DATE '2024-01-04', 'US', 300.0),
    -- 2024-01-05 and 2024-01-06 missing for US
    (DATE '2024-01-07', 'US', 210.0),
    (DATE '2024-01-01', 'CA',  60.0),
    (DATE '2024-01-02', 'CA',  90.0),
    -- 2024-01-03 missing for CA
    (DATE '2024-01-04', 'CA', 140.0),
    (DATE '2024-01-05', 'CA',  70.0)
    -- 2024-01-06 and 2024-01-07 missing for CA
        AS sparse_sales (sale_date, region, revenue);

---
-- 1. Generate a complete date spine for the full range
---

CREATE OR REPLACE TEMP VIEW date_spine AS
SELECT EXPLODE(
    SEQUENCE(DATE '2024-01-01', DATE '2024-01-07', INTERVAL 1 DAY)
) AS sale_date;

SELECT * FROM date_spine ORDER BY sale_date;

---
-- 2. Cross-join the spine with all regions to create a complete (date, region) grid
---

CREATE OR REPLACE TEMP VIEW full_grid AS
SELECT
    d.sale_date,
    r.region
FROM date_spine AS d
CROSS JOIN (SELECT DISTINCT region FROM sparse_sales) AS r;

SELECT * FROM full_grid ORDER BY region, sale_date;

---
-- 3. Zero-fill — replace missing revenue with 0
---

SELECT
    g.sale_date,
    g.region,
    COALESCE(s.revenue, 0.0) AS revenue
FROM full_grid AS g
LEFT JOIN sparse_sales AS s
    ON g.sale_date = s.sale_date
    AND g.region = s.region
ORDER BY g.region, g.sale_date;
-- Result: every (date, region) row exists; missing days have revenue = 0

---
-- 4. Forward-fill — carry the last known value forward into gaps
-- Uses LAST_VALUE with IGNORE NULLS over an unbounded-preceding frame.
---

WITH joined AS (
    SELECT
        g.sale_date,
        g.region,
        s.revenue  -- NULL where data is missing
    FROM full_grid AS g
    LEFT JOIN sparse_sales AS s
        ON g.sale_date = s.sale_date
        AND g.region = s.region
)

SELECT
    sale_date,
    region,
    revenue AS raw_revenue,
    LAST_VALUE(revenue IGNORE NULLS) OVER (
        PARTITION BY region
        ORDER BY sale_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS ffill_revenue
FROM joined
ORDER BY region, sale_date;
-- Result: NULL rows filled with the most recent non-NULL value per region

---
-- 5. Backward-fill — fill NULLs with the next known value
---

WITH joined AS (
    SELECT
        g.sale_date,
        g.region,
        s.revenue
    FROM full_grid AS g
    LEFT JOIN sparse_sales AS s
        ON g.sale_date = s.sale_date
        AND g.region = s.region
)

SELECT
    sale_date,
    region,
    revenue AS raw_revenue,
    FIRST_VALUE(revenue IGNORE NULLS) OVER (
        PARTITION BY region
        ORDER BY sale_date
        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    ) AS bfill_revenue
FROM joined
ORDER BY region, sale_date;

---
-- 6. Linear interpolation — fill gaps with values on the straight line between
--    the surrounding known points.
--    Steps:
--      a. Identify the prior and next non-NULL values and their dates.
--      b. Interpolate: prev + (next - prev) * fraction_of_gap_elapsed.
---

WITH joined AS (
    SELECT
        g.sale_date,
        g.region,
        s.revenue,
        -- Distance (in days) from the reference epoch
        DATEDIFF(g.sale_date, DATE '2024-01-01') AS day_offset
    FROM full_grid AS g
    LEFT JOIN sparse_sales AS s
        ON g.sale_date = s.sale_date
        AND g.region = s.region
),

with_anchors AS (
    SELECT
        *,
        -- Last known value and its day offset
        LAST_VALUE(revenue IGNORE NULLS) OVER (
            PARTITION BY region ORDER BY sale_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS prev_value,
        LAST_VALUE(day_offset IGNORE NULLS) OVER (
            PARTITION BY region ORDER BY sale_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS prev_offset,
        -- Next known value and its day offset
        FIRST_VALUE(revenue IGNORE NULLS) OVER (
            PARTITION BY region ORDER BY sale_date
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        ) AS next_value,
        FIRST_VALUE(day_offset IGNORE NULLS) OVER (
            PARTITION BY region ORDER BY sale_date
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        ) AS next_offset
    FROM joined
)

SELECT
    sale_date,
    region,
    revenue AS raw_revenue,
    CASE
        WHEN revenue IS NOT NULL THEN revenue
        WHEN prev_offset = next_offset THEN prev_value
        ELSE ROUND(
            prev_value
            + (next_value - prev_value)
              * (day_offset - prev_offset)
              / NULLIF(next_offset - prev_offset, 0),
            2
        )
    END AS interpolated_revenue
FROM with_anchors
ORDER BY region, sale_date;

---
-- 7. Hourly spine with gap detection and zero-fill
---

CREATE OR REPLACE TEMP VIEW hourly_events AS
SELECT *
FROM
    VALUES
    (TIMESTAMP '2024-06-01 09:00:00', 'US', 10),
    (TIMESTAMP '2024-06-01 11:00:00', 'US', 25),   -- 10:00 missing
    (TIMESTAMP '2024-06-01 12:00:00', 'US',  5),
    (TIMESTAMP '2024-06-01 09:00:00', 'CA',  8),
    (TIMESTAMP '2024-06-01 10:00:00', 'CA', 12)
    -- 11:00 and 12:00 missing for CA
        AS hourly_events (event_hour, region, event_count);

WITH hour_spine AS (
    SELECT EXPLODE(
        SEQUENCE(
            TIMESTAMP '2024-06-01 09:00:00',
            TIMESTAMP '2024-06-01 12:00:00',
            INTERVAL 1 HOUR
        )
    ) AS event_hour
),

grid AS (
    SELECT h.event_hour, r.region
    FROM hour_spine AS h
    CROSS JOIN (SELECT DISTINCT region FROM hourly_events) AS r
)

SELECT
    g.event_hour,
    g.region,
    COALESCE(e.event_count, 0) AS event_count,
    e.event_count IS NULL AS is_gap
FROM grid AS g
LEFT JOIN hourly_events AS e
    ON g.event_hour = e.event_hour
    AND g.region = e.region
ORDER BY g.region, g.event_hour;

---
-- 8. Rolling 7-day average on a gap-filled series (combines gap fill + window)
---

WITH joined AS (
    SELECT
        g.sale_date,
        g.region,
        COALESCE(s.revenue, 0.0) AS revenue
    FROM full_grid AS g
    LEFT JOIN sparse_sales AS s
        ON g.sale_date = s.sale_date
        AND g.region = s.region
)

SELECT
    sale_date,
    region,
    revenue,
    ROUND(
        AVG(revenue) OVER (
            PARTITION BY region
            ORDER BY sale_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ),
        2
    ) AS ma_7d
FROM joined
ORDER BY region, sale_date;
-- Rolling average is now meaningful because every date is present (zero-filled)

---
-- 9. Detect and count gap lengths
-- For each gap (NULL block), report how many consecutive days are missing.
---

WITH joined AS (
    SELECT
        g.sale_date,
        g.region,
        s.revenue
    FROM full_grid AS g
    LEFT JOIN sparse_sales AS s
        ON g.sale_date = s.sale_date
        AND g.region = s.region
),

flagged AS (
    SELECT
        sale_date,
        region,
        revenue,
        revenue IS NULL AS in_gap,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY sale_date) AS rn,
        SUM(CASE WHEN revenue IS NOT NULL THEN 1 ELSE 0 END) OVER (
            PARTITION BY region ORDER BY sale_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS gap_group
    FROM joined
)

SELECT
    region,
    MIN(sale_date) AS gap_start,
    MAX(sale_date) AS gap_end,
    COUNT(*) AS gap_length_days
FROM flagged
WHERE in_gap
GROUP BY region, gap_group
ORDER BY region, gap_start;
-- Result: each row is a contiguous block of missing dates for a region
