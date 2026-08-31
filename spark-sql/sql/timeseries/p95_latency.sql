-- ============================================================
-- Topic: P95 latency analysis
-- Dialect: Databricks / Spark SQL 3.5
-- Description: Computes the 95th percentile of a latency/duration metric,
--              tracks it over time, checks SLA breaches, and flags outlier
--              requests using PERCENTILE_APPROX.
-- ============================================================

-- =============================================================================
-- Sample data: API request latencies per endpoint
-- =============================================================================
CREATE OR REPLACE TEMP VIEW api_requests AS
SELECT * FROM VALUES
    ('checkout', TIMESTAMP '2024-03-01 09:00:01', 120),
    ('checkout', TIMESTAMP '2024-03-01 09:00:05', 145),
    ('checkout', TIMESTAMP '2024-03-01 09:00:12', 110),
    ('checkout', TIMESTAMP '2024-03-01 09:00:20', 980),
    ('checkout', TIMESTAMP '2024-03-01 09:00:31', 130),
    ('checkout', TIMESTAMP '2024-03-01 09:00:40', 150),
    ('checkout', TIMESTAMP '2024-03-01 09:00:55', 125),
    ('checkout', TIMESTAMP '2024-03-01 09:01:10', 118),
    ('checkout', TIMESTAMP '2024-03-01 09:01:22', 640),
    ('checkout', TIMESTAMP '2024-03-01 09:01:40', 135),
    ('search', TIMESTAMP '2024-03-01 09:00:03', 45),
    ('search', TIMESTAMP '2024-03-01 09:00:18', 52),
    ('search', TIMESTAMP '2024-03-01 09:00:29', 48),
    ('search', TIMESTAMP '2024-03-01 09:00:47', 260),
    ('search', TIMESTAMP '2024-03-01 09:01:02', 51),
    ('search', TIMESTAMP '2024-03-01 09:01:15', 49),
    ('search', TIMESTAMP '2024-03-01 09:01:33', 55),
    ('search', TIMESTAMP '2024-03-01 09:01:50', 47)
    AS t(endpoint, request_ts, latency_ms);

-- =============================================================================
-- Section 1: P95 latency per endpoint
-- =============================================================================
SELECT
    endpoint,
    COUNT(*) AS request_count,
    ROUND(AVG(latency_ms), 1) AS avg_latency_ms,
    MAX(latency_ms) AS max_latency_ms,
    ROUND(PERCENTILE_APPROX(latency_ms, 0.50), 1) AS p50_latency_ms,
    ROUND(PERCENTILE_APPROX(latency_ms, 0.95), 1) AS p95_latency_ms,
    ROUND(PERCENTILE_APPROX(latency_ms, 0.99), 1) AS p99_latency_ms
FROM api_requests
GROUP BY endpoint
ORDER BY p95_latency_ms DESC;

-- =============================================================================
-- Section 2: P95 over time (per-minute windows)
-- =============================================================================
SELECT
    endpoint,
    DATE_TRUNC('minute', request_ts) AS minute_bucket,
    COUNT(*) AS request_count,
    ROUND(PERCENTILE_APPROX(latency_ms, 0.95), 1) AS p95_latency_ms
FROM api_requests
GROUP BY endpoint, DATE_TRUNC('minute', request_ts)
ORDER BY endpoint, minute_bucket;

-- =============================================================================
-- Section 3: Rolling P95 — pre-aggregate per day, then window over the buckets.
-- PERCENTILE_APPROX is an aggregate function, not a window function, so it
-- cannot be used directly with OVER; combine it with per-bucket aggregation
-- and a trailing window instead.
-- =============================================================================
WITH daily AS (
    SELECT
        endpoint,
        DATE(request_ts) AS request_date,
        latency_ms
    FROM api_requests
),

daily_p95 AS (
    SELECT
        endpoint,
        request_date,
        PERCENTILE_APPROX(latency_ms, 0.95) AS p95_latency_ms
    FROM daily
    GROUP BY endpoint, request_date
)

SELECT
    endpoint,
    request_date,
    p95_latency_ms,
    ROUND(AVG(p95_latency_ms) OVER (
        PARTITION BY endpoint
        ORDER BY request_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 1) AS rolling_7d_avg_p95
FROM daily_p95
ORDER BY endpoint, request_date;

-- =============================================================================
-- Section 4: SLA breach detection against a P95 target
-- =============================================================================
WITH sla_targets AS (
    SELECT * FROM VALUES
        ('checkout', 300.0),
        ('search', 150.0)
        AS t(endpoint, p95_target_ms)
),

endpoint_p95 AS (
    SELECT
        endpoint,
        ROUND(PERCENTILE_APPROX(latency_ms, 0.95), 1) AS p95_latency_ms
    FROM api_requests
    GROUP BY endpoint
)

SELECT
    e.endpoint,
    e.p95_latency_ms,
    s.p95_target_ms,
    ROUND(e.p95_latency_ms - s.p95_target_ms, 1) AS overage_ms,
    CASE
        WHEN e.p95_latency_ms > s.p95_target_ms THEN 'BREACHED'
        ELSE 'MET'
    END AS sla_status
FROM endpoint_p95 AS e
INNER JOIN sla_targets AS s ON e.endpoint = s.endpoint
ORDER BY overage_ms DESC;

-- =============================================================================
-- Section 5: Flagging individual outlier requests above the group's P95
-- =============================================================================
WITH endpoint_p95 AS (
    SELECT
        endpoint,
        PERCENTILE_APPROX(latency_ms, 0.95) AS p95_latency_ms
    FROM api_requests
    GROUP BY endpoint
)

SELECT
    r.endpoint,
    r.request_ts,
    r.latency_ms,
    p.p95_latency_ms,
    CASE
        WHEN r.latency_ms > p.p95_latency_ms THEN 'ABOVE_P95'
        ELSE 'NORMAL'
    END AS tail_flag
FROM api_requests AS r
INNER JOIN endpoint_p95 AS p ON r.endpoint = p.endpoint
ORDER BY r.endpoint, r.latency_ms DESC;

-- =============================================================================
-- Section 6: Higher accuracy sketch (more memory, tighter approximation)
-- =============================================================================
SELECT PERCENTILE_APPROX(latency_ms, 0.95, 100000) AS p95_latency_ms
FROM api_requests;
