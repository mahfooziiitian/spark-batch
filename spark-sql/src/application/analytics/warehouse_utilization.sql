-- ============================================================
-- Topic: SQL Warehouse Utilization Analysis
-- Dialect: Databricks / Spark SQL 3.5
-- Description: Produces one row per warehouse with cost, performance,
--   capacity, error-rate, and auto-stop metrics. Includes weighted
--   utilization scoring and actionable sizing recommendations.
-- [Databricks] Requires system tables (billing, compute, query.history)
-- ============================================================

-- ── Stage 1: Parameters & Filtering ─────────────────────────────────────────────

WITH eligible_workspaces AS (
    SELECT
        workspace_id,
        workspace_name
    FROM system.access.workspaces_latest
    WHERE
        :param_workspace = 'All'
        OR :param_workspace IS NULL
        OR workspace_name = :param_workspace
),

params AS (
    SELECT
        TO_DATE(:param_date_range_min) AS window_start,
        TO_DATE(:param_date_range_max) AS window_end,
        DATEDIFF(
            TO_DATE(:param_date_range_max),
            TO_DATE(:param_date_range_min)
        ) + 1                          AS window_days
),

-- ── Stage 2: Warehouse Configuration ────────────────────────────────────────────

wh_latest AS (
    SELECT
        h.workspace_id,
        h.warehouse_id,
        h.warehouse_name,
        h.warehouse_type,
        h.warehouse_size,
        h.created_by,
        h.auto_stop_minutes,
        h.max_clusters,
        h.min_clusters,
        h.tags['Env']    AS env,
        h.tags['Tenant'] AS tenant,
        h.change_time
    FROM system.compute.warehouses AS h
    QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY workspace_id, warehouse_id
            ORDER BY change_time DESC
        ) = 1
),

-- ── Stage 3: Cost Pipeline ──────────────────────────────────────────────────────

usage_in_window AS (
    SELECT
        u.*,
        p.window_start,
        p.window_end,
        p.window_days
    FROM system.billing.usage AS u
    CROSS JOIN params AS p
    WHERE u.usage_date BETWEEN p.window_start AND p.window_end
),

usage_enriched AS (
    SELECT
        u.workspace_id,
        w.workspace_name,
        u.window_start,
        u.window_end,
        u.window_days,
        u.usage_metadata.warehouse_id                              AS warehouse_id,
        TRIM(COALESCE(wh.tenant, 'Not-Tagged'))                    AS tenant,
        TRIM(COALESCE(wh.warehouse_name, 'Not-Mapped'))            AS warehouse_name,
        CASE
            WHEN TRIM(LOWER(COALESCE(wh.env, 'Not-Tagged'))) = 'stage' THEN 'stg'
            WHEN TRIM(LOWER(COALESCE(wh.env, 'Not-Tagged'))) = 'prod'  THEN 'prd'
            ELSE TRIM(COALESCE(wh.env, 'Not-Tagged'))
        END                                                        AS env,
        TRIM(COALESCE(
            wh.warehouse_type,
            CASE
                WHEN u.product_features.is_serverless = TRUE       THEN 'SERVERLESS'
                WHEN UPPER(u.product_features.sql_tier) = 'PRO'    THEN 'PRO'
                WHEN UPPER(u.product_features.sql_tier) = 'CLASSIC' THEN 'CLASSIC'
                ELSE 'UNKNOWN'
            END
        ))                                                         AS warehouse_type,
        COALESCE(wh.warehouse_size, 'Not-Mapped')                  AS warehouse_size,
        COALESCE(u.usage_quantity, 0)                               AS dbus,
        COALESCE(lp.pricing.effective_list.default * u.usage_quantity, 0) AS cost_usd
    FROM usage_in_window AS u
    INNER JOIN system.billing.list_prices AS lp
        ON  u.cloud     = lp.cloud
        AND u.sku_name  = lp.sku_name
        AND u.usage_start_time >= lp.price_start_time
        AND (u.usage_end_time <= lp.price_end_time OR lp.price_end_time IS NULL)
    INNER JOIN eligible_workspaces AS w
        ON u.workspace_id = w.workspace_id
    LEFT JOIN wh_latest AS wh
        ON  wh.workspace_id  = u.workspace_id
        AND wh.warehouse_id  = u.usage_metadata.warehouse_id
    WHERE
        u.usage_unit = 'DBU'
        AND u.usage_metadata.warehouse_id IS NOT NULL
),

usage_enriched_filtered AS (
    SELECT
        u.workspace_id,
        u.workspace_name,
        u.warehouse_id,
        u.tenant,
        u.warehouse_name,
        u.env,
        u.warehouse_type,
        u.warehouse_size,
        u.dbus,
        u.cost_usd,
        u.window_start,
        u.window_end,
        u.window_days
    FROM usage_enriched AS u
    WHERE
        (:tenant = 'All' OR :tenant IS NULL OR :tenant = u.tenant)
        AND (:env = 'All' OR :env IS NULL OR :env = u.env)
        AND (:warehouse = 'All' OR :warehouse IS NULL OR :warehouse = u.warehouse_name)
),

usage_agg AS (
    SELECT
        workspace_id,
        MAX(workspace_name)  AS workspace_name,
        warehouse_id,
        MIN(warehouse_name)  AS warehouse_name,
        MIN(warehouse_type)  AS warehouse_type,
        MIN(warehouse_size)  AS warehouse_size,
        MIN(tenant)          AS tenant,
        MIN(env)             AS env,
        SUM(dbus)            AS total_dbus,
        SUM(cost_usd)        AS total_cost_usd,
        MIN(window_days)     AS window_days
    FROM usage_enriched_filtered
    GROUP BY workspace_id, warehouse_id
),

-- ── Stage 4: Uptime & Scaling Pipeline ──────────────────────────────────────────

all_warehouse_events AS (
    SELECT
        e.workspace_id,
        e.warehouse_id,
        e.event_type,
        e.event_time,
        e.cluster_count,
        p.window_start,
        p.window_end
    FROM system.compute.warehouse_events AS e
    CROSS JOIN params AS p
    WHERE
        e.event_time >= p.window_start - INTERVAL 1 DAY
        AND e.event_time <= p.window_end
),

running_candidates AS (
    SELECT
        workspace_id,
        warehouse_id,
        event_time AS running_start_time,
        running_end_time
    FROM (
        SELECT
            workspace_id,
            warehouse_id,
            event_type,
            event_time,
            MIN(CASE WHEN event_type IN ('STOPPING', 'STOPPED') THEN event_time END) OVER (
                PARTITION BY workspace_id, warehouse_id
                ORDER BY event_time
                ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
            ) AS running_end_time
        FROM all_warehouse_events
    )
    WHERE event_type = 'STARTING'
),

running_windows_deduped AS (
    SELECT
        workspace_id,
        warehouse_id,
        running_start_time,
        running_end_time
    FROM running_candidates
    QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY
                workspace_id,
                warehouse_id,
                COALESCE(running_end_time, TIMESTAMP('9999-12-31 00:00:00'))
            ORDER BY running_start_time ASC
        ) = 1
),

running_windows AS (
    SELECT
        r.workspace_id,
        r.warehouse_id,
        GREATEST(r.running_start_time, p.window_start)                  AS windowed_running_start,
        LEAST(COALESCE(r.running_end_time, p.window_end), p.window_end) AS windowed_running_end
    FROM running_windows_deduped AS r
    CROSS JOIN params AS p
    WHERE COALESCE(r.running_end_time, p.window_end) > p.window_start
),

uptime_by_warehouse AS (
    SELECT
        workspace_id,
        warehouse_id,
        SUM(TIMESTAMPDIFF(SECOND, windowed_running_start, windowed_running_end)) AS running_seconds
    FROM running_windows
    WHERE windowed_running_end > windowed_running_start
    GROUP BY workspace_id, warehouse_id
),

-- Restart count (how often the warehouse cold-started)
restart_counts AS (
    SELECT
        workspace_id,
        warehouse_id,
        COUNT_IF(event_type = 'STARTING')  AS restart_count
    FROM all_warehouse_events
    WHERE event_time >= window_start AND event_time < window_end
    GROUP BY workspace_id, warehouse_id
),

-- ── Stage 5: Consolidated Query History (single scan) ───────────────────────────

query_base AS (
    SELECT
        q.workspace_id,
        q.compute.warehouse_id                                         AS warehouse_id,
        q.statement_id,
        q.start_time,
        q.end_time,
        q.execution_status,
        q.total_duration_ms,
        q.execution_duration_ms,
        q.waiting_at_capacity_duration_ms,
        q.waiting_for_compute_duration_ms,
        q.spilled_local_bytes,
        q.read_bytes,
        TIMESTAMPADD(MILLISECOND, q.execution_duration_ms, q.start_time) AS query_end_time,
        HOUR(q.start_time)                                             AS start_hour
    FROM system.query.history AS q
    CROSS JOIN params AS p
    WHERE
        q.start_time  >= p.window_start - INTERVAL 1 DAY
        AND q.start_time < p.window_end
        AND q.end_time   IS NOT NULL
        AND q.compute.warehouse_id IS NOT NULL
),

-- Overlap intervals for busy-time calculation
query_running_overlap AS (
    SELECT
        rw.workspace_id,
        rw.warehouse_id,
        GREATEST(q.start_time, rw.windowed_running_start)              AS overlap_start,
        LEAST(q.query_end_time, rw.windowed_running_end)               AS overlap_end
    FROM running_windows AS rw
    INNER JOIN query_base AS q
        ON  rw.workspace_id  = q.workspace_id
        AND rw.warehouse_id  = q.warehouse_id
        AND rw.windowed_running_end   > q.start_time
        AND rw.windowed_running_start < q.query_end_time
    CROSS JOIN params AS p
    WHERE q.start_time >= p.window_start
),

normalized_overlap AS (
    SELECT workspace_id, warehouse_id, overlap_start, overlap_end
    FROM query_running_overlap
    WHERE overlap_end > overlap_start
),

overlap_with_running_max AS (
    SELECT
        workspace_id,
        warehouse_id,
        overlap_start,
        overlap_end,
        MAX(overlap_end) OVER (
            PARTITION BY workspace_id, warehouse_id
            ORDER BY overlap_start, overlap_end
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_max_end
    FROM normalized_overlap
),

overlap_with_prev_max AS (
    SELECT
        workspace_id,
        warehouse_id,
        overlap_start,
        overlap_end,
        LAG(running_max_end) OVER (
            PARTITION BY workspace_id, warehouse_id
            ORDER BY overlap_start, overlap_end
        ) AS prev_running_max_end
    FROM overlap_with_running_max
),

overlap_grouped AS (
    SELECT
        workspace_id,
        warehouse_id,
        overlap_start,
        overlap_end,
        SUM(
            CASE
                WHEN overlap_start > COALESCE(prev_running_max_end, overlap_start) THEN 1
                ELSE 0
            END
        ) OVER (
            PARTITION BY workspace_id, warehouse_id
            ORDER BY overlap_start, overlap_end
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS overlap_group_id
    FROM overlap_with_prev_max
),

merged_busy_intervals AS (
    SELECT
        workspace_id,
        warehouse_id,
        overlap_group_id,
        MIN(overlap_start) AS busy_start_time,
        MAX(overlap_end)   AS busy_end_time
    FROM overlap_grouped
    GROUP BY workspace_id, warehouse_id, overlap_group_id
),

busy_by_warehouse AS (
    SELECT
        workspace_id,
        warehouse_id,
        SUM(TIMESTAMPDIFF(SECOND, busy_start_time, busy_end_time)) AS busy_seconds
    FROM merged_busy_intervals
    WHERE busy_end_time > busy_start_time
    GROUP BY workspace_id, warehouse_id
),

-- ── Stage 6: Query Metrics (from the single query_base scan) ────────────────────

query_metrics AS (
    SELECT
        workspace_id,
        warehouse_id,
        COUNT(DISTINCT statement_id)                                    AS query_count,
        COUNT_IF(execution_status = 'FINISHED')                        AS queries_succeeded,
        COUNT_IF(execution_status IN ('FAILED', 'CANCELED'))           AS queries_failed,
        ROUND(
            100.0 * COUNT_IF(execution_status IN ('FAILED', 'CANCELED'))
            / NULLIF(COUNT(DISTINCT statement_id), 0), 2
        )                                                              AS error_rate_pct,
        ROUND(AVG(total_duration_ms) / 1000.0, 2)                      AS avg_duration_seconds,
        ROUND(
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY total_duration_ms)
            / 1000.0, 2
        )                                                              AS p50_duration_seconds,
        ROUND(
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_duration_ms)
            / 1000.0, 2
        )                                                              AS p75_duration_seconds,
        ROUND(
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_duration_ms)
            / 1000.0, 2
        )                                                              AS p95_duration_seconds,
        ROUND(
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY total_duration_ms)
            / 1000.0, 2
        )                                                              AS p99_duration_seconds,
        ROUND(SUM(waiting_at_capacity_duration_ms) / 1000.0, 2)        AS total_queued_seconds,
        ROUND(AVG(waiting_at_capacity_duration_ms) / 1000.0, 2)        AS avg_queued_seconds,
        ROUND(
            100.0 * COUNT_IF(waiting_at_capacity_duration_ms > 0)
            / NULLIF(COUNT(statement_id), 0), 2
        )                                                              AS pct_queries_queued,
        ROUND(SUM(waiting_for_compute_duration_ms) / 1000.0, 2)        AS total_provisioning_wait_seconds,
        ROUND(AVG(waiting_for_compute_duration_ms) / 1000.0, 2)        AS avg_provisioning_wait_seconds,
        ROUND(
            100.0 * COUNT_IF(waiting_for_compute_duration_ms > 5000)
            / NULLIF(COUNT(statement_id), 0), 2
        )                                                              AS pct_queries_cold_start,
        ROUND(SUM(spilled_local_bytes) / (1024.0 * 1024 * 1024), 2)    AS total_spill_gb,
        ROUND(
            SUM(spilled_local_bytes) * 1.0
            / NULLIF(SUM(read_bytes), 0), 4
        )                                                              AS spill_to_read_ratio,
        ROUND(
            100.0 * COUNT_IF(spilled_local_bytes > 0)
            / NULLIF(COUNT(statement_id), 0), 2
        )                                                              AS pct_queries_spilling
    FROM query_base
    CROSS JOIN params AS p
    WHERE start_time >= p.window_start
    GROUP BY workspace_id, warehouse_id
),

-- Peak hour analysis (busiest hour of day by query count)
peak_hour_analysis AS (
    SELECT
        workspace_id,
        warehouse_id,
        start_hour                                                     AS peak_hour,
        query_count_in_hour
    FROM (
        SELECT
            workspace_id,
            warehouse_id,
            start_hour,
            COUNT(DISTINCT statement_id)                                AS query_count_in_hour,
            ROW_NUMBER() OVER (
                PARTITION BY workspace_id, warehouse_id
                ORDER BY COUNT(DISTINCT statement_id) DESC
            ) AS rn
        FROM query_base
        CROSS JOIN params AS p
        WHERE start_time >= p.window_start
        GROUP BY workspace_id, warehouse_id, start_hour
    )
    WHERE rn = 1
),

-- ── Stage 7: Scaling & Concurrency ──────────────────────────────────────────────

scaling_counts AS (
    SELECT
        workspace_id,
        warehouse_id,
        COUNT_IF(event_type = 'SCALED_UP')   AS scale_up_count,
        COUNT_IF(event_type = 'SCALED_DOWN') AS scale_down_count,
        MAX(cluster_count)                   AS peak_cluster_count
    FROM all_warehouse_events
    WHERE event_time >= window_start AND event_time < window_end
    GROUP BY workspace_id, warehouse_id
),

scaling_intervals AS (
    SELECT
        se.workspace_id,
        se.warehouse_id,
        se.event_time AS interval_start,
        LEAD(se.event_time) OVER (
            PARTITION BY se.workspace_id, se.warehouse_id
            ORDER BY se.event_time
        )             AS interval_end,
        se.cluster_count
    FROM all_warehouse_events AS se
    WHERE se.event_time >= se.window_start AND se.event_time < se.window_end
),

time_at_max AS (
    SELECT
        si.workspace_id,
        si.warehouse_id,
        ROUND(
            100.0 * SUM(
                CASE
                    WHEN si.cluster_count >= w.max_clusters
                    THEN TIMESTAMPDIFF(SECOND, si.interval_start, si.interval_end)
                    ELSE 0
                END
            ) / NULLIF(
                SUM(TIMESTAMPDIFF(SECOND, si.interval_start, si.interval_end)), 0
            ), 2
        ) AS pct_time_at_max_clusters
    FROM scaling_intervals AS si
    INNER JOIN wh_latest AS w
        ON  si.workspace_id = w.workspace_id
        AND si.warehouse_id = w.warehouse_id
    WHERE si.interval_end IS NOT NULL
    GROUP BY si.workspace_id, si.warehouse_id
),

-- Concurrency from start/end deltas (finished queries only)
concurrency_events AS (
    SELECT
        workspace_id,
        warehouse_id,
        event_time,
        SUM(delta) OVER (
            PARTITION BY workspace_id, warehouse_id
            ORDER BY event_time, delta DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS concurrent_queries
    FROM (
        SELECT workspace_id, warehouse_id, start_time AS event_time, 1 AS delta
        FROM query_base
        CROSS JOIN params AS p
        WHERE start_time >= p.window_start AND execution_status = 'FINISHED'
        UNION ALL
        SELECT workspace_id, warehouse_id, end_time AS event_time, -1 AS delta
        FROM query_base
        CROSS JOIN params AS p
        WHERE start_time >= p.window_start AND execution_status = 'FINISHED'
    )
),

peak_concurrency AS (
    SELECT
        workspace_id,
        warehouse_id,
        MAX(concurrent_queries)            AS peak_concurrent_queries,
        ROUND(AVG(concurrent_queries), 2)  AS avg_concurrent_queries
    FROM concurrency_events
    GROUP BY workspace_id, warehouse_id
),

-- ── Stage 8: Final Assembly ─────────────────────────────────────────────────────

combined AS (
    SELECT
        ug.workspace_id,
        ug.warehouse_id,
        ug.workspace_name,
        ug.tenant,
        ug.env,
        COALESCE(w.created_by, 'Unknown')                              AS created_by,
        COALESCE(w.warehouse_name, ug.warehouse_name)                  AS warehouse_name,
        COALESCE(w.warehouse_type, ug.warehouse_type)                  AS warehouse_type,
        COALESCE(w.warehouse_size, ug.warehouse_size)                  AS warehouse_size,
        COALESCE(w.min_clusters, 1)                                    AS min_clusters,
        COALESCE(w.max_clusters, 1)                                    AS max_clusters,
        COALESCE(w.auto_stop_minutes, 0)                               AS auto_stop_minutes,
        ug.window_days,
        ug.total_dbus,
        ug.total_cost_usd,

        -- Uptime metrics
        COALESCE(u.running_seconds, 0)                                 AS total_running_seconds,
        COALESCE(b.busy_seconds, 0)                                    AS busy_seconds,
        COALESCE(u.running_seconds, 0) - COALESCE(b.busy_seconds, 0)  AS idle_seconds,
        ROUND(
            100.0 * (COALESCE(u.running_seconds, 0) - COALESCE(b.busy_seconds, 0))
            / NULLIF(u.running_seconds, 0), 2
        )                                                              AS idle_pct,
        ROUND(
            100.0 * COALESCE(b.busy_seconds, 0)
            / NULLIF(u.running_seconds, 0), 2
        )                                                              AS busy_pct,

        -- Restart & auto-stop efficiency
        COALESCE(rc.restart_count, 0)                                  AS restart_count,
        ROUND(
            COALESCE(rc.restart_count, 0) * 1.0
            / NULLIF(ug.window_days, 0), 2
        )                                                              AS restarts_per_day,

        -- Query performance
        COALESCE(qm.query_count, 0)                                    AS query_count,
        COALESCE(qm.queries_succeeded, 0)                              AS queries_succeeded,
        COALESCE(qm.queries_failed, 0)                                 AS queries_failed,
        COALESCE(qm.error_rate_pct, 0)                                 AS error_rate_pct,
        COALESCE(qm.avg_duration_seconds, 0)                           AS avg_duration_seconds,
        COALESCE(qm.p50_duration_seconds, 0)                           AS p50_duration_seconds,
        COALESCE(qm.p75_duration_seconds, 0)                           AS p75_duration_seconds,
        COALESCE(qm.p95_duration_seconds, 0)                           AS p95_duration_seconds,
        COALESCE(qm.p99_duration_seconds, 0)                           AS p99_duration_seconds,
        ROUND(
            COALESCE(qm.query_count, 0) * 3600.0
            / NULLIF(COALESCE(b.busy_seconds, 0), 0), 2
        )                                                              AS queries_per_busy_hour,

        -- Queuing & provisioning
        COALESCE(qm.total_queued_seconds, 0)                           AS total_queued_seconds,
        COALESCE(qm.avg_queued_seconds, 0)                             AS avg_queued_seconds,
        COALESCE(qm.pct_queries_queued, 0)                             AS pct_queries_queued,
        COALESCE(qm.total_provisioning_wait_seconds, 0)                AS total_provisioning_wait_seconds,
        COALESCE(qm.avg_provisioning_wait_seconds, 0)                  AS avg_provisioning_wait_seconds,
        COALESCE(qm.pct_queries_cold_start, 0)                         AS pct_queries_cold_start,

        -- Spill
        COALESCE(qm.total_spill_gb, 0)                                 AS total_spill_gb,
        COALESCE(qm.spill_to_read_ratio, 0)                            AS spill_to_read_ratio,
        COALESCE(qm.pct_queries_spilling, 0)                           AS pct_queries_spilling,

        -- Scaling & concurrency
        COALESCE(sc.scale_up_count, 0)                                 AS scale_up_count,
        COALESCE(sc.scale_down_count, 0)                               AS scale_down_count,
        COALESCE(sc.peak_cluster_count, COALESCE(w.min_clusters, 1))   AS peak_cluster_count,
        COALESCE(tm.pct_time_at_max_clusters, 0)                       AS pct_time_at_max_clusters,
        COALESCE(pc.peak_concurrent_queries, 0)                        AS peak_concurrent_queries,
        COALESCE(pc.avg_concurrent_queries, 0)                         AS avg_concurrent_queries,

        -- Peak hour
        COALESCE(ph.peak_hour, -1)                                     AS peak_hour,
        COALESCE(ph.query_count_in_hour, 0)                            AS peak_hour_query_count,

        -- ── Signal flags (boolean indicators) ───────────────────────────────────
        -- Overutilization signals
        (COALESCE(qm.pct_queries_queued, 0) > 10)                     AS sig_high_queue_pct,
        (COALESCE(qm.avg_queued_seconds, 0) > 5)                      AS sig_high_avg_queue,
        (COALESCE(qm.spill_to_read_ratio, 0) > 0.1)                   AS sig_high_spill,
        (COALESCE(tm.pct_time_at_max_clusters, 0) > 30)               AS sig_at_max_clusters,
        (COALESCE(qm.p95_duration_seconds, 0) > 120)                  AS sig_high_p95,
        (COALESCE(qm.error_rate_pct, 0) > 5)                          AS sig_high_error_rate,
        (COALESCE(qm.pct_queries_cold_start, 0) > 20)                 AS sig_frequent_cold_starts,

        -- Underutilization signals
        (ROUND(
            100.0 * (COALESCE(u.running_seconds, 0) - COALESCE(b.busy_seconds, 0))
            / NULLIF(u.running_seconds, 0), 2
        ) > 70)                                                        AS sig_high_idle,
        (COALESCE(pc.peak_concurrent_queries, 0) <= 1
         AND COALESCE(w.max_clusters, 1) > 1)                         AS sig_low_concurrency,
        (COALESCE(sc.scale_up_count, 0) = 0
         AND COALESCE(w.max_clusters, 1) > 1)                         AS sig_never_scaled,
        (COALESCE(qm.query_count, 0) < 10
         AND COALESCE(u.running_seconds, 0) > 3600)                   AS sig_very_low_usage

    FROM usage_agg AS ug
    LEFT JOIN uptime_by_warehouse AS u
        ON  u.workspace_id = ug.workspace_id AND u.warehouse_id = ug.warehouse_id
    LEFT JOIN busy_by_warehouse AS b
        ON  b.workspace_id = ug.workspace_id AND b.warehouse_id = ug.warehouse_id
    LEFT JOIN wh_latest AS w
        ON  w.workspace_id = ug.workspace_id AND w.warehouse_id = ug.warehouse_id
    LEFT JOIN query_metrics AS qm
        ON  qm.workspace_id = ug.workspace_id AND qm.warehouse_id = ug.warehouse_id
    LEFT JOIN scaling_counts AS sc
        ON  sc.workspace_id = ug.workspace_id AND sc.warehouse_id = ug.warehouse_id
    LEFT JOIN time_at_max AS tm
        ON  tm.workspace_id = ug.workspace_id AND tm.warehouse_id = ug.warehouse_id
    LEFT JOIN peak_concurrency AS pc
        ON  pc.workspace_id = ug.workspace_id AND pc.warehouse_id = ug.warehouse_id
    LEFT JOIN restart_counts AS rc
        ON  rc.workspace_id = ug.workspace_id AND rc.warehouse_id = ug.warehouse_id
    LEFT JOIN peak_hour_analysis AS ph
        ON  ph.workspace_id = ug.workspace_id AND ph.warehouse_id = ug.warehouse_id
    WHERE COALESCE(u.running_seconds, 0) > 0
)

-- ── Output: Utilization report with weighted scoring ─────────────────────────────
SELECT
    workspace_id,
    workspace_name,
    tenant,
    env,
    created_by,
    warehouse_id,
    warehouse_name,
    warehouse_type,
    warehouse_size,
    min_clusters,
    max_clusters,
    auto_stop_minutes,

    -- Time metrics
    total_running_seconds,
    busy_seconds,
    idle_seconds,
    idle_pct,
    busy_pct,
    ROUND(total_running_seconds / 3600.0, 2)                           AS running_hours,
    ROUND(busy_seconds / 3600.0, 2)                                    AS busy_hours,
    ROUND(idle_seconds / 3600.0, 2)                                    AS idle_hours,
    restart_count,
    restarts_per_day,

    -- Cost metrics
    COALESCE(total_cost_usd, 0)                                        AS total_cost_usd,
    total_dbus,
    ROUND(COALESCE(total_cost_usd, 0) / NULLIF(total_dbus, 0), 4)     AS cost_per_dbu,
    ROUND(COALESCE(total_cost_usd, 0) / NULLIF(query_count, 0), 4)    AS cost_per_query,
    ROUND(COALESCE(total_cost_usd, 0) / NULLIF(busy_seconds / 3600.0, 0), 2) AS cost_per_busy_hour,
    ROUND(COALESCE(total_cost_usd, 0) / NULLIF(window_days, 0), 2)    AS daily_avg_cost,
    COALESCE(total_cost_usd, 0)
        * (1 - LEAST(1.0, COALESCE(busy_seconds, 0) / NULLIF(total_running_seconds, 0)))
                                                                       AS idle_cost_usd,
    -- Query performance
    query_count,
    queries_succeeded,
    queries_failed,
    error_rate_pct,
    avg_duration_seconds,
    p50_duration_seconds,
    p75_duration_seconds,
    p95_duration_seconds,
    p99_duration_seconds,
    queries_per_busy_hour,
    peak_hour,
    peak_hour_query_count,

    -- Queuing & provisioning
    total_queued_seconds,
    avg_queued_seconds,
    pct_queries_queued,
    total_provisioning_wait_seconds,
    avg_provisioning_wait_seconds,
    pct_queries_cold_start,

    -- Spill
    total_spill_gb,
    spill_to_read_ratio,
    pct_queries_spilling,

    -- Scaling & concurrency
    scale_up_count,
    scale_down_count,
    peak_cluster_count,
    pct_time_at_max_clusters,
    peak_concurrent_queries,
    avg_concurrent_queries,

    -- ── Weighted Utilization Score ──────────────────────────────────────────────
    -- Overutilization (higher weight = stronger signal for upsizing)
    (CAST(sig_high_queue_pct AS INT) * 3        -- queuing is the strongest overutil signal
        + CAST(sig_high_avg_queue AS INT) * 2
        + CAST(sig_high_spill AS INT) * 2
        + CAST(sig_at_max_clusters AS INT) * 3
        + CAST(sig_high_p95 AS INT) * 1
        + CAST(sig_high_error_rate AS INT) * 1
        + CAST(sig_frequent_cold_starts AS INT) * 1) AS overutil_score,

    -- Underutilization (higher weight = stronger signal for downsizing)
    (CAST(sig_high_idle AS INT) * 3             -- idle time is the strongest underutil signal
        + CAST(sig_low_concurrency AS INT) * 2
        + CAST(sig_never_scaled AS INT) * 2
        + CAST(sig_very_low_usage AS INT) * 3)  AS underutil_score,

    -- Net score: positive = overutilized, negative = underutilized
    (CAST(sig_high_queue_pct AS INT) * 3
        + CAST(sig_high_avg_queue AS INT) * 2
        + CAST(sig_high_spill AS INT) * 2
        + CAST(sig_at_max_clusters AS INT) * 3
        + CAST(sig_high_p95 AS INT) * 1
        + CAST(sig_high_error_rate AS INT) * 1
        + CAST(sig_frequent_cold_starts AS INT) * 1)
    - (CAST(sig_high_idle AS INT) * 3
        + CAST(sig_low_concurrency AS INT) * 2
        + CAST(sig_never_scaled AS INT) * 2
        + CAST(sig_very_low_usage AS INT) * 3)  AS utilization_score,

    -- Status classification
    CASE
        WHEN (CAST(sig_high_queue_pct AS INT) * 3
            + CAST(sig_high_avg_queue AS INT) * 2
            + CAST(sig_high_spill AS INT) * 2
            + CAST(sig_at_max_clusters AS INT) * 3
            + CAST(sig_high_p95 AS INT) * 1
            + CAST(sig_high_error_rate AS INT) * 1
            + CAST(sig_frequent_cold_starts AS INT) * 1) >= 5
        THEN 'OVERUTILIZED'
        WHEN (CAST(sig_high_idle AS INT) * 3
            + CAST(sig_low_concurrency AS INT) * 2
            + CAST(sig_never_scaled AS INT) * 2
            + CAST(sig_very_low_usage AS INT) * 3) >= 5
        THEN 'UNDERUTILIZED'
        ELSE 'RIGHT_SIZED'
    END                                                                AS utilization_status,

    -- Actionable recommendation
    CASE
        -- Severe overutilization
        WHEN (CAST(sig_high_queue_pct AS INT) * 3
            + CAST(sig_high_avg_queue AS INT) * 2
            + CAST(sig_high_spill AS INT) * 2
            + CAST(sig_at_max_clusters AS INT) * 3) >= 8
        THEN 'Critical overutilization — upsize warehouse AND increase max_clusters immediately'

        WHEN sig_high_spill AND sig_high_p95
        THEN 'Memory pressure — upsize warehouse (spill + slow P95 indicate insufficient memory)'

        WHEN sig_at_max_clusters AND sig_high_queue_pct
        THEN 'Scaling ceiling reached — increase max_clusters or upsize warehouse'

        WHEN (CAST(sig_high_queue_pct AS INT) * 3
            + CAST(sig_high_avg_queue AS INT) * 2
            + CAST(sig_high_spill AS INT) * 2
            + CAST(sig_at_max_clusters AS INT) * 3
            + CAST(sig_high_p95 AS INT) * 1) >= 5
        THEN 'Moderate overutilization — consider upsizing or increasing max_clusters'

        -- Cold-start / auto-stop tuning
        WHEN sig_frequent_cold_starts AND restarts_per_day > 5
        THEN 'Excessive cold starts — increase auto_stop_minutes or set min_clusters >= 1'

        -- Severe underutilization
        WHEN sig_high_idle AND sig_very_low_usage
        THEN 'Warehouse barely used — consider decommissioning or consolidating with another warehouse'

        WHEN sig_high_idle AND idle_pct > 85 AND sig_never_scaled
        THEN 'Significantly underutilized — downsize warehouse, reduce max_clusters, or switch to serverless'

        WHEN sig_high_idle AND sig_low_concurrency
        THEN 'Underutilized — reduce max_clusters to 1 or downsize warehouse'

        WHEN sig_never_scaled AND max_clusters > 1
        THEN 'Never scaled out — reduce max_clusters to save on idle overhead'

        -- Auto-stop tuning (not over/under but can save cost)
        WHEN auto_stop_minutes > 15 AND idle_pct > 50
        THEN 'High idle with long auto-stop — reduce auto_stop_minutes to lower idle cost'

        -- Error-rate issues
        WHEN sig_high_error_rate
        THEN 'High error rate — investigate failing queries before resizing'

        ELSE 'Right-sized — monitor periodically'
    END                                                                AS recommendation,

    -- Secondary recommendation for auto-stop tuning
    CASE
        WHEN auto_stop_minutes > 10 AND idle_pct > 60
        THEN CONCAT('Consider reducing auto_stop from ', auto_stop_minutes, ' to 5-10 minutes')
        WHEN restarts_per_day > 8 AND auto_stop_minutes < 10
        THEN CONCAT('Frequent restarts (', restart_count, ') — increase auto_stop_minutes to reduce cold starts')
        ELSE NULL
    END                                                                AS auto_stop_recommendation

FROM combined
ORDER BY
    utilization_score DESC,
    idle_cost_usd DESC,
    total_running_seconds DESC
;
