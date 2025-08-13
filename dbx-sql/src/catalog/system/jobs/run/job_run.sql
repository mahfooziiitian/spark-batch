-- The daily job count for a workspace for the last 7 days

SELECT
    workspace_id,
    COUNT(DISTINCT run_id) AS job_count,
    TO_DATE(period_start_time) AS date
FROM system.lakeflow.job_run_timeline
WHERE
    period_start_time > CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
GROUP BY ALL;

-- Average time of job runs.
-- A top 90 and a 95 percentile column show the average lengths of the job's longest runs.
WITH job_run_duration AS (
    SELECT
        workspace_id,
        job_id,
        run_id,
        CAST(SUM(period_end_time - period_start_time) AS LONG) AS duration
    FROM
        system.lakeflow.job_run_timeline
    WHERE
        period_start_time > CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
    GROUP BY ALL
)

SELECT
    workspace_id,
    job_id,
    COUNT(DISTINCT run_id) AS runs,
    MEAN(duration) AS mean_seconds,
    AVG(duration) AS avg_seconds,
    PERCENTILE(duration, 0.9) AS p90_seconds,
    PERCENTILE(duration, 0.95) AS p95_seconds
FROM
    job_run_duration
GROUP BY ALL
ORDER BY mean_seconds DESC
LIMIT 100;

-- Enrich job run with a job name
WITH jobs AS (
    SELECT
        *,
        ROW_NUMBER()
            OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC)
            AS rn
    FROM system.lakeflow.jobs QUALIFY rn = 1
)

SELECT
    job_run_timeline.*,
    jobs.name
FROM system.lakeflow.job_run_timeline
LEFT JOIN
    jobs
    ON
        job_run_timeline.workspace_id = jobs.workspace_id
        AND job_run_timeline.job_id = jobs.job_id
WHERE
    job_run_timeline.period_start_time > CURRENT_TIMESTAMP() - INTERVAL 7 DAYS;

-- Calculate cost per job run

WITH jobs_usage AS (
    SELECT
        *,
        u.`usage_metadata`.`job_id`,
        u.`usage_metadata`.`job_run_id` AS run_id,
        u.`identity_metadata`.`run_as` AS run_as
    FROM system.billing.usage AS u
    WHERE u.`billing_origin_product` = "JOBS"
),

jobs_usage_with_usd AS (
    SELECT
        jobs_usage.*,
        jobs_usage.usage_quantity * pricing.default AS usage_usd
    FROM jobs_usage
    LEFT JOIN system.billing.list_prices AS pricing ON
        jobs_usage.sku_name = pricing.sku_name
        AND jobs_usage.usage_start_time >= pricing.price_start_time
        AND (
            jobs_usage.usage_start_time <= pricing.price_end_time
            OR pricing.price_end_time IS NULL
        )
        AND pricing.currency_code = "USD"
),

jobs_usage_aggregated AS (
    SELECT
        workspace_id,
        job_id,
        run_id,
        sku_name,
        FIRST(run_as, TRUE) AS run_as,
        SUM(usage_usd) AS usage_usd,
        SUM(usage_quantity) AS usage_quantity
    FROM jobs_usage_with_usd
    GROUP BY ALL
)

SELECT
    t1.*,
    MIN(period_start_time) AS run_start_time,
    MAX(period_end_time) AS run_end_time,
    FIRST(result_state, TRUE) AS result_state
FROM jobs_usage_aggregated AS t1
LEFT JOIN
    system.lakeflow.job_run_timeline
    ON
        t1.workspace_id = job_run_timeline.workspace_id
        AND t1.job_id = job_run_timeline.job_id
        AND t1.run_id = job_run_timeline.run_id
GROUP BY ALL
ORDER BY usage_usd DESC
LIMIT 100;
