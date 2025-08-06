-- The daily job count for a workspace for the last 7 days

SELECT
  jrt.workspace_id,
  COUNT(DISTINCT jrt.run_id) as job_count,
  to_date(jrt.period_start_time) as date
FROM system.lakeflow.job_run_timeline AS jrt
WHERE
  jrt.period_start_time > CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
GROUP BY ALL

-- Average time of job runs.
-- A top 90 and a 95 percentile column show the average lengths of the job's longest runs.
with job_run_duration as (
    SELECT
        workspace_id,
        job_id,
        run_id,
        CAST(SUM(period_end_time - period_start_time) AS LONG) as duration
    FROM
        system.lakeflow.job_run_timeline
    WHERE
      period_start_time > CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
    GROUP BY ALL
)
SELECT
    t1.workspace_id,
    t1.job_id,
    COUNT(DISTINCT t1.run_id) as runs,
    MEAN(t1.duration) as mean_seconds,
    AVG(t1.duration) as avg_seconds,
    PERCENTILE(t1.duration, 0.9) as p90_seconds,
    PERCENTILE(t1.duration, 0.95) as p95_seconds
FROM
    job_run_duration t1
GROUP BY ALL
ORDER BY mean_seconds DESC
LIMIT 100;

-- Enrich job run with a job name
with jobs as (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) as rn
    FROM system.lakeflow.jobs QUALIFY rn=1
)
SELECT
    job_run_timeline.*,
    jobs.name
FROM system.lakeflow.job_run_timeline
    LEFT JOIN jobs USING (workspace_id, job_id)
WHERE
    job_run_timeline.period_start_time > CURRENT_TIMESTAMP() - INTERVAL 7 DAYS

-- Calculate cost per job run

with jobs_usage AS (
  SELECT
    *,
    usage_metadata.job_id,
    usage_metadata.job_run_id as run_id,
    identity_metadata.run_as as run_as
  FROM system.billing.usage
  WHERE billing_origin_product="JOBS"
),
jobs_usage_with_usd AS (
  SELECT
    jobs_usage.*,
    usage_quantity * pricing.default as usage_usd
  FROM jobs_usage
    LEFT JOIN system.billing.list_prices pricing ON
      jobs_usage.sku_name = pricing.sku_name
      AND pricing.price_start_time <= jobs_usage.usage_start_time
      AND (pricing.price_end_time >= jobs_usage.usage_start_time OR pricing.price_end_time IS NULL)
      AND pricing.currency_code="USD"
),
jobs_usage_aggregated AS (
  SELECT
    workspace_id,
    job_id,
    run_id,
    FIRST(run_as, TRUE) as run_as,
    sku_name,
    SUM(usage_usd) as usage_usd,
    SUM(usage_quantity) as usage_quantity
  FROM jobs_usage_with_usd
  GROUP BY ALL
)
SELECT
  t1.*,
  MIN(period_start_time) as run_start_time,
  MAX(period_end_time) as run_end_time,
  FIRST(result_state, TRUE) as result_state
FROM jobs_usage_aggregated t1
  LEFT JOIN system.lakeflow.job_run_timeline t2 USING (workspace_id, job_id, run_id)
GROUP BY ALL
ORDER BY usage_usd DESC
LIMIT 100;

