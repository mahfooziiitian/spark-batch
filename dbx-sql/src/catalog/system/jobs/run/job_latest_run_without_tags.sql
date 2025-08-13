WITH latest_runs AS (
    -- First, find the latest run for each job
    SELECT
        job_id,
        run_id,
        period_start_time,
        try_element_at(compute_ids, 1) AS cluster_id
    FROM (
        SELECT
            *,
            row_number()
                OVER (PARTITION BY job_id ORDER BY period_start_time DESC)
                AS rn
        FROM
            system.lakeflow.job_run_timeline
        WHERE period_start_time > current_timestamp - INTERVAL '7 days'
    )
    WHERE
        rn = 1
),

latest_clusters AS (
    -- Next, find the latest configuration for each cluster
    -- We'll also filter for clusters with null tags here for efficiency
    SELECT
        cluster_id,
        tags
    FROM (
        SELECT
            cluster_id,
            tags,
            change_time,
            row_number() OVER (
                PARTITION BY cluster_id
                ORDER BY change_time DESC
            ) AS rn
        FROM
            system.compute.clusters
        WHERE cluster_source = 'JOB'
    ) AS latest_clusters
    WHERE
        rn = 1 AND tags IS NULL
        AND change_time > current_timestamp - INTERVAL '7 days'
)

-- Finally, join the two to get the result
SELECT
    lr.*,
    lc.tags
FROM
    latest_runs AS lr
INNER JOIN latest_clusters AS lc
    ON lr.cluster_id = lc.cluster_id;
