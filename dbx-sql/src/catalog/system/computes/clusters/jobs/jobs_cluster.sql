DESCRIBE EXTENDED system.compute.clusters;
DESCRIBE EXTENDED system.lakeflow.job_task_run_timeline;

-- Latest job clusters
SELECT
    workspace_id,
    cluster_id,
    cluster_name,
    cluster_source,
    change_time,
    tags,
    split(cluster_name, '-')[1] AS job_id
FROM (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY workspace_id, cluster_id
            ORDER BY change_time DESC
        ) AS rn
    FROM system.compute.clusters
    WHERE cluster_source = 'JOB'
) AS latest_clusters
WHERE rn = 1;

-- latest job active clusters
SELECT
    workspace_id,
    cluster_id,
    cluster_name,
    cluster_source,
    change_time,
    tags,
    split(cluster_name, '-')[1] AS job_id
FROM (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY workspace_id, cluster_id
            ORDER BY change_time DESC
        ) AS rn
    FROM system.compute.clusters
    WHERE cluster_source = 'JOB'
) AS latest_clusters
WHERE rn = 1 AND delete_time IS NULL;

-- Job cluster with missing tenant tags
SELECT
    latest_clusters.workspace_id,
    latest_clusters.cluster_id,
    latest_clusters.cluster_name,
    latest_clusters.cluster_source,
    latest_clusters.change_time,
    latest_clusters.tags,
    split(latest_clusters.cluster_name, '-')[1] AS job_id
FROM (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY workspace_id, cluster_id
            ORDER BY change_time DESC
        ) AS rn
    FROM system.compute.clusters
    WHERE cluster_source = 'JOB'
) AS latest_clusters
WHERE
    latest_clusters.rn = 1 AND latest_clusters.delete_time IS NULL AND tags.tenant IS NULL;

-- Verify whether job clusters are having only one cluster per job
SELECT
    workspace_id,
    cluster_id,
    count(*) AS cluster_count
FROM system.compute.clusters
WHERE cluster_source = 'JOB'
GROUP BY workspace_id, cluster_id
HAVING count(*) > 1;

-- 
WITH clusters AS (
    SELECT
        *,
        row_number()
            OVER (
                PARTITION BY clusters.workspace_id, clusters.cluster_id
                ORDER BY clusters.change_time DESC
            )
            AS rn
    FROM system.compute.clusters
    WHERE
        clusters.cluster_source = 'JOB'
        AND clusters.change_time >= current_timestamp() - INTERVAL '20' DAY
    -- and tags.Tenant is NULL  -- Assuming you want to filter clusters with missing tenant tags
    -- QUALIFY should be after SELECT, not inside WHERE
    QUALIFY rn = 1 AND tags.tenant IS NULL
),

exploded_task_runs AS (
    SELECT
        *,
        explode(compute_ids) AS cluster_id
    FROM system.lakeflow.job_task_run_timeline
    WHERE
        period_start_time >= current_timestamp() - INTERVAL '20' DAY
        AND array_size(compute_ids) > 0
)

SELECT *
FROM exploded_task_runs
INNER JOIN clusters
    ON
        exploded_task_runs.workspace_id = clusters.workspace_id
        AND exploded_task_runs.cluster_id = clusters.cluster_id
