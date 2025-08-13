DESCRIBE EXTENDED system.lakeflow.job_task_run_timeline;
SELECT *
FROM system.lakeflow.job_task_run_timeline
WHERE job_id = '495076550603319';

-- Get the latest task run information
WITH latest_task_runs AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY workspace_id, job_id, task_key
            ORDER BY period_start_time DESC
        ) AS rn
    FROM system.lakeflow.job_task_run_timeline QUALIFY rn = 1
)

SELECT *
FROM latest_task_runs
WHERE job_id = '495076550603319';

-- Get the latest cluster information
WITH clusters AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                workspace_id,
                cluster_id
            ORDER BY change_time DESC
        ) AS rn
    FROM system.compute.clusters QUALIFY rn = 1
)

SELECT *
FROM clusters;
-- Get the latest task run information with cluster details
WITH clusters AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                workspace_id,
                cluster_id
            ORDER BY change_time DESC
        ) AS rn
    FROM system.compute.clusters QUALIFY rn = 1
),

exploded_task_runs AS (
    SELECT
        *,
        EXPLODE(compute_ids) AS cluster_id
    FROM system.lakeflow.job_task_run_timeline
    WHERE ARRAY_SIZE(compute_ids) > 0
)

SELECT *
FROM exploded_task_runs
LEFT JOIN
    clusters
    ON exploded_task_runs.workspace_id = clusters.workspace_id AND exploded_task_runs.cluster_id = clusters.cluster_id;
SELECT
    *,
    EXPLODE(compute_ids) AS cluster_id
FROM system.lakeflow.job_task_run_timeline
WHERE ARRAY_SIZE(compute_ids) > 0
