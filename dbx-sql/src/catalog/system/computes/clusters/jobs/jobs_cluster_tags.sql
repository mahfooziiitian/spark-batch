-- job cluster tags
WITH task_latest_run_per_job AS (
    SELECT
        workspace_id,
        job_id,
        task_key,
        job_run_id,
        run_id,
        compute_ids,
        result_state,
        row_number() OVER (
            PARTITION BY
                workspace_id,
                job_id,
                task_key
            ORDER BY period_start_time DESC
        ) AS rn
    FROM system.lakeflow.job_task_run_timeline
    WHERE
        period_start_time >= current_timestamp - INTERVAL '20 days' QUALIFY
        rn = 1
        AND array_size(compute_ids) > 0
),

exploded_task_runs AS (
    SELECT
        *,
        explode(compute_ids) AS cluster_id
    FROM task_latest_run_per_job
),

latest_job_clusters AS (
    SELECT
        *,
        split(clusters.cluster_name, '-')[1] AS job_id,
        row_number() OVER (
            PARTITION BY
                clusters.workspace_id,
                clusters.cluster_id
            ORDER BY clusters.change_time DESC
        ) AS rn
    FROM system.compute.clusters
    WHERE
        clusters.cluster_source = 'JOB'
        AND clusters.change_time >= current_timestamp - INTERVAL '20 days'
    QUALIFY
        rn = 1
        AND tags.tenant IS NULL
),

latest_job_clusters_run AS (
    SELECT DISTINCT
        r.workspace_id,
        r.job_id,
        r.task_key
    FROM exploded_task_runs AS r
    INNER JOIN latest_job_clusters
        AS c ON r.workspace_id = c.workspace_id
    AND r.cluster_id = c.cluster_id AND r.job_id = c.job_id
)

SELECT
    c.*,
    w.workspace_name
FROM latest_job_clusters_run AS c
LEFT JOIN mgmt_stg.metadata.workspace_id_crosswalk_view AS w
    ON c.workspace_id = w.workspace_id;


-- SELECT *,
--             row_number() OVER (
--                 PARTITION BY workspace_id, cluster_id
--                 ORDER BY change_time DESC
--             ) AS rn
-- FROM system.compute.clusters
-- WHERE cluster_source = 'JOB' and change_time >= current_timestamp - interval '5 days'
-- QUALIFY rn = 1;
