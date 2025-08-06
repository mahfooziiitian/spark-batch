DESCRIBE EXTENDED system.compute.clusters;
DESCRIBE EXTENDED system.lakeflow.job_task_run_timeline;

-- Latest job clusters
SELECT 
    workspace_id,
    cluster_id,
    split(cluster_name, '-')[1] as job_id,
    cluster_name,
    cluster_source,
    change_time,
    tags
FROM (
        SELECT *,
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
    split(cluster_name, '-')[1] as job_id,
    cluster_name,
    cluster_source,
    change_time,
    tags
FROM (
        SELECT *,
            row_number() OVER (
                PARTITION BY workspace_id, cluster_id
                ORDER BY change_time DESC
            ) AS rn
        FROM system.compute.clusters
        WHERE cluster_source = 'JOB'
    ) AS latest_clusters
WHERE rn = 1 and delete_time IS NULL;

-- Job cluster with missing tenant tags
SELECT 
    workspace_id,
    cluster_id,
    cluster_name,
    split(cluster_name, '-')[1] as job_id,
    cluster_source,
    change_time,
    tags
FROM (
        SELECT *,
            row_number() OVER (
                PARTITION BY workspace_id, cluster_id
                ORDER BY change_time DESC
            ) AS rn
        FROM system.compute.clusters
        WHERE cluster_source = 'JOB'
    ) AS latest_clusters
WHERE rn = 1 and delete_time is null and tags.Tenant is NULL;

-- Verify whether job clusters are having only one cluster per job
SELECT workspace_id,
       cluster_id, 
       count(*) as cluster_count
FROM system.compute.clusters
WHERE cluster_source = 'JOB'
GROUP BY workspace_id, cluster_id
HAVING count(*) > 1;

-- 
with clusters as (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY workspace_id, cluster_id ORDER BY change_time DESC) as rn
    FROM system.compute.clusters 
    WHERE cluster_source = 'JOB' and change_time >= current_timestamp() - interval '20' day
    -- and tags.Tenant is NULL  -- Assuming you want to filter clusters with missing tenant tags
    -- QUALIFY should be after SELECT, not inside WHERE
    QUALIFY rn = 1 and tags.Tenant is NULL  
),
exploded_task_runs AS (
  SELECT
    *,
    EXPLODE(compute_ids) as cluster_id
  FROM system.lakeflow.job_task_run_timeline
  WHERE period_start_time >= current_timestamp() - interval '20' day and array_size(compute_ids) > 0
)
SELECT
  *
FROM exploded_task_runs t1
  JOIN clusters t2
    USING (workspace_id, cluster_id)