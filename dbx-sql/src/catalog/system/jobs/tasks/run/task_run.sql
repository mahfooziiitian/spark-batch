DESCRIBE EXTENDED system.lakeflow.job_task_run_timeline;
select *
from system.lakeflow.job_task_run_timeline
where job_id = '495076550603319';

-- Get the latest task run information
with latest_task_runs as (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY workspace_id, job_id, task_key
      ORDER BY period_start_time DESC
    ) as rn
  FROM system.lakeflow.job_task_run_timeline QUALIFY rn = 1
)
SELECT *
FROM latest_task_runs
WHERE job_id = '495076550603319';

-- Get the latest cluster information
with clusters as (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY workspace_id,
      cluster_id
      ORDER BY change_time DESC
    ) as rn
  FROM system.compute.clusters QUALIFY rn = 1
)
SELECT *
FROM clusters;
-- Get the latest task run information with cluster details
with clusters as (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY workspace_id,
      cluster_id
      ORDER BY change_time DESC
    ) as rn
  FROM system.compute.clusters QUALIFY rn = 1
),
exploded_task_runs AS (
  SELECT *,
    EXPLODE(compute_ids) as cluster_id
  FROM system.lakeflow.job_task_run_timeline
  WHERE array_size(compute_ids) > 0
)
SELECT *
FROM exploded_task_runs t1
  LEFT JOIN clusters t2 USING (workspace_id, cluster_id);
SELECT *,
  EXPLODE(compute_ids) as cluster_id
FROM system.lakeflow.job_task_run_timeline
WHERE array_size(compute_ids) > 0