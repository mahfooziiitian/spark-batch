-- job cluster tags
with task_latest_run_per_job AS (
    SELECT workspace_id,
        job_id,
        task_key,
        job_run_id,
        run_id,
        compute_ids,
        result_state,
        row_number() OVER (
            PARTITION BY workspace_id,
            job_id,
            task_key
            ORDER BY period_start_time DESC
        ) as rn
    FROM system.lakeflow.job_task_run_timeline
    WHERE period_start_time >= current_timestamp - interval '20 days' QUALIFY rn = 1
        and array_size(compute_ids) > 0
),
exploded_task_runs AS (
    SELECT *,
        EXPLODE(compute_ids) as cluster_id
    FROM task_latest_run_per_job
),
latest_job_clusters AS (
    SELECT *,
        split(cluster_name, '-')[1] as job_id,
        row_number() OVER (
            PARTITION BY workspace_id,
            cluster_id
            ORDER BY change_time DESC
        ) AS rn
    FROM system.compute.clusters
    WHERE cluster_source = 'JOB'
        and change_time >= current_timestamp - interval '20 days' QUALIFY rn = 1
        and tags.Tenant is NULL
),
latest_job_clusters_run AS (
   select distinct 
    r.workspace_id,
    r.job_id,
    r.task_key
from exploded_task_runs r
    inner join latest_job_clusters c on r.workspace_id = c.workspace_id
    and r.cluster_id = c.cluster_id and r.job_id = c.job_id
)
select w.workspace_name, c.* 
from latest_job_clusters_run c
left join mgmt_stg.metadata.workspace_id_crosswalk_view w
    on c.workspace_id = w.workspace_id;
   


-- SELECT *,
--             row_number() OVER (
--                 PARTITION BY workspace_id, cluster_id
--                 ORDER BY change_time DESC
--             ) AS rn
-- FROM system.compute.clusters
-- WHERE cluster_source = 'JOB' and change_time >= current_timestamp - interval '5 days'
-- QUALIFY rn = 1;