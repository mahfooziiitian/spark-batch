-- Describe job tables
DESCRIBE EXTENDED system.lakeflow.jobs;

-- Job with latest details (scd2)

with jobs as (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) as rn
    FROM system.lakeflow.jobs QUALIFY rn=1
)
select
    w.workspace_name,
    j.name,
    j.job_id,
    j.workspace_id
from jobs j left join 
mgmt_stg.metadata.workspace_id_crosswalk_view w on j.workspace_id = w.workspace_id
where j.delete_time is NULL;
