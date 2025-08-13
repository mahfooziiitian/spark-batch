-- Describe job tables
DESCRIBE EXTENDED system.lakeflow.jobs;

-- Job with latest details (scd2)

WITH jobs AS (
    SELECT
        *,
        ROW_NUMBER()
            OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC)
            AS rn
    FROM system.lakeflow.jobs QUALIFY rn = 1
)

SELECT
    w.workspace_name,
    j.name,
    j.job_id,
    j.workspace_id
FROM jobs AS j LEFT JOIN
    mgmt_stg.metadata.workspace_id_crosswalk_view AS w
    ON j.workspace_id = w.workspace_id
WHERE j.delete_time IS NULL;
