-- Get the most recent version of a job task
SELECT *
FROM system.lakeflow.job_tasks limit 10;

-- Get all tasks for a specific job
SELECT *
FROM (
        SELECT *,
            ROW_NUMBER() OVER(
                PARTITION BY workspace_id,
                job_id,
                task_key
                ORDER BY change_time DESC
            ) as rn
        FROM system.lakeflow.job_tasks QUALIFY rn = 1
    )
WHERE job_id = '839695742115004';