DESCRIBE EXTENDED system.lakeflow.job_task_run_timeline;
-- Find the latest task run for each job run
SELECT
    workspace_id,
    job_id,
    task_key,
    job_run_id,
    run_id,
    compute_ids,
    result_state,
    row_number()
        OVER (
            PARTITION BY workspace_id, job_id, task_key
            ORDER BY period_start_time DESC
        )
        AS rn
FROM system.lakeflow.job_task_run_timeline
WHERE
    workspace_id = '1506473170500412'
    AND period_start_time >= current_timestamp - INTERVAL '5 days'
QUALIFY rn = 1
ORDER BY workspace_id, job_id, task_key;

SELECT * FROM system.lakeflow.job_task_run_timeline
WHERE
    job_id = '495076550603319'
    AND period_start_time = (
        SELECT max(period_start_time) FROM system.lakeflow.job_task_run_timeline
        WHERE job_id = '495076550603319'
    );
