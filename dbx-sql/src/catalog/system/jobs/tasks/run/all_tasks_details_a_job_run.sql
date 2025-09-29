DESC system.lakeflow.job_task_run_timeline;
DESCRIBE system.lakeflow.job_tasks;

SELECT *
FROM system.lakeflow.job_task_run_timeline
WHERE
    job_run_id = '602427424354474'
    AND period_start_time > current_date() - 30;

SELECT * FROM system.lakeflow.job_run_timeline
WHERE
    run_id = '602427424354474'
    AND period_start_time > current_date() - 30;


SELECT *
FROM system.access.table_lineage AS l
WHERE l.entity_run_id = '602427424354474';

SELECT * FROM system.lakeflow.job_tasks
WHERE
    task_key = 'gold_all_claims_3yrs_full_model_task'
    AND job_id = '569446239544353';
    -- and change_time > current_date() - 30;

--- Task run
SELECT *
FROM system.lakeflow.job_task_run_timeline
WHERE
    run_id = '466408713309891'
    AND period_start_time > current_date() - 30;
