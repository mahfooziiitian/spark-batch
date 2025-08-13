DESCRIBE EXTENDED system.lakeflow.job_run_timeline;

-- Job latest run
SELECT *
FROM (
    SELECT
        *,
        ROW_NUMBER()
            OVER (PARTITION BY job_id ORDER BY period_start_time DESC)
            AS rn
    FROM
        system.lakeflow.job_run_timeline
    WHERE
        job_id = '495076550603319'
        AND period_start_time > CURRENT_TIMESTAMP() - INTERVAL 30 DAYS
)
WHERE
    rn = 1;
