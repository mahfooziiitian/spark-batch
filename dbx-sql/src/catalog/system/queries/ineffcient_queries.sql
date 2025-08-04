SELECT
    history.workspace_id,
    compute.warehouse_id,
    history.statement_id,
    history.statement_text,
    SUM(history.shuffle_read_bytes) AS shuffle_bytes
FROM system.query.history
WHERE (
    history.start_time BETWEEN DATE_SUB(CURRENT_DATE, 30) AND CURRENT_DATE
)
AND compute.warehouse_id IS NOT NULL
GROUP BY
    history.workspace_id,
    compute.warehouse_id,
    history.statement_id,
    history.statement_text
HAVING shuffle_bytes > 0
ORDER BY shuffle_bytes DESC
LIMIT 10;
