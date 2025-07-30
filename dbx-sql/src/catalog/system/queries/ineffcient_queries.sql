SELECT
  workspace_id,
  compute.warehouse_id,
  statement_id,
  statement_text,
  SUM(shuffle_read_bytes) AS shuffle_bytes
FROM system.query.history
WHERE (start_time BETWEEN DATE_SUB(CURRENT_DATE, 30) AND CURRENT_DATE) and compute.warehouse_id is not NULL
GROUP BY workspace_id, compute.warehouse_id, statement_id, statement_text
HAVING shuffle_bytes > 0
ORDER BY shuffle_bytes DESC
LIMIT 10;
