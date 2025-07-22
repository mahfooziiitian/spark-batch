SELECT
  l.source_table_name,
  COUNT(*) AS access_count
FROM system.access.table_lineage l
WHERE event_time BETWEEN DATE_SUB(CURRENT_DATE, 30) AND CURRENT_DATE
GROUP BY l.source_table_name
ORDER BY access_count DESC
LIMIT 10;
