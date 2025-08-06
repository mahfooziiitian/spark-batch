DESCRIBE EXTENDED system.compute.clusters;

-- Latest job clusters
SELECT 
    *,count(*) OVER (PARTITION BY workspace_id, cluster_id) as cluster_count
FROM system.compute.clusters
WHERE create_time >= date_sub(current_date, 30)
    AND cluster_source = 'JOB'
QUALIFY cluster_count > 1
ORDER BY cluster_id;



    