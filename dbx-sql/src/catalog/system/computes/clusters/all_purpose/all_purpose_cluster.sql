DESCRIBE EXTENDED system.compute.clusters;
-- Latest clusters
SELECT 
    workspace_id,
    cluster_id,
    cluster_name,
    cluster_source,
    change_time,
    tags
FROM (
        SELECT *,
            row_number() OVER (
                PARTITION BY workspace_id, cluster_id
                ORDER BY change_time DESC
            ) AS rn
        FROM system.compute.clusters
        WHERE cluster_source in ("UI", "API")
    ) AS latest_clusters
WHERE rn = 1;

-- latest active clusters
SELECT 
    workspace_id,
    cluster_id,
    cluster_name,
    cluster_source,
    change_time,
    tags
FROM (
        SELECT *,
            row_number() OVER (
                PARTITION BY workspace_id, cluster_id
                ORDER BY change_time DESC
            ) AS rn
        FROM system.compute.clusters
        WHERE cluster_source in ("UI", "API")
    ) AS latest_clusters
WHERE rn = 1 and delete_time is NULL;

-- Personal cluster with missing tenant tags
SELECT 
    workspace_id,
    cluster_id,
    cluster_name,
    cluster_source,
    change_time,
    tags
FROM (
        SELECT *,
            row_number() OVER (
                PARTITION BY workspace_id, cluster_id
                ORDER BY change_time DESC
            ) AS rn
        FROM system.compute.clusters
        WHERE cluster_source in ("UI", "API")
    ) AS latest_clusters
WHERE rn = 1 and tags.Tenant is null;
