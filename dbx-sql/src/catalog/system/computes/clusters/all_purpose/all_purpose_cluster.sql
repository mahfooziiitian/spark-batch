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
    SELECT
        *,
        row_number() OVER (
            PARTITION BY workspace_id, cluster_id
            ORDER BY change_time DESC
        ) AS rn
    FROM system.compute.clusters
    WHERE cluster_source IN ("UI", "API")
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
    SELECT
        *,
        row_number() OVER (
            PARTITION BY workspace_id, cluster_id
            ORDER BY change_time DESC
        ) AS rn
    FROM system.compute.clusters
    WHERE cluster_source IN ("UI", "API")
) AS latest_clusters
WHERE rn = 1 AND delete_time IS NULL;

-- Personal cluster with missing tenant tags
SELECT
    latest_clusters.workspace_id,
    latest_clusters.cluster_id,
    latest_clusters.cluster_name,
    latest_clusters.cluster_source,
    latest_clusters.change_time,
    latest_clusters.tags
FROM (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY workspace_id, cluster_id
            ORDER BY change_time DESC
        ) AS rn
    FROM system.compute.clusters
    WHERE cluster_source IN ("UI", "API")
) AS latest_clusters
WHERE latest_clusters.rn = 1 AND tags.tenant IS NULL;
