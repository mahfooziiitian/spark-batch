-- Metadata
DESCRIBE EXTENDED system.compute.warehouses;

-- Identify the settings for all active warehouses

SELECT
    workspace_id,
    warehouse_id,
    warehouse_name,
    warehouse_type,
    warehouse_channel,
    warehouse_size,
    min_clusters,
    max_clusters,
    auto_stop_minutes,
    tags,
    change_time,
    delete_time
FROM
    system.compute.warehouses
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY change_time DESC) = 1
    and delete_time is null and tags.Tenant is NULL
ORDER BY workspace_id;

-- Which warehouses were created this week?

SELECT
    warehouse_id,
    warehouse_name,
    warehouse_type,
    warehouse_channel,
    warehouse_size,
    min_clusters,
    max_clusters,
    auto_stop_minutes,
    tags,
    change_time as datetime_created,
    delete_time
FROM
    system.compute.warehouses
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY change_time ASC) = 1
    and change_time >= DATE_TRUNC('day', CURRENT_DATE) - INTERVAL 7 days
    and delete_time is null;
