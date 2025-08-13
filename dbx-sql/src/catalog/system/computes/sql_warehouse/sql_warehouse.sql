-- Metadata
DESCRIBE EXTENDED system.compute.warehouses;

-- Identify the settings for all active warehouses

SELECT
    warehouses.workspace_id,
    warehouses.warehouse_id,
    warehouses.warehouse_name,
    warehouses.warehouse_type,
    warehouses.warehouse_channel,
    warehouses.warehouse_size,
    warehouses.min_clusters,
    warehouses.max_clusters,
    warehouses.auto_stop_minutes,
    warehouses.tags,
    warehouses.change_time,
    warehouses.delete_time
FROM
    system.compute.warehouses
QUALIFY
    ROW_NUMBER()
        OVER (
            PARTITION BY warehouses.warehouse_id
            ORDER BY warehouses.change_time DESC
        )
    = 1
    AND warehouses.delete_time IS null AND tags.tenant IS null
ORDER BY warehouses.workspace_id;

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
    change_time AS datetime_created,
    delete_time
FROM
    system.compute.warehouses
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY change_time ASC) = 1
    AND change_time >= DATE_TRUNC('day', CURRENT_DATE) - INTERVAL 7 DAYS
    AND delete_time IS null;
