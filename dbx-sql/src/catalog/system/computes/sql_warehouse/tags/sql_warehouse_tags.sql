WITH latest_warehouses AS (
    SELECT w.workspace_id,
        w.warehouse_id,
        w.warehouse_name,
        w.warehouse_type,
        ROW_NUMBER() OVER (
            PARTITION BY w.warehouse_id
            ORDER BY w.change_time DESC
        ) AS rn
    FROM system.compute.warehouses w QUALIFY rn = 1
        and w.delete_time is NULL
        and w.tags.Tenant is NULL
)
SELECT w.workspace_name,
    lw.workspace_id,
    lw.warehouse_id,
    lw.warehouse_name,
    lw.warehouse_type
FROM latest_warehouses lw
    left JOIN mgmt_stg.metadata.workspace_id_crosswalk_view w using (workspace_id);