WITH latest_warehouses AS (
    SELECT
        w.workspace_id,
        w.warehouse_id,
        w.warehouse_name,
        w.warehouse_type,
        ROW_NUMBER() OVER (
            PARTITION BY w.warehouse_id
            ORDER BY w.change_time DESC
        ) AS rn
    FROM
        system.compute.warehouses AS w QUALIFY rn = 1
    AND w.delete_time IS NULL
    AND w.tags.tenant IS NULL
)

SELECT
    w.workspace_name,
    lw.workspace_id,
    lw.warehouse_id,
    lw.warehouse_name,
    lw.warehouse_type
FROM latest_warehouses AS lw
LEFT JOIN
    mgmt_stg.metadata.workspace_id_crosswalk_view AS w
    ON lw.workspace_id = w.workspace_id;
