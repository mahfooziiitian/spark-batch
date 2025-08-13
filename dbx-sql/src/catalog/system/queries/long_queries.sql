WITH q AS (
    SELECT
        h.workspace_id,
        h.compute.warehouse_id,
        h.statement_id,
        h.statement_text,
        h.start_time,
        h.total_duration_ms
    FROM system.query.history AS h
    WHERE
        h.start_time >= DATE_SUB(CURRENT_DATE, 30)
        AND h.total_duration_ms > 120000 -- queries longer than 2 minutes
)

SELECT
    qt.*,
    u.usage_quantity AS dbus_used,
    lp.`pricing`.`default` * u.usage_quantity AS estimated_cost
FROM q AS qt
LEFT JOIN system.billing.usage AS u
    ON
        u.usage_date = CAST(qt.start_time AS DATE)
        AND qt.warehouse_id = u.`usage_metadata`.`warehouse_id`
INNER JOIN system.billing.list_prices AS lp ON u.sku_name = lp.sku_name
WHERE u.usage_unit = 'DBU';
