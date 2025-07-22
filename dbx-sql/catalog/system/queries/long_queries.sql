WITH q AS (
  SELECT
    workspace_id,
    compute.warehouse_id,
    statement_id,
    statement_text,
    start_time,
    total_duration_ms
  FROM system.query.history
  WHERE start_time >= DATE_SUB(CURRENT_DATE, 30)
    AND total_duration_ms > 120000  -- queries longer than 2 minutes
)
SELECT
  q.*,
  u.usage_quantity AS dbus_used,
  lp.pricing.default * u.usage_quantity AS estimated_cost
FROM q
LEFT JOIN system.billing.usage u
  ON u.usage_date = CAST(q.start_time AS DATE)
  AND u.usage_metadata.warehouse_id = q.warehouse_id
JOIN system.billing.list_prices lp
  ON lp.sku_name = u.sku_name
WHERE u.usage_unit = 'DBU';