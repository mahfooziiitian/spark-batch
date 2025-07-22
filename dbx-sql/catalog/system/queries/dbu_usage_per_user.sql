SELECT
  usage_date,
  usage_metadata.workspace_name,
  usage_metadata.user_email,
  sku_name,
  usage_quantity AS dbus_used
FROM system.billing.usage
WHERE usage_date BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE
ORDER BY usage_date DESC;
