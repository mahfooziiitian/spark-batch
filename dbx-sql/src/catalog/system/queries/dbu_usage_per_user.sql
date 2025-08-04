SELECT
    u.usage_date,
    u.usage_metadata.workspace_name,
    u.usage_metadata.user_email,
    u.sku_name,
    u.usage_quantity AS dbus_used
FROM system.billing.usage AS u
WHERE u.usage_date BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE
ORDER BY u.usage_date DESC;
