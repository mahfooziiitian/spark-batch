SELECT
    u.usage_date,
    u.sku_name,
    SUM(u.usage_quantity) AS total_dbus,
    SUM(u.usage_quantity * lp.pricing.default) AS estimated_cost_usd
FROM system.billing.usage AS u
INNER JOIN system.billing.list_prices AS lp ON u.sku_name = lp.sku_name
WHERE u.usage_date >= CURRENT_DATE - 30
GROUP BY
    u.usage_date,
    u.sku_name
ORDER BY u.usage_date DESC;
