SELECT
  usage_date,
  u.sku_name,
  SUM(u.usage_quantity) AS total_dbus,
  SUM(u.usage_quantity * lp.pricing.default) AS estimated_cost_usd
FROM system.billing.usage u
JOIN system.billing.list_prices lp
  ON u.sku_name = lp.sku_name
WHERE usage_date >= CURRENT_DATE - 30
GROUP BY usage_date, u.sku_name
ORDER BY usage_date DESC;
