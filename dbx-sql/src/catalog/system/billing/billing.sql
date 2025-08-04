SELECT
    list_prices.sku_name,
    list_prices.price_start_time,
    pricing.default
FROM system.billing.list_prices
WHERE list_prices.price_start_time BETWEEN "2024-05-01" AND "2025-07-01"
