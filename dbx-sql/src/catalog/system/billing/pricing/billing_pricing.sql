SELECT
    list_prices.sku_name,
    list_prices.price_start_time,
    pricing.default
FROM system.billing.list_prices
WHERE
    list_prices.sku_name = 'STANDARD_ALL_PURPOSE_COMPUTE'
    AND list_prices.price_start_time <= '2023-01-01'
ORDER BY list_prices.price_start_time DESC
LIMIT 1;
