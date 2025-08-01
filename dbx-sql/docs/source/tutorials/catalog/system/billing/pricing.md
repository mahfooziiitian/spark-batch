# Pricing

The pricing table gives you access to a historical log of SKU pricing. A record gets added each time there is a change to a SKU price. These logs can help you perform cost analysis and monitor pricing changes.

Table path: This system table is located at `system.billing.list_prices`.

## Pricing for a specific sku

```sql

```
## View prices that have changed between months

```sql
SELECT sku_name, price_start_time, pricing.default
FROM system.billing.list_prices
WHERE price_start_time BETWEEN "2023-05-01" AND "2023-07-01"
```
