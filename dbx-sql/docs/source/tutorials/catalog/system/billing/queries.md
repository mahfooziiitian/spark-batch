# Queries

## Calculate Monthly DBU Usage

```sql
SELECT 
    billing_origin_product,
    usage_date,
    SUM(usage_quantity) AS usage_quantity
FROM
    system.billing.usage
WHERE
    MONTH(usage_date) = MONTH(NOW())
    AND YEAR(usage_date) = YEAR(NOW())
GROUP BY 
    billing_origin_product, usage_date
```

## Attribute Costs to Specific Tags

```sql
SELECT
    sku_name,
    usage_unit,
    SUM(usage_quantity) AS Usage
FROM
    system.billing.usage
WHERE
    custom_tags[:key] = :value
GROUP BY
    sku_name, usage_unit;
```

## Cost per cluster per job

```sql
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
```

## Cost per sku per day

```sql
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
```

## DBU usage per user

```sql
SELECT
  usage_date,
  usage_metadata.workspace_name,
  usage_metadata.user_email,
  sku_name,
  usage_quantity AS dbus_used
FROM system.billing.usage
WHERE usage_date BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE
ORDER BY usage_date DESC;
```

## ineffcient_queries

```sql
SELECT
  workspace_id,
  compute.warehouse_id,
  statement_id,
  statement_text,
  SUM(shuffle_read_bytes) AS shuffle_bytes
FROM system.query.history
WHERE (start_time BETWEEN DATE_SUB(CURRENT_DATE, 30) AND CURRENT_DATE) and compute.warehouse_id is not NULL
GROUP BY workspace_id, compute.warehouse_id, statement_id, statement_text
HAVING shuffle_bytes > 0
ORDER BY shuffle_bytes DESC
LIMIT 10;
```