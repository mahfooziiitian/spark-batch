# Introduction

## ✅ Goal of SCD Type 2

1. Insert new records if customer_id doesn't exist in `dim_customer`.
2. Expire old records by setting `end_date` and `is_current = false` if the record changed.
3. Insert new version of changed records with `new start_date`, `end_date = 9999-12-31`, and `is_current = true`.

## SCD Type 2 Recap

SCD Type 2 tracks full history of changes by:

1. Keeping multiple versions of a record
2. Using `is_current`, `start_date`, `end_date` columns
3. Creating a new row for each change instead of updating in place

## 📝 View for Current Dimension

```sql
CREATE OR REPLACE VIEW vw_current_customer AS
SELECT *
FROM dim_customer
WHERE is_current = true;
```
