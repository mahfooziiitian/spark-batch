# Scd type 2

## ✅ Goal of SCD Type 2

1. Insert new records if customer_id doesn't exist in dim_customer.
2. Expire old records by setting end_date and is_current = false if the record changed.
3. Insert new version of changed records with new start_date, end_date = 9999-12-31, and is_current = true.

## 🏗 Table Definitions

dim_customer (target - history)

```sql
CREATE TABLE IF NOT EXISTS dim_customer (
  customer_id INT,
  name STRING,
  city STRING,
  start_date DATE,
  end_date DATE,
  is_current BOOLEAN
);
```

stg_customer (source - latest snapshot)

```sql
CREATE TABLE IF NOT EXISTS stg_customer (
  customer_id INT,
  name STRING,
  city STRING
);
```

### 1. Expire old records in dim_customer if the data has changed

```SQL
UPDATE dim_customer
SET end_date = current_date() - INTERVAL 1 DAY,
    is_current = false
WHERE is_current = true
  AND customer_id IN (
    SELECT s.customer_id
    FROM stg_customer s
    JOIN dim_customer d
      ON s.customer_id = d.customer_id
    WHERE d.is_current = true
      AND (s.name != d.name OR s.city != d.city)
  );

```

### 2. Insert new records from staging where:

1. The record is new, or
2. It has changed from the current version

```sql
INSERT INTO dim_customer
SELECT
  s.customer_id,
  s.name,
  s.city,
  current_date() AS start_date,
  DATE('9999-12-31') AS end_date,
  true AS is_current
FROM stg_customer s
LEFT JOIN dim_customer d
  ON s.customer_id = d.customer_id AND d.is_current = true
WHERE d.customer_id IS NULL
   OR s.name != d.name
   OR s.city != d.city;
```

### ✅ After this

You’ll have multiple versions of changed records (historical rows).

Only the latest has is_current = true and end_date = 9999-12-31.

## 📝 Optional: View for Current Dimension

```sql
CREATE OR REPLACE VIEW vw_current_customer AS
SELECT *
FROM dim_customer
WHERE is_current = true;
```
