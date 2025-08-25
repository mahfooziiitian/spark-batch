# Demo

## Schema

```sql
CREATE TABLE dim_customer (
  customer_id STRING,
  name STRING,
  current_city STRING,
  previous_city STRING,
  updated_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (current_city);
```

```sql
CREATE OR REPLACE TEMP VIEW staging_customer AS
SELECT * FROM VALUES
  ("cust1", "Alice", "NY"),       -- No change
  ("cust2", "Bob", "TX"),         -- Changed city from CA → TX
  ("cust3", "Charlie", "WA")      -- New customer
AS t(customer_id, name, city);
```

## 🔄 SCD Type 3 Merge Logic in SQL

```sql
MERGE INTO dim_customer AS tgt
USING staging_customer AS src
ON src.customer_id = tgt.customer_id

-- Update: If city has changed
WHEN MATCHED AND src.city <> tgt.current_city THEN
  UPDATE SET
    tgt.previous_city = tgt.current_city,
    tgt.current_city = src.city,
    tgt.name = src.name,
    tgt.updated_at = current_timestamp()

-- No change: Do nothing if city is same
-- Optional: add a WHEN MATCHED AND src.city = tgt.current_city THEN DO NOTHING

-- Insert: New customers
WHEN NOT MATCHED THEN
  INSERT (
    customer_id, name, current_city, previous_city, updated_at
  )
  VALUES (
    src.customer_id, src.name, src.city, null, current_timestamp()
  );
```
