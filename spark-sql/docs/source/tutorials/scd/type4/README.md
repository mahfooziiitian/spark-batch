# SCD Type 4?

SCD Type 4 (Hybrid) separates current data and historical data into two different tables:

1. `✅ Current Table`: always contains only the latest version of the record
2. `🕓 History Table`: stores full history of all previous versions

This model is great when:

1. You need both fast access to current state (like Type 1)
2. AND historical tracking (like Type 2)
3. But want to separate concerns for performance or design reasons

## 🧱 Table Design

### 1. Current Table: dim_customer_current

```sql
CREATE TABLE IF NOT EXISTS dim_customer_current (
  customer_id STRING,
  name STRING,
  email STRING,
  city STRING,
  row_hash STRING,
  updated_at TIMESTAMP
)
USING DELTA;
```

```sql
2. History Table: dim_customer_history
sql
Copy
Edit
CREATE TABLE IF NOT EXISTS dim_customer_history (
  customer_id STRING,
  name STRING,
  email STRING,
  city STRING,
  row_hash STRING,
  valid_from TIMESTAMP,
  valid_to TIMESTAMP
)
USING DELTA;
```

###  Incoming Data (Staging)

```sql
CREATE OR REPLACE TEMP VIEW staging_customer AS
SELECT * FROM VALUES
  ("cust1", "Alice", "alice@example.com", "NY"),     -- No change
  ("cust2", "Bobby", "bob@newdomain.com", "TX"),     -- Changed
  ("cust3", "Charlie", "charlie@example.com", "WA")  -- New
AS t(customer_id, name, email, city);
```

## 🔁 Step-by-Step SCD Type 4 Logic in SQL

### 1️⃣ Identify Changed Rows

```sql
CREATE OR REPLACE TEMP VIEW changed_customers AS
SELECT
  tgt.customer_id,
  tgt.name AS old_name,
  tgt.email AS old_email,
  tgt.city AS old_city,
  tgt.row_hash AS old_hash,
  src.name,
  src.email,
  src.city,
  md5(concat_ws('||', src.name, src.email, src.city)) AS new_hash
FROM dim_customer_current tgt
JOIN staging_customer src
  ON tgt.customer_id = src.customer_id
WHERE tgt.row_hash <> md5(concat_ws('||', src.name, src.email, src.city));
```

### 2️⃣ Archive Old Records to History Table

```sql
INSERT INTO dim_customer_history
SELECT
  customer_id,
  old_name AS name,
  old_email AS email,
  old_city AS city,
  old_hash AS row_hash,
  updated_at AS valid_from,
  current_timestamp() AS valid_to
FROM changed_customers
JOIN dim_customer_current tgt
  ON changed_customers.customer_id = tgt.customer_id;
```

### 3️⃣ Upsert into Current Table

```sql
MERGE INTO dim_customer_current tgt
USING (
  SELECT
    customer_id,
    name,
    email,
    city,
    md5(concat_ws('||', name, email, city)) AS row_hash,
    current_timestamp() AS updated_at
  FROM staging_customer
) src
ON tgt.customer_id = src.customer_id

WHEN MATCHED THEN UPDATE SET
  name = src.name,
  email = src.email,
  city = src.city,
  row_hash = src.row_hash,
  updated_at = src.updated_at

WHEN NOT MATCHED THEN INSERT (
  customer_id, name, email, city, row_hash, updated_at
) VALUES (
  src.customer_id, src.name, src.email, src.city, src.row_hash, src.updated_at
);
```

### 🕓 Historical Changes

```sql
SELECT * FROM dim_customer_history ORDER BY customer_id, valid_from;
```

## ✅ Summary
Feature            | dim_customer_current | dim_customer_history
-------------------|----------------------|---------------------
Latest state       | ✅                    | ❌
Change history     | ❌                    | ✅
Tracks all changes | ❌ (only latest)      | ✅
Fast queries       | ✅                    | ✅ (partitioned)

## 💡 When to Use SCD Type 4

1. You want to keep current table slim and fast
2. Full audit trail needed, but not mixed into current view
3. You want to avoid complex filtering for "latest" from history (as in SCD Type 2)
