# :material-layers: SCD Type 6?

SCD Type 6 is a hybrid of Types 1, 2, and 3. It allows:

Behavior               | From SCD Type | Stored In
-----------------------|---------------|----------------------------------------------
Overwrite current info | Type 1        | name, email, city in main row
Track history          | Type 2        | New row with start_date, end_date, is_current
Store previous value   | Type 3        | prev_city or similar column in main row

## ✅ Use Case:

You want to:

1. Keep current values easily queryable
2. Retain full change history
3. Track previous value for selected fields (e.g. last city)

🧱 Table Design
sql
Copy
Edit
CREATE TABLE IF NOT EXISTS dim_customer (
  customer_sk BIGINT GENERATED ALWAYS AS IDENTITY,
  customer_id STRING,
  name STRING,
  email STRING,
  city STRING,
  prev_city STRING,  -- Type 3: previous value of city
  row_hash STRING,   -- for change detection
  start_date TIMESTAMP,
  end_date TIMESTAMP,
  is_current BOOLEAN
)
USING DELTA;
📥 Incoming Staging Table
sql
Copy
Edit
CREATE OR REPLACE TEMP VIEW staging_customer AS
SELECT *FROM VALUES
  ("cust1", "Alice", "alice@example.com", "NY"),     -- No change
  ("cust2", "Bob", "bob@newdomain.com", "TX"),       -- Change (email)
  ("cust3", "Charlie", "charlie@example.com", "WA")  -- New
AS t(customer_id, name, email, city);
🔁 SCD Type 6 Logic in SQL
1️⃣ Add hash to detect changes
sql
Copy
Edit
CREATE OR REPLACE TEMP VIEW staged_hashed AS
SELECT
  customer_id,
  name,
  email,
  city,
  md5(concat_ws('||', name, email, city)) AS row_hash
FROM staging_customer;
2️⃣ Expire current rows that changed (Type 2 logic)
sql
Copy
Edit
UPDATE dim_customer
SET
  end_date = current_timestamp(),
  is_current = FALSE
WHERE customer_id IN (
  SELECT s.customer_id
  FROM staged_hashed s
  JOIN dim_customer d
    ON s.customer_id = d.customer_id
  WHERE d.is_current = TRUE
    AND d.row_hash <> s.row_hash
);
3️⃣ Insert new version (SCD 1, 2, and 3 combined)
sql
Copy
Edit
INSERT INTO dim_customer (
  customer_id, name, email, city, prev_city,
  row_hash, start_date, end_date, is_current
)
SELECT
  s.customer_id,
  s.name,
  s.email,
  s.city,
  CASE
    WHEN d.city IS NOT NULL AND d.city <> s.city THEN d.city
    ELSE d.prev_city
  END AS prev_city,
  s.row_hash,
  current_timestamp(), NULL, TRUE
FROM staged_hashed s
LEFT JOIN (
  SELECT* FROM dim_customer WHERE is_current = TRUE
) d ON s.customer_id = d.customer_id
WHERE d.customer_id IS NULL OR d.row_hash <> s.row_hash;
✅ Query Examples
Current state of customers:
sql
Copy
Edit
SELECT *FROM dim_customer WHERE is_current = TRUE;
Full history for a customer:
sql
Copy
Edit
SELECT* FROM dim_customer WHERE customer_id = 'cust2' ORDER BY start_date;
💡 Summary of SCD Type 6
Feature Handled By  In Table
Current value updates   Type 1  Overwrite name, email, city
Track history (all versions)    Type 2  is_current, start_date, end_date
Track last value (e.g. city)    Type 3  prev_city column

🚀 Bonus Enhancements
Add change_reason, source_system, changed_by

Use ZORDER by customer_id for optimization in Delta:

sql
Copy
Edit
OPTIMIZE dim_customer ZORDER BY (customer_id);
