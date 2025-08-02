# SCD Type 5?

SCD Type 5 combines:

1. `✅ Type 1`: Overwrites current dimension values in-place for reporting
2. `🕓 Type 4`: Stores full historical context in a separate mini-dimension

It provides the best of both worlds:

1. Fast access to current values directly in the main dimension
2. Ability to track changes through a linked historical dimension (like a mini-dim table)

🧱 Table Design
1. Main Dimension: dim_customer
Contains both current values and a reference to history (via surrogate key from mini-dim)

sql
Copy
Edit
CREATE TABLE IF NOT EXISTS dim_customer (
  customer_id STRING PRIMARY KEY,
  name STRING,
  email STRING,
  city STRING,
  hist_key BIGINT,  -- foreign key to history table
  updated_at TIMESTAMP
)
USING DELTA;
2. Mini-Dimension (History): dim_customer_history
Stores historical attributes for slowly changing fields.

sql
Copy
Edit
CREATE TABLE IF NOT EXISTS dim_customer_history (
  hist_key BIGINT GENERATED ALWAYS AS IDENTITY,
  customer_id STRING,
  name STRING,
  email STRING,
  city STRING,
  row_hash STRING,
  valid_from TIMESTAMP,
  valid_to TIMESTAMP
)
USING DELTA;
📥 Incoming Data: staging_customer
sql
Copy
Edit
CREATE OR REPLACE TEMP VIEW staging_customer AS
SELECT * FROM VALUES
  ("cust1", "Alice", "alice@example.com", "NY"),     -- No change
  ("cust2", "Bobby", "bob@newdomain.com", "TX"),     -- Changed
  ("cust3", "Charlie", "charlie@example.com", "WA")  -- New
AS t(customer_id, name, email, city);
🔄 SCD Type 5 SQL Logic
1️⃣ Identify changes using row hash
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
2️⃣ Insert historical row if changed
sql
Copy
Edit
-- Only insert if row_hash differs
INSERT INTO dim_customer_history (customer_id, name, email, city, row_hash, valid_from, valid_to)
SELECT
  s.customer_id,
  s.name,
  s.email,
  s.city,
  s.row_hash,
  current_timestamp(),
  NULL
FROM staged_hashed s
LEFT JOIN dim_customer c
  ON c.customer_id = s.customer_id
WHERE c.customer_id IS NULL OR s.row_hash <> md5(concat_ws('||', c.name, c.email, c.city));
3️⃣ Update main table with new values and new hist_key
sql
Copy
Edit
MERGE INTO dim_customer AS tgt
USING (
  SELECT
    h.customer_id,
    h.name,
    h.email,
    h.city,
    h.hist_key,
    current_timestamp() AS updated_at
  FROM dim_customer_history h
  JOIN staged_hashed s ON
    s.customer_id = h.customer_id AND
    s.row_hash = h.row_hash
) AS src
ON tgt.customer_id = src.customer_id

WHEN MATCHED THEN
  UPDATE SET
    name = src.name,
    email = src.email,
    city = src.city,
    hist_key = src.hist_key,
    updated_at = src.updated_at

WHEN NOT MATCHED THEN
  INSERT (
    customer_id, name, email, city, hist_key, updated_at
  )
  VALUES (
    src.customer_id, src.name, src.email, src.city, src.hist_key, src.updated_at
  );
✅ Final Output
Table	Purpose
dim_customer	Fast queryable table with current values
dim_customer_history	Full history of all attribute changes (linked by hist_key)

🧪 Queries
All current customers
sql
Copy
Edit
SELECT * FROM dim_customer;
Full history for a customer
sql
Copy
Edit
SELECT * FROM dim_customer_history WHERE customer_id = 'cust2' ORDER BY valid_from;
💡 When to Use SCD Type 5?
When current values must be overwritten (e.g. BI dashboards)

But auditability or change tracking is also required

This avoids complexity of querying is_current flags (like in SCD2)


