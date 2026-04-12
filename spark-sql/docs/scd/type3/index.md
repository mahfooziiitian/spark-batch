# :material-table-column-plus-after: Introduction

SCD Type 3 tracks limited history by storing previous values in additional columns alongside current values.
It’s best when:

1. You need only 1-level history (e.g., previous city)
2. You want to compare current vs previous values in the same row

## use case

1. Where did the customer move from?
2. What was their previous job title?

## 🔄 When to Use SCD Type 3

Criteria                        | SCD Type 3 Recommended?
--------------------------------|------------------------
Need full historical records    | ❌ Use SCD Type 2
Only care about last value      | ✅ Yes
Want to compare current vs last | ✅ Yes
High cardinality of changes     | ❌ Use SCD Type 2
Space/performance sensitive     | ✅ Lightweight

## Best Practices

### 1. Identify Which Columns Truly Need History

Only use Type 3 for a small, finite number of changing attributes.

Examples: city, department, manager, status.

### 2. Schema Naming Convention

Use clear suffixes:

1. current_city
2. previous_city
3. current_status, previous_status

### 3. Time of Change (Optional but Useful)

Add a change_timestamp or city_change_date column to track when the value changed.

```sql
Copy
Edit
ALTER TABLE dim_customer ADD COLUMNS (city_change_date TIMESTAMP);
```

### 4. Null-Safety in Logic

Use safe comparisons:

```sql
WHEN MATCHED AND (src.city <> tgt.current_city OR tgt.current_city IS NULL)
```

### 5. Merge Logic Pattern

1. When `src.value ≠ current`, update:
   - previous = current
   - current = src.value
   - timestamp = current_timestamp()
2. Else skip
   - Insert if not matched

### 6. Avoid Repeated Updates

Ensure hash or exact diff to prevent unnecessary rewrites (for performance & audit clarity).

## 🛑 Limitations of SCD Type 3

1. Can only store 1-level history
2. Becomes unmanageable if tracking multiple changes or frequent updates
3. Not suitable for auditing, regulatory, or timeline analytics

## 🧪 Testing Best Practices

Test all 3 scenarios:

1. Insert new customer
2. Update with city change
3. Re-ingest same city → no update

Ensure previous_city moves only when current_city changes

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

### 🔄 SCD Type 3 Merge Logic in SQL

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
