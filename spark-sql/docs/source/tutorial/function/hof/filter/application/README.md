# Filter use case

In Apache Spark SQL, the filter high-order function is a powerful tool used to process arrays in DataFrames. It allows you to apply a predicate function to each element of an array and return a new array containing only the elements that satisfy the condition.

## Real-Time Use Case: IoT Sensor Data Filtering

Imagine you're working with a stream of IoT sensor data from a manufacturing plant. Each record contains an array of temperature readings from different sensors. You want to filter out readings that are outside the acceptable range (e.g., below 10°C or above 80°C).

### Structure

### SQL

```sql
-- Create a temporary view with sample data
CREATE OR REPLACE TEMP VIEW sensor_data AS
SELECT * FROM VALUES
  ('D001', array(22.5, 85.0, 19.0, 5.0)),
  ('D002', array(45.0, 12.0, 90.0, 33.0))
AS sensor_data(device_id, temperature_readings);
SELECT 
  device_id,
  filter(temperature_readings, x -> x >= 10 AND x <= 80) AS valid_readings
FROM sensor_data;
```

## E-commerce: High-Value Transactions

```sql
CREATE OR REPLACE TEMP VIEW user_purchases AS
SELECT * FROM VALUES
  ('user01', array(5000, 12000, 8000, 15000)),
  ('user02', array(3000, 20000, 7000, 11000))
AS user_purchases(user_id, transactions);
SELECT
  user_id,
  filter(transactions, x -> x > 10000) AS high_value_txns
FROM user_purchases;

```

##  Telecom: Dropped Calls

```sql
CREATE OR REPLACE TEMP VIEW telecom_data AS
SELECT * FROM VALUES
  ('cust01', array(
    named_struct('duration', 0, 'status', 'dropped'),
    named_struct('duration', 5, 'status', 'completed'),
    named_struct('duration', 0, 'status', 'dropped')
  )),
  ('cust02', array(
    named_struct('duration', 10, 'status', 'completed'),
    named_struct('duration', 0, 'status', 'dropped')
  ))
AS telecom_data(customer_id, call_logs);
SELECT
  customer_id,
  filter(call_logs, x -> x.duration = 0 OR x.status = 'dropped') AS dropped_calls
FROM telecom_data;

```

##  Banking: Suspicious Transactions

```sql
CREATE OR REPLACE TEMP VIEW bank_txn_data AS
SELECT * FROM VALUES
  ('acc01', array(
    named_struct('timestamp', '08:30', 'location', 'Delhi'),
    named_struct('timestamp', '19:00', 'location', 'Chennai'),
    named_struct('timestamp', '14:00', 'location', 'Mumbai')
  )),
  ('acc02', array(
    named_struct('timestamp', '22:00', 'location', 'Dubai'),
    named_struct('timestamp', '10:00', 'location', 'Chennai')
  ))
AS bank_txn_data(account_id, transactions);

SELECT
  account_id,
  filter(transactions, x -> x.timestamp NOT BETWEEN '09:00' AND '18:00' OR x.location NOT IN ('Chennai', 'Mumbai')) AS suspicious_txns
FROM bank_txn_data;

```

## Healthcare: Critical Vitals

```sql
CREATE OR REPLACE TEMP VIEW patient_monitoring AS
SELECT * FROM VALUES
  ('pat01', array(
    named_struct('heart_rate', 130, 'bp_systolic', 190),
    named_struct('heart_rate', 85, 'bp_systolic', 120)
  )),
  ('pat02', array(
    named_struct('heart_rate', 125, 'bp_systolic', 160),
    named_struct('heart_rate', 70, 'bp_systolic', 110)
  ))
AS patient_monitoring(patient_id, vitals);

SELECT
  patient_id,
  filter(vitals, x -> x.heart_rate > 120 OR x.bp_systolic > 180) AS critical_alerts
FROM patient_monitoring;

```
