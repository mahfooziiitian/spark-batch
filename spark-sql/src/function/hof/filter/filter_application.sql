-- Real-Time Use Case: IoT Sensor Data Filtering

CREATE OR REPLACE TEMP VIEW sensor_data AS
SELECT --noqa
    * --noqa
FROM
    VALUES
    ('D001', array(22.5, 85.0, 19.0, 5.0)),
    ('D002', array(45.0, 12.0, 90.0, 33.0))
    AS sensor_data (device_id, temperature_readings);
SELECT
    device_id,
    filter(temperature_readings, x -> x >= 10 AND x <= 80) AS valid_readings
FROM sensor_data;


-- E-commerce: High-Value Transactions

CREATE OR REPLACE TEMP VIEW user_purchases AS
SELECT --noqa
    * --noqa
FROM
    VALUES
    ('user01', array(5000, 12000, 8000, 15000)),
    ('user02', array(3000, 20000, 7000, 11000))
    AS user_purchases (user_id, transactions);
SELECT
    user_id,
    filter(transactions, x -> x > 10000) AS high_value_txns
FROM user_purchases;


--  Telecom: Dropped Calls

CREATE OR REPLACE TEMP VIEW telecom_data AS
SELECT --noqa
    * --noqa
FROM
    VALUES
    ('cust01', array(
        named_struct('duration', 0, 'status', 'dropped'),
        named_struct('duration', 5, 'status', 'completed'),
        named_struct('duration', 0, 'status', 'dropped')
    )),
    ('cust02', array(
        named_struct('duration', 10, 'status', 'completed'),
        named_struct('duration', 0, 'status', 'dropped')
    ))
    AS telecom_data (customer_id, call_logs);
SELECT
    telecom_data.customer_id,
    filter(
        telecom_data.call_logs,
        x -> x.duration = 0 OR x.status = 'dropped' --noqa
    ) AS dropped_calls
FROM telecom_data;


--  Banking: Suspicious Transactions

CREATE OR REPLACE TEMP VIEW bank_txn_data AS
SELECT --noqa
    * --noqa
FROM
    VALUES
    ('acc01', array(
        named_struct('timestamp', '08:30', 'location', 'Delhi'),
        named_struct('timestamp', '19:00', 'location', 'Chennai'),
        named_struct('timestamp', '14:00', 'location', 'Mumbai')
    )),
    ('acc02', array(
        named_struct('timestamp', '22:00', 'location', 'Dubai'),
        named_struct('timestamp', '10:00', 'location', 'Chennai')
    ))
    AS bank_txn_data (account_id, transactions);

SELECT
    bank_txn_data.account_id,
    filter(
        bank_txn_data.transactions,
        x -> x.timestamp NOT BETWEEN '09:00' AND '18:00' --noqa
        OR x.location NOT IN ('Chennai', 'Mumbai') --noqa
    ) AS suspicious_txns
FROM bank_txn_data;


-- Healthcare: Critical Vitals


CREATE OR REPLACE TEMP VIEW patient_monitoring AS
SELECT --noqa
    * --noqa
FROM
    VALUES
    ('pat01', array(
        named_struct('heart_rate', 130, 'bp_systolic', 190),
        named_struct('heart_rate', 85, 'bp_systolic', 120)
    )),
    ('pat02', array(
        named_struct('heart_rate', 125, 'bp_systolic', 160),
        named_struct('heart_rate', 70, 'bp_systolic', 110)
    ))
    AS patient_monitoring (patient_id, vitals);

SELECT
    patient_monitoring.patient_id,
    filter(
        patient_monitoring.vitals,
        x -> x.heart_rate > 120 OR x.bp_systolic > 180 --noqa
    ) AS critical_alerts
FROM patient_monitoring;
