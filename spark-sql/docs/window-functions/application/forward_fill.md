# :material-numeric-7-circle: Forward-Fill (Last Observation Carried Forward)

Fill `NULL` readings with the most recent non-null value in each sensor's timeline.

---

## :material-flask-outline: Practical Examples

```sql
CREATE OR REPLACE TEMP VIEW sensor_readings AS
SELECT * FROM VALUES
  ('S1', '2024-01-01', 10.0),
  ('S1', '2024-01-02', NULL),
  ('S1', '2024-01-03', NULL),
  ('S1', '2024-01-04', 12.5),
  ('S1', '2024-01-05', NULL)
AS sensor_readings(sensor_id, reading_date, value);

SELECT
    sensor_id,
    reading_date,
    value,
    LAST_VALUE(value IGNORE NULLS) OVER (
        PARTITION BY sensor_id
        ORDER BY reading_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS value_ffill
FROM sensor_readings
ORDER BY sensor_id, reading_date;
-- Result:
-- | sensor_id | reading_date | value | value_ffill |
-- |-----------|--------------|-------|-------------|
-- | S1        | 2024-01-01   |  10.0 |        10.0 |
-- | S1        | 2024-01-02   |  NULL |        10.0 |  -- carried forward
-- | S1        | 2024-01-03   |  NULL |        10.0 |  -- carried forward
-- | S1        | 2024-01-04   |  12.5 |        12.5 |  -- new reading
-- | S1        | 2024-01-05   |  NULL |        12.5 |  -- carried forward
```

---

## :material-lightbulb-outline: When to Use

- IoT / sensor data — fill gaps in intermittent readings.
- Financial data — carry forward the last known price when markets are closed.
- Survey / form data — propagate a header value across detail rows.

!!! warning "IGNORE NULLS support"
    `LAST_VALUE ... IGNORE NULLS` requires Spark 3.3+. For older versions,
    use a self-join or `MAX` with a conditional window.

---

## :material-arrow-right: Related

- [NULL Handling in Windows](../nulls/null_options_wf.md) — `IGNORE NULLS`, `RESPECT NULLS`
- [Gap Detection](gap_detection.md) — detect missing data instead of filling it
