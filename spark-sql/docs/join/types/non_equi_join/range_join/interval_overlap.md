# Interval Overlap

This guide demonstrates how to perform a **range join** (also known as an **interval overlap join**) in Spark SQL. This is useful when you want to find overlapping time intervals between two tables.

## Scenario

Suppose you have:

- **`events_df`**: Contains events with start and end times.
- **`availability_df`**: Contains periods of availability.

**Goal:** Join these tables to find all event–availability pairs where the event overlaps with an availability period.

---

## 1. Create Example Tables

```sql
CREATE TABLE events_tbl (
    event_id INT,
    event_name STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP
);

INSERT INTO events_tbl (event_id, event_name, start_time, end_time) VALUES
    (1, 'Event A', CAST('2024-07-01 08:00:00' AS TIMESTAMP), CAST('2024-07-01 10:00:00' AS TIMESTAMP)),
    (2, 'Event B', CAST('2024-07-01 09:00:00' AS TIMESTAMP), CAST('2024-07-01 11:00:00' AS TIMESTAMP)),
    (3, 'Event C', CAST('2024-07-01 12:00:00' AS TIMESTAMP), CAST('2024-07-01 14:00:00' AS TIMESTAMP)),
    (4, 'Event D', CAST('2024-07-01 13:00:00' AS TIMESTAMP), CAST('2024-07-01 15:00:00' AS TIMESTAMP));
```

```sql
CREATE TABLE availability_tbl (
    avail_id INT,
    start_time TIMESTAMP,
    end_time TIMESTAMP
);

INSERT INTO availability_tbl (avail_id, start_time, end_time) VALUES
    (1, CAST('2024-07-01 07:00:00' AS TIMESTAMP), CAST('2024-07-01 09:30:00' AS TIMESTAMP)),
    (2, CAST('2024-07-01 09:00:00' AS TIMESTAMP), CAST('2024-07-01 11:30:00' AS TIMESTAMP)),
    (3, CAST('2024-07-01 10:00:00' AS TIMESTAMP), CAST('2024-07-01 12:00:00' AS TIMESTAMP)),
    (4, CAST('2024-07-01 14:00:00' AS TIMESTAMP), CAST('2024-07-01 16:00:00' AS TIMESTAMP));
```

---

## 2. Interval Overlap Range Join

To find overlapping intervals, use the following join condition:

> **Two intervals `[start1, end1)` and `[start2, end2)` overlap if:**  
> `start1 < end2 AND end1 > start2`

### Example Query

```sql
SELECT
    e.event_id,
    e.event_name,
    e.start_time AS event_start,
    e.end_time AS event_end,
    a.avail_id,
    a.start_time AS avail_start,
    a.end_time AS avail_end
FROM
    events_tbl e
JOIN
    availability_tbl a
ON
    e.start_time < a.end_time
    AND e.end_time > a.start_time
ORDER BY
    e.event_id, a.avail_id;
```

---

## 3. Explanation

- **`e.start_time < a.end_time`**: The event starts before the availability ends.
- **`e.end_time > a.start_time`**: The event ends after the availability starts.

This ensures that the event and availability intervals overlap.

---

## 4. Visual Representation

```text
Event:      |------|
Availability:   |------|
Overlap:     |--|
```

---

## 5. Summary

- Use a **range join** to match overlapping intervals.
- The key condition is:  

  ```sql
  event.start_time < availability.end_time
  AND event.end_time > availability.start_time
  ```

- This pattern is common in scheduling, booking, and time series analysis.
