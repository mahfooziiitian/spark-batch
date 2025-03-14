# Range join examples

Consider two tables:

1. `events_df` with event start and end times.
2. `availability_df` with availability periods.

We want to join these tables such that each event overlaps with an availability period.

## Create tables

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

    CREATE TABLE availability_tbl (
        avail_id INT,
        start_time TIMESTAMP,
        end_time TIMESTAMP
    )

    INSERT INTO availability_tbl (avail_id, start_time, end_time) VALUES
    (1, CAST('2024-07-01 07:00:00' AS TIMESTAMP), CAST('2024-07-01 09:30:00' AS TIMESTAMP)),
    (2, CAST('2024-07-01 09:00:00' AS TIMESTAMP), CAST('2024-07-01 11:30:00' AS TIMESTAMP)),
    (3, CAST('2024-07-01 10:00:00' AS TIMESTAMP), CAST('2024-07-01 12:00:00' AS TIMESTAMP)),
    (4, CAST('2024-07-01 14:00:00' AS TIMESTAMP), CAST('2024-07-01 16:00:00' AS TIMESTAMP));

### Interval overlap range join

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
        e.start_time < a.end_time AND e.end_time > a.start_time

### Correcting the Interval Overlap Condition

    start_time < a_end_time AND end_time > a_start_time
