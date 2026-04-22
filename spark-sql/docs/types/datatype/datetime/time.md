# :material-calendar-clock: Time

Spark SQL does not have a standalone `TIME` type. Time-of-day information is part of the
`TIMESTAMP` type. To work with time-only values, extract components from timestamps.

## :material-pin: Extracting Time Components

```sql
SELECT
  HOUR(TIMESTAMP '2024-07-15 14:30:45')   AS hr,
  MINUTE(TIMESTAMP '2024-07-15 14:30:45') AS min,
  SECOND(TIMESTAMP '2024-07-15 14:30:45') AS sec;
-- hr=14, min=30, sec=45
```

## :material-flask-outline: Practical Examples

### :material-toy-brick: 1. Format as Time String

```sql
SELECT DATE_FORMAT(TIMESTAMP '2024-07-15 14:30:45', 'HH:mm:ss') AS time_str;
-- Result: '14:30:45'

SELECT DATE_FORMAT(TIMESTAMP '2024-07-15 14:30:45', 'hh:mm:ss a') AS time_12h;
-- Result: '02:30:45 PM'
```

### :material-toy-brick: 2. Filter by Time of Day

```sql
SELECT * FROM events
WHERE HOUR(event_time) BETWEEN 9 AND 17;
-- Business hours only
```

### :material-toy-brick: 3. Time-Based Bucketing

```sql
SELECT
  CASE
    WHEN HOUR(ts) BETWEEN 6 AND 11  THEN 'Morning'
    WHEN HOUR(ts) BETWEEN 12 AND 17 THEN 'Afternoon'
    WHEN HOUR(ts) BETWEEN 18 AND 21 THEN 'Evening'
    ELSE 'Night'
  END AS time_bucket,
  COUNT(*) AS cnt
FROM events
GROUP BY 1;
```

### :material-toy-brick: 4. Time Arithmetic with Intervals

```sql
SELECT TIMESTAMP '2024-07-15 14:30:00' + INTERVAL '2' HOUR AS later;
-- Result: 2024-07-15 16:30:00

SELECT TIMESTAMP '2024-07-15 14:30:00' - INTERVAL '45' MINUTE AS earlier;
-- Result: 2024-07-15 13:45:00
```

## :material-brain: Time-Related Functions

| Function | Description |
|----------|-------------|
| `HOUR(ts)` | Extract hour (0-23) |
| `MINUTE(ts)` | Extract minute (0-59) |
| `SECOND(ts)` | Extract second (0-59) |
| `DATE_FORMAT(ts, fmt)` | Format as string |
| `DATE_TRUNC('HOUR', ts)` | Truncate to hour |
| `MAKE_TIMESTAMP(y,m,d,h,min,s)` | Build timestamp from components |
