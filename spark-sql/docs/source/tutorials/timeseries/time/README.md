# Time-Based Aggregation

## Daily Total per User

```sql
CREATE OR REPLACE TEMP VIEW events AS
SELECT * FROM VALUES
  (1001, 1, TIMESTAMP('2025-07-20 10:00:00'), 'click', 50),
  (1002, 2, TIMESTAMP('2025-07-20 11:05:00'), 'view', 30),
  (1003, 1, TIMESTAMP('2025-07-20 10:10:00'), 'click', 70),
  (1004, 3, TIMESTAMP('2025-07-20 11:15:00'), 'purchase', 90),
  (1005, 2, TIMESTAMP('2025-07-20 10:20:00'), 'click', 20)
AS events (event_id, user_id, event_time, event_type, value);
SELECT 
  user_id,
  TO_DATE(event_time) AS event_date,
  COUNT(*) AS daily_count
FROM events
GROUP BY user_id, TO_DATE(event_time)
ORDER BY user_id, event_date
```

## Weekly Active Users (WAU)

```sql
CREATE OR REPLACE TEMP VIEW events AS
SELECT * FROM VALUES
  (1001, 1, TIMESTAMP('2025-07-20 10:00:00'), 'click', 50),
  (1002, 2, TIMESTAMP('2025-07-20 11:05:00'), 'view', 30),
  (1003, 1, TIMESTAMP('2025-07-20 10:10:00'), 'click', 70),
  (1004, 3, TIMESTAMP('2025-07-20 11:15:00'), 'purchase', 90),
  (1005, 2, TIMESTAMP('2025-07-20 10:20:00'), 'click', 20)
AS events (event_id, user_id, event_time, event_type, value);
SELECT 
  user_id,
  COUNT(DISTINCT TO_DATE(event_time)) AS active_days,
  MIN(event_time) AS first_seen,
  MAX(event_time) AS last_seen
FROM events
WHERE event_time >= current_date() - interval 7 days
GROUP BY user_id
```
