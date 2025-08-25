# Sliding Window Aggregation

Use overlapping time windows to smooth trends (e.g., rolling 1-day window every 30 minutes).

```sql
-- Create or replace the temporary view
CREATE OR REPLACE TEMP VIEW events AS
SELECT * FROM VALUES
  (1001, 1, TIMESTAMP('2025-07-20 10:00:00'), 'click', 50),
  (1002, 2, TIMESTAMP('2025-07-20 11:05:00'), 'view', 30),
  (1003, 1, TIMESTAMP('2025-07-20 10:10:00'), 'click', 70),
  (1004, 3, TIMESTAMP('2025-07-20 11:15:00'), 'purchase', 90),
  (1005, 2, TIMESTAMP('2025-07-20 10:20:00'), 'click', 20)
AS events (event_id, user_id, event_time, event_type, value);

SELECT 
  window.start AS start_time,
  window.end AS end_time,
  COUNT(*) AS total_events
FROM (
  SELECT window(event_time, '1 day', '1 hour') AS window
  FROM events
)
GROUP BY window
ORDER BY start_time;
```
