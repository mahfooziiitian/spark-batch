# Queries

## Which warehouses are actively running and for how long?

```sql
USE CATALOG `system`;

SELECT
  we.warehouse_id,
  we.event_time,
  TIMESTAMPDIFF(MINUTE, we.event_time, NOW()) / 60.0 AS running_hours,
  we.cluster_count
FROM
  compute.warehouse_events we
WHERE
  we.event_type = 'RUNNING'
  AND NOT EXISTS (
    SELECT
      1
    FROM
      compute.warehouse_events we2
    WHERE
      we2.warehouse_id = we.warehouse_id
      AND we2.event_time > we.event_time
  )
```

## Identify warehouses that are upscaled longer than expected

```sql
use catalog `system`;

SELECT
   we.warehouse_id,
   we.event_time,
   TIMESTAMPDIFF(MINUTE, we.event_time, CURRENT_TIMESTAMP()) / 60.0 AS upscaled_hours,
   we.cluster_count
FROM
   compute.warehouse_events we
WHERE
   we.event_type = 'SCALED_UP'
   AND we.cluster_count >= 2
   AND NOT EXISTS (
       SELECT 1
       FROM compute.warehouse_events we2
       WHERE we2.warehouse_id = we.warehouse_id
       AND (
           (we2.event_type = 'SCALED_DOWN') OR
           (we2.event_type = 'SCALED_UP' AND we2.cluster_count < 2)
       )
       AND we2.event_time > we.event_time
   )
```