# :material-calendar-clock: DateTime Types

Spark SQL provides `DATE`, `TIMESTAMP`, and `INTERVAL` types for working with
temporal data — from simple calendar dates to precise timestamps with timezone handling.

## :material-pin: Type Summary

| Type | Stores | Precision | Timezone |
|------|--------|-----------|----------|
| `DATE` | Year, month, day | Day | No |
| `TIMESTAMP` | Date + time | Microsecond | Session timezone |
| `TIMESTAMP_NTZ` | Date + time | Microsecond | No timezone |
| `INTERVAL` | Duration | Variable | N/A |

## :material-flask-outline: Quick Examples

```sql
-- Literals
SELECT DATE '2024-01-15';
SELECT TIMESTAMP '2024-01-15 10:30:00';
SELECT INTERVAL '2' DAY + INTERVAL '3' HOUR;

-- Current values
SELECT CURRENT_DATE(), CURRENT_TIMESTAMP();

-- Arithmetic
SELECT DATE '2024-01-15' + INTERVAL '30' DAY;
-- Result: 2024-02-14

SELECT DATEDIFF(DATE '2024-12-31', DATE '2024-01-01');
-- Result: 365
```

See the sub-pages for detailed coverage of each type and formatting patterns.
