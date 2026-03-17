# DateTime Functions

Spark SQL provides comprehensive functions for working with dates, timestamps, intervals,
and time zones.

## 📌 Categories

| Category | Description | Key Functions |
|----------|-------------|---------------|
| **Date** | Create, manipulate, and extract parts from dates | `DATE_ADD`, `DATE_SUB`, `DATEDIFF`, `DATE_TRUNC`, `YEAR`, `MONTH`, `DAY` |
| **Timestamp** | Work with date+time precision values | `CURRENT_TIMESTAMP`, `TO_TIMESTAMP`, `DATE_FORMAT`, `UNIX_TIMESTAMP` |
| **Interval** | Add or subtract time durations | `INTERVAL '1' DAY`, `INTERVAL '2' HOUR`, `MAKE_INTERVAL` |
| **Timezone** | Convert between time zones | `FROM_UTC_TIMESTAMP`, `TO_UTC_TIMESTAMP`, `CONVERT_TIMEZONE` |

## 🧪 Quick Examples

```sql
-- Date arithmetic
SELECT DATE_ADD(CURRENT_DATE(), 30) AS thirty_days_from_now;
SELECT DATEDIFF('2024-12-31', '2024-01-01') AS days_in_year;

-- Timestamp formatting
SELECT DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss') AS formatted;

-- Date parts
SELECT YEAR(CURRENT_DATE()), MONTH(CURRENT_DATE()), DAY(CURRENT_DATE());

-- Interval arithmetic
SELECT CURRENT_TIMESTAMP() + INTERVAL '2' HOUR AS two_hours_later;

-- Timezone conversion
SELECT FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/New_York') AS eastern_time;
```

## 🧠 Type Overview

| Type | Storage | Example | Precision |
|------|---------|---------|-----------|
| `DATE` | Calendar date | `2024-01-15` | Day |
| `TIMESTAMP` | Date + time | `2024-01-15 10:30:00` | Microsecond |
| `TIMESTAMP_NTZ` | Without timezone | `2024-01-15 10:30:00` | Microsecond |
| `INTERVAL` | Time duration | `INTERVAL '1' DAY` | Variable |
