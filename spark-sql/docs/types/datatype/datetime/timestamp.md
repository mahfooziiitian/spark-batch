# :material-calendar-clock: Timestamp

The `TIMESTAMP` type stores a date and time with microsecond precision. Spark SQL provides
two variants: timezone-aware (`TIMESTAMP`) and timezone-free (`TIMESTAMP_NTZ`).

## 📌 Syntax

```sql
-- Literal
TIMESTAMP '2024-01-15 10:30:00'
TIMESTAMP_NTZ '2024-01-15 10:30:00'

-- From string
CAST('2024-01-15 10:30:00' AS TIMESTAMP)
TO_TIMESTAMP('15/01/2024 10:30', 'dd/MM/yyyy HH:mm')
```

## 🔍 Behavior

1. **TIMESTAMP** — stored as microseconds since epoch in UTC; displayed in the session timezone.
2. **TIMESTAMP_NTZ** — stored as-is without timezone conversion (Spark 3.4+).
3. Precision: microseconds (6 decimal digits of seconds).
4. Supports arithmetic with `INTERVAL` types.

## 🧪 Practical Examples

### 🧱 1. Current Timestamp

```sql
SELECT CURRENT_TIMESTAMP() AS now;
```

### 🧱 2. Timestamp Arithmetic

```sql
SELECT TIMESTAMP '2024-01-15 10:00:00' + INTERVAL '3' HOUR AS later;
-- Result: 2024-01-15 13:00:00

SELECT TIMESTAMP '2024-01-15 10:00:00' - INTERVAL '30' MINUTE AS earlier;
-- Result: 2024-01-15 09:30:00
```

### 🧱 3. Difference in Seconds

```sql
SELECT UNIX_TIMESTAMP(TIMESTAMP '2024-01-15 12:00:00')
     - UNIX_TIMESTAMP(TIMESTAMP '2024-01-15 10:00:00') AS diff_seconds;
-- Result: 7200
```

### 🧱 4. Convert Between Types

```sql
-- Timestamp to Date (drops time)
SELECT CAST(TIMESTAMP '2024-01-15 10:30:00' AS DATE);
-- Result: 2024-01-15

-- Date to Timestamp (midnight)
SELECT CAST(DATE '2024-01-15' AS TIMESTAMP);
-- Result: 2024-01-15 00:00:00

-- Epoch seconds to Timestamp
SELECT FROM_UNIXTIME(1705312200);
-- Result: 2024-01-15 10:30:00
```

### 🧱 5. Truncate Timestamps

```sql
SELECT DATE_TRUNC('HOUR', TIMESTAMP '2024-07-15 14:37:22') AS truncated;
-- Result: 2024-07-15 14:00:00

SELECT DATE_TRUNC('DAY', TIMESTAMP '2024-07-15 14:37:22') AS truncated;
-- Result: 2024-07-15 00:00:00
```

### 🧱 6. Build from Components

```sql
SELECT MAKE_TIMESTAMP(2024, 7, 15, 14, 30, 0) AS ts;
-- Result: 2024-07-15 14:30:00
```

## 🧠 Key Functions

| Function | Description |
|----------|-------------|
| `CURRENT_TIMESTAMP()` | Current timestamp |
| `TO_TIMESTAMP(str, fmt)` | Parse string to timestamp |
| `FROM_UNIXTIME(epoch)` | Epoch seconds to timestamp |
| `UNIX_TIMESTAMP(ts)` | Timestamp to epoch seconds |
| `DATE_TRUNC(unit, ts)` | Truncate to unit |
| `MAKE_TIMESTAMP(...)` | Build from components |
| `TIMESTAMPADD(unit, n, ts)` | Add interval |
| `TIMESTAMPDIFF(unit, ts1, ts2)` | Difference in units |
