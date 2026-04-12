# :material-calendar-clock: Date

The `DATE` type represents a calendar date (year, month, day) without a time component.

## 📌 Syntax

```sql
-- Literal
DATE '2024-01-15'

-- From string
CAST('2024-01-15' AS DATE)
TO_DATE('15/01/2024', 'dd/MM/yyyy')
```

## 🔍 Behavior

1. Stored as the number of days since `1970-01-01` (epoch).
2. Valid range: `0001-01-01` to `9999-12-31`.
3. No time component — comparisons are date-only.
4. Supports arithmetic with `INTERVAL` types.

## 🧪 Practical Examples

### 🧱 1. Current Date

```sql
SELECT CURRENT_DATE() AS today;
```

### 🧱 2. Date Arithmetic

```sql
SELECT DATE '2024-01-15' + INTERVAL '30' DAY AS future;
-- Result: 2024-02-14

SELECT DATE '2024-03-01' - INTERVAL '1' MONTH AS past;
-- Result: 2024-02-01
```

### 🧱 3. Difference Between Dates

```sql
SELECT DATEDIFF(DATE '2024-12-31', DATE '2024-01-01') AS days;
-- Result: 365

SELECT MONTHS_BETWEEN(DATE '2024-06-15', DATE '2024-01-15') AS months;
-- Result: 5.0
```

### 🧱 4. Extract Components

```sql
SELECT
  YEAR(DATE '2024-07-15')       AS yr,
  MONTH(DATE '2024-07-15')      AS mo,
  DAY(DATE '2024-07-15')        AS dy,
  DAYOFWEEK(DATE '2024-07-15')  AS dow,
  QUARTER(DATE '2024-07-15')    AS qtr;
-- yr=2024, mo=7, dy=15, dow=2 (Monday), qtr=3
```

### 🧱 5. Date Truncation

```sql
SELECT DATE_TRUNC('MONTH', DATE '2024-07-15') AS first_of_month;
-- Result: 2024-07-01

SELECT DATE_TRUNC('YEAR', DATE '2024-07-15') AS first_of_year;
-- Result: 2024-01-01
```

### 🧱 6. Generate Date Ranges

```sql
SELECT EXPLODE(SEQUENCE(DATE '2024-01-01', DATE '2024-01-05')) AS day;
-- 2024-01-01, 2024-01-02, ..., 2024-01-05
```

## 🧠 Common Functions

| Function | Description |
|----------|-------------|
| `CURRENT_DATE()` | Today's date |
| `DATE_ADD(date, days)` | Add days |
| `DATE_SUB(date, days)` | Subtract days |
| `DATEDIFF(end, start)` | Days between two dates |
| `MONTHS_BETWEEN(d1, d2)` | Months between two dates |
| `YEAR/MONTH/DAY(date)` | Extract component |
| `DAYOFWEEK(date)` | Day of week (1=Sun, 7=Sat) |
| `DATE_TRUNC(unit, date)` | Truncate to unit |
| `LAST_DAY(date)` | Last day of month |
| `NEXT_DAY(date, dayOfWeek)` | Next occurrence of day |
