# :material-calendar-clock: Date & Time Formatting

Spark SQL uses Java's `SimpleDateFormat` pattern letters for parsing and formatting
dates and timestamps.

## 📌 Pattern Reference

| Pattern | Meaning | Example |
|---------|---------|---------|
| `yyyy` | 4-digit year | `2024` |
| `yy` | 2-digit year | `24` |
| `MM` | Month (01-12) | `07` |
| `MMM` | Month abbreviation | `Jul` |
| `MMMM` | Month full name | `July` |
| `dd` | Day of month (01-31) | `15` |
| `HH` | Hour 24h (00-23) | `14` |
| `hh` | Hour 12h (01-12) | `02` |
| `mm` | Minute (00-59) | `30` |
| `ss` | Second (00-59) | `45` |
| `SSS` | Milliseconds | `123` |
| `a` | AM/PM marker | `PM` |
| `E` | Day of week abbreviation | `Mon` |
| `EEEE` | Day of week full | `Monday` |
| `D` | Day of year (1-366) | `196` |
| `z` | Timezone abbreviation | `PST` |
| `Z` | Timezone offset | `+0000` |

## 🧪 Practical Examples

### 🧱 1. DATE_FORMAT — Timestamp to String

```sql
SELECT DATE_FORMAT(TIMESTAMP '2024-07-15 14:30:45', 'yyyy-MM-dd') AS date_only;
-- Result: '2024-07-15'

SELECT DATE_FORMAT(TIMESTAMP '2024-07-15 14:30:45', 'MMMM dd, yyyy') AS pretty;
-- Result: 'July 15, 2024'

SELECT DATE_FORMAT(TIMESTAMP '2024-07-15 14:30:45', 'HH:mm:ss') AS time_only;
-- Result: '14:30:45'

SELECT DATE_FORMAT(TIMESTAMP '2024-07-15 14:30:45', 'yyyy/MM/dd hh:mm a') AS custom;
-- Result: '2024/07/15 02:30 PM'
```

### 🧱 2. TO_DATE — String to Date

```sql
SELECT TO_DATE('15/07/2024', 'dd/MM/yyyy') AS parsed;
-- Result: 2024-07-15

SELECT TO_DATE('July 15, 2024', 'MMMM dd, yyyy') AS parsed;
-- Result: 2024-07-15
```

### 🧱 3. TO_TIMESTAMP — String to Timestamp

```sql
SELECT TO_TIMESTAMP('2024-07-15 14:30', 'yyyy-MM-dd HH:mm') AS parsed;
-- Result: 2024-07-15 14:30:00

SELECT TO_TIMESTAMP('07/15/2024 02:30 PM', 'MM/dd/yyyy hh:mm a') AS parsed;
-- Result: 2024-07-15 14:30:00
```

### 🧱 4. FROM_CSV / FROM_JSON with Date Formats

```sql
SELECT FROM_CSV('26/08/2015', 'time TIMESTAMP',
  MAP('timestampFormat', 'dd/MM/yyyy')) AS parsed;

SELECT FROM_JSON('{"dt":"2024-07-15"}', 'dt DATE',
  MAP('dateFormat', 'yyyy-MM-dd')) AS parsed;
```

## 🧠 Common Format Strings

| Use Case | Pattern | Example Output |
|----------|---------|----------------|
| ISO date | `yyyy-MM-dd` | `2024-07-15` |
| ISO datetime | `yyyy-MM-dd HH:mm:ss` | `2024-07-15 14:30:45` |
| US date | `MM/dd/yyyy` | `07/15/2024` |
| EU date | `dd/MM/yyyy` | `15/07/2024` |
| Readable | `MMMM dd, yyyy` | `July 15, 2024` |
| Time only (24h) | `HH:mm:ss` | `14:30:45` |
| Time only (12h) | `hh:mm:ss a` | `02:30:45 PM` |
| Year-month | `yyyy-MM` | `2024-07` |
| Day of week | `EEEE` | `Monday` |

> **Tip:** Patterns are case-sensitive — `MM` is month, `mm` is minute. `HH` is 24-hour,
> `hh` is 12-hour.
