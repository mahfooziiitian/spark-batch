# Date truncate

date_trunc is a handy function used to truncate a timestamp or date to the specified unit of time — like truncating to the start of the year, month, day, hour, etc.

## Purpose

Truncate a timestamp/date to a specified precision, returning the start of that unit.

## Syntax

```sql
date_trunc(format, timestamp)
```

1. `format`: A string specifying the unit to truncate to (e.g., 'year', 'month', 'day', 'hour', 'minute', 'second').
2. `timestamp`: The timestamp or date to be truncated.
3. `Returns`: A timestamp truncated to the specified unit.

It Returns timestamp ts truncated to the unit specified by the format model fmt.

## Examples

### 1. Truncate to Year

```sql
SELECT date_trunc('YEAR', '2015-03-05T09:32:05.359');
```

### 2. Truncate to Month

```sql
SELECT date_trunc('MM', '2015-03-05T09:32:05.359');
```

### 3. Truncate to Day

```sql
SELECT date_trunc('DD', '2015-03-05T09:32:05.359');
```

### 4. Truncate to Hour

```sql
SELECT date_trunc('HOUR', '2015-03-05T09:32:05.359');
```

### 5. Truncate to minute

```sql
SELECT date_trunc('MINUTE', '2015-03-05T09:32:05.123456');
```

### 6. Truncate to second

```sql
SELECT date_trunc('SECOND', '2015-03-05T09:32:05.123456');
```

### 7. Truncate to milli-second

```sql
SELECT date_trunc('MILLISECOND', '2015-03-05T09:32:05.123456');
```
