# 🧠 date Function in Spark SQL

The date function in Spark SQL is used to cast a value to a DATE type or extract the date part from a timestamp or string.

## Purpose

1. Convert a string or timestamp to a date (year-month-day).
2. Extract only the date part (without time) from a timestamp.

## Current date

```sql

curdate()
current_date()
```

### curdate

It returns the current date at the start of query evaluation.
All calls of `curdate` within the same query return the same value.

```sql
SELECT curdate();
```

### current_date

It returns the current date at the start of query evaluation.
All calls of current_date within the same query return the same value.
current_date - Returns the current date at the start of query evaluation.

```sql
SELECT current_date();
SELECT current_date;
```

## date

### Syntax

```sql
date(expr)
```

expr can be a string (in date or timestamp format) or timestamp type.

### date_diff

```sql
date_diff(endDate, startDate)
```

Returns the number of days from startDate to endDate.

Examples:

```sql
SELECT date_diff('2009-07-31', '2009-07-30'), date_diff('2009-07-30', '2009-07-31');
```

## Formatting the date

### date_format

```sql
date_format(timestamp, fmt)
```

It Converts timestamp to a value of string in the format specified by the date format fmt.

Arguments:

1. `timestamp` - A date/timestamp or string to be converted to the given format.
2. `fmt` - Date/time format pattern to follow.

```sql
SELECT date_format('2016-04-08', 'y');
```

## date_from_unix_date

```sql
date_from_unix_date(days)
```

It creates date from the number of days since 1970-01-01.

```sql
SELECT date_from_unix_date(1);
```

## Parts of date

### date_part

```sql
date_part(field, source)
```

Extracts a part of the date/timestamp or interval source.

Arguments:

1. `field` - selects which part of the source should be extracted, and supported string values are as same as the fields of the equivalent function EXTRACT.
2. `source` - a date/timestamp or interval column from where field should be extracted

```sql
SELECT date_part('YEAR', TIMESTAMP '2019-08-12 01:00:00.123456');
SELECT date_part('week', timestamp'2019-08-12 01:00:00.123456');
SELECT date_part('doy', DATE'2019-08-12');
SELECT date_part('SECONDS', timestamp'2019-10-01 00:00:01.000001');
SELECT date_part('days', interval 5 days 3 hours 7 minutes);
SELECT date_part('seconds', interval 5 hours 30 seconds 1 milliseconds 1 microseconds);
SELECT date_part('MONTH', INTERVAL '2021-11' YEAR TO MONTH);
SELECT date_part('MINUTE', INTERVAL '123 23:55:59.002001' DAY TO SECOND);
```
