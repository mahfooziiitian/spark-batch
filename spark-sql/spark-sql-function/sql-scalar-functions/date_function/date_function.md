# Date time function

## `curdate`

    curdate()

It returns the current date at the start of query evaluation.
All calls of `curdate` within the same query return the same value.

    SELECT curdate();

## current_date

    current_date()

It returns the current date at the start of query evaluation.
All calls of current_date within the same query return the same value.
current_date - Returns the current date at the start of query evaluation.

    SELECT current_date();
    SELECT current_date;

## date

    date(expr)

It casts the value expr to the target data type date.

## date_add

    date_add(start_date, num_days)

Returns the date that is num_days after start_date.

    SELECT date_add('2016-07-30', 1);

## date_diff

    date_diff(endDate, startDate)

Returns the number of days from startDate to endDate.

Examples:

    SELECT date_diff('2009-07-31', '2009-07-30');
    SELECT date_diff('2009-07-30', '2009-07-31');

## date_format

    date_format(timestamp, fmt)

It Converts timestamp to a value of string in the format specified by the date format fmt.

Arguments:

`timestamp` - A date/timestamp or string to be converted to the given format.

`fmt` - Date/time format pattern to follow. 

    SELECT date_format('2016-04-08', 'y');

## date_from_unix_date

    date_from_unix_date(days)

It creates date from the number of days since 1970-01-01.

    SELECT date_from_unix_date(1);

## date_part

    date_part(field, source)

Extracts a part of the date/timestamp or interval source.

Arguments:

`field` - selects which part of the source should be extracted, and supported string values are as same as the fields of the equivalent function EXTRACT.

`source` - a date/timestamp or interval column from where field should be extracted

    SELECT date_part('YEAR', TIMESTAMP '2019-08-12 01:00:00.123456');
    SELECT date_part('week', timestamp'2019-08-12 01:00:00.123456');
    SELECT date_part('doy', DATE'2019-08-12');
    SELECT date_part('SECONDS', timestamp'2019-10-01 00:00:01.000001');
    SELECT date_part('days', interval 5 days 3 hours 7 minutes);
    SELECT date_part('seconds', interval 5 hours 30 seconds 1 milliseconds 1 microseconds);
    SELECT date_part('MONTH', INTERVAL '2021-11' YEAR TO MONTH);
    SELECT date_part('MINUTE', INTERVAL '123 23:55:59.002001' DAY TO SECOND);

## date_sub

    date_sub(start_date, num_days)

It Returns the date that is num_days before start_date.

    SELECT date_sub('2016-07-30', 1);

## date_trunc

    date_trunc(fmt, ts)

It Returns timestamp ts truncated to the unit specified by the format model fmt.

    SELECT date_trunc('YEAR', '2015-03-05T09:32:05.359');
    SELECT date_trunc('MM', '2015-03-05T09:32:05.359');
    SELECT date_trunc('DD', '2015-03-05T09:32:05.359');
    SELECT date_trunc('HOUR', '2015-03-05T09:32:05.359');
    SELECT date_trunc('MILLISECOND', '2015-03-05T09:32:05.123456');

## `dateadd`

    dateadd(start_date, num_days)

It returns the date that is num_days after start_date.

    SELECT dateadd('2016-07-30', 1);

## `datediff`

    datediff(endDate, startDate)

It Returns the number of days from startDate to endDate.

    SELECT datediff('2009-07-31', '2009-07-30');
    SELECT datediff('2009-07-30', '2009-07-31');

## day

    day(date)

Returns the day of month of the date/timestamp.

## `dayofmonth`

    dayofmonth(date)

Returns the day of month of the date/timestamp.

    SELECT dayofmonth('2009-07-30');

## dayofweek

    dayofweek(date)

It Returns the day of the week for date/timestamp (1 = Sunday, 2 = Monday, ..., 7 = Saturday).

    SELECT dayofweek('2009-07-30');

## `dayofyear`

    dayofyear(date)

It returns the day of year of the date/timestamp.

    SELECT dayofyear('2016-04-09');
