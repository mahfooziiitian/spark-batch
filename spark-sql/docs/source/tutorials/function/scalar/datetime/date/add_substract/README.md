# Date addition/substraction

## date_add

```sql
date_add(start_date, num_days)
```

1. `start_date`: Date or timestamp.
2. `num_days`: Integer number of days to add (can be negative to subtract).

```sql
SELECT date_add('2016-07-30', 1);
```

## date_sub

```sql
date_sub(start_date, num_days)
```

It Returns the date that is num_days before start_date.

```sql
SELECT date_sub('2016-07-30', 1);
```

## dateadd

```sql
dateadd(start_date, num_days)
```

It returns the date that is num_days after start_date.

```sql
SELECT dateadd('2016-07-30', 1);
```

## add_months()
