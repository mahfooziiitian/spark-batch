# Aggregate

The aggregate higher-order function in Spark SQL is used to aggregate elements in an array into a single value.

It takes four arguments:

1. the array to be aggregated
2. the initial value
3. the lambda function to combine elements
4. the lambda function to combine the result with the initial value.

## Summing Elements in an Array

```sql

SELECT aggregate(array(1, 2, 3, 4, 5), 0, (acc, x) -> acc + x) AS sum;
```

## Concatenating Strings

```sql
SELECT aggregate(array('a', 'b', 'c'), '', (acc, x) -> acc || ',' || x) AS concatenated;
```

## Finding the Maximum Value

```sql

SELECT aggregate(array(1, 2, 3, 4, 5), 0, (acc, x) -> CASE WHEN x > acc THEN x ELSE acc END) AS max_value;
```

## Counting Non-Null Elements

```sql
    SELECT aggregate(array(1, NULL, 3, NULL, 5), 0, (acc, x) -> acc + CASE WHEN x IS NOT NULL THEN 1 ELSE 0 END) AS non_null_count;
```

## Summing Elements of Nested Arrays

```sql
    SELECT aggregate(
        array(array(1, 2), array(3, 4), array(5)),
        0,
        (acc, a) -> acc + aggregate(a, 0, (inner_acc, x) -> inner_acc + x)
    ) AS nested_sum;
```

## Aggregating Struct Elements

```sql
    SELECT aggregate(
        array(named_struct('name', 'Alice', 'age', 25), named_struct('name', 'Bob', 'age', 30)),
        0,
        (acc, s) -> acc + s.age
    ) AS total_age;
```
