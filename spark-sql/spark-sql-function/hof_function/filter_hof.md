# Filter hof

## Filtering Even Numbers

    SELECT filter(array(1, 2, 3, 4, 5), x -> x % 2 = 0) AS even_numbers;

## Filtering Based on a Condition

    SELECT filter(array(1, 2, 3, 4, 5), x -> x > 2) AS greater_than_two;

## Filtering Null Values

    SELECT filter(array(1, NULL, 3, NULL, 5), x -> x IS NOT NULL) AS no_nulls;

## Filtering Nested Arrays

    SELECT filter(array(array(1, 2), array(), array(3, 4)), a -> size(a) > 0) AS non_empty_arrays;

## Filtering Based on String Length

    SELECT filter(array('apple', 'cat', 'banana', 'dog'), x -> length(x) > 3) AS long_strings;

## Filtering Struct Elements

    SELECT filter(
    array(named_struct('name', 'Alice', 'age', 25), named_struct('name', 'Bob', 'age', 30)),
    s -> s.age > 25
    ) AS filtered_structs;
