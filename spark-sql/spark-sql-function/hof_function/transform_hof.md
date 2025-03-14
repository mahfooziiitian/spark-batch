# transfrom hof

## Doubling Elements in an Array

    SELECT transform(array(1, 2, 3), x -> x * 2) AS doubled_array;

## Adding an Index to Each Element

    SELECT transform(array(10, 20, 30), (x, i) -> x + i) AS indexed_array;

## Applying a Condition

    SELECT transform(array(1, 2, 3, 4), x -> CASE WHEN x < 3 THEN 0 ELSE x END) AS conditioned_array;

## Using a Struct

    SELECT transform(
    array(named_struct('name', 'Alice', 'age', 25), named_struct('name', 'Bob', 'age', 30)),
    s -> named_struct('name', s.name, 'age', s.age + 1)
    ) AS transformed_structs;

## Nested Arrays

    SELECT transform(array(array(1, 2), array(3, 4)), a -> transform(a, x -> x * 2)) AS nested_transformed;
