# Null handling in set

## Set Operators (UNION, INTERSECT, EXCEPT) 

`NULL` values are compared in a `null-safe manner` for equality in the context of set operations.
That means when comparing rows, two NULL values are considered equal unlike the regular `EqualTo(=)` operator.

    CREATE VIEW unknown_age as SELECT * FROM person WHERE age IS NULL;

## Only common rows between two legs of `INTERSECT` are in the result set. 

The comparison between columns of the row are done in a null-safe manner.

    SELECT name, age FROM person
    INTERSECT
    SELECT name, age from unknown_age;

## `NULL` values from two legs of the `EXCEPT` are not in output. 

This basically shows that the comparison happens in a `null-safe manner`.

    SELECT age, name FROM person
    EXCEPT
    SELECT age, name FROM unknown_age;

## Performs `UNION` operation between two sets of data. 

The comparison between columns of the row ae done in `null-safe manner`.

    SELECT name, age FROM person
    UNION 
    SELECT name, age FROM unknown_age;
