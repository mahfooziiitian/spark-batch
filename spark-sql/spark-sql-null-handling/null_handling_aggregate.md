# Builtin Aggregate Expressions 

Aggregate functions compute a single result by processing a set of input rows.

Below are the rules of how NULL values are handled by aggregate functions.

NULL values are ignored from processing by all the aggregate functions.

Only exception to this rule is `COUNT(*)` function.

Some aggregate functions return NULL when all input values are NULL or the input data set is empty.

The list of these functions is:

    MAX
    MIN
    SUM
    AVG
    EVERY
    ANY
    SOME


## `count(*)` does not skip `NULL` values.

    SELECT count(*) FROM person;

## `NULL` values in column `age` are skipped from processing.

    SELECT count(age) FROM person;
    
## `count(*)` on an empty input set returns 0. 

This is unlike the other aggregate functions, such as `max`, which return `NULL`.

    SELECT count(*) FROM person where 1 = 0;

## `NULL` values are excluded from computation of maximum value.

    SELECT max(age) FROM person;

## `max` returns `NULL` on an empty input set.

    SELECT max(age) FROM person where 1 = 0;
