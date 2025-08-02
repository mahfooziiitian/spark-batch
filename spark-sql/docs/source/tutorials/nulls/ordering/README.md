# Sort Operator (ORDER BY Clause) 

Spark SQL supports null ordering specification in ORDER BY clause.

Spark processes the ORDER BY clause by placing all the NULL values at first or at last depending on the null ordering specification.

`By default, all the NULL values are placed at first.`

## `NULL` values are shown at first and other values are sorted in ascending way.

    SELECT age, name FROM person ORDER BY age;

## Column values other than `NULL` are sorted in ascending way and `NULL` values are shown at the last.

    SELECT age, name FROM person ORDER BY age NULLS LAST;

