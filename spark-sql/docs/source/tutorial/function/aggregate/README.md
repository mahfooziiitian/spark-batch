# Introduction

In Spark SQL, an aggregate scalar function is a type of function that performs a calculation on a set of values and returns a single scalar value. These are commonly used in `GROUP BY` queries or with `OVER` clauses for window functions.

## Common Aggregate Scalar Functions in Spark SQL

| Function         | Description                                 |
|------------------|---------------------------------------------|
| `COUNT()`        | Returns the number of rows.                 |
| `SUM()`          | Returns the sum of all values in a column.  |
| `AVG()`          | Returns the average of the values.          |
| `MIN()`          | Returns the minimum value.                  |
| `MAX()`          | Returns the maximum value.                  |
| `FIRST()`        | Returns the first value in a group.         |
| `LAST()`         | Returns the last value in a group.          |
| `COLLECT_LIST()` | Returns a list of objects with duplicates.  |
| `COLLECT_SET()`  | Returns a set of objects with duplicates removed. |