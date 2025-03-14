# Null in sub-query

In Spark, `EXISTS and NOT EXISTS` expressions are allowed inside a `WHERE` clause.

These are boolean expressions which return either `TRUE or FALSE`.

In other words, `EXISTS` is a `membership` condition and returns TRUE when the subquery it refers to returns one or more rows.

Similarly, `NOT EXISTS` is a `non-membership` condition and returns TRUE when no rows or zero rows are returned from the subquery.

These two expressions are not affected by presence of NULL in the result of the subquery.

They are normally faster because they can be converted to `semijoins / anti-semijoins` without special provisions for null awareness.


### Even if subquery produces rows with `NULL` values, the `EXISTS` expression evaluates to `TRUE` as the subquery produces 1 row.

    SELECT * FROM person WHERE EXISTS (SELECT null);

### `NOT EXISTS` expression returns `FALSE`.
It returns `TRUE` only when subquery produces no rows. 
In this case, it returns 1 row.

    SELECT * FROM person WHERE NOT EXISTS (SELECT null);

### `NOT EXISTS` expression returns `TRUE`.

    SELECT * FROM person WHERE NOT EXISTS (SELECT 1 WHERE 1 = 0);

## IN/NOT IN Subquery 

In Spark, IN and NOT IN expressions are allowed inside a WHERE clause of a query. Unlike the EXISTS expression, IN expression can return a TRUE, FALSE or UNKNOWN (NULL) value. Conceptually a IN expression is semantically equivalent to a set of equality condition separated by a disjunctive operator (OR). For example, c1 IN (1, 2, 3) is semantically equivalent to (C1 = 1 OR c1 = 2 OR c1 = 3).

As far as handling NULL values are concerned, the semantics can be deduced from the NULL value handling in comparison operators(=) and logical operators(OR). To summarize, below are the rules for computing the result of an IN expression.

TRUE is returned when the non-NULL value in question is found in the list
FALSE is returned when the non-NULL value is not found in the list and the list does not contain NULL values
UNKNOWN is returned when the value is NULL, or the non-NULL value is not found in the list and the list contains at least one NULL value
NOT IN always returns UNKNOWN when the list contains NULL, regardless of the input value. This is because IN returns UNKNOWN if the value is not in the list containing NULL, and because NOT UNKNOWN is again UNKNOWN.

-- The subquery has only `NULL` value in its result set. Therefore,
-- the result of `IN` predicate is UNKNOWN.
SELECT * FROM person WHERE age IN (SELECT null);
+----+---+
|name|age|
+----+---+
+----+---+

-- The subquery has `NULL` value in the result set as well as a valid 
-- value `50`. Rows with age = 50 are returned. 
SELECT * FROM person
    WHERE age IN (SELECT age FROM VALUES (50), (null) sub(age));
+----+---+
|name|age|
+----+---+
|Fred| 50|
| Dan| 50|
+----+---+

### Since subquery has `NULL` value in the result set, the `NOT IN` predicate would return UNKNOWN. Hence, no rows are
-- qualified for this query.

    SELECT * FROM person
    WHERE age NOT IN (SELECT age FROM VALUES (50), (null) sub(age));
