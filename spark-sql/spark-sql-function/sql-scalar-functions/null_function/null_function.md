# Null function

## coalesce

    coalesce(expr1, expr2, ...)

It returns the first non-null argument if exists. Otherwise, null.

    SELECT coalesce(NULL, 1, NULL);

## equal_null

    equal_null(expr1, expr2)

Returns same result as the EQUAL(=) operator for non-null operands, but returns true if both are null, false if one of the them is null.

Arguments:

`expr1, expr2` - the two expressions must be same type or can be casted to a common type, and must be a type that can be used in equality comparison.

Map type is not supported.

For complex types such array/struct, the data types of fields must be orderable.

    SELECT equal_null(3, 3);
    SELECT equal_null(1, '11');
    SELECT equal_null(true, NULL);
    SELECT equal_null(NULL, 'abc');
    SELECT equal_null(NULL, NULL);

## first

    first(expr[, isIgnoreNull])

Returns the first value of expr for a group of rows.
If isIgnoreNull is true, returns only non-null values.

    SELECT first(col) FROM VALUES (10), (5), (20) AS tab(col);
    SELECT first(col) FROM VALUES (NULL), (5), (20) AS tab(col);
    SELECT first(col, true) FROM VALUES (NULL), (5), (20) AS tab(col);

The function is non-deterministic because its results depends on the order of the rows which may be non-deterministic after a shuffle.

## first_value

    first_value(expr[, isIgnoreNull])

Returns the first value of expr for a group of rows.
If isIgnoreNull is true, returns only non-null values.

    SELECT first_value(col) FROM VALUES (10), (5), (20) AS tab(col);
    SELECT first_value(col) FROM VALUES (NULL), (5), (20) AS tab(col);
    SELECT first_value(col, true) FROM VALUES (NULL), (5), (20) AS tab(col);

The function is non-deterministic because its results depends on the order of the rows which may be non-deterministic after a shuffle.

## ifnull

    ifnull(expr1, expr2)

Returns expr2 if expr1 is null, or expr1 otherwise.

    SELECT ifnull(NULL, array('2'));

## nvl

    nvl(expr1, expr2)

Returns expr2 if expr1 is NULL, or expr1 otherwise.

Arguments:

expr1: An expression of any type.

expr2: An expression that shares a least common type with expr1.

Returns: The result type is the least common type of the argument types.

    SELECT nvl(column_name, default_value) AS new_column_name
    FROM table_name;

## isnotnull

    isnotnull(expr)

Returns true if expr is not null, or false otherwise.

    SELECT isnotnull(1);

## isnan
    
    SELECT isnan(null) AS expression_output;

## Builtin Aggregate Expressions

Aggregate functions compute a single result by processing a set of input rows. Below are the rules of how NULL values are handled by aggregate functions.
## Ignoring null

NULL values are ignored from processing by all the aggregate functions.Only exception to this rule is COUNT(*) function.

    SELECT count(*) FROM person;

## Respect null

Some aggregate functions return NULL when all input values are NULL or the input data set is empty.

The list of these functions is:

    MAX
    MIN
    SUM
    AVG
    EVERY
    ANY
    SOME