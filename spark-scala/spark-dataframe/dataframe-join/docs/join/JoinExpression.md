# Join Expression

A join brings together two sets of data, the left and the right, by comparing the value of one or
more keys of the left and right and evaluating the result of a join expression that determines
whether Spark should bring together the left set of data with the right set of data.

## join_type

Specify the join type.

### Syntax:

    [ INNER ] | CROSS | LEFT [ OUTER ] | [ LEFT ] SEMI | RIGHT [ OUTER ] | FULL [ OUTER ] | [ LEFT ] ANTI


## Column Object Expression

Column object expressions are recommended for join operations in Spark SQL, as they provide a clean and readable syntax
for joining tables based on common columns. They are efficient and well-supported by Spark SQL.

## Spark SQL Expression

Spark SQL expressions are powerful, as they allow you to perform complex operations using Spark SQL functions.
However, they may not be as readable as column object expressions and may require additional parsing and processing.

## String Expression

String expressions are convenient and flexible, as they allow you to write join conditions as plain text.
However, they may not be as readable as column object expressions and may require additional parsing and processing.

## Criteria

Specify how the rows from one relation will be combined with the rows of another relation.

Syntax: 

    ON boolean_expression | USING ( column_name [ , ... ] )

### boolean_expression

Specify an expression with a return type of boolean.

