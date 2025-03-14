# Expressions 

The comparison operators and logical operators are treated as expressions in Spark. Other than these two kinds of expressions, Spark supports other form of expressions such as function expressions, cast expressions, etc. The expressions in Spark can be broadly classified as :

## Null intolerant expressions

Null intolerant expressions return NULL when one or more arguments of expression are NULL.

Most of the expressions fall in this category.
    
    SELECT concat('John', null) AS expression_output;
    SELECT positive(null) AS expression_output;
    SELECT to_date(null) AS expression_output;

## Expressions that can process NULL value operands
The result of these expressions depends on the expression itself.

This class of expressions are designed to handle NULL values.

The result of the expressions depends on the expression itself.

As an example, function expression isnull returns a true on null input and false on non-null input whereas function coalesce returns the first non-NULL value in its list of operands. 

However, coalesce returns NULL when all its operands are NULL. Below is an incomplete list of expressions of this category.

    COALESCE
    NULLIF
    IFNULL
    NVL
    NVL2
    ISNAN
    NANVL
    ISNULL
    ISNOTNULL
    ATLEASTNNONNULLS
    IN
