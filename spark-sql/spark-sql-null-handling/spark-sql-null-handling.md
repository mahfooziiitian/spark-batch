# NULL Semantics

A table consists of a set of rows and each row contains a set of columns. A column is associated with a data type and represents a specific attribute of an entity (for example, age is a column of an entity called person). Sometimes, the value of a column specific to a row is not known at the time the row comes into existence. In SQL, such values are represented as NULL.


   CREATE TABLE person (
        Id INT,
        Name VARCHAR(50),
        Age INT
    );
    
    INSERT INTO person (Id, Name, Age) VALUES (100, 'Joe', 30);
    INSERT INTO person (Id, Name, Age) VALUES (200, 'Marry', NULL);
    INSERT INTO person (Id, Name, Age) VALUES (300, 'Mike', 18);
    INSERT INTO person (Id, Name, Age) VALUES (400, 'Fred', 50);
    INSERT INTO person (Id, Name, Age) VALUES (500, 'Albert', NULL);
    INSERT INTO person (Id, Name, Age) VALUES (600, 'Michelle', 30);
    INSERT INTO person (Id, Name, Age) VALUES (700, 'Dan', 50);


## Comparison Operators 

Apache spark supports the standard comparison operators such as ‘>’, ‘>=’, ‘=’, ‘<’ and ‘<=’. The result of these operators is unknown or NULL when one of the operands or both the operands are unknown or NULL.
    
    SELECT 5 > null AS expression_output;
    SELECT null = null AS expression_output;
    SELECT 5 <=> null AS expression_output;
    SELECT NULL <=> NULL;


### null-safe equal operator (‘<=>’)

In order to compare the NULL values for equality, Spark provides a null-safe equal operator (‘<=>’), which returns False when one of the operand is NULL and returns

## Logical Operators

Spark supports standard logical operators such as AND, OR and NOT.

These operators take Boolean expressions as the arguments and return a Boolean value.

    SELECT (true OR null) AS expression_output;
    SELECT (null OR false) AS expression_output
    SELECT NOT(null) AS expression_output;

## Expressions

The comparison operators and logical operators are treated as expressions in Spark. Other than these two kinds of expressions, Spark supports other form of expressions such as function expressions, cast expressions, etc. 

The expressions in Spark can be broadly classified as :

1. Null intolerant expressions
2. Expressions that can process NULL value operands 
3. The result of these expressions depends on the expression itself.

### Null Intolerant Expressions 

Null intolerant expressions return NULL when one or more arguments of expression are NULL and most of the expressions fall in this category.

    SELECT concat('John', null) AS expression_output;
    SELECT positive(null) AS expression_output;
    SELECT to_date(null) AS expression_output;

### Expressions That Can Process Null Value Operands

Below is an incomplete list of expressions of this category.

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