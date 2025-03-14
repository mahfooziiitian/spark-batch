# Comparison Operators 

Apache spark supports the standard comparison operators such as `'>', '>=', '=', '<' and '<='`.

The result of these operators is unknown or NULL when one of the operands or both the operands 
are unknown or NULL.

In order to compare the `NULL` values for equality, Spark provides a null-safe equal operator 
`('<=>')`, which returns False when one of the operand is NULL and returns `True` when both the operands are NULL.

The following table illustrates the behaviour of comparison operators when one or both operands are NULL`:

| Left Operand | Right Operand | `>`   | `>=`   | `=`    | `<`   | `<=`   | `<=>`  |
|--------------|---------------|-------|--------|--------|-------|--------|--------|
| NULL         | Any value     | NULL  | NULL   | 	NULL	 | NULL	 | NULL   | 	False |
| Any value	   | NULL	         | NULL	 | NULL	  | NULL	  | NULL  | 	NULL	 | False  |
| NULL	        | NULL	         | NULL  | 	NULL	 | NULL	  | NULL	 | NULL	  | True   |

## '>' operator

    SELECT 5 > null AS expression_output;
    SELECT null > null AS expression_output;

## '=' operator

    SELECT null = null AS expression_output;

## '<=>' null safe operator

    SELECT 5 <=> null AS expression_output;
    SELECT null <=> null AS expression_output;