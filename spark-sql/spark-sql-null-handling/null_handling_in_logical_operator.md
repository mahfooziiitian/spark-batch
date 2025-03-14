# Logical Operators 

Spark supports standard logical operators such as `AND`, `OR` and `NOT`.

These operators take Boolean expressions as the arguments and return a Boolean value.

The following tables illustrate the behavior of logical operators when one or both operands are `NULL`.

| Left Operand | 	Right Operand | 	OR   | 	AND  |
|--------------|----------------|-------|-------|
| True         | 	NULL	         | True	 | NULL  |
| False	       | NULL	          | NULL	 | False |
| NULL	        | True	          | True	 | NULL  |
| NULL	        | False	         | NULL	 | False |
| NULL	        | NULL	          | NULL	 | NULL  |

## operand	NOT
    NULL	-> NULL
    
    select not null;

## ADR
    
    select true and null;
    select false and null;
    select null and false;
    select null and null;

## OR

    SELECT (true OR null) AS expression_output;
    SELECT (null OR false) AS expression_output

    