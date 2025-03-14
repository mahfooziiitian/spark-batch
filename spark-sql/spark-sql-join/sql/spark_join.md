# Join

## Syntax

  relation { [ join_type ] JOIN [ LATERAL ] relation [ join_criteria ]
  | NATURAL join_type JOIN [ LATERAL ] relation }

### Parameters

### relation

Specifies the relation to be joined.

### join_type

Specifies the join type.

Syntax:

  [ INNER ] | CROSS | LEFT [ OUTER ] | [ LEFT ] SEMI | RIGHT [ OUTER ] | FULL [ OUTER ] | [ LEFT ] ANTI

### join_criteria

Specifies how the rows from one relation will be combined with the rows of another relation.

Syntax: ON boolean_expression | USING ( column_name [ , ... ] )

### boolean_expression

Specifies an expression with a return type of boolean.
