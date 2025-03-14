# Inner join

Returns rows from both datasets when the join expression evaluates to true.
The inner join is the default join in Spark SQL.
It selects rows that have matching values in both relations.

## Syntax

    relation { [ join_type ] JOIN relation [ join_criteria ] | NATURAL join_type JOIN relation }

## Examples:

    relation [LEFT] [ INNER ] JOIN relation [ join_criteria ]
