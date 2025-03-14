# Condition Expressions in WHERE, HAVING and JOIN Clauses

WHERE, HAVING operators filter rows based on the user specified condition.

A JOIN operator is used to combine rows from two tables based on a join condition.

For all the three operators, a `condition expression` is a boolean expression and can return True, False or Unknown (
NULL).

They are `satisfied` if the result of the condition is `True`.

## Persons whose age is unknown (`NULL`) are filtered out from the result set.

    SELECT * FROM person WHERE age > 0;

## `IS NULL` expression is used in disjunction to select the persons with unknown (`NULL`) records.

    SELECT * FROM person WHERE age > 0 OR age IS NULL;

## Person with unknown(`NULL`) ages are skipped from processing.

    SELECT age, count(*) FROM person GROUP BY age HAVING max(age) > 18;

## A self join case with a join condition `p1.age = p2.age AND p1.name = p2.name`

The persons with unknown age (`NULL`) are filtered out by the join operator.

        SELECT 
                * 
        FROM 
                person p1, person p2
        WHERE 
                p1.age = p2.age
                AND p1.name = p2.name;

## The age column from both legs of join are compared using null-safe equal

which is why the persons with unknown age (`NULL`) are qualified by the join.
    
    SELECT * FROM person p1, person p2
        WHERE p1.age <=> p2.age
        AND p1.name = p2.name;
