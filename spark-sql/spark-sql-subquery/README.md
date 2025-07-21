# spark sql subquery

Queries nested inside another query, used to perform more complex operations.

```sql

    SELECT
        name,
        salary
    FROM
        employees
    WHERE
        salary > (SELECT AVG(salary) FROM employees)
```
