# Aggregate Functions

## Syntax

    MAX | MIN | COUNT | SUM | AVG

## Examples

    SELECT 
        name,
        dept,
        salary,
        MIN(salary) OVER (PARTITION BY dept ORDER BY salary) AS min
    FROM 
        employees;

